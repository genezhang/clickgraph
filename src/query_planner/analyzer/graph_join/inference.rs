//! Graph Join Inference Analyzer
//!
//! This module handles inferring SQL JOINs from Cypher graph patterns.
//! It converts MATCH patterns like `(a)-[r:FOLLOWS]->(b)` into appropriate
//! SQL JOIN conditions for ClickHouse.
//!
//! ## Architecture Overview
//!
//! The analyzer uses a phased approach:
//!
//! 1. **Pattern Metadata Construction** - Build a lightweight index over GraphRel trees
//! 2. **PatternSchemaContext Integration** - Map patterns to schema-aware join strategies
//! 3. **Join Generation** - Create SQL JOINs based on pattern type (standard, FK-edge, denormalized)
//! 4. **Cross-Branch Detection** - Handle branching patterns like `(a)-[:R1]->(b), (a)-[:R2]->(c)`
//!
//! ## Key Types
//!
//! - [`GraphJoinInference`] - Main analyzer pass implementing [`AnalyzerPass`]
//! - [`PatternGraphMetadata`] - Cached pattern information for efficient lookup
//! - [`NodeAppearance`] - Tracks where node variables appear for cross-branch detection
//!
//! ## Supported Pattern Types
//!
//! - Standard edge tables with separate node/edge tables
//! - FK-edge patterns (foreign key relationships)
//! - Denormalized patterns (node properties embedded in edge table)
//! - Variable-length paths (VLP) via CTE generation
//! - Cross-branch shared nodes

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

// Import metadata module types and functions
use super::helpers;
use super::metadata::{
    plan_references_alias as metadata_plan_references_alias, PatternEdgeInfo, PatternGraphMetadata,
    PatternNodeInfo,
};

use crate::{
    graph_catalog::{
        config::Identifier,
        graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
        pattern_schema::{JoinStrategy, NodeAccessStrategy, PatternSchemaContext},
    },
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
            graph_context,
        },
        logical_expr::{Direction, LogicalExpr},
        logical_plan::{Filter, GraphJoins, GraphRel, Join, JoinType, LogicalPlan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
    utils::cte_naming::generate_cte_base_name,
};

// Re-export NodeAppearance from cross_branch module
use super::cross_branch::NodeAppearance;

// ============================================================================
// Pattern Graph Metadata types imported from metadata module
// See super::metadata for PatternNodeInfo, PatternEdgeInfo, PatternGraphMetadata
// ============================================================================

// Import JoinContext types from shared module
pub use crate::query_planner::join_context::{JoinContext, VlpEndpointInfo, VlpPosition};

pub struct GraphJoinInference;

impl AnalyzerPass for GraphJoinInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        println!(
            "DEBUG GraphJoinInference: analyze_with_graph_schema called, plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );

        // Phase 1: Build pattern graph metadata (caches reference checks)
        let pattern_metadata = Self::build_pattern_metadata(&logical_plan, plan_ctx)?;
        log::debug!(
            "📊 Pattern metadata built: {} nodes, {} edges",
            pattern_metadata.nodes.len(),
            pattern_metadata.edges.len()
        );

        // CRITICAL: Before collecting joins, scan for WITH clauses and register their
        // exported aliases as CTE references in plan_ctx. This enables proper variable
        // resolution when subsequent patterns reference those aliases.
        let mut captured_cte_refs = Vec::new(); // Vec<(CTE name, refs map)>
        self.register_with_cte_references(&logical_plan, plan_ctx, &mut captured_cte_refs)?;

        log::info!(
            "🔍 Captured {} WITH clause CTE references",
            captured_cte_refs.len()
        );
        for (cte_name, refs) in &captured_cte_refs {
            log::info!("   {} → {:?}", cte_name, refs);
        }

        let mut collected_graph_joins: Vec<Join> = vec![];
        let mut join_ctx = JoinContext::new();
        let mut node_appearances: HashMap<String, Vec<NodeAppearance>> = HashMap::new(); // Track cross-branch shared nodes
        let cte_scope_aliases = HashSet::new(); // Start with empty CTE scope
        self.collect_graph_joins(
            logical_plan.clone(),
            logical_plan.clone(), // Pass root plan for reference checking
            plan_ctx,
            graph_schema,
            &mut collected_graph_joins,
            &mut join_ctx,
            &cte_scope_aliases,
            &mut node_appearances,
            &pattern_metadata, // Phase 1: Pass metadata for cached lookups
        )?;

        // Phase 2: Generate cross-branch joins using metadata (simplified!)
        // Instead of tracking NodeAppearance during traversal, use pre-computed
        // appearance_count from metadata to identify shared nodes naturally.
        log::debug!("🔍 Phase 2: Generating cross-branch joins from metadata...");
        let cross_branch_joins = super::cross_branch::generate_cross_branch_joins_from_metadata(
            &pattern_metadata,
            plan_ctx,
            graph_schema,
        )?;

        if !cross_branch_joins.is_empty() {
            log::info!(
                "✅ Generated {} cross-branch joins from metadata",
                cross_branch_joins.len()
            );
            collected_graph_joins.extend(cross_branch_joins);
        }

        // Phase 4: Generate relationship uniqueness constraints
        // Prevents duplicate traversal of same relationship in multi-hop patterns
        let uniqueness_constraints =
            crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&pattern_metadata, graph_schema);

        println!(
            "DEBUG GraphJoinInference: collected_graph_joins.len() = {}",
            collected_graph_joins.len()
        );
        for (i, join) in collected_graph_joins.iter().enumerate() {
            println!(
                "DEBUG GraphJoinInference: JOIN #{}: {} (alias {}) on {:?}",
                i, join.table_name, join.table_alias, join.joining_on
            );
        }

        // CRITICAL: Always wrap in GraphJoins, even if empty!
        // Empty joins vector = fully denormalized pattern (no JOINs needed)
        // Without this wrapper, RenderPlan will try to generate JOINs from raw GraphRel
        let optional_aliases = plan_ctx.get_optional_aliases().clone();
        let mut correlation_predicates: Vec<LogicalExpr> = vec![];

        // Phase 4: Add uniqueness constraints to correlation predicates
        correlation_predicates.extend(uniqueness_constraints);

        Self::build_graph_joins(
            logical_plan,
            &mut collected_graph_joins,
            &mut correlation_predicates,
            optional_aliases,
            plan_ctx,
            graph_schema,
            &captured_cte_refs,
        )
    }
}

impl Default for GraphJoinInference {
    fn default() -> Self {
        GraphJoinInference
    }
}

impl GraphJoinInference {
    #[must_use]
    pub fn new() -> Self {
        GraphJoinInference
    }

    // ========================================================================
    // Pattern Graph Metadata Construction (POC)
    // ========================================================================
    // Lightweight pre-pass that builds an index over the GraphRel tree.
    // Caches reference checks and computes pattern structure information
    // to enable cleaner join inference logic.

    /// Build pattern graph metadata by traversing the GraphRel tree.
    /// This is a pre-pass that extracts and caches pattern structure information.
    ///
    /// Phase 1: Extract pattern info (nodes and edges)
    /// Phase 2: Compute node references (which nodes are used in SELECT/WHERE/etc)
    /// Phase 3: Compute edge references (which edges are used)
    /// Phase 4: Count node appearances (for cross-branch detection)
    fn build_pattern_metadata(
        logical_plan: &LogicalPlan,
        plan_ctx: &PlanCtx,
    ) -> AnalyzerResult<PatternGraphMetadata> {
        let mut metadata = PatternGraphMetadata::default();

        // Phase 1: Extract pattern structure from GraphRel tree
        Self::extract_pattern_info(logical_plan, plan_ctx, &mut metadata)?;

        // Phase 2: Compute which nodes are referenced in the query
        Self::compute_node_references(logical_plan, &mut metadata);

        // Phase 3: Compute which edges are referenced
        Self::compute_edge_references(logical_plan, &mut metadata);

        // Phase 4: Count node appearances (appearance_count)
        Self::compute_node_appearances(&mut metadata);

        log::debug!(
            "📊 Built PatternGraphMetadata: {} nodes, {} edges",
            metadata.nodes.len(),
            metadata.edges.len()
        );

        Ok(metadata)
    }

    /// Phase 1: Extract pattern info from GraphRel nodes
    fn extract_pattern_info(
        plan: &LogicalPlan,
        plan_ctx: &PlanCtx,
        metadata: &mut PatternGraphMetadata,
    ) -> AnalyzerResult<()> {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                // Extract edge info from this GraphRel
                let edge_info = PatternEdgeInfo {
                    alias: graph_rel.alias.clone(),
                    rel_types: graph_rel.labels.clone().unwrap_or_default(),
                    from_node: graph_rel.left_connection.clone(),
                    to_node: graph_rel.right_connection.clone(),
                    is_referenced: false, // Computed later
                    is_vlp: graph_rel.variable_length.is_some(),
                    is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                    direction: graph_rel.direction.clone(),
                    is_optional: graph_rel.is_optional.unwrap_or(false),
                };
                metadata.edges.push(edge_info);

                // Extract node info for left and right nodes (if not already present)
                Self::extract_node_info(&graph_rel.left_connection, plan_ctx, metadata)?;
                Self::extract_node_info(&graph_rel.right_connection, plan_ctx, metadata)?;

                // Recurse into left and right branches
                Self::extract_pattern_info(&graph_rel.left, plan_ctx, metadata)?;
                Self::extract_pattern_info(&graph_rel.right, plan_ctx, metadata)?;
            }
            LogicalPlan::GraphNode(graph_node) => {
                // Extract node info
                Self::extract_node_info(&graph_node.alias, plan_ctx, metadata)?;

                // Recurse into input
                Self::extract_pattern_info(&graph_node.input, plan_ctx, metadata)?;
            }
            // Recurse through container nodes
            LogicalPlan::Projection(p) => {
                Self::extract_pattern_info(&p.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Filter(f) => {
                Self::extract_pattern_info(&f.input, plan_ctx, metadata)?;
            }
            LogicalPlan::GraphJoins(gj) => {
                Self::extract_pattern_info(&gj.input, plan_ctx, metadata)?;
            }
            LogicalPlan::GroupBy(gb) => {
                Self::extract_pattern_info(&gb.input, plan_ctx, metadata)?;
            }
            LogicalPlan::OrderBy(ob) => {
                Self::extract_pattern_info(&ob.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Skip(s) => {
                Self::extract_pattern_info(&s.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Limit(l) => {
                Self::extract_pattern_info(&l.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Cte(cte) => {
                Self::extract_pattern_info(&cte.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    Self::extract_pattern_info(input, plan_ctx, metadata)?;
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                Self::extract_pattern_info(&cp.left, plan_ctx, metadata)?;
                Self::extract_pattern_info(&cp.right, plan_ctx, metadata)?;
            }
            LogicalPlan::Unwind(uw) => {
                Self::extract_pattern_info(&uw.input, plan_ctx, metadata)?;
            }
            LogicalPlan::WithClause(wc) => {
                Self::extract_pattern_info(&wc.input, plan_ctx, metadata)?;
            }
            // Leaf nodes - nothing to extract
            LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => {}
        }

        Ok(())
    }

    /// Extract node info from an alias if not already present
    fn extract_node_info(
        alias: &str,
        plan_ctx: &PlanCtx,
        metadata: &mut PatternGraphMetadata,
    ) -> AnalyzerResult<()> {
        // Skip if already extracted
        if metadata.nodes.contains_key(alias) {
            return Ok(());
        }

        // Get node label from plan_ctx
        let table_ctx = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(alias.to_string()))
            .map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::GraphJoinInference,
                source: e,
            })?;

        let label = table_ctx.get_label_str().ok();

        // TODO: Extract has_explicit_label from TableCtx once field is available
        // For POC, we'll set it to false (conservative - assume all nodes need JOINs)
        let has_explicit_label = false;

        let node_info = PatternNodeInfo {
            alias: alias.to_string(),
            label,
            is_referenced: false, // Computed later
            appearance_count: 0,  // Computed later
            has_explicit_label,
        };

        metadata.nodes.insert(alias.to_string(), node_info);
        Ok(())
    }

    /// Phase 2: Compute which nodes are referenced in SELECT/WHERE/ORDER BY/etc
    fn compute_node_references(plan: &LogicalPlan, metadata: &mut PatternGraphMetadata) {
        // Note: is_node_referenced uses a PlanCtx but we can't pass the real one here
        // due to borrowing constraints. Instead, we do direct plan traversal.
        // This is fine since we're just checking if the alias appears in projections/filters.
        for (alias, node_info) in metadata.nodes.iter_mut() {
            node_info.is_referenced = metadata_plan_references_alias(plan, alias);
        }
    }

    /// Phase 3: Compute which edges are referenced
    fn compute_edge_references(plan: &LogicalPlan, metadata: &mut PatternGraphMetadata) {
        for edge_info in metadata.edges.iter_mut() {
            // Check if edge alias is referenced in the plan
            edge_info.is_referenced = metadata_plan_references_alias(plan, &edge_info.alias);
        }
    }

    /// Phase 4: Count how many edges use each node (for cross-branch detection)
    fn compute_node_appearances(metadata: &mut PatternGraphMetadata) {
        for node_info in metadata.nodes.values_mut() {
            let count = metadata
                .edges
                .iter()
                .filter(|e| e.from_node == node_info.alias || e.to_node == node_info.alias)
                .count();
            node_info.appearance_count = count;
        }
    }

    // ========================================================================
    // Existing Implementation (unchanged)
    // ========================================================================

    /// Scan the plan for WITH clauses and register their exported aliases as CTE references.
    /// This enables proper variable resolution when subsequent patterns reference those aliases.
    ///
    /// Example: MATCH (a) WITH a MATCH (a)-[:F]->(b) WITH a,b MATCH (b)-[:F]->(c)
    /// - After first WITH: 'a' resolves to with_a_cte1
    /// - After second WITH: 'a' and 'b' resolve to with_a_b_cte2
    fn register_with_cte_references(
        &self,
        plan: &Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        captured_refs: &mut Vec<(String, std::collections::HashMap<String, String>)>, // (CTE name, refs map)
    ) -> AnalyzerResult<()> {
        use crate::query_planner::plan_ctx::TableCtx;

        match plan.as_ref() {
            LogicalPlan::WithClause(wc) => {
                // IMPORTANT: Recurse into input FIRST, then process this WithClause
                // This ensures inner (nested) WITH clauses are processed before outer ones
                // So the LATEST (outermost) WITH clause's CTE reference takes precedence
                // Example: WITH a (outer) WITH a, b (inner) → final a should reference outer CTE
                self.register_with_cte_references(&wc.input, plan_ctx, captured_refs)?;

                // CRITICAL: Capture CTE references BEFORE updating plan_ctx
                // This preserves which variables come from previous CTEs
                let mut refs_for_this_with = std::collections::HashMap::new();
                for alias in &wc.exported_aliases {
                    if let Ok(table_ctx) = plan_ctx.get_table_ctx(alias) {
                        if let Some(cte_name) = table_ctx.get_cte_name() {
                            refs_for_this_with.insert(alias.clone(), cte_name.clone());
                            log::info!(
                                "   📌 Captured '{}' → '{}' (from previous CTE)",
                                alias,
                                cte_name
                            );
                        }
                    }
                }

                // Now register this WithClause's CTE references (will overwrite inner ones for same alias)
                // Found a WITH clause - register exported aliases as CTE references
                // Use the CTE name from the analyzer (CteSchemaResolver sets this with proper counter)
                let cte_name = wc.cte_name.clone().unwrap_or_else(|| {
                    log::error!("⚠️ BUG: WithClause.cte_name is None in graph_join/inference.rs");
                    generate_cte_base_name(&wc.exported_aliases)
                });

                log::info!(
                    "🔍 register_with_cte_references: Found WITH exporting {:?} → CTE '{}'",
                    wc.exported_aliases,
                    cte_name
                );

                // Register CTE columns for column resolution in join conditions
                // This extracts the projection items and their aliases to track
                // what columns this CTE exports
                plan_ctx.register_cte_columns(&cte_name, &wc.items);

                // Store captured refs for later use by build_graph_joins
                captured_refs.push((cte_name.clone(), refs_for_this_with));

                // For each exported alias, add a TableCtx pointing to the CTE
                for alias in &wc.exported_aliases {
                    // Check if this alias already has a TableCtx (from parsing phase)
                    if let Ok(existing_ctx) = plan_ctx.get_table_ctx(alias) {
                        // Update the existing context to reference the CTE
                        // Clone it, update cte_reference, and re-insert
                        let mut updated_ctx = existing_ctx.clone();
                        updated_ctx.set_cte_reference(Some(cte_name.clone()));
                        plan_ctx.insert_table_ctx(alias.clone(), updated_ctx);
                        log::info!("   ✓ Updated '{}' to reference CTE '{}'", alias, cte_name);
                    } else {
                        // No existing context - create a minimal one with entity type lookup
                        // This shouldn't happen in normal queries, but handle it gracefully
                        let entity_info = plan_ctx
                            .get_cte_entity_type(&cte_name, alias)
                            .map(|(r, l)| (*r, l.clone()));
                        let table_ctx = TableCtx::new_with_cte_reference(
                            alias.clone(),
                            cte_name.clone(),
                            entity_info,
                        );
                        plan_ctx.insert_table_ctx(alias.clone(), table_ctx);
                        log::info!("   ✓ Created '{}' → CTE '{}'", alias, cte_name);
                    }
                }
            }

            // Recurse through all container nodes
            LogicalPlan::Projection(p) => {
                self.register_with_cte_references(&p.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::GraphNode(gn) => {
                self.register_with_cte_references(&gn.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::GraphRel(gr) => {
                self.register_with_cte_references(&gr.left, plan_ctx, captured_refs)?;
                self.register_with_cte_references(&gr.right, plan_ctx, captured_refs)?;
            }
            LogicalPlan::GraphJoins(gj) => {
                self.register_with_cte_references(&gj.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Filter(f) => {
                self.register_with_cte_references(&f.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::GroupBy(gb) => {
                self.register_with_cte_references(&gb.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::OrderBy(ob) => {
                self.register_with_cte_references(&ob.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Skip(s) => {
                self.register_with_cte_references(&s.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Limit(l) => {
                self.register_with_cte_references(&l.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    self.register_with_cte_references(input, plan_ctx, captured_refs)?;
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                self.register_with_cte_references(&cp.left, plan_ctx, captured_refs)?;
                self.register_with_cte_references(&cp.right, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Unwind(uw) => {
                self.register_with_cte_references(&uw.input, plan_ctx, captured_refs)?;
            }
            LogicalPlan::Cte(cte) => {
                self.register_with_cte_references(&cte.input, plan_ctx, captured_refs)?;
            }

            // Leaf nodes - nothing to recurse
            LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => {}
        }

        Ok(())
    }

    // Note: expr_references_alias wrapper removed - use metadata_expr_references_alias directly

    /// Extract table aliases referenced in an expression
    fn extract_table_refs_from_expr(
        expr: &LogicalExpr,
        refs: &mut std::collections::HashSet<String>,
    ) {
        match expr {
            LogicalExpr::PropertyAccessExp(prop) => {
                refs.insert(prop.table_alias.0.clone());
            }
            LogicalExpr::Column(_col) => {
                // Columns without table references are ignored
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                for operand in &op_app.operands {
                    Self::extract_table_refs_from_expr(operand, refs);
                }
            }
            LogicalExpr::ScalarFnCall(func) => {
                for arg in &func.args {
                    Self::extract_table_refs_from_expr(arg, refs);
                }
            }
            LogicalExpr::AggregateFnCall(func) => {
                for arg in &func.args {
                    Self::extract_table_refs_from_expr(arg, refs);
                }
            }
            // Other expression types don't contain table references
            _ => {}
        }
    }

    /// Attach pre_filter predicates to LEFT JOINs for optional aliases.
    /// This extracts predicates from GraphRel.where_predicate that reference ONLY
    /// the optional alias, and moves them into the JOIN's pre_filter field.
    fn attach_pre_filters_to_joins(
        joins: Vec<Join>,
        optional_aliases: &std::collections::HashSet<String>,
        logical_plan: &Arc<LogicalPlan>,
    ) -> Vec<Join> {
        use crate::query_planner::logical_expr::{
            LogicalExpr, Operator, OperatorApplication as LogicalOpApp,
        };

        // First, collect all predicates from GraphRel.where_predicate nodes
        fn collect_graphrel_predicates(
            plan: &LogicalPlan,
        ) -> Vec<(LogicalExpr, String, String, String)> {
            // Returns (predicate, left_connection, alias, right_connection) tuples
            let mut results = Vec::new();
            match plan {
                LogicalPlan::GraphRel(gr) => {
                    if let Some(ref pred) = gr.where_predicate {
                        if gr.is_optional.unwrap_or(false) {
                            results.push((
                                pred.clone(),
                                gr.left_connection.clone(),
                                gr.alias.clone(),
                                gr.right_connection.clone(),
                            ));
                        }
                    }
                    results.extend(collect_graphrel_predicates(&gr.left));
                    results.extend(collect_graphrel_predicates(&gr.center));
                    results.extend(collect_graphrel_predicates(&gr.right));
                }
                LogicalPlan::GraphNode(gn) => {
                    results.extend(collect_graphrel_predicates(&gn.input));
                }
                LogicalPlan::Projection(proj) => {
                    results.extend(collect_graphrel_predicates(&proj.input));
                }
                LogicalPlan::Filter(filter) => {
                    results.extend(collect_graphrel_predicates(&filter.input));
                }
                _ => {}
            }
            results
        }

        // Helper: check if expression references ONLY a single alias
        fn references_only_alias(expr: &LogicalExpr, alias: &str) -> bool {
            let mut refs = std::collections::HashSet::new();
            GraphJoinInference::extract_table_refs_from_expr(expr, &mut refs);
            refs.len() == 1 && refs.contains(alias)
        }

        // Split AND-connected predicates
        fn split_and_predicates(expr: &LogicalExpr) -> Vec<LogicalExpr> {
            match expr {
                LogicalExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
                    let mut result = Vec::new();
                    for operand in &op.operands {
                        result.extend(split_and_predicates(operand));
                    }
                    result
                }
                _ => vec![expr.clone()],
            }
        }

        // Combine predicates with AND
        fn combine_with_and(predicates: Vec<LogicalExpr>) -> Option<LogicalExpr> {
            if predicates.is_empty() {
                None
            } else if predicates.len() == 1 {
                Some(
                    predicates
                        .into_iter()
                        .next()
                        .expect("Vector with len==1 must have element"),
                )
            } else {
                Some(LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: Operator::And,
                    operands: predicates,
                }))
            }
        }

        // Collect predicates from all optional GraphRels
        let graphrel_preds = collect_graphrel_predicates(logical_plan);

        // Build a map of alias -> predicates for optional aliases
        // Only include predicates that reference the optional parts (rel alias or right_connection)
        let mut alias_predicates: std::collections::HashMap<String, Vec<LogicalExpr>> =
            std::collections::HashMap::new();

        for (predicate, _left_conn, rel_alias, right_conn) in graphrel_preds {
            let all_preds = split_and_predicates(&predicate);

            for pred in all_preds {
                // Only extract predicates for optional aliases (rel and right, not left which is anchor)
                if references_only_alias(&pred, &rel_alias) && optional_aliases.contains(&rel_alias)
                {
                    alias_predicates
                        .entry(rel_alias.clone())
                        .or_default()
                        .push(pred.clone());
                }
                if references_only_alias(&pred, &right_conn)
                    && optional_aliases.contains(&right_conn)
                {
                    alias_predicates
                        .entry(right_conn.clone())
                        .or_default()
                        .push(pred.clone());
                }
            }
        }

        // Now attach predicates to the corresponding LEFT JOINs
        joins
            .into_iter()
            .map(|mut join| {
                if matches!(
                    join.join_type,
                    crate::query_planner::logical_plan::JoinType::Left
                ) {
                    if let Some(preds) = alias_predicates.get(&join.table_alias) {
                        if !preds.is_empty() {
                            let combined = combine_with_and(preds.clone());
                            if combined.is_some() {
                                crate::debug_print!(
                                    "DEBUG: Attaching pre_filter to LEFT JOIN on '{}': {:?}",
                                    join.table_alias,
                                    combined
                                );
                                join.pre_filter = combined;
                            }
                        }
                    }
                }
                join
            })
            .collect()
    }

    fn build_graph_joins(
        logical_plan: Arc<LogicalPlan>,
        collected_graph_joins: &mut Vec<Join>,
        correlation_predicates: &mut Vec<LogicalExpr>,
        optional_aliases: std::collections::HashSet<String>,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
        captured_cte_refs: &[(String, std::collections::HashMap<String, String>)],
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            // If input is a Union, process each branch
            // NOTE: When Union is nested inside a GraphRel (for undirected relationships),
            // we need to INHERIT the collected_graph_joins from outer context so that
            // outer relationship joins are applied to both branches.
            LogicalPlan::Union(union) => {
                log::info!(
                    "🔄 Union detected in build_graph_joins, processing {} branches",
                    union.inputs.len()
                );
                log::info!(
                    "🔄 Inherited {} joins from outer context",
                    collected_graph_joins.len()
                );
                let mut any_transformed = false;
                let graph_join_inference = GraphJoinInference::new();

                let transformed_branches: Result<Vec<Arc<LogicalPlan>>, _> = union
                    .inputs
                    .iter()
                    .map(|branch| {
                        // Start with inherited joins from outer context (important for nested Unions in GraphRel)
                        let mut branch_joins: Vec<Join> = collected_graph_joins.clone();
                        let mut branch_join_ctx = JoinContext::new();

                        // Build pattern metadata for THIS branch (critical for is_referenced checks)
                        // Each Union branch is a complete pattern (created by BidirectionalUnion)
                        // and needs its own metadata for proper reference tracking
                        let mut branch_plan_ctx = plan_ctx.clone();
                        let branch_metadata =
                            Self::build_pattern_metadata(branch.as_ref(), &branch_plan_ctx)
                                .unwrap_or_default();

                        log::debug!(
                            "🔄 Union branch metadata: {} nodes, {} edges",
                            branch_metadata.nodes.len(),
                            branch_metadata.edges.len()
                        );

                        // Collect additional joins for this specific branch
                        graph_join_inference.collect_graph_joins(
                            branch.clone(),
                            branch.clone(),
                            &mut branch_plan_ctx, // Use branch-specific PlanCtx
                            graph_schema,
                            &mut branch_joins,
                            &mut branch_join_ctx,
                            &HashSet::new(),     // Empty CTE scope for Union branches
                            &mut HashMap::new(), // Empty node_appearances for each Union branch
                            &branch_metadata,    // Use branch-specific metadata
                        )?;

                        crate::debug_print!(
                            "🔹 Union branch collected {} total joins (including inherited)",
                            branch_joins.len()
                        );

                        // Build GraphJoins for this branch with combined joins
                        let result = Self::build_graph_joins(
                            branch.clone(),
                            &mut branch_joins,
                            &mut Vec::new(),
                            optional_aliases.clone(),
                            plan_ctx,
                            graph_schema,
                            captured_cte_refs,
                        )?;
                        if matches!(result, Transformed::Yes(_)) {
                            any_transformed = true;
                        }
                        Ok(result.get_plan())
                    })
                    .collect();

                let branches = transformed_branches?;
                if any_transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Union(
                        crate::query_planner::logical_plan::Union {
                            inputs: branches,
                            union_type: union.union_type.clone(),
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }
            LogicalPlan::Projection(projection) => {
                // CRITICAL FIX: Process the projection's input first!
                // This allows CartesianProduct (and other nodes) to add their joins
                // to collected_graph_joins before we wrap with GraphJoins.
                let child_tf = Self::build_graph_joins(
                    projection.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;

                // Get the processed child (or original if unchanged)
                let processed_child = match &child_tf {
                    Transformed::Yes(p) => p.clone(),
                    Transformed::No(p) => p.clone(),
                };

                // DEBUG: Check cte_references in processed_child
                fn count_with_cte_refs(plan: &LogicalPlan) -> usize {
                    match plan {
                        LogicalPlan::WithClause(wc) => {
                            wc.cte_references.len() + count_with_cte_refs(&wc.input)
                        }
                        _ => 0,
                    }
                }
                eprintln!(
                    "🔬 GraphJoinInference Projection: processed_child has {} cte_references",
                    count_with_cte_refs(&processed_child)
                );

                // Build the new projection with the processed child
                let new_projection = Arc::new(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: processed_child,
                        items: projection.items.clone(),
                        distinct: projection.distinct,
                        pattern_comprehensions: projection.pattern_comprehensions.clone(),
                    },
                ));

                // DEDUPLICATION: Remove duplicate joins for the same table_alias
                // When there are multiple joins for the same alias (e.g., from both infer_graph_join
                // and cross-table join extraction), keep the one that references WITH clause aliases
                // (like client_ip) rather than internal node aliases (like src2).
                let deduped_joins = helpers::deduplicate_joins(collected_graph_joins.clone());

                // Reorder JOINs using clean topological sort
                let anchor_table = super::join_generation::select_anchor(&deduped_joins);
                let reordered_joins =
                    super::join_generation::topo_sort_joins(deduped_joins, &HashSet::new())?;

                // Extract predicates for optional aliases and attach them to LEFT JOINs
                let joins_with_pre_filter = Self::attach_pre_filters_to_joins(
                    reordered_joins,
                    &optional_aliases,
                    &new_projection,
                );

                // Build CTE references map from plan_ctx
                let mut cte_references = std::collections::HashMap::new();
                for (alias, table_ctx) in plan_ctx.iter_table_contexts() {
                    if let Some(cte_name) = table_ctx.get_cte_name() {
                        cte_references.insert(alias.clone(), cte_name.clone());
                    }
                }

                println!(
                    "DEBUG GraphJoinInference: Creating GraphJoins with {} joins",
                    joins_with_pre_filter.len()
                );
                for (i, join) in joins_with_pre_filter.iter().enumerate() {
                    println!("  JOIN #{}: {} AS {}", i, join.table_name, join.table_alias);
                }

                // Separate correlation_predicates into JOIN conditions and WHERE predicates
                // NOT PathPattern predicates must go in WHERE clause (ClickHouse limitation)
                let (where_predicates, join_predicates): (Vec<_>, Vec<_>) = correlation_predicates
                    .iter()
                    .partition(|pred| pred.contains_not_path_pattern());

                if !where_predicates.is_empty() {
                    log::info!(
                        "🔍 GraphJoinInference: Separated {} NOT PathPattern predicates to WHERE",
                        where_predicates.len()
                    );
                }

                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                let graph_joins = Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                    input: new_projection,
                    joins: joins_with_pre_filter,
                    optional_aliases,
                    anchor_table,
                    cte_references,
                    correlation_predicates: join_predicates.into_iter().cloned().collect(),
                }));

                // If we have WHERE predicates (e.g., NOT PathPattern), wrap in Filter
                if !where_predicates.is_empty() {
                    log::info!(
                        "🔍 GraphJoinInference: Adding {} WHERE predicates to Filter",
                        where_predicates.len()
                    );
                    // Combine multiple predicates with AND
                    let combined_predicate =
                        helpers::and_conditions(where_predicates.into_iter().cloned().collect());
                    Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                        input: graph_joins,
                        predicate: combined_predicate,
                    })))
                } else {
                    Transformed::Yes(graph_joins)
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = Self::build_graph_joins(
                    graph_node.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;

                // is_denormalized flag is set by view_optimizer pass - just rebuild
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = Self::build_graph_joins(
                    graph_rel.left.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                let center_tf = Self::build_graph_joins(
                    graph_rel.center.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                let right_tf = Self::build_graph_joins(
                    graph_rel.right.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = Self::build_graph_joins(
                    cte.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }

            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = Self::build_graph_joins(
                    graph_joins.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = Self::build_graph_joins(
                    filter.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                // CRITICAL: If this is a materialization boundary, process inner joins SEPARATELY
                // The inner query block must have its own GraphJoins, not merged with outer joins
                if group_by.is_materialization_boundary {
                    crate::debug_print!("🛑 build_graph_joins: GroupBy is_materialization_boundary=true, processing inner joins separately");

                    // Create fresh vectors for the inner query block
                    let mut inner_joins = Vec::new();
                    let mut inner_join_ctx = JoinContext::new();
                    let inner_optional_aliases = std::collections::HashSet::new();

                    // Build pattern metadata for inner scope (for proper reference checking)
                    let mut inner_plan_ctx = plan_ctx.clone();
                    let inner_metadata =
                        Self::build_pattern_metadata(group_by.input.as_ref(), &inner_plan_ctx)
                            .unwrap_or_default();

                    // IMPORTANT: We need to collect joins for the inner scope FIRST
                    // because collect_graph_joins stopped at the boundary during the main traversal
                    let graph_join_inference = GraphJoinInference;
                    graph_join_inference.collect_graph_joins(
                        group_by.input.clone(),
                        group_by.input.clone(), // root plan for inner scope
                        &mut inner_plan_ctx,    // Use inner scope's PlanCtx
                        graph_schema,
                        &mut inner_joins,
                        &mut inner_join_ctx,
                        &HashSet::new(), // Empty CTE scope for inner GroupBy scope
                        &mut HashMap::new(), // Empty node_appearances for inner GroupBy scope
                        &inner_metadata, // Use inner scope's metadata
                    )?;

                    crate::debug_print!(
                        "🛑 build_graph_joins: Collected {} inner joins for boundary GroupBy",
                        inner_joins.len()
                    );

                    // Now build the graph joins for the inner scope
                    let child_tf = Self::build_graph_joins(
                        group_by.input.clone(),
                        &mut inner_joins, // Use the inner joins we just collected
                        &mut Vec::new(),
                        inner_optional_aliases,
                        plan_ctx,
                        graph_schema,
                        captured_cte_refs,
                    )?;
                    group_by.rebuild_or_clone(child_tf, logical_plan.clone())
                } else {
                    let child_tf = Self::build_graph_joins(
                        group_by.input.clone(),
                        collected_graph_joins,
                        correlation_predicates,
                        optional_aliases,
                        plan_ctx,
                        graph_schema,
                        captured_cte_refs,
                    )?;
                    group_by.rebuild_or_clone(child_tf, logical_plan.clone())
                }
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = Self::build_graph_joins(
                    order_by.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = Self::build_graph_joins(
                    skip.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = Self::build_graph_joins(
                    limit.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            // Note: LogicalPlan::Union is handled earlier in this function for independent branch processing
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = Self::build_graph_joins(
                    u.input.clone(),
                    collected_graph_joins,
                    correlation_predicates,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                match child_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(
                        crate::query_planner::logical_plan::Unwind {
                            input: new_input,
                            expression: u.expression.clone(),
                            alias: u.alias.clone(),
                            label: u.label.clone(),
                            tuple_properties: u.tuple_properties.clone(),
                        },
                    ))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                // CartesianProduct with join_condition represents a cross-table join pattern
                // We need to:
                // 1. Process both sides to get their joins
                // 2. Combine all joins into the parent collected_graph_joins
                // 3. Add the join_condition as a join between the patterns

                crate::debug_print!(
                    "📦 CartesianProduct: Processing with join_condition={:?}",
                    cp.join_condition.is_some()
                );

                // Create separate join collections for each side
                let mut left_joins: Vec<Join> = vec![];
                let mut right_joins: Vec<Join> = vec![];

                let left_tf = Self::build_graph_joins(
                    cp.left.clone(),
                    &mut left_joins,
                    &mut Vec::new(),
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;
                let right_tf = Self::build_graph_joins(
                    cp.right.clone(),
                    &mut right_joins,
                    &mut Vec::new(),
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                    captured_cte_refs,
                )?;

                crate::debug_print!(
                    "📦 CartesianProduct: left_joins={}, right_joins={}",
                    left_joins.len(),
                    right_joins.len()
                );

                // CRITICAL FIX: When LEFT is a simple GraphNode (node-only MATCH pattern) and has no joins,
                // we need to create a FROM marker for it. Without this, when OPTIONAL MATCH comes first
                // and required MATCH has a node-only pattern, the required node would be missing from SQL.
                // This happens in: OPTIONAL MATCH (a)-[]->(b) MATCH (x) RETURN ...
                // After swap fix in match_clause.rs: CartesianProduct(left=x, right=optional, is_optional=true)
                // But x has no FROM marker because GraphNode doesn't generate joins.
                if left_joins.is_empty() {
                    if let LogicalPlan::GraphNode(gn) = cp.left.as_ref() {
                        // Extract table info from GraphNode's ViewScan
                        if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                            log::info!(
                                "📦 CartesianProduct: Creating FROM marker for GraphNode '{}' (table='{}')",
                                gn.alias,
                                vs.source_table
                            );
                            // Insert at the beginning so it becomes the anchor
                            helpers::JoinBuilder::from_marker(&vs.source_table, &gn.alias)
                                .build_and_insert_at(collected_graph_joins, 0);
                            crate::debug_print!(
                                "📦 CartesianProduct: Added FROM marker for left GraphNode '{}'",
                                gn.alias
                            );
                        }
                    }
                }

                // CRITICAL: Bubble up all joins to the parent collected_graph_joins
                // The left side joins need to come first
                collected_graph_joins.extend(left_joins.clone());
                collected_graph_joins.extend(right_joins.clone());

                // Extract correlation predicate for WITH...MATCH cross-table patterns
                // This will be used by the renderer to generate proper JOIN conditions
                // CRITICAL: Check if the join_condition contains NOT PathPattern
                // If so, it MUST go in WHERE clause, not JOIN ON (ClickHouse limitation)
                // We'll add it to correlation_predicates but the renderer will separate it
                if let Some(join_cond) = &cp.join_condition {
                    log::info!(
                        "📦 CartesianProduct: Extracting predicate: NOT PathPattern={}",
                        join_cond.contains_not_path_pattern()
                    );
                    correlation_predicates.push(join_cond.clone());
                }

                // CROSS-TABLE COMMA PATTERN FIX: For comma-separated patterns with shared node aliases,
                // we need to generate a JOIN even when there's NO explicit join_condition.
                // Example: MATCH (srcip:IP)-[:REQUESTED]->(d), (srcip)-[:ACCESSED]->(dest)
                // Both patterns share "srcip" but there's no WHERE clause to create join_condition.
                // We need to detect this and generate: dns_log JOIN conn_log ON dns.orig_h = conn.orig_h
                //
                // IMPORTANT: We check this even when left_joins/right_joins are empty because
                // simple single-hop patterns don't have intermediate JOINs - the shared-node JOIN
                // IS the JOIN we need to create!
                if cp.join_condition.is_none() {
                    log::info!("📦 CartesianProduct: No join_condition but have joins on both sides - checking for shared nodes");

                    // Extract node aliases from both sides using existing helper
                    let left_nodes = helpers::collect_node_aliases_from_plan(&cp.left);
                    let right_nodes = helpers::collect_node_aliases_from_plan(&cp.right);

                    // Find shared nodes
                    let shared_nodes: Vec<String> = left_nodes
                        .iter()
                        .filter(|n| right_nodes.contains(n))
                        .cloned()
                        .collect();

                    if !shared_nodes.is_empty() {
                        log::info!(
                            "📦 CartesianProduct: Found {} shared nodes: {:?}",
                            shared_nodes.len(),
                            shared_nodes
                        );
                        log::info!(
                            "📦 CartesianProduct: Generating cross-table JOINs for shared nodes"
                        );

                        // For each shared node, we need to generate a JOIN between the two relationship tables
                        // We'll use the existing cross-branch JOIN generation infrastructure
                        for shared_node in &shared_nodes {
                            // Extract table info from both sides using existing helper
                            if let (
                                Some((_left_table, _left_alias)),
                                Some((_right_table, _right_alias)),
                            ) = (
                                helpers::extract_right_table_from_plan(&cp.left, graph_schema),
                                helpers::extract_right_table_from_plan(&cp.right, graph_schema),
                            ) {
                                // Try to extract node appearances to get column names
                                // We need to find the GraphRel from each side to call extract_node_appearance
                                if let (Some(left_rel), Some(right_rel)) = (
                                    helpers::find_graph_rel_in_plan(&cp.left),
                                    helpers::find_graph_rel_in_plan(&cp.right),
                                ) {
                                    // Determine which side the shared node is on for each GraphRel
                                    let left_is_from = left_rel.left_connection == *shared_node;
                                    let right_is_from = right_rel.left_connection == *shared_node;

                                    // Get node appearances using cross_branch module function
                                    if let (Ok(left_appearance), Ok(right_appearance)) = (
                                        super::cross_branch::extract_node_appearance(
                                            shared_node,
                                            left_rel,
                                            left_is_from,
                                            plan_ctx,
                                            graph_schema,
                                        ),
                                        super::cross_branch::extract_node_appearance(
                                            shared_node,
                                            right_rel,
                                            right_is_from,
                                            plan_ctx,
                                            graph_schema,
                                        ),
                                    ) {
                                        // Build table name with database prefix if needed
                                        let table_name = if left_appearance.database.is_empty() {
                                            left_appearance.table_name.clone()
                                        } else {
                                            format!(
                                                "{}.{}",
                                                left_appearance.database,
                                                left_appearance.table_name
                                            )
                                        };

                                        // Generate JOIN using JoinBuilder
                                        helpers::JoinBuilder::new(
                                            &table_name,
                                            &left_appearance.rel_alias,
                                        )
                                        .add_condition(
                                            &right_appearance.rel_alias,
                                            &right_appearance.column_name,
                                            &left_appearance.rel_alias,
                                            &left_appearance.column_name,
                                        )
                                        .build_and_push(collected_graph_joins);

                                        log::info!("📦 Generated JOIN for shared node '{}': {} JOIN {} ON {}.{} = {}.{}",
                                            shared_node,
                                            right_appearance.rel_alias, left_appearance.rel_alias,
                                            right_appearance.rel_alias, right_appearance.column_name,
                                            left_appearance.rel_alias, left_appearance.column_name);
                                    }
                                }
                            }
                        }
                    }
                }

                // CROSS-TABLE DENORMALIZED FIX: If both sides have 0 joins (fully denormalized)
                // AND there's a join_condition, we need to create a JOIN for the right-side table.
                // This connects the two fully denormalized patterns.
                if left_joins.is_empty() && right_joins.is_empty() {
                    if let Some(join_cond) = &cp.join_condition {
                        // CRITICAL: Check if join_condition contains correlated subquery
                        // If so, it MUST stay in WHERE clause - ClickHouse limitation
                        if join_cond.contains_not_path_pattern() {
                            log::info!("⚠️ CartesianProduct join_condition contains correlated subquery - keeping in correlation_predicates for WHERE clause");
                            crate::debug_print!("⚠️ CartesianProduct join_condition contains correlated subquery - will NOT create JOIN, must stay in WHERE");
                            // Don't create JOIN - let it stay in correlation_predicates for WHERE clause
                        } else {
                            crate::debug_print!("📦 CartesianProduct: Creating cross-table JOIN for fully denormalized patterns");

                            // CRITICAL: First, extract the LEFT-side table to use as FROM clause
                            // This is the anchor table that other tables join TO
                            if let Some((left_table, left_alias)) =
                                helpers::extract_right_table_from_plan(&cp.left, graph_schema)
                            {
                                crate::debug_print!(
                                    "📦 CartesianProduct: Left (anchor) table='{}', alias='{}'",
                                    left_table,
                                    left_alias
                                );

                                // Create a "FROM" marker join with empty joining_on
                                // This will be picked up by reorder_joins_by_dependencies as the anchor
                                helpers::JoinBuilder::from_marker(&left_table, &left_alias)
                                    .build_and_push(collected_graph_joins);
                                crate::debug_print!(
                                    "📦 CartesianProduct: Added FROM marker for left table"
                                );

                                // Extract the right-side table info from the join_condition
                                // The join_condition should be: left_alias.column = right_alias.column
                                if let LogicalExpr::OperatorApplicationExp(op_app) = join_cond {
                                    // Find the right-side alias and table from the right GraphRel
                                    if let Some((right_table, right_alias)) =
                                        helpers::extract_right_table_from_plan(
                                            &cp.right,
                                            graph_schema,
                                        )
                                    {
                                        crate::debug_print!(
                                            "📦 CartesianProduct: Right table='{}', alias='{}'",
                                            right_table,
                                            right_alias
                                        );

                                        // Remap node aliases in join condition to the relationship aliases
                                        // BOTH sides need remapping:
                                        // - left-side node aliases (e.g., ip1) -> left_alias (dns_log alias)
                                        // - right-side node aliases (e.g., ip2) -> right_alias (conn_log alias)
                                        let mut remapped_join_cond =
                                            helpers::remap_node_aliases_to_relationship(
                                                op_app.clone(),
                                                &cp.right,
                                                &right_alias,
                                            );
                                        // Also remap left-side node aliases to the left table alias
                                        remapped_join_cond =
                                            helpers::remap_node_aliases_to_relationship(
                                                remapped_join_cond,
                                                &cp.left,
                                                &left_alias,
                                            );

                                        // Create a JOIN for the right-side table using the remapped join_condition
                                        helpers::JoinBuilder::new(&right_table, &right_alias)
                                            .optional(cp.is_optional)
                                            .add_raw_condition(remapped_join_cond)
                                            .build_and_push(collected_graph_joins);
                                        crate::debug_print!("📦 CartesianProduct: Added cross-table JOIN, total joins now={}",
                                        collected_graph_joins.len());
                                    }
                                }
                            }
                        } // End else (not correlated subquery)
                    }
                }

                crate::debug_print!(
                    "📦 CartesianProduct: Total bubbled up joins={}",
                    collected_graph_joins.len()
                );

                match (&left_tf, &right_tf) {
                    (Transformed::No(_), Transformed::No(_)) => {
                        Transformed::No(logical_plan.clone())
                    }
                    _ => Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(
                        crate::query_planner::logical_plan::CartesianProduct {
                            left: left_tf.get_plan().clone(),
                            right: right_tf.get_plan().clone(),
                            is_optional: cp.is_optional,
                            join_condition: cp.join_condition.clone(),
                        },
                    ))),
                }
            }
            LogicalPlan::WithClause(with_clause) => {
                // CRITICAL: WITH creates a scope boundary - DON'T traverse into it!
                // The WithScopeSplitter pass has already marked this as a boundary.
                // Joins should only be computed within each scope, not across scopes.
                //
                // Why: WITH materializes intermediate results. The pattern BEFORE the WITH
                // is independent from the pattern AFTER the WITH. Computing joins across
                // this boundary would waste work and create stale join data.
                //
                // Example:
                //   MATCH (a)-[:F]->(b) WITH a, b  [Scope 1: compute joins for a→b]
                //   MATCH (b)-[:F]->(c) RETURN c   [Scope 2: compute joins for b→c]
                //
                // GraphJoinInference should compute:
                //   - Scope 1: joins for (a)-[:F]->(b)
                //   - Scope 2: joins for (b)-[:F]->(c)
                // NOT: joins for the entire (a)-[:F]->(b)-[:F]->(c) pattern!
                log::info!(
                    "⛔ GraphJoinInference: Encountered WITH scope boundary with {} exported aliases - NOT traversing",
                    with_clause.exported_aliases.len()
                );

                // CRITICAL: Preserve cte_references from VariableResolver!
                // VariableResolver already populated the correct cte_references.
                // We should NOT overwrite them with our lookup logic.
                eprintln!("🔬 GraphJoinInference::build_graph_joins: WithClause has {} cte_references: {:?}",
                           with_clause.cte_references.len(), with_clause.cte_references);

                // IMPORTANT: Return the logical_plan parameter directly, NOT plan.clone()
                // This preserves the cte_references that VariableResolver populated
                Transformed::No(logical_plan.clone())
            }
        };
        Ok(transformed_plan)
    }

    fn collect_graph_joins(
        &self,
        logical_plan: Arc<LogicalPlan>,
        root_plan: Arc<LogicalPlan>, // Root plan for reference checking
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        join_ctx: &mut JoinContext,
        cte_scope_aliases: &HashSet<String>, // Aliases exported from WITH CTEs in parent scopes
        node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
        pattern_metadata: &PatternGraphMetadata, // Phase 1: Pattern metadata for cached lookups
    ) -> AnalyzerResult<()> {
        crate::debug_print!("\n+- collect_graph_joins ENTER");
        crate::debug_print!(
            "📋 Plan variant: {:?}",
            std::mem::discriminant(&*logical_plan)
        );
        crate::debug_print!(
            "📋 Joins before: {}, Context: {}",
            collected_graph_joins.len(),
            join_ctx.debug_summary()
        );

        let result = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                crate::debug_print!("📋 Projection, recursing into input");
                self.collect_graph_joins(
                    projection.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::GraphNode(graph_node) => {
                crate::debug_print!("🟢 GraphNode({}), recursing into input", graph_node.alias);

                // CRITICAL FIX FOR IC9: Check if this node references a CTE from WITH clause
                // If so, we need to ensure it's marked as a join target for subsequent patterns
                // Example: WITH ... (friend) MATCH (friend)<-[:REL]-(other)
                // The second MATCH creates GraphNode(friend) but it should JOIN to the CTE
                log::info!(
                    "🔍 GraphNode '{}' - checking for CTE reference",
                    graph_node.alias
                );

                if let Ok(table_ctx) = plan_ctx.get_table_ctx(&graph_node.alias) {
                    log::info!("  ✓ Found TableCtx for '{}'", graph_node.alias);
                    if let Some(cte_name) = table_ctx.get_cte_name() {
                        log::info!(
                            "🔍 GraphNode '{}' references CTE '{}' - marking as joined",
                            graph_node.alias,
                            cte_name
                        );
                        // Mark this alias as already joined (it comes from the CTE)
                        // This prevents duplicate joins and ensures subsequent patterns
                        // reference the CTE columns instead of creating new ViewScans
                        join_ctx.insert(graph_node.alias.clone());

                        // If this GraphNode has a ViewScan input, we should skip it
                        // because the data comes from the CTE, not a fresh table scan
                        // But we still need to recurse in case there's nested structure
                        crate::debug_print!(
                            "  ✓ Skipping ViewScan for '{}' (data from CTE '{}')",
                            graph_node.alias,
                            cte_name
                        );
                    } else {
                        log::info!(
                            "  ✗ TableCtx for '{}' has NO CTE reference",
                            graph_node.alias
                        );
                    }
                } else {
                    log::info!("  ✗ No TableCtx found for '{}'", graph_node.alias);
                }

                // NOTE: We do NOT add the node alias to join_ctx here (unless from CTE).
                // The relationship inference (infer_graph_join) will determine anchors
                // based on direction and is_optional flags. This prevents breaking
                // single-pattern MATCH queries where anchor is determined semantically.
                self.collect_graph_joins(
                    graph_node.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::ViewScan(_) => {
                crate::debug_print!("📋 ViewScan, nothing to collect");
                Ok(())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                crate::debug_print!("📊 --- GraphRel({}) ---", graph_rel.alias);
                crate::debug_print!("📊   left_connection: {}", graph_rel.left_connection);
                crate::debug_print!("📊   right_connection: {}", graph_rel.right_connection);
                crate::debug_print!("📊   direction: {:?}", graph_rel.direction);
                crate::debug_print!(
                    "📊   left type: {:?}",
                    std::mem::discriminant(&*graph_rel.left)
                );
                crate::debug_print!(
                    "📊   right type: {:?}",
                    std::mem::discriminant(&*graph_rel.right)
                );

                // CRITICAL FIX: Process branches in pattern-order, not AST-order
                // For Incoming direction `(a)->(b)<-(c)`, AST is: left=c, right=(a->b)
                // But pattern order is: a, then b, then c
                // So for Incoming: process RIGHT first (contains earlier part of pattern)
                // For Outgoing: process LEFT first (standard order)

                if graph_rel.direction == Direction::Incoming {
                    // Incoming: pattern flows right-to-left in AST
                    // Process RIGHT subtree first (earlier in pattern)
                    crate::debug_print!(
                        "📊   ⬅️ INCOMING: Processing RIGHT branch first (pattern order)..."
                    );
                    self.collect_graph_joins(
                        graph_rel.right.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        cte_scope_aliases,
                        node_appearances,
                        pattern_metadata,
                    )?;
                    crate::debug_print!(
                        "📊   ✓ RIGHT done. Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Phase 2: Cross-branch joins now generated once at the end using metadata
                    // (Commented out old approach - was generating during traversal)
                    // self.check_and_generate_cross_branch_joins(
                    //     graph_rel, plan_ctx, graph_schema, node_appearances, collected_graph_joins
                    // )?;
                    crate::debug_print!(
                        "📊   ✓ Cross-branch check skipped (handled by Phase 2). Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Process CURRENT relationship (connects right to left)
                    crate::debug_print!("📊   ⬅️ Processing CURRENT relationship...");
                    self.infer_graph_join(
                        graph_rel,
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        pattern_metadata,
                    )?;
                    crate::debug_print!(
                        "📊   ✓ CURRENT done. Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Process LEFT branch last (end of pattern)
                    crate::debug_print!("📊   ⬅️ Processing LEFT branch last...");
                    let result = self.collect_graph_joins(
                        graph_rel.left.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        cte_scope_aliases,
                        node_appearances,
                        pattern_metadata,
                    );
                    crate::debug_print!(
                        "📊   ✓ LEFT done. Joins now: {}",
                        collected_graph_joins.len()
                    );
                    result
                } else {
                    // Outgoing or Either: standard left-to-right order
                    crate::debug_print!("📊   ➡️ OUTGOING: Processing LEFT branch first...");
                    self.collect_graph_joins(
                        graph_rel.left.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        cte_scope_aliases,
                        node_appearances,
                        pattern_metadata,
                    )?;
                    crate::debug_print!(
                        "📊   ✓ LEFT done. Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Phase 2: Cross-branch joins now generated once at the end using metadata
                    // (Commented out old approach - was generating during traversal)
                    // self.check_and_generate_cross_branch_joins(
                    //     graph_rel, plan_ctx, graph_schema, node_appearances, collected_graph_joins
                    // )?;
                    crate::debug_print!(
                        "📊   ✓ Cross-branch check skipped (handled by Phase 2). Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Process CURRENT relationship
                    crate::debug_print!("📊   ➡️ Processing CURRENT relationship...");
                    self.infer_graph_join(
                        graph_rel,
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        pattern_metadata,
                    )?;
                    crate::debug_print!(
                        "📊   ✓ CURRENT done. Joins now: {}",
                        collected_graph_joins.len()
                    );

                    // Process RIGHT branch
                    crate::debug_print!("📊   ➡️ Processing RIGHT branch...");
                    let result = self.collect_graph_joins(
                        graph_rel.right.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        cte_scope_aliases,
                        node_appearances,
                        pattern_metadata,
                    );
                    crate::debug_print!(
                        "📊   ✓ RIGHT done. Joins now: {}",
                        collected_graph_joins.len()
                    );
                    result
                }
            }
            LogicalPlan::Cte(cte) => {
                crate::debug_print!("📋 Cte, recursing into input");
                self.collect_graph_joins(
                    cte.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::Empty => {
                crate::debug_print!("📋 Empty, nothing to collect");
                Ok(())
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                crate::debug_print!("📋 GraphJoins, recursing into input");
                self.collect_graph_joins(
                    graph_joins.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::Filter(filter) => {
                crate::debug_print!("📋 Filter, recursing into input");
                self.collect_graph_joins(
                    filter.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::GroupBy(group_by) => {
                // CRITICAL: Check if this GroupBy is a MATERIALIZATION BOUNDARY
                // If so, DO NOT recurse into its input - the inner joins belong
                // to a separate query block that must be executed first (as a CTE).
                if group_by.is_materialization_boundary {
                    crate::debug_print!("🛑 GroupBy is_materialization_boundary=true, STOPPING join collection here (exposed_alias={:?})", group_by.exposed_alias);
                    // Don't recurse - the inner query block has its own joins
                    Ok(())
                } else {
                    crate::debug_print!("📍 GroupBy, recursing into input");
                    self.collect_graph_joins(
                        group_by.input.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        join_ctx,
                        cte_scope_aliases,
                        node_appearances,
                        pattern_metadata,
                    )
                }
            }
            LogicalPlan::OrderBy(order_by) => {
                crate::debug_print!("📋 OrderBy, recursing into input");
                self.collect_graph_joins(
                    order_by.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::Skip(skip) => {
                crate::debug_print!("📋 Skip, recursing into input");
                self.collect_graph_joins(
                    skip.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::Limit(limit) => {
                crate::debug_print!("📋 Limit, recursing into input");
                self.collect_graph_joins(
                    limit.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::Union(_union) => {
                // CRITICAL: Don't recurse into UNION branches here!
                // Each branch will be processed independently by build_graph_joins,
                // which properly clones the state for each branch.
                // If we recurse here with shared state, branches pollute each other.
                crate::debug_print!("🔀 Union detected in collect_graph_joins - skipping recursion (handled by build_graph_joins)");
                Ok(())
            }
            LogicalPlan::PageRank(_) => {
                crate::debug_print!("📋 PageRank, nothing to collect");
                Ok(())
            }
            LogicalPlan::Unwind(u) => {
                crate::debug_print!("📋 Unwind, recursing into input");
                self.collect_graph_joins(
                    u.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::CartesianProduct(cp) => {
                crate::debug_print!("📋 CartesianProduct, processing children INDEPENDENTLY");
                // IMPORTANT: CartesianProduct children should be collected INDEPENDENTLY
                // because they represent separate graph patterns that will be CROSS JOINed.
                // We DON'T want aliases from one side affecting the other side's join inference.

                // Process LEFT side into the shared collections
                // The left side is the "base" pattern (e.g., from WITH clause)
                self.collect_graph_joins(
                    cp.left.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )?;

                // For the RIGHT side, we still collect into shared collections,
                // but the key is that join_ctx from LEFT will prevent
                // the RIGHT side from trying to create conflicting joins
                self.collect_graph_joins(
                    cp.right.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    join_ctx,
                    cte_scope_aliases,
                    node_appearances,
                    pattern_metadata,
                )
            }
            LogicalPlan::WithClause(with_clause) => {
                // CRITICAL: WITH creates a scope boundary - the pattern INSIDE belongs to a different scope
                // However, EXPORTED aliases are visible to downstream patterns and should be tracked
                // in cte_scope_aliases so GraphNodes can resolve them as CTE references.
                //
                // What we do:
                // 1. Stop recursion (don't collect joins from inside the WITH)
                // 2. Pass exported_aliases to downstream patterns (they're in CTE scope)
                //
                // This respects the materialization boundary set by WithScopeSplitter.
                crate::debug_print!("⛔ WithClause scope boundary - stopping join collection");
                crate::debug_print!(
                    "   Exported aliases (will be in CTE scope): {:?}",
                    with_clause.exported_aliases
                );

                // The exported aliases are NOW in CTE scope for any code that follows
                // We would pass them down, but we've hit a boundary so there's nothing to recurse into
                // The ACTUAL propagation happens in the outer scope that contains this WITH

                // Don't recurse - treat this as a boundary
                Ok(())
            }
        };

        crate::debug_print!("+- collect_graph_joins EXIT");
        crate::debug_print!(
            "   Joins after: {}, Context: {}\n",
            collected_graph_joins.len(),
            join_ctx.debug_summary()
        );

        result
    }

    // ========================================================================
    // PatternSchemaContext Integration (Phase 2)
    // ========================================================================

    /// Compute PatternSchemaContext for a GraphRel.
    ///
    /// This is the bridge between the logical plan (GraphRel) and the unified
    /// schema abstraction (PatternSchemaContext). Once computed, the context
    /// can be used for exhaustive pattern matching instead of scattered detection.
    ///
    /// # Arguments
    /// * `graph_rel` - The relationship pattern from the logical plan
    /// * `plan_ctx` - Planning context with table contexts
    /// * `graph_schema` - The graph schema for schema lookups
    /// * `prev_edge_info` - Info about previous edge for multi-hop patterns
    ///
    /// # Returns
    /// * `Some(PatternSchemaContext)` - If schemas can be resolved
    /// * `None` - If node/relationship schemas cannot be found (anonymous patterns)
    #[allow(dead_code)]
    fn compute_pattern_context(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
        prev_edge_info: Option<(&str, &str, bool)>,
    ) -> Option<PatternSchemaContext> {
        // 1. Get node labels from plan_ctx (or infer from relationship schema)
        let left_alias = &graph_rel.left_connection;
        let right_alias = &graph_rel.right_connection;

        let left_ctx = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(left_alias.clone()))
            .ok()?;
        let right_ctx = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(right_alias.clone()))
            .ok()?;

        // Try to get labels from plan_ctx, but allow empty for anonymous nodes
        let left_label_opt = left_ctx.get_label_str().ok();
        let right_label_opt = right_ctx.get_label_str().ok();

        // 2. Get relationship type(s) from labels
        let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();

        if rel_types.is_empty() {
            crate::debug_print!("    ⚠️ compute_pattern_context: no relationship types found");
            return None;
        }

        // 3. Handle anonymous nodes by inferring labels from relationship schema
        // First try to get relationship schema with explicit labels (if provided)
        // If labels are missing (anonymous nodes), try without them and infer labels
        let (left_label, right_label, rel_schema) =
            if left_label_opt.is_some() && right_label_opt.is_some() {
                // Both labels provided - use them
                let (left, right) = match (left_label_opt, right_label_opt) {
                    (Some(l), Some(r)) => (l, r),
                    _ => unreachable!("Already checked both are Some"),
                };
                let rel = graph_schema
                    .get_rel_schema_with_nodes(&rel_types[0], Some(&left), Some(&right))
                    .ok()?;
                (left, right, rel)
            } else {
                // One or both labels missing (anonymous nodes) - infer from relationship schema
                crate::debug_print!(
                    "    🔍 Anonymous node(s) detected - inferring labels from relationship schema"
                );

                // Get relationship schema without node labels (matches any compatible schema)
                let rel = graph_schema
                    .get_rel_schema_with_nodes(&rel_types[0], None, None)
                    .ok()?;

                // Infer labels from relationship schema
                let inferred_left = rel.from_node.clone();
                let inferred_right = rel.to_node.clone();

                crate::debug_print!(
                    "    ✅ Inferred labels: left='{}', right='{}'",
                    inferred_left,
                    inferred_right
                );

                (inferred_left, inferred_right, rel)
            };

        // For denormalized edges, use composite key (database::table::label) to get the correct node schema
        // Format: "database::table::label" (matching config.rs format)
        let composite_left_key = format!(
            "{}::{}::{}",
            rel_schema.database, rel_schema.table_name, left_label
        );
        let composite_right_key = format!(
            "{}::{}::{}",
            rel_schema.database, rel_schema.table_name, right_label
        );

        // Try composite key first, fallback to label-only
        let left_node_schema = graph_schema
            .node_schema_opt(&composite_left_key)
            .or_else(|| graph_schema.node_schema_opt(&left_label))?;
        let right_node_schema = graph_schema
            .node_schema_opt(&composite_right_key)
            .or_else(|| graph_schema.node_schema_opt(&right_label))?;

        crate::debug_print!(
            "    🔍 Node schema lookup: left='{}' → '{}', right='{}' → '{}'",
            composite_left_key,
            left_node_schema.full_table_name(),
            composite_right_key,
            right_node_schema.full_table_name()
        );

        // 4. Compute PatternSchemaContext
        let ctx = PatternSchemaContext::analyze(
            left_alias,
            right_alias,
            left_node_schema,
            right_node_schema,
            rel_schema,
            graph_schema,
            &graph_rel.alias,
            rel_types,
            prev_edge_info,
        )
        .ok()?; // Convert Result to Option - if error, return None

        crate::debug_print!("    ✅ compute_pattern_context: {}", ctx.debug_summary());
        Some(ctx)
    }

    /// Log the pattern context for debugging purposes.
    /// This helps verify that the new abstraction correctly identifies schema patterns.
    #[allow(dead_code)]
    fn log_pattern_context_comparison(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) {
        if let Some(_ctx) = self.compute_pattern_context(graph_rel, plan_ctx, graph_schema, None) {
            crate::debug_print!("    📊 PatternSchemaContext for {}:", graph_rel.alias);
            crate::debug_print!(
                "       Left node:  {}",
                match &_ctx.left_node {
                    NodeAccessStrategy::OwnTable { table, .. } => format!("OwnTable({})", table),
                    NodeAccessStrategy::EmbeddedInEdge { edge_alias, .. } =>
                        format!("Embedded({})", edge_alias),
                    NodeAccessStrategy::Virtual { label } => format!("Virtual({})", label),
                }
            );
            crate::debug_print!(
                "       Right node: {}",
                match &_ctx.right_node {
                    NodeAccessStrategy::OwnTable { table, .. } => format!("OwnTable({})", table),
                    NodeAccessStrategy::EmbeddedInEdge { edge_alias, .. } =>
                        format!("Embedded({})", edge_alias),
                    NodeAccessStrategy::Virtual { label } => format!("Virtual({})", label),
                }
            );
            crate::debug_print!("       Join:       {}", _ctx.join_strategy.description());
            crate::debug_print!("       Rel types:  {:?}", _ctx.rel_types);
        } else {
            crate::debug_print!("    📊 PatternSchemaContext: Unable to compute (missing schemas)");
        }
    }

    // ========================================================================
    // PatternSchemaContext-Based Join Generation (Phase 3)
    // ========================================================================

    /// Helper function to get table name with database prefix if needed.
    ///
    /// CTEs (Common Table Expressions) from WITH clauses should NOT have database prefixes.
    /// Base tables from schema SHOULD have database prefixes.
    ///
    /// # Arguments
    /// * `cte_name` - The CTE or table name (without database prefix)
    /// * `alias` - The variable alias (to check if it's a CTE reference)
    /// * `schema` - The node/rel schema (provides database name for base tables)
    /// * `plan_ctx` - The plan context (to check if alias references a CTE)
    ///
    /// # Returns
    /// Table name with database prefix if it's a base table, without prefix if it's a CTE.
    fn get_table_name_with_prefix(
        cte_name: &str,
        alias: &str,
        schema: &NodeSchema,
        plan_ctx: &PlanCtx,
    ) -> String {
        // Check if this alias references a CTE from WITH clause
        if let Ok(table_ctx) = plan_ctx.get_table_ctx_from_alias_opt(&Some(alias.to_string())) {
            if let Some(cte_ref) = table_ctx.get_cte_name() {
                // CTE reference - use the cte_reference from TableCtx (may have counter suffix)
                // During rendering, update_graph_joins_cte_refs() updates this to final name
                crate::debug_print!(
                    "    🔍 Table name for alias '{}': '{}' (CTE - from TableCtx.cte_reference)",
                    alias,
                    cte_ref
                );
                return cte_ref.to_string();
            }
        }

        // Base table - add database prefix
        let table_name = format!("{}.{}", schema.database, cte_name);
        crate::debug_print!(
            "    🔍 Table name for alias '{}': '{}' (base table - added prefix)",
            alias,
            table_name
        );
        table_name
    }

    /// Helper function to get table name with database prefix for relationship tables.
    fn get_rel_table_name_with_prefix(
        cte_name: &str,
        alias: &str,
        schema: &RelationshipSchema,
        plan_ctx: &PlanCtx,
    ) -> String {
        // Check if this alias references a CTE from WITH clause
        if let Ok(table_ctx) = plan_ctx.get_table_ctx_from_alias_opt(&Some(alias.to_string())) {
            if let Some(cte_ref) = table_ctx.get_cte_name() {
                // CTE reference - use the cte_reference from TableCtx (may have counter suffix)
                // During rendering, update_graph_joins_cte_refs() updates this to final name
                crate::debug_print!(
                    "    🔍 Rel table name for alias '{}': '{}' (CTE - from TableCtx.cte_reference)",
                    alias,
                    cte_ref
                );
                return cte_ref.to_string();
            }
        }

        // Base table - add database prefix
        let table_name = format!("{}.{}", schema.database, cte_name);
        crate::debug_print!(
            "    🔍 Rel table name for alias '{}': '{}' (base table - added prefix)",
            alias,
            table_name
        );
        table_name
    }

    /// Generate graph JOINs using PatternSchemaContext for exhaustive pattern matching.
    ///
    /// This is the new implementation that replaces the scattered detection logic
    /// with unified schema abstraction. The key insight is:
    ///
    /// 1. `PatternSchemaContext::analyze()` computes all schema decisions ONCE
    /// 2. Exhaustive `match` on `ctx.join_strategy` handles all cases cleanly
    /// 3. Each variant produces the appropriate JOINs without nested conditionals
    ///
    /// # Strategy Mapping
    ///
    /// | JoinStrategy      | JOINs Generated                                    |
    /// |-------------------|---------------------------------------------------|
    /// | SingleTableScan   | None - all data from one table                    |
    /// | Traditional       | node-edge-node: LEFT JOIN rel, RIGHT JOIN rel     |
    /// | MixedAccess       | Partial: only JOIN the non-embedded node          |
    /// | EdgeToEdge        | Multi-hop: edge2.from_id = edge1.to_id           |
    /// | CoupledSameRow    | None - unify aliases, same physical row           |
    fn handle_graph_pattern_v2(
        &self,
        ctx: &PatternSchemaContext,
        left_alias: &str,
        rel_alias: &str,
        right_alias: &str,
        left_cte_name: &str,
        rel_cte_name: &str,
        right_cte_name: &str,
        left_label: &str,
        right_label: &str,
        left_is_optional: bool,
        rel_is_optional: bool,
        right_is_optional: bool,
        left_node_schema: &NodeSchema,
        right_node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
        plan_ctx: &mut PlanCtx,
        collected_graph_joins: &mut Vec<Join>,
        join_ctx: &mut JoinContext,
        _graph_rel: &GraphRel, // Added to check for path variables
    ) -> AnalyzerResult<()> {
        log::warn!(
            "🚨 handle_graph_pattern_v2 ENTER: rel={}, left={}, right={}, strategy={:?}",
            rel_alias,
            left_alias,
            right_alias,
            ctx.join_strategy
        );
        crate::debug_print!("    📐 handle_graph_pattern_v2: {}", ctx.debug_summary());
        crate::debug_print!(
            "    📐 Node labels: left='{}', right='{}'",
            left_label,
            right_label
        );

        // Pre-filter for polymorphic edges:
        // 1. type_column IN (...) for relationship type
        // 2. from_label_column = 'X' and to_label_column = 'Y' for node type
        let type_filter = ctx.edge.get_type_filter(rel_alias);
        let label_filter = ctx
            .edge
            .get_label_filter(rel_alias, left_label, right_label);

        // Combine filters
        let pre_filter: Option<LogicalExpr> = match (type_filter, label_filter) {
            (Some(tf), Some(lf)) => Some(LogicalExpr::Raw(format!("{} AND {}", tf, lf))),
            (Some(tf), None) => Some(LogicalExpr::Raw(tf)),
            (None, Some(lf)) => Some(LogicalExpr::Raw(lf)),
            (None, None) => None,
        };

        if pre_filter.is_some() {
            crate::debug_print!("    🔹 Polymorphic pre_filter: {:?}", pre_filter);
        }

        // ================================================================
        // Clean delegation: generate joins + side effects
        // ================================================================
        use super::join_generation::{self, ResolvedTables};

        // Resolve table names (CTE vs base table)
        let left_table =
            Self::get_table_name_with_prefix(left_cte_name, left_alias, left_node_schema, plan_ctx);
        let rel_table =
            Self::get_rel_table_name_with_prefix(rel_cte_name, rel_alias, rel_schema, plan_ctx);
        let right_table = Self::get_table_name_with_prefix(
            right_cte_name,
            right_alias,
            right_node_schema,
            plan_ctx,
        );

        let tables = ResolvedTables {
            left_alias,
            left_table: &left_table,
            left_cte_name,
            rel_alias,
            rel_table: &rel_table,
            rel_cte_name,
            right_alias,
            right_table: &right_table,
            right_cte_name,
        };

        // Step 1: Generate anchor-aware joins based on strategy
        // Pass join_ctx aliases so the generator knows which nodes are already available.
        let already_available = join_ctx.to_hashset();
        let mut new_joins = join_generation::generate_pattern_joins(
            ctx,
            &tables,
            rel_schema,
            plan_ctx,
            pre_filter,
            &already_available,
        )?;

        // Step 2: VLP endpoint rewriting (before collection, affects dependencies)
        join_generation::apply_vlp_rewrites(&mut new_joins, plan_ctx);

        // Step 3: Apply optionality
        let optional_aliases: HashSet<String> = [
            (left_alias, left_is_optional),
            (rel_alias, rel_is_optional),
            (right_alias, right_is_optional),
        ]
        .iter()
        .filter(|(_, opt)| *opt)
        .map(|(a, _)| a.to_string())
        .collect();
        join_generation::apply_optional_marking(&mut new_joins, &optional_aliases);

        // Step 4: Collect with dedup (handles shared nodes across patterns)
        join_generation::collect_with_dedup(collected_graph_joins, new_joins);

        // Step 5: Side effects — register denormalized aliases
        join_generation::register_denormalized_aliases(
            ctx,
            left_alias,
            rel_alias,
            right_alias,
            plan_ctx,
        );

        // Step 6: Update join context (track what's been joined)
        // For CoupledSameRow, all aliases are conceptually joined even with 0 joins
        join_ctx.insert(left_alias.to_string());
        join_ctx.insert(rel_alias.to_string());
        join_ctx.insert(right_alias.to_string());

        Ok(())
    }

    fn infer_graph_join(
        &self,
        graph_rel: &GraphRel,
        _root_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        join_ctx: &mut JoinContext,
        pattern_metadata: &PatternGraphMetadata, // Phase 1: Cached metadata
    ) -> AnalyzerResult<()> {
        log::info!(
            "🔧 infer_graph_join ENTER: rel='{}', left='{}', right='{}', labels={:?}, join_ctx={}",
            graph_rel.alias,
            graph_rel.left_connection,
            graph_rel.right_connection,
            graph_rel.labels,
            join_ctx.debug_summary()
        );
        crate::debug_print!(
            "    +- infer_graph_join ENTER for GraphRel({})",
            graph_rel.alias
        );
        crate::debug_print!(
            "    📋 left_connection: {}, right_connection: {}",
            graph_rel.left_connection,
            graph_rel.right_connection
        );
        crate::debug_print!("    📋 join_ctx before: {}", join_ctx.debug_summary());

        // Phase 2: Log PatternSchemaContext for validation
        // This compares the new unified abstraction against the old detection logic
        self.log_pattern_context_comparison(graph_rel, plan_ctx, graph_schema);

        // Phase 3: Check if we should skip this pattern due to VLP
        // JoinContext directly tracks VLP endpoints
        if let Some(should_skip) = self.should_skip_for_vlp(graph_rel, join_ctx) {
            if should_skip {
                // CRITICAL: Store VLP endpoints in plan_ctx for subsequent JOIN condition generation
                // This enables FkEdgeJoin handling to use t.end_id instead of u2.user_id
                for (alias, info) in join_ctx.vlp_endpoints() {
                    plan_ctx.register_vlp_endpoint(alias.clone(), info.clone());
                }

                log::info!(
                    "🔧 infer_graph_join: SKIP due to VLP for rel='{}', join_ctx: {}",
                    graph_rel.alias,
                    join_ctx.debug_summary()
                );
                crate::debug_print!("    +- infer_graph_join EXIT\n");
                return Ok(());
            }
        }

        // Phase 3: Validate node contexts (check for missing contexts and $any nodes)
        if self
            .validate_node_contexts(graph_rel, plan_ctx, join_ctx)
            .is_err()
        {
            log::warn!(
                "🔧 infer_graph_join: SKIP due to missing node context for rel='{}'",
                graph_rel.alias
            );
            crate::debug_print!("    +- infer_graph_join EXIT\n");
            return Ok(());
        }

        // Phase 3: Extract node label information
        let (left_has_explicit_label, right_has_explicit_label) =
            self.extract_node_labels(graph_rel);

        // Clone the optional_aliases set before calling get_graph_context
        // to avoid borrow checker issues
        let optional_aliases = plan_ctx.get_optional_aliases().clone();

        // Phase 1: Use cached node reference checks from metadata (no tree traversal!)
        // Previously: Called is_node_referenced() twice per GraphRel (expensive tree traversal)
        // Now: Instant HashMap lookup of pre-computed result
        let left_is_referenced = pattern_metadata
            .nodes
            .get(&graph_rel.left_connection)
            .map(|n| n.is_referenced)
            .unwrap_or(false); // Conservative: if not in metadata, assume not referenced

        crate::debug_print!(
            "    ⚡ LEFT '{}' referenced: {} (cached)",
            graph_rel.left_connection,
            left_is_referenced
        );

        let right_is_referenced = pattern_metadata
            .nodes
            .get(&graph_rel.right_connection)
            .map(|n| n.is_referenced)
            .unwrap_or(false);

        crate::debug_print!(
            "    ⚡ RIGHT '{}' referenced: {} (cached)",
            graph_rel.right_connection,
            right_is_referenced
        );

        // Track C: Check if relationship has types (may be filtered to 0 by property-based pruning)
        // If no types, skip join inference - the Empty plan handles this case
        if let Ok(rel_ctx) = plan_ctx.get_rel_table_ctx(&graph_rel.alias) {
            if rel_ctx.get_labels().is_none_or(|labels| labels.is_empty()) {
                log::info!(
                    "🔧 GraphJoinInference: Skipping for relationship '{}' with no types (filtered by Track C)",
                    graph_rel.alias
                );
                crate::debug_print!("    +- infer_graph_join EXIT (empty relationship)\n");
                return Ok(());
            }
        }

        // Extract all necessary data from graph_context BEFORE passing plan_ctx mutably
        let (
            left_alias_str,
            rel_alias_str,
            right_alias_str,
            _left_node_id_column,
            _right_node_id_column,
            left_label,
            right_label,
            _rel_labels,
            left_node_schema,
            right_node_schema,
            rel_schema,
            left_alias,
            rel_alias,
            right_alias,
            left_cte_name,
            rel_cte_name,
            right_cte_name,
        ) = {
            let graph_context = graph_context::get_graph_context(
                graph_rel,
                plan_ctx,
                graph_schema,
                Pass::GraphJoinInference,
            )?;

            (
                graph_context.left.alias.to_string(),
                graph_context.rel.alias.to_string(),
                graph_context.right.alias.to_string(),
                graph_context
                    .left
                    .schema
                    .node_id
                    .columns()
                    .first()
                    .ok_or_else(|| {
                        AnalyzerError::SchemaNotFound(
                            "Left node schema has no ID columns defined".to_string(),
                        )
                    })?
                    .to_string(),
                graph_context
                    .right
                    .schema
                    .node_id
                    .columns()
                    .first()
                    .ok_or_else(|| {
                        AnalyzerError::SchemaNotFound(
                            "Right node schema has no ID columns defined".to_string(),
                        )
                    })?
                    .to_string(),
                graph_context.left.label.clone(),
                graph_context.right.label.clone(),
                // Get all labels from table_ctx for polymorphic IN clause support
                graph_context
                    .rel
                    .table_ctx
                    .get_labels()
                    .cloned()
                    .unwrap_or_else(|| vec![graph_context.rel.label.clone()]),
                graph_context.left.schema.clone(),
                graph_context.right.schema.clone(),
                graph_context.rel.schema.clone(),
                graph_context.left.alias.clone(),
                graph_context.rel.alias.clone(),
                graph_context.right.alias.clone(),
                graph_context.left.cte_name.clone(),
                graph_context.rel.cte_name.clone(),
                graph_context.right.cte_name.clone(),
            )
            // graph_context drops here, releasing the borrow of plan_ctx
        };

        // Check which aliases are optional
        // Check BOTH plan_ctx (for pre-marked optionals) AND graph_rel.is_optional (for marked patterns)
        let left_is_optional = optional_aliases.contains(&left_alias_str);
        let rel_is_optional =
            optional_aliases.contains(&rel_alias_str) || graph_rel.is_optional.unwrap_or(false);
        let right_is_optional = optional_aliases.contains(&right_alias_str);

        crate::debug_print!(
            "    � OPTIONAL CHECK: left='{}' optional={}, rel='{}' optional={} (graph_rel.is_optional={:?}), right='{}' optional={}",
            left_alias_str,
            left_is_optional,
            rel_alias_str,
            rel_is_optional,
            graph_rel.is_optional,
            right_alias_str,
            right_is_optional
        );
        crate::debug_print!("    � optional_aliases set: {:?}", optional_aliases);

        // Check for standalone relationship join.
        // e.g. MATCH (a)-[f1:Follows]->(b)-[f2:Follows]->(c), (a)-[f3:Follows]->(c)
        // In the duplicate scan removing pass, we remove the already scanned nodes. We do this from bottom to up.
        // So there could be a graph_rel who has LogicalPlan::Empty as left. In such case just join the relationship but on both nodes columns.
        // In case of f3, both of its nodes a and b are already joined. So just join f3 on both a and b's joining keys.
        let _is_standalone_rel: bool = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);

        crate::debug_print!("    📋 Creating joins for relationship...");
        let joins_before = collected_graph_joins.len();

        // ============================================================
        // Phase 4: Use PatternSchemaContext for exhaustive pattern matching
        // ============================================================

        // Get previous edge info for multi-hop detection
        // This is critical for EdgeToEdge and CoupledSameRow strategies
        // Store in locals to avoid lifetime issues with borrowed references
        let prev_edge_data: Option<(String, String, bool)> = plan_ctx
            .get_denormalized_alias_info(&left_alias)
            .filter(|(prev_alias, _, _, _)| prev_alias != &rel_alias)
            .map(|(prev_alias, is_from, _, prev_type)| {
                crate::debug_print!("    📍 MULTI-HOP detected: left '{}' was on prev edge '{}' (type={}, is_from={})",
                    left_alias, prev_alias, prev_type, is_from);
                (prev_alias.clone(), prev_type.clone(), is_from)
            });

        // Convert owned strings to borrowed references for the API
        let prev_edge_info: Option<(&str, &str, bool)> = prev_edge_data
            .as_ref()
            .map(|(alias, rel_type, is_from)| (alias.as_str(), rel_type.as_str(), *is_from));

        // Compute PatternSchemaContext for this pattern
        let mut ctx = self
            .compute_pattern_context(graph_rel, plan_ctx, graph_schema, prev_edge_info)
            .ok_or_else(|| {
                AnalyzerError::SchemaNotFound(format!(
                    "Pattern context for: left={}, rel={}, right={}",
                    left_alias, rel_alias, right_alias
                ))
            })?;

        // Register the PatternSchemaContext in PlanCtx for property resolution
        // (Phase 1A-2: Enable property_resolver.rs to access schema strategies)
        plan_ctx.register_pattern_context(rel_alias.to_string(), ctx.clone());

        // Check if node properties are actually used in the query
        // If neither node is referenced (no properties accessed downstream), we can optimize
        // by using only the relationship table without JOINing to node tables.
        // This applies whether nodes are anonymous () or named (a) - only usage matters.
        // Examples:
        //   MATCH (a)-[r:FOLLOWS]->(b) RETURN count(r)  → no node JOINs needed
        //   MATCH ()-[r:FOLLOWS]->() RETURN count(r)    → no node JOINs needed
        //   MATCH (a)-[r:FOLLOWS]->(b) RETURN a.name    → JOIN left node table for a.name
        //
        // IMPORTANT: Skip this optimization for:
        // - Variable-length paths and shortest paths (need CTEs with node JOINs)
        // - Multi-hop patterns (intermediate nodes needed for chaining JOINs)
        let is_vlp = graph_rel.variable_length.is_some();
        let is_shortest_path = graph_rel.shortest_path_mode.is_some();
        let is_first_relationship = !join_ctx.contains(&left_alias)
            && !join_ctx.contains(&right_alias)
            && join_ctx.vlp_endpoints().is_empty();

        // CRITICAL: Detect multi-hop patterns using PatternGraphMetadata
        // Multi-hop patterns like (a)-[t1]->(b)-[t2]->(c) have multiple edges in metadata.
        // Even if intermediate nodes (b) aren't in RETURN, they're needed for JOIN chaining.
        // Example: MATCH (u)-[:FOLLOWS]->(f1)-[:FOLLOWS]->(f2) RETURN f2.name
        //   - f1 is NOT referenced, but we MUST JOIN users_bench AS f1 to chain t1→f1→t2
        let is_multi_hop_pattern = pattern_metadata.edges.len() > 1;

        // Apply SingleTableScan optimization when:
        // 1. Neither node is referenced in RETURN/WHERE (unreferenced)
        // 2. OR both nodes are anonymous (no explicit label in Cypher)
        // AND:
        // - Not a variable-length path (VLP needs CTEs)
        // - Not a shortest path
        // - Not a path variable query (path needs all node properties)
        // - This is the first relationship AND it's a single-hop pattern
        //   (multi-hop needs ALL node tables for chaining, even if unreferenced)
        //
        // Anonymous nodes with explicit label: (a:User) → has_label=true, needs JOIN if referenced
        // Anonymous nodes without label: () → has_label=false, never needs JOIN for its own table
        let both_nodes_anonymous = !left_has_explicit_label && !right_has_explicit_label;
        let neither_node_referenced = !left_is_referenced && !right_is_referenced;
        let has_path_variable = graph_rel.path_variable.is_some();

        let apply_optimization = (both_nodes_anonymous || neither_node_referenced)
            && !is_vlp
            && !is_shortest_path
            && !has_path_variable  // CRITICAL: Path queries need node properties!
            && is_first_relationship
            && !is_multi_hop_pattern; // CRITICAL: Multi-hop patterns need node JOINs for chaining!

        if apply_optimization {
            crate::debug_print!("    ⚡ SingleTableScan: both_anonymous={}, neither_referenced={}, left_ref={}, right_ref={}, has_path_var={}",
                both_nodes_anonymous, neither_node_referenced, left_is_referenced, right_is_referenced, has_path_variable);
            // Override join strategy: no node JOINs needed, only relationship table
            ctx.join_strategy = JoinStrategy::SingleTableScan {
                table: rel_schema.full_table_name(),
            };
        }

        crate::debug_print!("    🔬 Using PatternSchemaContext: {}", ctx.debug_summary());

        // SPECIAL HANDLING: Optional Variable-Length Paths
        // For optional VLP, we need to create GraphJoins manually since the normal
        // join inference logic doesn't handle VLP CTEs as JOINs.
        if graph_rel.variable_length.is_some() && rel_is_optional {
            crate::debug_print!(
                "    🎯 OPTIONAL VLP: Creating GraphJoins for LEFT JOIN to VLP CTE"
            );

            // 1. Create FROM marker for anchor node (left node)
            helpers::JoinBuilder::from_marker(left_node_schema.full_table_name(), &left_alias)
                .build_and_push(collected_graph_joins);

            // 2. Create LEFT JOIN to VLP CTE
            let cte_name = format!("vlp_{}_{}", left_alias, right_alias);
            let left_id_col = left_node_schema
                .node_id
                .columns()
                .first()
                .expect("Node ID must have at least one column")
                .to_string();

            let vlp_join = Join {
                table_name: cte_name.clone(),
                table_alias: "t".to_string(), // VLP_CTE_FROM_ALIAS
                joining_on: vec![helpers::eq_condition(
                    &left_alias,
                    &left_id_col,
                    "t",
                    "start_id",
                )],
                join_type: JoinType::Left,
                pre_filter: None,
                from_id_column: Some(left_id_col),
                to_id_column: Some("start_id".to_string()),
                graph_rel: Some(Arc::new(graph_rel.clone())),
            };
            collected_graph_joins.push(vlp_join);

            crate::debug_print!(
                "    ✅ Created OPTIONAL VLP joins: FROM {} LEFT JOIN {} AS t",
                left_alias,
                cte_name
            );
            crate::debug_print!("    +- infer_graph_join EXIT\n");

            return Ok(());
        }

        let result = self.handle_graph_pattern_v2(
            &ctx,
            &left_alias,
            &rel_alias,
            &right_alias,
            &left_cte_name,
            &rel_cte_name,
            &right_cte_name,
            &left_label,
            &right_label,
            left_is_optional,
            rel_is_optional,
            right_is_optional,
            &left_node_schema,
            &right_node_schema,
            &rel_schema,
            plan_ctx,
            collected_graph_joins,
            join_ctx,
            graph_rel, // Pass graph_rel to check for path variables
        );

        let _joins_added = collected_graph_joins.len() - joins_before;
        crate::debug_print!("    📊 Added {} joins", _joins_added);
        crate::debug_print!("    📋 join_ctx after: {}", join_ctx.debug_summary());
        crate::debug_print!("    +- infer_graph_join EXIT\n");

        result
    }

    // ========================================================================
    // Cross-Branch Shared Node Detection (Phase 4)
    // ========================================================================

    // ========================================================================
    // Phase 3: Extracted Helper Methods (Breaking Up God Method)
    // ========================================================================

    /// Check if this pattern should skip JOIN inference due to variable-length path.
    ///
    /// Returns `Some(true)` if pattern should be skipped (required VLP/shortest path that needs CTE).
    /// Returns `Some(false)` if pattern should continue (fixed-length like *1, *2, *3, or optional VLP).
    /// Returns `None` if not a VLP pattern at all.
    fn should_skip_for_vlp(
        &self,
        graph_rel: &GraphRel,
        join_ctx: &mut JoinContext,
    ) -> Option<bool> {
        let spec = graph_rel.variable_length.as_ref()?;

        let is_fixed_length =
            spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

        let is_optional = graph_rel.is_optional.unwrap_or(false);

        if !is_fixed_length {
            if is_optional {
                // Optional variable-length (*1..3, *, etc.) - DON'T skip, create GraphJoins for LEFT JOIN
                crate::debug_print!(
                    "    🎯 OPTIONAL VLP: Not skipping, will create GraphJoins for LEFT JOIN for rel={}, left={}, right={}",
                    graph_rel.alias, graph_rel.left_connection, graph_rel.right_connection
                );

                let left_alias = graph_rel.left_connection.to_string();
                let right_alias = graph_rel.right_connection.to_string();
                let rel_alias = graph_rel.alias.to_string();

                // Mark VLP endpoints with proper CTE access information
                // For OPTIONAL VLP, only mark the END node as VLP endpoint
                // The START/anchor node should remain a regular table reference

                // Only mark the END node as VLP endpoint for optional VLP
                // The START/anchor node stays as regular table
                join_ctx.mark_vlp_endpoint(
                    right_alias.clone(),
                    VlpEndpointInfo {
                        position: VlpPosition::End,
                        other_endpoint_alias: left_alias.clone(),
                        rel_alias: rel_alias.clone(),
                    },
                );

                log::debug!(
                    "  🎯 OPTIONAL VLP: Marked only END endpoint '{}' for rel '{}' - START endpoint '{}' remains regular table",
                    right_alias, rel_alias, left_alias
                );
                log::debug!("  📊 JoinContext: {}", join_ctx.debug_summary());

                Some(false) // Don't skip - create GraphJoins for optional VLP
            } else {
                // Required variable-length (*1..3, *, etc.) - skip, will use CTE path
                crate::debug_print!(
                    "    🔍 SKIP: Required variable-length path detected (not fixed-length) for rel={}, left={}, right={}",
                    graph_rel.alias, graph_rel.left_connection, graph_rel.right_connection
                );

                let left_alias = graph_rel.left_connection.to_string();
                let right_alias = graph_rel.right_connection.to_string();
                let rel_alias = graph_rel.alias.to_string();

                // Mark VLP endpoints with proper CTE access information
                // This is the key fix: subsequent JOINs will now use t.start_id/t.end_id
                join_ctx.mark_vlp_endpoint(
                    left_alias.clone(),
                    VlpEndpointInfo {
                        position: VlpPosition::Start,
                        other_endpoint_alias: right_alias.clone(),
                        rel_alias: rel_alias.clone(),
                    },
                );
                join_ctx.mark_vlp_endpoint(
                    right_alias.clone(),
                    VlpEndpointInfo {
                        position: VlpPosition::End,
                        other_endpoint_alias: left_alias.clone(),
                        rel_alias: rel_alias.clone(),
                    },
                );

                log::debug!(
                    "  🎯 VLP: Marked endpoints '{}' (start) and '{}' (end) for rel '{}' - subsequent JOINs will use CTE refs",
                    left_alias, right_alias, rel_alias
                );
                log::debug!("  📊 JoinContext: {}", join_ctx.debug_summary());

                Some(true) // Skip this pattern
            }
        } else {
            // Fixed-length (*1, *2, *3) - continue to generate JOINs
            crate::debug_print!(
                "    ⚡ Fixed-length pattern (*{}) detected - will generate inline JOINs",
                spec.exact_hop_count()
                    .expect("Fixed-length pattern must have exact hop count")
            );
            Some(false) // Don't skip, process normally
        }
    }

    /// Validate node contexts and check for polymorphic $any nodes.
    /// Returns `Ok(())` to continue processing, `Err(true)` to skip pattern.
    fn validate_node_contexts(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &PlanCtx,
        _join_ctx: &mut JoinContext,
    ) -> Result<(), bool> {
        let left_alias = &graph_rel.left_connection;
        let right_alias = &graph_rel.right_connection;

        let left_ctx_opt = plan_ctx.get_table_ctx_from_alias_opt(&Some(left_alias.clone()));
        let right_ctx_opt = plan_ctx.get_table_ctx_from_alias_opt(&Some(right_alias.clone()));

        log::debug!(
            "🔍 validate_node_contexts: rel='{}', left_alias='{}' (ctx_ok={}), right_alias='{}' (ctx_ok={})",
            graph_rel.alias,
            left_alias,
            left_ctx_opt.is_ok(),
            right_alias,
            right_ctx_opt.is_ok()
        );

        // Skip if nodes truly don't exist in plan_ctx
        if left_ctx_opt.is_err() || right_ctx_opt.is_err() {
            log::warn!(
                "🔧 validate_node_contexts SKIP: Node context missing - left='{}' (ok={}), right='{}' (ok={})",
                left_alias,
                left_ctx_opt.is_ok(),
                right_alias,
                right_ctx_opt.is_ok()
            );
            crate::debug_print!("    🔍 SKIP: Node context missing entirely");
            return Err(true);
        }

        // $any polymorphic nodes: the node label is determined at runtime by a column
        // in the edge table (from_label_column/to_label_column). This is simpler than
        // the standard case — the edge table already contains the type info. Let join
        // inference proceed normally; the WHERE type_column filter handles the rest.
        //
        // Note: This assumes type_column-based filters are generated by earlier analyzer
        // passes (type_inference, pattern_resolver) and/or the render layer. If stricter
        // validation for polymorphic joins is needed (e.g., asserting that a type_column
        // filter exists for a given edge), this is the appropriate place to add it.

        Ok(())
    }

    /// Extract label information for both nodes.
    /// Returns (left_has_explicit_label, right_has_explicit_label).
    fn extract_node_labels(&self, graph_rel: &GraphRel) -> (bool, bool) {
        let left_has_explicit_label = match graph_rel.left.as_ref() {
            LogicalPlan::GraphNode(gn) => gn.label.is_some(),
            _ => true,
        };
        let right_has_explicit_label = match graph_rel.right.as_ref() {
            LogicalPlan::GraphNode(gn) => gn.label.is_some(),
            _ => true,
        };

        crate::debug_print!(
            "    🏷️ Label check: left_has_label={}, right_has_label={}",
            left_has_explicit_label,
            right_has_explicit_label
        );

        (left_has_explicit_label, right_has_explicit_label)
    }
}
