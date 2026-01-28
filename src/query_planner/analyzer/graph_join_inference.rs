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

use crate::{
    graph_catalog::{
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
        pattern_schema::{JoinStrategy, NodeAccessStrategy, PatternSchemaContext},
    },
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
            graph_context,
        },
        logical_expr::{
            Direction, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
        },
        logical_plan::{
            Filter, GraphJoins, GraphRel, Join, JoinType, LogicalPlan, Projection, ProjectionItem,
        },
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
    utils::cte_naming::generate_cte_base_name,
};

/// Tracks where a node variable appears in the query plan.
/// Used for detecting cross-branch shared nodes that require JOINs.
#[derive(Debug, Clone)]
struct NodeAppearance {
    /// The table alias to use in JOIN conditions.
    /// For regular patterns: the relationship alias (e.g., "t1")
    /// For VLP patterns: the node alias (e.g., "g") since VLP CTE replaces the relationship
    rel_alias: String,
    /// Node label (e.g., "IP", "Domain")
    node_label: String,
    /// Table where this node's data lives
    /// For regular patterns: edge table (from relationship schema)
    /// For VLP patterns: node table (from node schema)
    table_name: String,
    /// Database where the table lives
    database: String,
    /// Column name for node ID in the table
    column_name: String,
    /// Whether this is the from-side (true) or to-side (false) of the relationship
    is_from_side: bool,
    /// Whether this appearance is from a VLP (Variable-Length Path) pattern
    is_vlp: bool,
}

// ============================================================================
// Pattern Graph Metadata (Evolution toward clean conceptual model)
// ============================================================================
// These structures provide a lightweight "index" over the existing GraphRel tree,
// caching information that's currently computed repeatedly throughout the algorithm.
// This enables cleaner join inference logic without rewriting the entire system.

/// Metadata about a node in the MATCH pattern graph.
/// Cached information to avoid repeated traversals and reference checking.
#[derive(Debug, Clone)]
struct PatternNodeInfo {
    /// Node variable alias (e.g., "a", "b", "person")
    alias: String,
    /// Optional label constraint (e.g., Some("User"), None for unlabeled nodes)
    label: Option<String>,
    /// Whether this node is referenced in SELECT/WHERE/ORDER BY/etc.
    /// Cached result of is_node_referenced() to avoid repeated tree traversals.
    is_referenced: bool,
    /// How many edges (relationships) use this node.
    /// appearance_count > 1 indicates cross-branch pattern (needs JOIN between edges)
    appearance_count: usize,
    /// Whether this node has an explicit label in Cypher (e.g., (a:User) vs (a))
    /// Used for SingleTableScan optimization decisions.
    has_explicit_label: bool,
}

/// Metadata about an edge (relationship) in the MATCH pattern graph.
/// Represents a single relationship pattern like -[r:TYPE]->
#[derive(Debug, Clone)]
struct PatternEdgeInfo {
    /// Edge variable alias (e.g., "r", "follows", "t1")
    alias: String,
    /// Relationship types (e.g., ["FOLLOWS"], or ["FOLLOWS", "FRIENDS"] for [:FOLLOWS|FRIENDS])
    rel_types: Vec<String>,
    /// Source node variable (e.g., "a" in (a)-[r]->(b))
    from_node: String,
    /// Target node variable (e.g., "b" in (a)-[r]->(b))
    to_node: String,
    /// Whether this edge's properties are referenced in the query
    /// Cached to avoid repeated checks
    is_referenced: bool,
    /// Whether this is a variable-length path (e.g., *1..3, *)
    /// VLP patterns are handled by CTE generation, not regular JOINs
    is_vlp: bool,
    /// Whether this is a shortest path pattern
    /// Shortest path patterns have special handling similar to VLP
    is_shortest_path: bool,
    /// Direction: Outgoing (-[r]->), Incoming (<-[r]-), Either (-[r]-)
    direction: Direction,
    /// Whether this edge is part of an OPTIONAL MATCH
    is_optional: bool,
}

/// Complete pattern graph metadata extracted from a MATCH clause.
/// Provides a "map" view of the pattern structure to enable cleaner join inference.
#[derive(Debug, Clone, Default)]
struct PatternGraphMetadata {
    /// All nodes in the pattern, indexed by alias
    nodes: HashMap<String, PatternNodeInfo>,
    /// All edges in the pattern (in order of appearance)
    edges: Vec<PatternEdgeInfo>,
}

impl PatternGraphMetadata {
    /// Get edge metadata by alias
    fn get_edge_by_alias(&self, alias: &str) -> Option<&PatternEdgeInfo> {
        self.edges.iter().find(|e| e.alias == alias)
    }

    /// Get all edges that use a specific node (by node alias)
    fn edges_using_node(&self, node_alias: &str) -> Vec<&PatternEdgeInfo> {
        self.edges
            .iter()
            .filter(|e| e.from_node == node_alias || e.to_node == node_alias)
            .collect()
    }

    /// Check if a node appears in multiple edges (cross-branch pattern indicator)
    fn is_cross_branch_node(&self, node_alias: &str) -> bool {
        self.nodes
            .get(node_alias)
            .map(|n| n.appearance_count > 1)
            .unwrap_or(false)
    }
}

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
        let cross_branch_joins = self.generate_cross_branch_joins_from_metadata(
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
            self.generate_relationship_uniqueness_constraints(&pattern_metadata, graph_schema);

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

impl GraphJoinInference {
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
            node_info.is_referenced = Self::plan_references_alias(plan, alias);
        }
    }

    /// Phase 3: Compute which edges are referenced
    fn compute_edge_references(plan: &LogicalPlan, metadata: &mut PatternGraphMetadata) {
        for edge_info in metadata.edges.iter_mut() {
            // Check if edge alias is referenced in the plan
            edge_info.is_referenced = Self::plan_references_alias(plan, &edge_info.alias);
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
                // CTE name format: with_{sorted_aliases}_cte (no counter - render phase adds it)
                let cte_name = generate_cte_base_name(&wc.exported_aliases);

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

    /// Determines the appropriate join type based on whether the table alias
    /// is part of an OPTIONAL MATCH pattern. Returns LEFT for optional aliases,
    /// INNER for regular aliases.
    fn determine_join_type(is_optional: bool) -> JoinType {
        if is_optional {
            JoinType::Left
        } else {
            JoinType::Inner
        }
    }

    /// Resolve a schema column name to the actual column name in the target table/CTE
    ///
    /// For base tables, returns the schema column unchanged.
    /// For CTE references, looks up the exported column name.
    ///
    /// # Arguments
    /// * `schema_column` - The column name from schema (e.g., "firstName")
    /// * `table_name` - The table or CTE name (e.g., "with_p_cte_1" or "ldbc.Person")
    /// * `plan_ctx` - The planning context with CTE column mappings
    ///
    /// # Returns
    /// The resolved column name (e.g., "p_firstName" for CTE, "firstName" for base table)
    fn resolve_column(schema_column: &str, table_name: &str, plan_ctx: &PlanCtx) -> String {
        // Check if this is a CTE reference
        if plan_ctx.is_cte(table_name) {
            // Look up the exported column name from registered mappings
            if let Some(cte_column) = plan_ctx.get_cte_column(table_name, schema_column) {
                log::debug!(
                    "  ✅ Resolved CTE column: {} (schema) → {} (CTE '{}')",
                    schema_column,
                    cte_column,
                    table_name
                );
                return cte_column.to_string();
            }
        }

        // Base table or unmapped - use schema column as-is
        schema_column.to_string()
    }

    /// Deduplicate joins by table_alias
    /// When there are multiple joins for the same alias, prefer the one that:
    /// 1. References TableAlias (WITH clause alias like client_ip) over PropertyAccessExp (like src2.ip)
    /// 2. Has fewer PropertyAccessExp operands (simpler join condition)
    /// This handles the case where both infer_graph_join and cross-table extraction create joins
    /// for the same fully denormalized table.
    fn deduplicate_joins(joins: Vec<Join>) -> Vec<Join> {
        use std::collections::HashMap;
        // Use (alias, join_condition) as key to allow multiple joins to same table with different conditions
        let mut seen_joins: HashMap<(String, String), Join> = HashMap::new();

        for join in joins {
            let alias = join.table_alias.clone();

            // Create a stable key from the join condition
            let join_condition_key = format!("{:?}", join.joining_on);
            let key = (alias.clone(), join_condition_key);

            if let Some(existing) = seen_joins.get(&key) {
                // Compare joins - prefer one with TableAlias in joining_on (cross-table join)
                let new_has_table_alias = Self::join_references_table_alias(&join);
                let existing_has_table_alias = Self::join_references_table_alias(existing);

                log::debug!("🔄 deduplicate_joins: key='{:?}' has duplicate. new_has_table_alias={}, existing_has_table_alias={}",
                    key, new_has_table_alias, existing_has_table_alias);

                if new_has_table_alias && !existing_has_table_alias {
                    // Prefer the new join (it references WITH aliases)
                    log::debug!("🔄 deduplicate_joins: replacing with new join (has TableAlias)");
                    seen_joins.insert(key, join);
                }
                // Otherwise keep existing
            } else {
                seen_joins.insert(key, join);
            }
        }

        seen_joins.into_values().collect()
    }

    /// Check if a join's joining_on condition references a TableAlias (WITH clause alias)
    fn join_references_table_alias(join: &Join) -> bool {
        for condition in &join.joining_on {
            if Self::operator_application_references_table_alias(condition) {
                return true;
            }
        }
        false
    }

    /// Check if an OperatorApplication contains a TableAlias reference
    fn operator_application_references_table_alias(op_app: &OperatorApplication) -> bool {
        for operand in &op_app.operands {
            if matches!(operand, LogicalExpr::TableAlias(_)) {
                return true;
            }
            if let LogicalExpr::OperatorApplicationExp(nested) = operand {
                if Self::operator_application_references_table_alias(nested) {
                    return true;
                }
            }
        }
        false
    }

    /// Extract the right-side anchor table info from a plan
    /// For fully denormalized patterns, this finds the edge table that serves as the anchor
    /// Returns (table_name, alias) for the right-side table
    fn extract_right_table_from_plan(
        plan: &Arc<LogicalPlan>,
        _graph_schema: &GraphSchema,
    ) -> Option<(String, String)> {
        match plan.as_ref() {
            LogicalPlan::GraphRel(rel) => {
                // For GraphRel, the center ViewScan contains the edge table
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    // For denormalized schemas, use the relationship alias since that's what
                    // property mappings resolve to. The relationship alias is what the SELECT
                    // clause will use for property references on nodes that belong to this edge.
                    // This ensures consistency between JOIN alias and SELECT column aliases.
                    return Some((scan.source_table.clone(), rel.alias.clone()));
                }
                None
            }
            LogicalPlan::Projection(proj) => {
                Self::extract_right_table_from_plan(&proj.input, _graph_schema)
            }
            LogicalPlan::Filter(filter) => {
                Self::extract_right_table_from_plan(&filter.input, _graph_schema)
            }
            LogicalPlan::GraphNode(node) => {
                Self::extract_right_table_from_plan(&node.input, _graph_schema)
            }
            _ => None,
        }
    }

    /// Remap node aliases in a join condition to use the relationship alias
    /// For denormalized patterns where the filter references src2.column but we're aliasing as c
    fn remap_node_aliases_to_relationship(
        op_app: OperatorApplication,
        right_plan: &Arc<LogicalPlan>,
        target_alias: &str,
    ) -> OperatorApplication {
        // Collect all node aliases from the right-side plan that should be remapped
        let node_aliases = Self::collect_node_aliases_from_plan(right_plan);
        crate::debug_print!(
            "📦 remap_node_aliases: target_alias='{}', node_aliases={:?}",
            target_alias,
            node_aliases
        );

        // Remap operands
        let remapped_operands: Vec<LogicalExpr> = op_app
            .operands
            .iter()
            .map(|operand| Self::remap_alias_in_expr(operand.clone(), &node_aliases, target_alias))
            .collect();

        OperatorApplication {
            operator: op_app.operator,
            operands: remapped_operands,
        }
    }

    /// Collect all node aliases from a plan (left_connection, right_connection from GraphRel)
    fn collect_node_aliases_from_plan(plan: &Arc<LogicalPlan>) -> Vec<String> {
        let mut aliases = Vec::new();
        Self::collect_node_aliases_recursive(plan, &mut aliases);
        aliases
    }

    fn collect_node_aliases_recursive(plan: &Arc<LogicalPlan>, aliases: &mut Vec<String>) {
        match plan.as_ref() {
            LogicalPlan::GraphRel(rel) => {
                aliases.push(rel.left_connection.clone());
                aliases.push(rel.right_connection.clone());
                Self::collect_node_aliases_recursive(&rel.left, aliases);
                Self::collect_node_aliases_recursive(&rel.right, aliases);
            }
            LogicalPlan::GraphNode(node) => {
                aliases.push(node.alias.clone());
                Self::collect_node_aliases_recursive(&node.input, aliases);
            }
            LogicalPlan::Projection(proj) => {
                Self::collect_node_aliases_recursive(&proj.input, aliases)
            }
            LogicalPlan::Filter(filter) => {
                Self::collect_node_aliases_recursive(&filter.input, aliases)
            }
            _ => {}
        }
    }

    /// Remap table aliases in an expression
    fn remap_alias_in_expr(
        expr: LogicalExpr,
        source_aliases: &[String],
        target_alias: &str,
    ) -> LogicalExpr {
        match expr {
            LogicalExpr::PropertyAccessExp(mut prop_acc) => {
                if source_aliases.contains(&prop_acc.table_alias.0) {
                    crate::debug_print!(
                        "📦 remap_alias_in_expr: remapping '{}' -> '{}'",
                        prop_acc.table_alias.0,
                        target_alias
                    );
                    prop_acc.table_alias = TableAlias(target_alias.to_string());
                }
                LogicalExpr::PropertyAccessExp(prop_acc)
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                let remapped_operands: Vec<LogicalExpr> = op_app
                    .operands
                    .into_iter()
                    .map(|operand| Self::remap_alias_in_expr(operand, source_aliases, target_alias))
                    .collect();
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: op_app.operator,
                    operands: remapped_operands,
                })
            }
            LogicalExpr::ScalarFnCall(mut fn_call) => {
                fn_call.args = fn_call
                    .args
                    .into_iter()
                    .map(|arg| Self::remap_alias_in_expr(arg, source_aliases, target_alias))
                    .collect();
                LogicalExpr::ScalarFnCall(fn_call)
            }
            // Other expression types pass through unchanged
            other => other,
        }
    }

    /// Check if a node is actually referenced in the query (SELECT, WHERE, ORDER BY, etc.)
    /// Returns true if the node has any projections or filters, meaning it must be joined.
    fn is_node_referenced(alias: &str, plan_ctx: &PlanCtx, logical_plan: &LogicalPlan) -> bool {
        crate::debug_print!("        DEBUG: is_node_referenced('{}') called", alias);

        // Search the logical plan tree for any Projection nodes
        if Self::plan_references_alias(logical_plan, alias) {
            crate::debug_print!("        DEBUG: '{}' IS referenced in logical plan", alias);
            return true;
        }

        // Also check filters in plan_ctx
        for (_ctx_alias, table_ctx) in plan_ctx.get_alias_table_ctx_map().iter() {
            for filter in table_ctx.get_filters() {
                if Self::expr_references_alias(filter, alias) {
                    crate::debug_print!("        DEBUG: '{}' IS referenced in filters", alias);
                    return true;
                }
            }
        }

        crate::debug_print!("        DEBUG: '{}' is NOT referenced", alias);
        false
    }

    /// Recursively search a logical plan tree for references to an alias
    fn plan_references_alias(plan: &LogicalPlan, alias: &str) -> bool {
        match plan {
            LogicalPlan::Projection(proj) => {
                // Check projection items
                for item in &proj.items {
                    if Self::expr_references_alias(&item.expression, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&proj.input, alias)
            }
            LogicalPlan::GroupBy(group_by) => {
                // Check group expressions
                for expr in &group_by.expressions {
                    if Self::expr_references_alias(expr, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&group_by.input, alias)
            }
            LogicalPlan::Filter(filter) => {
                // Check filter expression
                if Self::expr_references_alias(&filter.predicate, alias) {
                    return true;
                }
                // Recurse into input
                Self::plan_references_alias(&filter.input, alias)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // Don't recurse into graph structure - just because a node appears in MATCH
                // doesn't mean it's referenced in SELECT/WHERE/etc.
                // Only check if there are filters on the relationship itself
                if let Some(where_pred) = &graph_rel.where_predicate {
                    if Self::expr_references_alias(where_pred, alias) {
                        return true;
                    }
                }
                false
            }
            LogicalPlan::GraphNode(_graph_node) => {
                // Don't recurse into graph structure nodes
                // These represent the MATCH pattern, not actual data references
                false
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                Self::plan_references_alias(&graph_joins.input, alias)
            }
            LogicalPlan::Cte(cte) => Self::plan_references_alias(&cte.input, alias),
            LogicalPlan::OrderBy(order_by) => {
                // Check order expressions
                for sort_expr in &order_by.items {
                    if Self::expr_references_alias(&sort_expr.expression, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&order_by.input, alias)
            }
            LogicalPlan::Skip(skip) => {
                // Skip doesn't have expressions, just recurse
                Self::plan_references_alias(&skip.input, alias)
            }
            LogicalPlan::Limit(limit) => {
                // Limit doesn't have expressions, just recurse
                Self::plan_references_alias(&limit.input, alias)
            }
            LogicalPlan::Union(union) => {
                // Recurse into all Union branches - Projection may be inside each branch
                union
                    .inputs
                    .iter()
                    .any(|input| Self::plan_references_alias(input, alias))
            }
            LogicalPlan::Unwind(unwind) => {
                // Check if the unwind expression or output alias reference this alias
                Self::expr_references_alias(&unwind.expression, alias)
                    || Self::plan_references_alias(&unwind.input, alias)
            }
            LogicalPlan::CartesianProduct(cp) => {
                // Recurse into both sides
                Self::plan_references_alias(&cp.left, alias)
                    || Self::plan_references_alias(&cp.right, alias)
            }
            LogicalPlan::WithClause(wc) => {
                // Check exported aliases and recurse
                Self::plan_references_alias(&wc.input, alias)
            }
            _ => false, // ViewScan, Scan, Empty, etc.
        }
    }

    /// Recursively check if an expression references a given alias
    /// This handles cases like COUNT(b) where 'b' is inside an aggregation function
    fn expr_references_alias(expr: &LogicalExpr, alias: &str) -> bool {
        match expr {
            LogicalExpr::TableAlias(table_alias) => table_alias.0 == alias,
            LogicalExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
            LogicalExpr::AggregateFnCall(agg) => {
                // Check arguments of aggregation functions (e.g., COUNT(b))
                agg.args
                    .iter()
                    .any(|arg| Self::expr_references_alias(arg, alias))
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                // Check arguments of scalar functions
                fn_call
                    .args
                    .iter()
                    .any(|arg| Self::expr_references_alias(arg, alias))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // Check operands of operators
                op.operands
                    .iter()
                    .any(|operand| Self::expr_references_alias(operand, alias))
            }
            LogicalExpr::List(list) => {
                // Check elements in lists
                list.iter()
                    .any(|item| Self::expr_references_alias(item, alias))
            }
            LogicalExpr::Case(case) => {
                // Check CASE expressions
                if let Some(expr) = &case.expr {
                    if Self::expr_references_alias(expr, alias) {
                        return true;
                    }
                }
                for (when_expr, then_expr) in &case.when_then {
                    if Self::expr_references_alias(when_expr, alias)
                        || Self::expr_references_alias(then_expr, alias)
                    {
                        return true;
                    }
                }
                if let Some(else_expr) = &case.else_expr {
                    if Self::expr_references_alias(else_expr, alias) {
                        return true;
                    }
                }
                false
            }
            // Literals, columns, parameters, etc. don't reference table aliases
            _ => false,
        }
    }

    /// Reorder JOINs so that each JOIN only references tables that are already available
    /// (either from the FROM clause or from previous JOINs in the sequence)
    fn reorder_joins_by_dependencies(
        joins: Vec<Join>,
        optional_aliases: &std::collections::HashSet<String>,
        _plan_ctx: &PlanCtx,
    ) -> (Option<String>, Vec<Join>) {
        if joins.is_empty() {
            // No joins means denormalized pattern - no anchor needed (will use relationship table)
            return (None, joins);
        }

        crate::debug_print!("\n🔄 REORDERING {} JOINS by dependencies", joins.len());

        // SPECIAL CASE: Check for FROM marker joins (empty joining_on)
        // These are explicitly marked as the FROM table by CartesianProduct cross-table handling
        // IMPORTANT: Prefer NON-OPTIONAL (Inner) joins over OPTIONAL (Left) joins as anchors
        // This ensures required patterns become FROM, not optional patterns.
        let mut from_marker_index: Option<usize> = None;
        let mut first_optional_marker_index: Option<usize> = None;
        for (i, join) in joins.iter().enumerate() {
            if join.joining_on.is_empty() {
                if join.join_type == JoinType::Inner {
                    // Prefer Inner (non-optional) as FROM
                    crate::debug_print!(
                        "  🏠 Found NON-OPTIONAL FROM marker join: '{}' (Inner)",
                        join.table_alias
                    );
                    from_marker_index = Some(i);
                    break;
                } else if first_optional_marker_index.is_none() {
                    // Track first optional marker as fallback
                    crate::debug_print!(
                        "  🏠 Found OPTIONAL marker join: '{}' (Left) - will prefer Inner if found",
                        join.table_alias
                    );
                    first_optional_marker_index = Some(i);
                }
            }
        }
        // Fall back to optional marker if no Inner marker found
        if from_marker_index.is_none() {
            from_marker_index = first_optional_marker_index;
        }

        // If we found a FROM marker, use it as anchor but STILL reorder the other joins!
        // Previously this returned early, but that skipped dependency-based reordering.
        // FIX: Extract FROM marker, reorder the rest, then prepend FROM marker.
        if let Some(idx) = from_marker_index {
            let from_marker = joins[idx].clone(); // Keep the original FROM marker (preserves table_name!)
            let from_alias = from_marker.table_alias.clone();
            crate::debug_print!(
                "  🏠 Found FROM marker '{}' (table='{}') - will reorder other joins",
                from_alias,
                from_marker.table_name
            );

            // Extract FROM marker and reorder the rest
            let mut remaining_joins: Vec<Join> = joins
                .into_iter()
                .enumerate()
                .filter(|(i, _)| *i != idx)
                .map(|(_, j)| j)
                .collect();

            // From alias is available for dependency checking
            let mut available_tables: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            available_tables.insert(from_alias.clone());

            // Reorder remaining joins
            let mut ordered_joins = Vec::new();
            let mut made_progress = true;
            while made_progress && !remaining_joins.is_empty() {
                made_progress = false;
                let mut i = 0;
                while i < remaining_joins.len() {
                    let mut referenced_tables = std::collections::HashSet::new();
                    let table_alias = remaining_joins[i].table_alias.clone();

                    for condition in &remaining_joins[i].joining_on {
                        for operand in &condition.operands {
                            Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                        }
                    }
                    referenced_tables.remove(&table_alias);

                    let all_available = referenced_tables
                        .iter()
                        .all(|t| available_tables.contains(t));
                    if all_available {
                        let join = remaining_joins.remove(i);
                        available_tables.insert(table_alias);
                        ordered_joins.push(join);
                        made_progress = true;
                    } else {
                        i += 1;
                    }
                }
            }

            // Any remaining joins (circular deps) go at the end
            ordered_joins.extend(remaining_joins);

            // Prepend the ORIGINAL FROM marker (preserves table_name!)
            let mut final_joins = vec![from_marker];
            final_joins.extend(ordered_joins);

            crate::debug_print!(
                "  🏠 Final JOIN order with FROM marker: {:?}",
                final_joins
                    .iter()
                    .map(|j| &j.table_alias)
                    .collect::<Vec<_>>()
            );
            return (Some(from_alias), final_joins);
        }

        // Collect all aliases that are being joined
        let mut join_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
        for join in &joins {
            join_aliases.insert(join.table_alias.clone());
        }

        // Find all tables referenced in JOIN conditions
        let mut referenced_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for join in &joins {
            for condition in &join.joining_on {
                for operand in &condition.operands {
                    Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                }
            }
        }

        // CRITICAL FIX: The ONLY tables that should start as "available" are those that:
        // 1. Are referenced in JOIN conditions (needed by some JOIN)
        // 2. Are NOT themselves being joined (they go in FROM clause)
        // 3. Are NOT optional (OPTIONAL MATCH tables can't be anchors - they use LEFT JOIN)
        // This is the true anchor - the table that other JOINs depend on but doesn't need a JOIN itself
        let mut available_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for table in &referenced_tables {
            if !join_aliases.contains(table) && !optional_aliases.contains(table) {
                available_tables.insert(table.clone());
                crate::debug_print!(
                    "  ✅ Found TRUE ANCHOR table (referenced but not joined, not optional): {}",
                    table
                );
            } else if !join_aliases.contains(table) && optional_aliases.contains(table) {
                crate::debug_print!(
                    "  ⚠️ Skipping optional table as anchor candidate: {}",
                    table
                );
            }
        }

        // Track if we pulled a join out to be the FROM clause (for cyclic patterns)
        let mut from_join_index: Option<usize> = None;

        // If no anchor found (all referenced tables are also being joined = cyclic pattern),
        // we need to pick a starting point and move it from JOIN list to FROM clause.
        if available_tables.is_empty() {
            crate::debug_print!("  ⚠️ No natural anchor - picking FROM table from joins...");

            // Strategy: Find a join that can start the chain
            // Priority 1: Node tables (short aliases like 'a', 'b') - they're likely to be JOIN targets
            // Priority 2: Any required table

            // First, find the best candidate for FROM clause
            for (i, join) in joins.iter().enumerate() {
                // Skip optional tables - they can't be FROM
                if optional_aliases.contains(&join.table_alias) {
                    continue;
                }

                // Prefer short aliases (likely node tables like 'a', 'b')
                let is_likely_node_table =
                    !join.table_alias.starts_with("a") || join.table_alias.len() < 5;

                if is_likely_node_table {
                    from_join_index = Some(i);
                    crate::debug_print!(
                        "  📌 Moving '{}' to FROM clause (node table)",
                        join.table_alias
                    );
                    break;
                }
            }

            // If no node table found, use first required table
            if from_join_index.is_none() {
                for (i, join) in joins.iter().enumerate() {
                    if !optional_aliases.contains(&join.table_alias) {
                        from_join_index = Some(i);
                        crate::debug_print!(
                            "  📌 Moving '{}' to FROM clause (first required)",
                            join.table_alias
                        );
                        break;
                    }
                }
            }
        }

        crate::debug_print!(
            "  🔍 Initial available tables (anchors): {:?}",
            available_tables
        );

        let mut ordered_joins = Vec::new();
        let mut remaining_joins = joins;

        // Track the FROM clause table separately (for cyclic patterns where we pick from joins)
        let mut from_clause_alias: Option<String> = None;

        // If we're pulling a join out for FROM clause, do that first
        if let Some(idx) = from_join_index {
            let from_join = remaining_joins.remove(idx);
            crate::debug_print!(
                "  🏠 Extracted '{}' for FROM clause (will NOT be in JOINs list)",
                from_join.table_alias
            );
            from_clause_alias = Some(from_join.table_alias.clone());
            available_tables.insert(from_join.table_alias.clone());
            // DON'T push to ordered_joins - the FROM table should not appear as a JOIN!
            // The anchor return value will specify the FROM table.
        }

        // Keep trying to add joins until we can't make progress
        let mut made_progress = true;
        while made_progress && !remaining_joins.is_empty() {
            made_progress = false;
            let mut i = 0;

            while i < remaining_joins.len() {
                // Check if all tables referenced by this JOIN are available
                let mut referenced_tables = std::collections::HashSet::new();
                let table_alias = remaining_joins[i].table_alias.clone();

                for condition in &remaining_joins[i].joining_on {
                    for operand in &condition.operands {
                        Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                    }
                }

                // Remove self-reference (the table being joined)
                referenced_tables.remove(&table_alias);

                // Check if all referenced tables are available
                let all_available = referenced_tables
                    .iter()
                    .all(|t| available_tables.contains(t));

                if all_available {
                    crate::debug_print!(
                        "  ? JOIN '{}' can be added (references: {:?})",
                        table_alias,
                        referenced_tables
                    );
                    // This JOIN can be added now
                    let join = remaining_joins.remove(i);
                    available_tables.insert(table_alias.clone());
                    ordered_joins.push(join);
                    made_progress = true;
                    // Don't increment i - we removed an element
                } else {
                    crate::debug_print!(
                        "  ? JOIN '{}' must wait (needs: {:?}, have: {:?})",
                        table_alias,
                        referenced_tables,
                        available_tables
                    );
                    i += 1;
                }
            }
        }

        // If there are still remaining joins, we have a circular dependency or missing anchor
        if !remaining_joins.is_empty() {
            crate::debug_print!(
                "  ??  WARNING: {} JOINs could not be ordered (circular dependency?)",
                remaining_joins.len()
            );
            // Just append them at the end
            ordered_joins.extend(remaining_joins);
        }

        crate::debug_print!(
            "  ? Final JOIN order: {:?}\n",
            ordered_joins
                .iter()
                .map(|j| &j.table_alias)
                .collect::<Vec<_>>()
        );

        // CRITICAL FIX: For cyclic patterns, we extracted a FROM table from the joins list.
        // Use that directly if available. Otherwise, compute the anchor from join conditions.
        let anchor = if let Some(from_alias) = from_clause_alias {
            // We explicitly picked this table for FROM clause
            Some(from_alias)
        } else if let Some(first_join) = ordered_joins.first() {
            // Compute anchor from first join's references
            let mut refs = std::collections::HashSet::new();
            for condition in &first_join.joining_on {
                for operand in &condition.operands {
                    Self::extract_table_refs_from_expr(operand, &mut refs);
                }
            }
            // Remove the table being joined (it shouldn't be the anchor)
            refs.remove(&first_join.table_alias);

            // Find a reference that is not being joined anywhere else (this is the anchor)
            refs.into_iter()
                .find(|r| !ordered_joins.iter().any(|j| &j.table_alias == r))
                .or_else(|| available_tables.iter().next().cloned())
        } else {
            None
        };

        crate::debug_print!("  ?? ANCHOR TABLE for FROM clause: {:?}\n", anchor);
        (anchor, ordered_joins)
    }

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
                    },
                ));

                // DEDUPLICATION: Remove duplicate joins for the same table_alias
                // When there are multiple joins for the same alias (e.g., from both infer_graph_join
                // and cross-table join extraction), keep the one that references WITH clause aliases
                // (like client_ip) rather than internal node aliases (like src2).
                let deduped_joins = Self::deduplicate_joins(collected_graph_joins.clone());

                // Reorder JOINs before creating GraphJoins to ensure proper dependency order
                let (anchor_table, reordered_joins) =
                    Self::reorder_joins_by_dependencies(deduped_joins, &optional_aliases, plan_ctx);

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
                    let combined_predicate = if where_predicates.len() == 1 {
                        where_predicates[0].clone()
                    } else {
                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: where_predicates.into_iter().cloned().collect(),
                        })
                    };
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
                            let from_marker = Join {
                                table_name: vs.source_table.clone(),
                                table_alias: gn.alias.clone(),
                                joining_on: vec![], // Empty = this is the FROM table
                                join_type: JoinType::Inner,
                                pre_filter: None,
                                from_id_column: None,
                                to_id_column: None,
                                graph_rel: None,
                            };
                            // Insert at the beginning so it becomes the anchor
                            collected_graph_joins.insert(0, from_marker);
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
                    let left_nodes = Self::collect_node_aliases_from_plan(&cp.left);
                    let right_nodes = Self::collect_node_aliases_from_plan(&cp.right);

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
                                Some((left_table, left_alias)),
                                Some((right_table, right_alias)),
                            ) = (
                                Self::extract_right_table_from_plan(&cp.left, graph_schema),
                                Self::extract_right_table_from_plan(&cp.right, graph_schema),
                            ) {
                                // Try to extract node appearances to get column names
                                // We need to find the GraphRel from each side to call extract_node_appearance
                                if let (Some(left_rel), Some(right_rel)) = (
                                    Self::find_graph_rel_in_plan(&cp.left),
                                    Self::find_graph_rel_in_plan(&cp.right),
                                ) {
                                    // Determine which side the shared node is on for each GraphRel
                                    let left_is_from = left_rel.left_connection == *shared_node;
                                    let right_is_from = right_rel.left_connection == *shared_node;

                                    // Get node appearances using existing method (via the disabled cross-branch logic)
                                    let graph_join_inference = GraphJoinInference::new();
                                    if let (Ok(left_appearance), Ok(right_appearance)) = (
                                        graph_join_inference.extract_node_appearance(
                                            shared_node,
                                            left_rel,
                                            left_is_from,
                                            plan_ctx,
                                            graph_schema,
                                        ),
                                        graph_join_inference.extract_node_appearance(
                                            shared_node,
                                            right_rel,
                                            right_is_from,
                                            plan_ctx,
                                            graph_schema,
                                        ),
                                    ) {
                                        // Generate JOIN using existing generate_cross_branch_join method
                                        let join = Join {
                                            table_name: if left_appearance.database.is_empty() {
                                                left_appearance.table_name.clone()
                                            } else {
                                                format!(
                                                    "{}.{}",
                                                    left_appearance.database,
                                                    left_appearance.table_name
                                                )
                                            },
                                            table_alias: left_appearance.rel_alias.clone(),
                                            joining_on: vec![OperatorApplication {
                                                operator: Operator::Equal,
                                                operands: vec![
                                                    LogicalExpr::PropertyAccessExp(
                                                        PropertyAccess {
                                                            table_alias: TableAlias(
                                                                right_appearance.rel_alias.clone(),
                                                            ),
                                                            column: PropertyValue::Column(
                                                                right_appearance
                                                                    .column_name
                                                                    .clone(),
                                                            ),
                                                        },
                                                    ),
                                                    LogicalExpr::PropertyAccessExp(
                                                        PropertyAccess {
                                                            table_alias: TableAlias(
                                                                left_appearance.rel_alias.clone(),
                                                            ),
                                                            column: PropertyValue::Column(
                                                                left_appearance.column_name.clone(),
                                                            ),
                                                        },
                                                    ),
                                                ],
                                            }],
                                            join_type: JoinType::Inner,
                                            pre_filter: None,
                                            from_id_column: None,
                                            to_id_column: None,
                                            graph_rel: None,
                                        };

                                        log::info!("📦 Generated JOIN for shared node '{}': {} JOIN {} ON {}.{} = {}.{}",
                                            shared_node,
                                            right_appearance.rel_alias, left_appearance.rel_alias,
                                            right_appearance.rel_alias, right_appearance.column_name,
                                            left_appearance.rel_alias, left_appearance.column_name);
                                        Self::push_join_if_not_duplicate(
                                            collected_graph_joins,
                                            join,
                                        );
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
                                Self::extract_right_table_from_plan(&cp.left, graph_schema)
                            {
                                crate::debug_print!(
                                    "📦 CartesianProduct: Left (anchor) table='{}', alias='{}'",
                                    left_table,
                                    left_alias
                                );

                                // Create a "FROM" marker join with empty joining_on
                                // This will be picked up by reorder_joins_by_dependencies as the anchor
                                let from_marker = Join {
                                    table_name: left_table.clone(),
                                    table_alias: left_alias.clone(),
                                    joining_on: vec![], // Empty = this is the FROM table
                                    join_type: JoinType::Inner,
                                    pre_filter: None,
                                    from_id_column: None,
                                    to_id_column: None,
                                    graph_rel: None,
                                };
                                Self::push_join_if_not_duplicate(
                                    collected_graph_joins,
                                    from_marker,
                                );
                                crate::debug_print!(
                                    "📦 CartesianProduct: Added FROM marker for left table"
                                );

                                // Extract the right-side table info from the join_condition
                                // The join_condition should be: left_alias.column = right_alias.column
                                if let LogicalExpr::OperatorApplicationExp(op_app) = join_cond {
                                    // Find the right-side alias and table from the right GraphRel
                                    if let Some((right_table, right_alias)) =
                                        Self::extract_right_table_from_plan(&cp.right, graph_schema)
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
                                            Self::remap_node_aliases_to_relationship(
                                                op_app.clone(),
                                                &cp.right,
                                                &right_alias,
                                            );
                                        // Also remap left-side node aliases to the left table alias
                                        remapped_join_cond =
                                            Self::remap_node_aliases_to_relationship(
                                                remapped_join_cond,
                                                &cp.left,
                                                &left_alias,
                                            );

                                        // Create a JOIN for the right-side table using the remapped join_condition
                                        let cross_join = Join {
                                            table_name: right_table,
                                            table_alias: right_alias,
                                            joining_on: vec![remapped_join_cond],
                                            join_type: if cp.is_optional {
                                                JoinType::Left
                                            } else {
                                                JoinType::Inner
                                            },
                                            pre_filter: None,
                                            from_id_column: None,
                                            to_id_column: None,
                                            graph_rel: None,
                                        };
                                        Self::push_join_if_not_duplicate(
                                            collected_graph_joins,
                                            cross_join,
                                        );
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
        let rel_types: Vec<String> = graph_rel
            .labels
            .as_ref()
            .map(|labels| labels.clone())
            .unwrap_or_default();

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

        match &ctx.join_strategy {
            // ================================================================
            // SingleTableScan: Fully denormalized - NO JOINs needed
            // ================================================================
            JoinStrategy::SingleTableScan { table } => {
                crate::debug_print!("    ✅ SingleTableScan: No JOINs needed (fully denormalized)");

                // Register both nodes as embedded on the edge for property resolution
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.left_node {
                    let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
                    plan_ctx.register_denormalized_alias(
                        left_alias.to_string(),
                        rel_alias.to_string(),
                        *is_from_node,
                        String::new(), // label not needed for property resolution
                        rel_type.clone(),
                    );
                }
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.right_node {
                    let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
                    plan_ctx.register_denormalized_alias(
                        right_alias.to_string(),
                        rel_alias.to_string(),
                        *is_from_node,
                        String::new(),
                        rel_type,
                    );
                }

                // CRITICAL FIX: Create a FROM marker join for the relationship table!
                // When optimization is applied (nodes aren't referenced), we use only the
                // relationship table without JOINing to node tables. For multi-hop patterns,
                // subsequent relationships need to know which table is the anchor/FROM clause.
                // We create a "FROM marker" join with empty joining_on to signal that this
                // table should be the FROM clause.
                //
                // Example: MATCH (a)-[r1]->(b)-[r2]->(c) RETURN count(r1)
                //   - r1 optimized: SingleTableScan (only user_follows_bench, nodes not referenced)
                //   - Creates FROM marker: Join { table: user_follows_bench, alias: r1, joining_on: [] }
                //   - r2 processing: Creates joins for b→r2→c
                //   - Reorder logic: Sees FROM marker for r1, uses it as anchor
                //   - Final SQL: FROM user_follows_bench AS r1 INNER JOIN ... (correct!)
                //
                // Without this marker, r2's joins have no anchor and pick arbitrary node as FROM.
                let from_marker = Join {
                    table_name: table.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![], // Empty = FROM table marker
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                };
                Self::push_join_if_not_duplicate(collected_graph_joins, from_marker);
                crate::debug_print!(
                    "    🏠 Added FROM marker for relationship table '{}'",
                    rel_alias
                );

                // Mark relationship as "joined" but NOT the nodes (they're embedded in rel table)
                join_ctx.insert(rel_alias.to_string());

                Ok(())
            }

            // ================================================================
            // Traditional: Standard node-edge-node JOINs
            // ================================================================
            JoinStrategy::Traditional {
                left_join_col,
                right_join_col,
            } => {
                crate::debug_print!("    🔗 Traditional: Creating node-edge-node JOINs");

                // Get node ID columns from NodeAccessStrategy
                let left_id_col = match &ctx.left_node {
                    NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                    _ => {
                        return Err(AnalyzerError::OptimizerError {
                            message: "Traditional strategy requires OwnTable nodes".to_string(),
                        })
                    }
                };
                let right_id_col = match &ctx.right_node {
                    NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                    _ => {
                        return Err(AnalyzerError::OptimizerError {
                            message: "Traditional strategy requires OwnTable nodes".to_string(),
                        })
                    }
                };

                // Determine which node is already available (anchor) to connect the edge to
                let left_available = join_ctx.contains(left_alias);
                let right_available = join_ctx.contains(right_alias);
                let is_first_relationship = !join_ctx.contains(left_alias)
                    && !join_ctx.contains(right_alias)
                    && join_ctx.vlp_endpoints().is_empty();

                crate::debug_print!("       left_available={}, right_available={}, is_first={}, left_opt={}, right_opt={}",
                    left_available, right_available, is_first_relationship, left_is_optional, right_is_optional);

                // Determine connect order based on what's available and optionality:
                // Priority order:
                // 1. If one node is already joined, connect to it first
                // 2. For OPTIONAL MATCH: non-optional node is anchor (from prior MATCH)
                // 3. Default: left node is anchor (semantic source)
                let connect_left_first = if left_available {
                    // Left is already joined, use it as anchor
                    true
                } else if right_available {
                    // Right is already joined, use it as anchor
                    false
                } else if left_is_optional && !right_is_optional {
                    // Left is optional, right is non-optional (from prior MATCH)
                    // Use right as anchor
                    false
                } else if !left_is_optional && right_is_optional {
                    // Left is non-optional, right is optional
                    // Use left as anchor
                    true
                } else {
                    // Both same optionality - always connect LEFT first for first relationship
                    // This ensures proper JOIN order: LEFT → EDGE → RIGHT
                    // (prevents referencing RIGHT in join condition before it's joined)
                    is_first_relationship
                };

                log::debug!("🔍 JOIN strategy for CONTAINER_OF: connect_left_first={}, left_alias={}, right_alias={}, is_first_relationship={}",
                    connect_left_first, left_alias, right_alias, is_first_relationship);

                if connect_left_first {
                    // Standard order: LEFT → EDGE → RIGHT
                    crate::debug_print!("       Connect order: LEFT → EDGE → RIGHT");
                    log::debug!("  📍 Connect order: LEFT → EDGE → RIGHT");

                    // If first relationship, mark left as entry point with FROM marker (empty joining_on)
                    // This prevents circular dependencies by establishing left as the anchor
                    // that other joins can reference.
                    if is_first_relationship {
                        crate::debug_print!(
                            "       LEFT '{}' is anchor - will be entry point (optional={})",
                            left_alias,
                            left_is_optional
                        );
                        log::debug!("  🎯 LEFT '{}' marked as entry point (is_first_relationship={}, left_is_optional={})", left_alias, is_first_relationship, left_is_optional);

                        // CRITICAL: Create FROM marker for the anchor node!
                        // This preserves the table name so extract_from can find it.
                        // For optional patterns, this becomes a LEFT JOIN entry point that
                        // CartesianProduct can properly handle (no forward-referencing conditions).
                        let left_table_name = Self::get_table_name_with_prefix(
                            left_cte_name,
                            left_alias,
                            left_node_schema,
                            plan_ctx,
                        );
                        let from_marker = Join {
                            table_name: left_table_name,
                            table_alias: left_alias.to_string(),
                            joining_on: vec![], // Empty = entry point marker (FROM or cross-join base)
                            join_type: Self::determine_join_type(left_is_optional),
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        };
                        Self::push_join_if_not_duplicate(collected_graph_joins, from_marker);
                        crate::debug_print!(
                            "       🏠 Added entry point marker for anchor node '{}' (join_type={:?})",
                            left_alias, Self::determine_join_type(left_is_optional)
                        );

                        join_ctx.insert(left_alias.to_string());
                    }

                    log::debug!(
                        "  🔍 Checking if LEFT '{}' needs JOIN... join_ctx={}",
                        left_alias,
                        join_ctx.debug_summary()
                    );

                    // JOIN: Left node (if not yet joined)
                    if !join_ctx.contains(left_alias) {
                        // Resolve columns for CTE references
                        let resolved_left_id =
                            Self::resolve_column(&left_id_col, left_cte_name, plan_ctx);
                        let resolved_left_join_col =
                            Self::resolve_column(left_join_col, rel_cte_name, plan_ctx);

                        // Get table name with database prefix if needed (not for CTEs)
                        let left_table_name = Self::get_table_name_with_prefix(
                            left_cte_name,
                            left_alias,
                            left_node_schema,
                            plan_ctx,
                        );

                        let left_join = Join {
                            table_name: left_table_name,
                            table_alias: left_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_id),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_join_col.clone()),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(left_is_optional),
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        };
                        Self::push_join_if_not_duplicate(collected_graph_joins, left_join);
                        join_ctx.insert(left_alias.to_string());
                    }

                    // JOIN: Edge table (connects to left via from_id)
                    // Note: resolved_left_join_col was already computed above for the left_join
                    let resolved_left_id_for_rel =
                        Self::resolve_column(&left_id_col, left_cte_name, plan_ctx);

                    // Get table name with database prefix if needed (not for CTEs)
                    let rel_table_name = Self::get_rel_table_name_with_prefix(
                        rel_cte_name,
                        rel_alias,
                        rel_schema,
                        plan_ctx,
                    );

                    log::debug!(
                        "🔍 Creating rel JOIN: rel_alias='{}', rel_cte_name='{}', rel_table_name='{}', rel_schema.table_name='{}'",
                        rel_alias, rel_cte_name, rel_table_name, rel_schema.table_name
                    );

                    let rel_join = Join {
                        table_name: rel_table_name,
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        Self::resolve_column(left_join_col, rel_cte_name, plan_ctx)
                                    ),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(left_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_id_for_rel),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(rel_is_optional),
                        pre_filter: pre_filter.clone(),
                        from_id_column: Some(rel_schema.from_id.clone()),
                        to_id_column: Some(rel_schema.to_id.clone()),
                        graph_rel: None,
                    };
                    Self::push_join_if_not_duplicate(collected_graph_joins, rel_join);
                    join_ctx.insert(rel_alias.to_string());

                    // JOIN: Right node (connects to edge via to_id)
                    log::warn!(
                        "🚨 Checking RIGHT node '{}' - join_ctx={}, contains={}",
                        right_alias,
                        join_ctx.debug_summary(),
                        join_ctx.contains(right_alias)
                    );
                    if !join_ctx.contains(right_alias) {
                        let resolved_right_id =
                            Self::resolve_column(&right_id_col, right_cte_name, plan_ctx);
                        let resolved_right_join_col =
                            Self::resolve_column(right_join_col, rel_cte_name, plan_ctx);

                        // Get table name with database prefix if needed (not for CTEs)
                        let right_table_name = Self::get_table_name_with_prefix(
                            right_cte_name,
                            right_alias,
                            right_node_schema,
                            plan_ctx,
                        );

                        let right_join = Join {
                            table_name: right_table_name,
                            table_alias: right_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_id),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_join_col),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(right_is_optional),
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        };

                        log::debug!(
                            "📌 Adding RIGHT node JOIN: {} AS {}",
                            right_cte_name,
                            right_alias
                        );
                        Self::push_join_if_not_duplicate(collected_graph_joins, right_join);
                        join_ctx.insert(right_alias.to_string());
                    } else {
                        // CRITICAL FIX: Right node is already joined - add correlation condition to edge JOIN
                        // This handles patterns like OPTIONAL MATCH (a)-[r:KNOWS]-(p) where p is already bound
                        log::debug!(
                            "🔗 RIGHT node '{}' already joined - adding correlation to edge JOIN",
                            right_alias
                        );

                        let resolved_right_id =
                            Self::resolve_column(&right_id_col, right_cte_name, plan_ctx);
                        let resolved_right_join_col =
                            Self::resolve_column(right_join_col, rel_cte_name, plan_ctx);

                        // Find the edge JOIN we just added and append the correlation condition
                        if let Some(edge_join) = collected_graph_joins
                            .iter_mut()
                            .rev() // Search from end (most recently added)
                            .find(|j| j.table_alias == rel_alias)
                        {
                            let correlation_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_join_col),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_id),
                                    }),
                                ],
                            };
                            edge_join.joining_on.push(correlation_condition);
                            log::debug!("✓ Added correlation condition to edge JOIN '{}': connects to already-bound '{}'", rel_alias, right_alias);
                        }
                    }
                } else {
                    // Reverse order: RIGHT → EDGE → LEFT (right is available, connect to it first)
                    crate::debug_print!(
                        "       Connect order: RIGHT → EDGE → LEFT (right already available)"
                    );
                    log::debug!(
                        "  📍 Connect order: RIGHT → EDGE → LEFT (right already available)"
                    );

                    // Resolve columns for CTE references
                    let resolved_right_join_col =
                        Self::resolve_column(right_join_col, rel_cte_name, plan_ctx);
                    let resolved_right_id =
                        Self::resolve_column(&right_id_col, right_cte_name, plan_ctx);

                    // JOIN: Edge table (connects to RIGHT via to_id)
                    let rel_join = Join {
                        table_name: rel_cte_name.to_string(),
                        table_alias: rel_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_join_col),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(right_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_right_id),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(rel_is_optional),
                        pre_filter: pre_filter.clone(),
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    };
                    Self::push_join_if_not_duplicate(collected_graph_joins, rel_join);
                    join_ctx.insert(rel_alias.to_string());

                    // JOIN: Left node (connects to edge via from_id)
                    if !join_ctx.contains(left_alias) {
                        // Resolve columns for CTE references
                        let resolved_left_id =
                            Self::resolve_column(&left_id_col, left_cte_name, plan_ctx);
                        let resolved_left_join_col =
                            Self::resolve_column(left_join_col, rel_cte_name, plan_ctx);

                        log::debug!(
                            "🔧 Creating LEFT node JOIN: {} AS {} (not in join_ctx: {})",
                            left_cte_name,
                            left_alias,
                            join_ctx.debug_summary()
                        );

                        let left_join = Join {
                            table_name: left_cte_name.to_string(),
                            table_alias: left_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_id),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_join_col),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(left_is_optional),
                            pre_filter: None,
                            from_id_column: None,
                            to_id_column: None,
                            graph_rel: None,
                        };

                        log::debug!(
                            "📌 Adding LEFT node JOIN: {} AS {}",
                            left_cte_name,
                            left_alias
                        );
                        Self::push_join_if_not_duplicate(collected_graph_joins, left_join);
                        join_ctx.insert(left_alias.to_string());
                    } else {
                        // CRITICAL FIX: Left node is already joined - add correlation condition to edge JOIN
                        // This handles patterns where LEFT node is already bound from a previous MATCH
                        log::debug!(
                            "🔗 LEFT node '{}' already joined - adding correlation to edge JOIN",
                            left_alias
                        );

                        let resolved_left_id =
                            Self::resolve_column(&left_id_col, left_cte_name, plan_ctx);
                        let resolved_left_join_col =
                            Self::resolve_column(left_join_col, rel_cte_name, plan_ctx);

                        // Find the edge JOIN we just added and append the correlation condition
                        if let Some(edge_join) = collected_graph_joins
                            .iter_mut()
                            .rev() // Search from end (most recently added)
                            .find(|j| j.table_alias == rel_alias)
                        {
                            let correlation_condition = OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_join_col),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_left_id),
                                    }),
                                ],
                            };
                            edge_join.joining_on.push(correlation_condition);
                            log::debug!("✓ Added correlation condition to edge JOIN '{}': connects to already-bound '{}'", rel_alias, left_alias);
                        }
                    }
                }

                Ok(())
            }

            // ================================================================
            // MixedAccess: One node embedded, one requires JOIN
            // ================================================================
            JoinStrategy::MixedAccess {
                joined_node,
                join_col,
            } => {
                use crate::graph_catalog::pattern_schema::NodePosition;

                crate::debug_print!("    🔀 MixedAccess: {:?} node requires JOIN", joined_node);

                // Register the embedded node for property resolution
                let (embedded_alias, embedded_is_from) = match joined_node {
                    NodePosition::Left => {
                        // Right is embedded
                        if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } =
                            &ctx.right_node
                        {
                            (right_alias, *is_from_node)
                        } else {
                            return Err(AnalyzerError::OptimizerError {
                                message: "MixedAccess but right node not embedded".to_string(),
                            });
                        }
                    }
                    NodePosition::Right => {
                        // Left is embedded
                        if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } =
                            &ctx.left_node
                        {
                            (left_alias, *is_from_node)
                        } else {
                            return Err(AnalyzerError::OptimizerError {
                                message: "MixedAccess but left node not embedded".to_string(),
                            });
                        }
                    }
                };

                let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
                plan_ctx.register_denormalized_alias(
                    embedded_alias.to_string(),
                    rel_alias.to_string(),
                    embedded_is_from,
                    String::new(),
                    rel_type,
                );
                join_ctx.insert(embedded_alias.to_string());

                // Join the relationship table
                // The join connects to the non-embedded node
                let (join_node_alias, join_node_cte, join_node_id_col, join_node_optional) =
                    match joined_node {
                        NodePosition::Left => {
                            let id = match &ctx.left_node {
                                NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                                _ => {
                                    return Err(AnalyzerError::OptimizerError {
                                        message: "MixedAccess joined node must be OwnTable"
                                            .to_string(),
                                    })
                                }
                            };
                            (left_alias, left_cte_name, id, left_is_optional)
                        }
                        NodePosition::Right => {
                            let id = match &ctx.right_node {
                                NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                                _ => {
                                    return Err(AnalyzerError::OptimizerError {
                                        message: "MixedAccess joined node must be OwnTable"
                                            .to_string(),
                                    })
                                }
                            };
                            (right_alias, right_cte_name, id, right_is_optional)
                        }
                    };

                // Determine anchor
                let is_first_relationship =
                    !join_ctx.contains(join_node_alias) && join_ctx.vlp_endpoints().is_empty();
                let node_is_anchor = is_first_relationship && !join_node_optional;

                if node_is_anchor {
                    crate::debug_print!(
                        "       {:?} node '{}' is anchor",
                        joined_node,
                        join_node_alias
                    );
                    join_ctx.insert(join_node_alias.to_string());
                }

                // JOIN: Relationship to non-embedded node
                if !join_ctx.contains(join_node_alias) {
                    // Resolve columns for CTE references
                    let resolved_node_id =
                        Self::resolve_column(&join_node_id_col, join_node_cte, plan_ctx);
                    let resolved_join_col = Self::resolve_column(join_col, rel_cte_name, plan_ctx);

                    // Get table name with database prefix if needed (not for CTEs)
                    // Determine which schema to use based on joined_node position
                    let join_node_schema = match joined_node {
                        NodePosition::Left => left_node_schema,
                        NodePosition::Right => right_node_schema,
                    };
                    let join_table_name = Self::get_table_name_with_prefix(
                        join_node_cte,
                        join_node_alias,
                        join_node_schema,
                        plan_ctx,
                    );

                    let node_join = Join {
                        table_name: join_table_name,
                        table_alias: join_node_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(join_node_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_node_id.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(resolved_join_col.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(join_node_optional),
                        pre_filter: None,
                        from_id_column: None,
                        to_id_column: None,
                        graph_rel: None,
                    };
                    Self::push_join_if_not_duplicate(collected_graph_joins, node_join);
                    join_ctx.insert(join_node_alias.to_string());
                }

                // JOIN: Relationship table itself
                // Note: resolved_join_col and resolved_node_id already computed above
                let resolved_node_id_for_rel =
                    Self::resolve_column(&join_node_id_col, join_node_cte, plan_ctx);
                let resolved_join_col_for_rel =
                    Self::resolve_column(join_col, rel_cte_name, plan_ctx);

                // Get table name with database prefix if needed (not for CTEs)
                let rel_table_name = Self::get_rel_table_name_with_prefix(
                    rel_cte_name,
                    rel_alias,
                    rel_schema,
                    plan_ctx,
                );

                let rel_join = Join {
                    table_name: rel_table_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        resolved_join_col_for_rel,
                                    ),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(join_node_alias.to_string()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        resolved_node_id_for_rel,
                                    ),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter,
                    from_id_column: Some(rel_schema.from_id.clone()),
                    to_id_column: Some(rel_schema.to_id.clone()),
                    graph_rel: None,
                };
                Self::push_join_if_not_duplicate(collected_graph_joins, rel_join);
                join_ctx.insert(rel_alias.to_string());

                Ok(())
            }

            // ================================================================
            // EdgeToEdge: Multi-hop denormalized (edge-to-edge JOIN)
            // ================================================================
            JoinStrategy::EdgeToEdge {
                prev_edge_alias,
                prev_edge_col,
                curr_edge_col,
            } => {
                crate::debug_print!("    ⛓️ EdgeToEdge: Multi-hop denormalized JOIN");
                crate::debug_print!(
                    "       {}.{} = {}.{}",
                    prev_edge_alias,
                    prev_edge_col,
                    rel_alias,
                    curr_edge_col
                );

                // Register nodes as embedded
                let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.left_node {
                    plan_ctx.register_denormalized_alias(
                        left_alias.to_string(),
                        rel_alias.to_string(),
                        *is_from_node,
                        String::new(),
                        rel_type.clone(),
                    );
                }
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.right_node {
                    plan_ctx.register_denormalized_alias(
                        right_alias.to_string(),
                        rel_alias.to_string(),
                        *is_from_node,
                        String::new(),
                        rel_type,
                    );
                }

                // JOIN: Current edge to previous edge
                // Resolve curr_edge_col with current edge's CTE name
                let resolved_curr_edge_col =
                    Self::resolve_column(curr_edge_col, rel_cte_name, plan_ctx);

                // For prev_edge_col, try to get the previous edge's table name from plan_ctx
                // If it's a CTE reference, resolve the column; otherwise use as-is
                let prev_edge_table = plan_ctx
                    .get_table_ctx_from_alias_opt(&Some(prev_edge_alias.clone()))
                    .ok()
                    .and_then(|ctx| ctx.get_cte_name().map(|s| s.as_str()))
                    .unwrap_or(prev_edge_alias); // Fallback to alias if not a CTE
                let resolved_prev_edge_col =
                    Self::resolve_column(prev_edge_col, prev_edge_table, plan_ctx);

                let edge_join = Join {
                    table_name: rel_cte_name.to_string(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        resolved_curr_edge_col,
                                    ),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_edge_alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        resolved_prev_edge_col,
                                    ),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter,
                    from_id_column: None,
                    to_id_column: None,
                    graph_rel: None,
                };
                Self::push_join_if_not_duplicate(collected_graph_joins, edge_join);

                // Mark all as joined
                join_ctx.insert(left_alias.to_string());
                join_ctx.insert(rel_alias.to_string());
                join_ctx.insert(right_alias.to_string());

                Ok(())
            }

            // ================================================================
            // CoupledSameRow: Same physical row, no additional JOIN
            // ================================================================
            JoinStrategy::CoupledSameRow { unified_alias } => {
                crate::debug_print!("    🔄 CoupledSameRow: Unifying with '{}'", unified_alias);

                // Both edges read from the same row - just unify aliases
                // The previous edge already created the FROM/JOIN, this one shares it

                // Register property resolution to use unified alias
                let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.left_node {
                    plan_ctx.register_denormalized_alias(
                        left_alias.to_string(),
                        unified_alias.clone(),
                        *is_from_node,
                        String::new(),
                        rel_type.clone(),
                    );
                }
                if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = &ctx.right_node {
                    plan_ctx.register_denormalized_alias(
                        right_alias.to_string(),
                        unified_alias.clone(),
                        *is_from_node,
                        String::new(),
                        rel_type,
                    );
                }

                // Mark all as joined (they share the unified alias's table)
                join_ctx.insert(left_alias.to_string());
                join_ctx.insert(rel_alias.to_string());
                join_ctx.insert(right_alias.to_string());

                Ok(())
            }

            // ================================================================
            // FkEdgeJoin: Edge table IS one of the node tables (FK pattern)
            // ================================================================
            JoinStrategy::FkEdgeJoin {
                from_id,
                to_id,
                join_side,
                is_self_referencing: _is_self_referencing,
            } => {
                use crate::graph_catalog::pattern_schema::NodePosition;

                crate::debug_print!(
                    "    🔑 FkEdgeJoin: join_side={:?}, self_ref={}",
                    join_side,
                    _is_self_referencing
                );

                // FK-edge pattern: edge table IS one of the node tables
                // We only need ONE join (to the node that ISN'T the edge table)
                //
                // join_side=Left: edge IS right node table
                //   Example: (u:User)-[:PLACED]->(o:Order) where orders IS the edge
                //   Right (o/orders) is anchor, JOIN left (u/users)
                //   JOIN condition: orders.from_id = users.id  ->  o.user_id = u.id
                //
                // join_side=Right: edge IS left node table
                //   Example: (o:Order)-[:PLACED_BY]->(c:Customer) where orders IS the edge
                //   Left (o/orders) is anchor, JOIN right (c/customers)
                //   JOIN condition: customers.id = orders.to_id  ->  c.id = o.customer_id

                let is_first_relationship = !join_ctx.contains(left_alias)
                    && !join_ctx.contains(right_alias)
                    && join_ctx.vlp_endpoints().is_empty();

                // Get node ID columns
                let left_id_col = match &ctx.left_node {
                    NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                    _ => from_id.clone(),
                };
                let right_id_col = match &ctx.right_node {
                    NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                    _ => to_id.clone(),
                };

                match join_side {
                    NodePosition::Left => {
                        // Edge IS the right/to_node table
                        // Right node is the anchor, JOIN left node
                        let right_is_anchor = is_first_relationship && !right_is_optional;
                        if right_is_anchor {
                            crate::debug_print!(
                                "       RIGHT '{}' is anchor (IS edge table)",
                                right_alias
                            );
                            join_ctx.insert(right_alias.to_string());
                        }

                        // Edge conceptually lives on right node's table
                        join_ctx.insert(rel_alias.to_string());

                        // CRITICAL FIX: For VLP + chained FK-edge patterns
                        // When left is already joined via VLP CTE, we need to:
                        // 1. JOIN the right/edge table (posts_bench) to make 'p' accessible
                        // 2. The join should connect edge.from_id to the VLP end_id
                        //
                        // Example: MATCH (u1)-[:FOLLOWS*1..2]-(u2:User)-[:AUTHORED]->(p:Post)
                        // - VLP marks u2 as joined (accessed via t.end_id)
                        // - FK-edge has left=u2, right=p (edge IS posts_bench)
                        // - We need: JOIN posts_bench AS p ON p.author_id = t.end_id
                        let left_already_joined = join_ctx.contains(left_alias);
                        let right_needs_join = !join_ctx.contains(right_alias);

                        log::debug!(
                            "🔑 FkEdgeJoin(Left): left='{}' already_joined={}, right='{}' needs_join={}",
                            left_alias, left_already_joined, right_alias, right_needs_join
                        );

                        if left_already_joined && right_needs_join {
                            // LEFT is already joined (e.g., from VLP) but RIGHT needs to be accessed
                            // Create JOIN for the edge/right table
                            log::info!(
                                "🔧 VLP+FK-edge: Creating JOIN for right/edge '{}' (left '{}' already joined)",
                                right_alias, left_alias
                            );

                            // Get the proper table name for the right/edge
                            let right_table_name = Self::get_table_name_with_prefix(
                                right_cte_name,
                                right_alias,
                                right_node_schema,
                                plan_ctx,
                            );

                            // CRITICAL FIX: Use VLP-aware join reference for left alias
                            // If left_alias is a VLP endpoint, this returns ("t", "end_id")
                            // Otherwise, it returns (left_alias, left_id_col) unchanged
                            let (join_table_alias, join_column) =
                                plan_ctx.get_vlp_join_reference(left_alias, &left_id_col);

                            log::debug!(
                                "🔑 VLP JOIN reference for '{}': ({}, {})",
                                left_alias,
                                join_table_alias,
                                join_column
                            );

                            let right_join = Join {
                                table_name: right_table_name,
                                table_alias: right_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(right_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(from_id.clone()),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(join_table_alias),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(join_column),
                                        }),
                                    ],
                                }],
                                join_type: Self::determine_join_type(right_is_optional),
                                pre_filter: pre_filter.clone(),
                                from_id_column: Some(from_id.clone()),
                                to_id_column: Some(to_id.clone()),
                                graph_rel: None,
                            };
                            Self::push_join_if_not_duplicate(collected_graph_joins, right_join);
                            join_ctx.insert(right_alias.to_string());
                        } else if !left_already_joined {
                            // Standard case: JOIN left node to the already-anchored right/edge table
                            // JOIN left: left.id = right.from_id (right table has the FK column)
                            crate::debug_print!(
                                "       JOIN: {}.{} = {}.{}",
                                left_alias,
                                left_id_col,
                                right_alias,
                                from_id
                            );
                            let left_join = Join {
                                table_name: left_cte_name.to_string(),
                                table_alias: left_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(left_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_id_col),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(right_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(from_id.clone()),
                                        }),
                                    ],
                                }],
                                join_type: Self::determine_join_type(left_is_optional),
                                pre_filter: None,
                                from_id_column: Some(from_id.clone()),
                                to_id_column: Some(to_id.clone()),
                                graph_rel: None,
                            };
                            Self::push_join_if_not_duplicate(collected_graph_joins, left_join);
                            join_ctx.insert(left_alias.to_string());
                        }
                    }
                    NodePosition::Right => {
                        // Edge IS the left/from_node table
                        // Left node is the anchor, JOIN right node
                        let left_is_anchor = is_first_relationship && !left_is_optional;
                        if left_is_anchor {
                            crate::debug_print!(
                                "       LEFT '{}' is anchor (IS edge table)",
                                left_alias
                            );
                            join_ctx.insert(left_alias.to_string());
                        }

                        // Edge conceptually lives on left node's table
                        join_ctx.insert(rel_alias.to_string());

                        // CRITICAL FIX: For VLP + chained FK-edge patterns (symmetric to NodePosition::Left)
                        let right_already_joined = join_ctx.contains(right_alias);
                        let left_needs_join = !join_ctx.contains(left_alias);

                        log::debug!(
                            "🔑 FkEdgeJoin(Right): left='{}' needs_join={}, right='{}' already_joined={}",
                            left_alias, left_needs_join, right_alias, right_already_joined
                        );

                        if right_already_joined && left_needs_join {
                            // RIGHT is already joined (e.g., from VLP) but LEFT/edge needs to be accessed
                            log::info!(
                                "🔧 VLP+FK-edge: Creating JOIN for left/edge '{}' (right '{}' already joined)",
                                left_alias, right_alias
                            );

                            let left_table_name = Self::get_table_name_with_prefix(
                                left_cte_name,
                                left_alias,
                                left_node_schema,
                                plan_ctx,
                            );

                            // CRITICAL FIX: Use VLP-aware join reference for right alias
                            // If right_alias is a VLP endpoint, this returns ("t", "start_id" or "end_id")
                            // Otherwise, it returns (right_alias, right_id_col) unchanged
                            let (join_table_alias, join_column) =
                                plan_ctx.get_vlp_join_reference(right_alias, &right_id_col);

                            log::debug!(
                                "🔑 VLP JOIN reference for '{}': ({}, {})",
                                right_alias,
                                join_table_alias,
                                join_column
                            );

                            let left_join = Join {
                                table_name: left_table_name,
                                table_alias: left_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(left_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(to_id.clone()),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(join_table_alias),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(join_column),
                                        }),
                                    ],
                                }],
                                join_type: Self::determine_join_type(left_is_optional),
                                pre_filter: pre_filter.clone(),
                                from_id_column: Some(from_id.clone()),
                                to_id_column: Some(to_id.clone()),
                                graph_rel: None,
                            };
                            Self::push_join_if_not_duplicate(collected_graph_joins, left_join);
                            join_ctx.insert(left_alias.to_string());
                        } else if !right_already_joined {
                            // Standard case: JOIN right node to the already-anchored left/edge table
                            // JOIN right: right.id = left.to_id (left table has the FK column)
                            crate::debug_print!(
                                "       JOIN: {}.{} = {}.{}",
                                right_alias,
                                right_id_col,
                                left_alias,
                                to_id
                            );
                            let right_join = Join {
                                table_name: right_cte_name.to_string(),
                                table_alias: right_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(right_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_id_col),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(left_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(to_id.clone()),
                                        }),
                                    ],
                                }],
                                join_type: Self::determine_join_type(right_is_optional),
                                pre_filter: None,
                                from_id_column: Some(from_id.clone()),
                                to_id_column: Some(to_id.clone()),
                                graph_rel: None,
                            };
                            Self::push_join_if_not_duplicate(collected_graph_joins, right_join);
                            join_ctx.insert(right_alias.to_string());
                        }
                    }
                }

                Ok(())
            }
        }
    }

    /// Add a JOIN to the collection, but only if it's not a duplicate.
    /// Duplicates are detected by comparing table_alias (which must be unique).
    fn push_join_if_not_duplicate(collected_graph_joins: &mut Vec<Join>, new_join: Join) {
        // Check if this alias already exists
        if collected_graph_joins
            .iter()
            .any(|j| j.table_alias == new_join.table_alias)
        {
            log::debug!(
                "   ⏭️  Skipping duplicate JOIN: {} AS {} (already in collection)",
                new_join.table_name,
                new_join.table_alias
            );
            return;
        }

        log::debug!(
            "   ✅ Adding JOIN: {} AS {}",
            new_join.table_name,
            new_join.table_alias
        );
        collected_graph_joins.push(new_join);
    }

    fn infer_graph_join(
        &self,
        graph_rel: &GraphRel,
        root_plan: Arc<LogicalPlan>,
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

        // Extract all necessary data from graph_context BEFORE passing plan_ctx mutably
        let (
            left_alias_str,
            rel_alias_str,
            right_alias_str,
            left_node_id_column,
            right_node_id_column,
            left_label,
            right_label,
            rel_labels,
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
        let is_standalone_rel: bool = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);

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
        // - This is the first relationship AND it's a single-hop pattern
        //   (multi-hop needs ALL node tables for chaining, even if unreferenced)
        //
        // Anonymous nodes with explicit label: (a:User) → has_label=true, needs JOIN if referenced
        // Anonymous nodes without label: () → has_label=false, never needs JOIN for its own table
        let both_nodes_anonymous = !left_has_explicit_label && !right_has_explicit_label;
        let neither_node_referenced = !left_is_referenced && !right_is_referenced;

        let apply_optimization = (both_nodes_anonymous || neither_node_referenced)
            && !is_vlp
            && !is_shortest_path
            && is_first_relationship
            && !is_multi_hop_pattern; // CRITICAL: Multi-hop patterns need node JOINs for chaining!

        if apply_optimization {
            crate::debug_print!("    ⚡ SingleTableScan: both_anonymous={}, neither_referenced={}, left_ref={}, right_ref={}",
                both_nodes_anonymous, neither_node_referenced, left_is_referenced, right_is_referenced);
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
            let anchor_join = Join {
                table_name: left_node_schema.full_table_name(),
                table_alias: left_alias.clone(),
                joining_on: vec![], // Empty = FROM marker
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            };
            collected_graph_joins.push(anchor_join);

            // 2. Create LEFT JOIN to VLP CTE
            let cte_name = format!("vlp_{}_{}", left_alias, right_alias);
            let vlp_join = Join {
                table_name: cte_name.clone(),
                table_alias: "t".to_string(), // VLP_CTE_FROM_ALIAS
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(left_alias.clone()),
                            column: PropertyValue::Column(
                                left_node_schema
                                    .node_id
                                    .columns()
                                    .first()
                                    .expect("Node ID must have at least one column")
                                    .to_string(),
                            ),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("t".to_string()),
                            column: PropertyValue::Column("start_id".to_string()),
                        }),
                    ],
                }],
                join_type: JoinType::Left,
                pre_filter: None,
                from_id_column: Some(
                    left_node_schema
                        .node_id
                        .columns()
                        .first()
                        .expect("Node ID must have at least one column")
                        .to_string(),
                ),
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
                // This is needed for property resolution and subsequent JOINs
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
                    "  🎯 OPTIONAL VLP: Marked endpoints '{}' (start) and '{}' (end) for rel '{}' - will create GraphJoins",
                    left_alias, right_alias, rel_alias
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
        join_ctx: &mut JoinContext,
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

        // Check for $any polymorphic nodes
        let left_is_polymorphic_any = if let Ok(left_ctx) = &left_ctx_opt {
            if let Ok(left_label) = left_ctx.get_label_str() {
                left_label == "$any"
            } else {
                false
            }
        } else {
            false
        };

        let right_is_polymorphic_any = if let Ok(right_ctx) = &right_ctx_opt {
            if let Ok(right_label) = right_ctx.get_label_str() {
                crate::debug_print!("    🔍 DEBUG: right_label = '{}'", right_label);
                right_label == "$any"
            } else {
                crate::debug_print!("    🔍 DEBUG: right_ctx.get_label_str() failed");
                false
            }
        } else {
            crate::debug_print!("    🔍 DEBUG: right_ctx_opt is Err");
            false
        };

        crate::debug_print!(
            "    🔍 DEBUG: right_is_polymorphic_any = {}",
            right_is_polymorphic_any
        );

        // Skip if LEFT is $any (nothing to join from)
        if left_is_polymorphic_any {
            crate::debug_print!("    🚫 SKIP: Polymorphic $any left node - nothing to join from");
            return Err(true);
        }

        // Skip if RIGHT is $any (CTE will handle)
        if right_is_polymorphic_any {
            crate::debug_print!(
                "    🎯 SKIP: Polymorphic $any right node - CTE will handle relationship join"
            );
            join_ctx.insert(graph_rel.alias.clone());
            return Err(true);
        }

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

    // ========================================================================
    // Phase 4: Relationship Uniqueness Constraints
    // ========================================================================

    /// Generate relationship uniqueness constraints for multi-hop patterns.
    ///
    /// Prevents the same physical relationship from being traversed multiple times,
    /// which can happen with bidirectional edges: (a)-[r1]-(b)-[r2]-(c)
    ///
    /// Generates WHERE clauses like: r1.id != r2.id or composite checks for multi-column IDs.
    fn generate_relationship_uniqueness_constraints(
        &self,
        pattern_metadata: &PatternGraphMetadata,
        graph_schema: &GraphSchema,
    ) -> Vec<LogicalExpr> {
        let mut constraints = Vec::new();

        // Only generate constraints if we have 2+ relationships
        if pattern_metadata.edges.len() < 2 {
            return constraints;
        }

        crate::debug_print!(
            "🔐 Phase 4: Generating relationship uniqueness constraints for {} edges",
            pattern_metadata.edges.len()
        );

        // For each pair of edges, generate r_i.id != r_j.id constraint
        for i in 0..pattern_metadata.edges.len() {
            for j in (i + 1)..pattern_metadata.edges.len() {
                let edge1 = &pattern_metadata.edges[i];
                let edge2 = &pattern_metadata.edges[j];

                // Skip if either edge is VLP (handled differently in CTE)
                if edge1.is_vlp || edge2.is_vlp {
                    continue;
                }

                // Get relationship schemas to determine edge ID columns
                let rel1_schema = match graph_schema.get_rel_schema(&edge1.rel_types[0]) {
                    Ok(schema) => schema,
                    Err(_) => continue, // Skip if schema not found
                };
                let rel2_schema = match graph_schema.get_rel_schema(&edge2.rel_types[0]) {
                    Ok(schema) => schema,
                    Err(_) => continue,
                };

                // Determine edge ID columns (use edge_id if specified, else [from_id, to_id])
                let edge1_id_cols = rel1_schema
                    .edge_id
                    .as_ref()
                    .map(|id| id.columns())
                    .unwrap_or_else(|| {
                        vec![rel1_schema.from_id.as_str(), rel1_schema.to_id.as_str()]
                    });
                let edge2_id_cols = rel2_schema
                    .edge_id
                    .as_ref()
                    .map(|id| id.columns())
                    .unwrap_or_else(|| {
                        vec![rel2_schema.from_id.as_str(), rel2_schema.to_id.as_str()]
                    });

                // Generate inequality constraint
                // For single column: r1.id != r2.id
                // For composite: (r1.col1 != r2.col1) OR (r1.col2 != r2.col2) OR ...
                let constraint = if edge1_id_cols.len() == 1 && edge2_id_cols.len() == 1 {
                    // Simple case: single column ID
                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::NotEqual,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(edge1.alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        edge1_id_cols[0].to_string(),
                                    ),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(edge2.alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        edge2_id_cols[0].to_string(),
                                    ),
                            }),
                        ],
                    })
                } else {
                    // Composite case: (r1.col1, r1.col2) != (r2.col1, r2.col2)
                    // SQL: (r1.col1 != r2.col1) OR (r1.col2 != r2.col2) OR ...
                    let mut or_operands = Vec::new();
                    for (col1, col2) in edge1_id_cols.iter().zip(edge2_id_cols.iter()) {
                        or_operands.push(LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::NotEqual,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(edge1.alias.clone()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        col1.to_string()
                                    ),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(edge2.alias.clone()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        col2.to_string()
                                    ),
                                }),
                            ],
                        }));
                    }

                    // Combine with OR
                    if or_operands.len() == 1 {
                        or_operands
                            .into_iter()
                            .next()
                            .expect("Vector with len==1 must have element")
                    } else {
                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Or,
                            operands: or_operands,
                        })
                    }
                };

                crate::debug_print!(
                    "   🔐 Adding uniqueness constraint: {} != {}",
                    edge1.alias,
                    edge2.alias
                );
                constraints.push(constraint);
            }
        }

        crate::debug_print!("✅ Generated {} uniqueness constraints", constraints.len());
        constraints
    }

    // ========================================================================
    // Phase 2: Simplified Cross-Branch Detection Using Metadata
    // ========================================================================

    /// Generate cross-branch joins using pattern metadata (Phase 2 - simplified approach).
    /// Uses appearance_count from metadata instead of building NodeAppearance HashMap.
    ///
    /// **Key Insight**: Cross-branch patterns have a node in the SAME ROLE (from/to) in multiple edges.
    /// - Linear: `(a)-[:R1]->(b)-[:R2]->(c)` - 'b' is to_node of R1, from_node of R2 (chain) → NO cross-branch
    /// - Comma: `(a)-[:R1]->(b), (a)-[:R2]->(c)` - 'a' is from_node in BOTH (branches) → YES cross-branch
    fn generate_cross_branch_joins_from_metadata(
        &self,
        pattern_metadata: &PatternGraphMetadata,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Vec<Join>> {
        let mut joins = Vec::new();

        // Find all nodes that appear in multiple edges (potential cross-branch candidates)
        for (node_alias, node_info) in &pattern_metadata.nodes {
            if node_info.appearance_count <= 1 {
                continue; // Not a cross-branch node
            }

            // CRITICAL FIX: If the shared node IS REFERENCED, skip cross-branch join!
            // When the node is referenced (used in RETURN/WHERE), we'll JOIN the node table,
            // which provides the connection between edges. The cross-branch join would be
            // redundant and can create circular dependencies.
            // Cross-branch joins are only needed when we optimize away the node table JOIN.
            if node_info.is_referenced {
                log::debug!("🔍 Skipping cross-branch for '{}' - node IS REFERENCED (node table JOIN provides connection)",
                    node_alias);
                continue;
            }

            log::debug!(
                "🔍 Potential cross-branch node '{}' appears in {} edges (NOT referenced)",
                node_alias,
                node_info.appearance_count
            );

            // Get all edges that use this node
            let edges = pattern_metadata.edges_using_node(node_alias);
            if edges.len() < 2 {
                continue; // Need at least 2 edges to generate cross-branch join
            }

            // Group edges by (table_name, role)
            // Cross-branch patterns have edges with SAME ROLE in DIFFERENT tables
            // Linear chains have edges with DIFFERENT ROLES (to_node → from_node)
            let mut from_edges: HashMap<String, Vec<&PatternEdgeInfo>> = HashMap::new();
            let mut to_edges: HashMap<String, Vec<&PatternEdgeInfo>> = HashMap::new();

            for edge in &edges {
                let rel_schema = graph_schema
                    .get_rel_schema(&edge.rel_types[0])
                    .map_err(|e| AnalyzerError::GraphSchema {
                        pass: Pass::GraphJoinInference,
                        source: e,
                    })?;

                // Determine the role of this node in this edge
                if edge.from_node == *node_alias {
                    from_edges
                        .entry(rel_schema.table_name.clone())
                        .or_insert_with(Vec::new)
                        .push(*edge);
                } else if edge.to_node == *node_alias {
                    to_edges
                        .entry(rel_schema.table_name.clone())
                        .or_insert_with(Vec::new)
                        .push(*edge);
                }
            }

            // Cross-branch pattern: node is from_node in multiple DIFFERENT tables
            // OR node is to_node in multiple DIFFERENT tables
            let has_from_branch = from_edges.len() > 1;
            let has_to_branch = to_edges.len() > 1;

            if has_from_branch {
                log::debug!(
                    "   ✅ Node '{}' is CROSS-BRANCH (from_node in {} different tables)",
                    node_alias,
                    from_edges.len()
                );

                // Generate JOIN between first two edges from different tables
                let mut table_edges: Vec<_> = from_edges.values().collect();
                let edge1 = table_edges[0][0];
                let edge2 = table_edges[1][0];

                joins.push(self.create_cross_branch_join(
                    edge1,
                    edge2,
                    node_alias,
                    true,
                    graph_schema,
                )?);
            }

            if has_to_branch {
                log::debug!(
                    "   ✅ Node '{}' is CROSS-BRANCH (to_node in {} different tables)",
                    node_alias,
                    to_edges.len()
                );

                // Generate JOIN between first two edges from different tables
                let mut table_edges: Vec<_> = to_edges.values().collect();
                let edge1 = table_edges[0][0];
                let edge2 = table_edges[1][0];

                joins.push(self.create_cross_branch_join(
                    edge1,
                    edge2,
                    node_alias,
                    false,
                    graph_schema,
                )?);
            }

            if !has_from_branch && !has_to_branch {
                log::debug!(
                    "   ⏭️  Node '{}' NOT cross-branch (different roles in edges - linear chain)",
                    node_alias
                );
            }
        }

        Ok(joins)
    }

    /// Helper to create a cross-branch JOIN between two edges sharing a node.
    fn create_cross_branch_join(
        &self,
        edge1: &PatternEdgeInfo,
        edge2: &PatternEdgeInfo,
        node_alias: &str,
        is_from_side: bool, // true if shared node is from_node, false if to_node
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Join> {
        log::debug!(
            "   Generating cross-branch JOIN between '{}' and '{}' on shared node '{}' ({})",
            edge1.alias,
            edge2.alias,
            node_alias,
            if is_from_side { "from_node" } else { "to_node" }
        );

        // Get relationship schemas
        let rel1_schema = graph_schema
            .get_rel_schema(&edge1.rel_types[0])
            .map_err(|e| AnalyzerError::GraphSchema {
                pass: Pass::GraphJoinInference,
                source: e,
            })?;
        let rel2_schema = graph_schema
            .get_rel_schema(&edge2.rel_types[0])
            .map_err(|e| AnalyzerError::GraphSchema {
                pass: Pass::GraphJoinInference,
                source: e,
            })?;

        // Determine join columns based on shared node's role
        let edge1_col = if is_from_side {
            &rel1_schema.from_id
        } else {
            &rel1_schema.to_id
        };

        let edge2_col = if is_from_side {
            &rel2_schema.from_id
        } else {
            &rel2_schema.to_id
        };

        // Create the cross-branch join
        let join = Join {
            table_name: rel2_schema.full_table_name(),
            table_alias: edge2.alias.clone(),
            joining_on: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(edge2.alias.clone()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            edge2_col.to_string(),
                        ),
                    }),
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(edge1.alias.clone()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            edge1_col.to_string(),
                        ),
                    }),
                ],
            }],
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        };

        log::debug!(
            "   ➕ Adding cross-branch JOIN: {} AS {} ON {}.{} = {}.{}",
            join.table_name,
            join.table_alias,
            edge2.alias,
            edge2_col,
            edge1.alias,
            edge1_col
        );

        Ok(join)
    }

    // ========================================================================
    // Legacy Cross-Branch Detection (Still used, will be removed after Phase 2)
    // ========================================================================

    /// Check for cross-branch shared nodes and generate JOINs where needed.
    ///
    /// This handles branching patterns like: (a)-[:R1]->(b), (a)-[:R2]->(c)
    /// where node 'a' appears in multiple GraphRel branches and requires
    /// a JOIN between the edge tables.
    ///
    /// # Algorithm
    /// 1. Extract node info for left_connection and right_connection
    /// 2. Check if each node was already seen in a DIFFERENT GraphRel
    /// 3. If yes, generate a cross-branch JOIN
    /// 4. Record this GraphRel's nodes for future checks
    fn check_and_generate_cross_branch_joins(
        &self,
        graph_rel: &GraphRel,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
        node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
        collected_graph_joins: &mut Vec<Join>,
    ) -> AnalyzerResult<()> {
        log::debug!(
            "🔍 check_and_generate_cross_branch_joins for GraphRel({})",
            graph_rel.alias
        );
        log::debug!(
            "   left_connection: {}, right_connection: {}",
            graph_rel.left_connection,
            graph_rel.right_connection
        );

        // Process left_connection (source node)
        self.check_node_for_cross_branch_join(
            &graph_rel.left_connection,
            graph_rel,
            true, // is_from_side
            plan_ctx,
            graph_schema,
            node_appearances,
            collected_graph_joins,
        )?;

        // Process right_connection (target node)
        self.check_node_for_cross_branch_join(
            &graph_rel.right_connection,
            graph_rel,
            false, // is_from_side
            plan_ctx,
            graph_schema,
            node_appearances,
            collected_graph_joins,
        )?;

        Ok(())
    }

    /// Check a single node for cross-branch sharing and generate JOIN if needed.
    fn check_node_for_cross_branch_join(
        &self,
        node_alias: &str,
        graph_rel: &GraphRel,
        is_from_side: bool,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
        node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
        collected_graph_joins: &mut Vec<Join>,
    ) -> AnalyzerResult<()> {
        log::debug!(
            "   📍 check_node_for_cross_branch_join: node='{}', GraphRel({}), is_from_side={}",
            node_alias,
            graph_rel.alias,
            is_from_side
        );
        log::debug!(
            "   📍 node_appearances currently has {} entries",
            node_appearances.len()
        );

        // Extract node appearance info
        let current_appearance = match self.extract_node_appearance(
            node_alias,
            graph_rel,
            is_from_side,
            plan_ctx,
            graph_schema,
        ) {
            Ok(appearance) => {
                log::debug!(
                    "   📍 Successfully extracted appearance for '{}': table={}, rel={}, column={}",
                    node_alias,
                    appearance.table_name,
                    appearance.rel_alias,
                    appearance.column_name
                );
                appearance
            }
            Err(e) => {
                log::debug!(
                    "   ⚠️  Cannot extract node appearance for '{}': {}",
                    node_alias,
                    e
                );
                return Ok(()); // Skip if we can't extract info (might be a CTE reference or other special case)
            }
        };

        log::debug!(
            "   📍 Node '{}' in GraphRel({}) → {}.{}",
            node_alias,
            current_appearance.rel_alias,
            current_appearance.table_name,
            current_appearance.column_name
        );

        // SELECTIVE Cross-Branch JOIN generation
        //
        // Re-enabled on Dec 21, 2025 to fix comma-separated pattern bug.
        //
        // Key insight: The original logic was disabled because it caused duplicate JOINs for linear patterns.
        // However, comma-separated patterns like `MATCH (a)-[:R1]->(b), (a)-[:R2]->(c)` NEED cross-branch JOINs!
        //
        // The fix: Only generate cross-branch JOIN when the shared node appears in DIFFERENT relationship tables.
        // Linear pattern: (a)-[:R1]->(b)-[:R2]->(c) - 'b' appears in R1 and R2 but it's sequential (no cross-branch)
        // Comma pattern: (a)-[:R1]->(b), (a)-[:R2]->(c) - 'a' appears in TWO independent branches (needs cross-branch)
        //
        // We detect comma patterns by checking if the shared node appears in different rel tables.

        if let Some(prev_appearances) = node_appearances.get(node_alias) {
            log::debug!(
                "   🔍 Node '{}' seen before - checking if cross-branch JOIN needed",
                node_alias
            );

            // Check if this is a new relationship table (comma pattern indicator)
            for prev_appearance in prev_appearances {
                if prev_appearance.table_name != current_appearance.table_name {
                    // Different relationship tables - this is a comma pattern!
                    log::info!("   ✅ COMMA PATTERN: Node '{}' appears in different relationship tables: {} vs {}",
                        node_alias, prev_appearance.table_name, current_appearance.table_name);
                    log::info!(
                        "   ✅ Generating cross-branch JOIN between {} and {}",
                        prev_appearance.rel_alias,
                        current_appearance.rel_alias
                    );

                    // Generate JOIN between the two relationship tables
                    self.generate_cross_branch_join(
                        node_alias,
                        &current_appearance,
                        prev_appearance,
                        collected_graph_joins,
                    )?;

                    break; // Only need one JOIN per shared node
                }
            }
        }

        // Record this appearance for future checks
        node_appearances
            .entry(node_alias.to_string())
            .or_insert_with(Vec::new)
            .push(current_appearance);

        Ok(())
    }

    /// Extract node appearance information from a GraphRel.
    fn extract_node_appearance(
        &self,
        node_alias: &str,
        graph_rel: &GraphRel,
        is_from_side: bool,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<NodeAppearance> {
        log::debug!(
            "      🔎 extract_node_appearance: node='{}', GraphRel({}), is_from_side={}",
            node_alias,
            graph_rel.alias,
            is_from_side
        );

        // Check if this is a VLP (Variable-Length Path) pattern
        let is_vlp = graph_rel.variable_length.is_some();

        // 1. Get node label for the current node from plan_ctx
        let table_ctx = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(node_alias.to_string()))
            .map_err(|e| {
                log::debug!(
                    "      ❌ Failed to get table_ctx for '{}': {}",
                    node_alias,
                    e
                );
                AnalyzerError::PlanCtx {
                    pass: Pass::GraphJoinInference,
                    source: e,
                }
            })?;

        let node_label = table_ctx
            .get_label_str()
            .map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::GraphJoinInference,
                source: e,
            })?;

        // 2. Get left and right node labels from GraphRel for relationship lookup
        let left_label_opt = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(graph_rel.left_connection.clone()))
            .ok()
            .and_then(|ctx| ctx.get_label_str().ok());

        let right_label_opt = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(graph_rel.right_connection.clone()))
            .ok()
            .and_then(|ctx| ctx.get_label_str().ok());

        // 3. Get relationship schema using composite key (rel_type::from_label::to_label)
        let rel_types: Vec<String> = graph_rel
            .labels
            .as_ref()
            .map(|labels| labels.clone())
            .unwrap_or_default();

        if rel_types.is_empty() {
            return Err(AnalyzerError::SchemaNotFound(format!(
                "No relationship types found for GraphRel({})",
                graph_rel.alias
            )));
        }

        let rel_schema = graph_schema
            .get_rel_schema_with_nodes(
                &rel_types[0],
                left_label_opt.as_deref(),
                right_label_opt.as_deref(),
            )
            .map_err(|e| {
                AnalyzerError::SchemaNotFound(format!(
                    "Failed to get rel schema for {}::{}::{}: {}",
                    rel_types[0],
                    left_label_opt.as_deref().unwrap_or("None"),
                    right_label_opt.as_deref().unwrap_or("None"),
                    e
                ))
            })?;

        // 3. Build composite key and get node schema
        let composite_key = format!(
            "{}::{}::{}",
            rel_schema.database, rel_schema.table_name, node_label
        );

        let node_schema = graph_schema
            .node_schema_opt(&composite_key)
            .or_else(|| graph_schema.node_schema_opt(&node_label))
            .ok_or_else(|| {
                AnalyzerError::NodeLabelNotFound(format!(
                    "{} (composite: {})",
                    node_label, composite_key
                ))
            })?;

        // 🔧 VLP FIX: For Variable-Length Paths, use node alias and node table instead of
        // relationship alias and edge table. This is because VLP CTEs replace the relationship
        // table, and the outer query JOINs VLP results with node tables using node aliases.
        //
        // Example: MATCH (u)-[:MEMBER_OF*1..5]->(g)-[:HAS_ACCESS]->(target)
        //   - Without VLP fix: cross-branch JOIN uses t1.group_id (relationship alias)
        //   - With VLP fix: cross-branch JOIN uses g.group_id (node alias)
        //
        // The VLP CTE (vlp_cte) provides start_id and end_id, which are JOINed to:
        //   - u.user_id (start node)
        //   - g.group_id (end node)  <-- This is what subsequent patterns should reference
        if is_vlp {
            log::info!("🔧 VLP NodeAppearance: Using node alias '{}' instead of rel alias '{}' for cross-branch JOIN",
                       node_alias, graph_rel.alias);

            // Use node table and node ID column
            let column_name = node_schema.node_id.column().to_string();

            return Ok(NodeAppearance {
                rel_alias: node_alias.to_string(), // Use node alias, not relationship alias
                node_label: node_label.clone(),
                table_name: node_schema.table_name.clone(), // Use node table
                database: node_schema.database.clone(),
                column_name,
                is_from_side,
                is_vlp: true,
            });
        }

        // 4. Determine which column to use based on side
        // For denormalized nodes (embedded in edge table), use rel_schema's from_id/to_id
        let column_name = if is_from_side {
            rel_schema.from_id.clone()
        } else {
            rel_schema.to_id.clone()
        };

        // 5. Determine actual table name (may be CTE, not base table)
        // CRITICAL: Check if GraphRel center is wrapped in LogicalCte
        // If so, use CTE name WITHOUT database prefix (CTEs don't have databases)
        // This matches logic in graph_context.rs
        let (table_name, database) = if let LogicalPlan::Cte(cte) = graph_rel.center.as_ref() {
            // Wrapped in CTE - use CTE name, no database prefix
            log::info!(
                "🔍 NodeAppearance: REL '{}' wrapped in CTE '{}' - using CTE name without database",
                graph_rel.alias,
                cte.name
            );
            (cte.name.clone(), String::new()) // Empty database for CTEs
        } else if let Some(labels) = &graph_rel.labels {
            // Check if multi-variant relationship (UNION CTE should exist)
            if labels.len() > 1 {
                // Multi-variant: use standardized CTE name (matches graph_traversal_planning.rs)
                let cte_name = format!(
                    "rel_{}_{}",
                    graph_rel.left_connection, graph_rel.right_connection
                );
                log::info!(
                    "🔍 NodeAppearance: REL '{}' has {} labels - using multi-variant CTE: '{}'",
                    graph_rel.alias,
                    labels.len(),
                    cte_name
                );
                (cte_name, String::new()) // Empty database for CTEs
            } else {
                // Single label - use schema table name
                (rel_schema.table_name.clone(), rel_schema.database.clone())
            }
        } else {
            // No labels - use schema table name
            (rel_schema.table_name.clone(), rel_schema.database.clone())
        };

        Ok(NodeAppearance {
            rel_alias: graph_rel.alias.clone(),
            node_label: node_label.clone(),
            table_name,
            database,
            column_name,
            is_from_side,
            is_vlp: false,
        })
    }

    /// Generate a cross-branch JOIN between two GraphRels that share a node.
    fn generate_cross_branch_join(
        &self,
        node_alias: &str,
        prev_appearance: &NodeAppearance,
        current_appearance: &NodeAppearance,
        collected_graph_joins: &mut Vec<Join>,
    ) -> AnalyzerResult<()> {
        log::debug!(
            "   🔗 Generating cross-branch JOIN for node '{}': {} ({}.{}) ↔ {} ({}.{})",
            node_alias,
            prev_appearance.rel_alias,
            prev_appearance.table_name,
            prev_appearance.column_name,
            current_appearance.rel_alias,
            current_appearance.table_name,
            current_appearance.column_name,
        );

        // CRITICAL: Skip JOIN if both GraphRels use the SAME table
        // This handles "coupled edges" where multiple relationships are in the same table
        // Example: (src)-[:REQUESTED]->(d)-[:RESOLVED_TO]->(rip) both use dns_log
        // They should NOT generate a JOIN - they're already in the same FROM clause!
        let same_table = prev_appearance.database == current_appearance.database
            && prev_appearance.table_name == current_appearance.table_name;

        if same_table {
            log::debug!(
                "   ⏭️  Skipping cross-branch JOIN: both GraphRels use same table {}.{}",
                prev_appearance.database,
                prev_appearance.table_name
            );
            return Ok(());
        }

        // Create JOIN: current_table JOIN prev_table ON current.col = prev.col
        // CRITICAL: Only add database prefix if database is not empty (CTEs have no database)
        let table_name = if prev_appearance.database.is_empty() {
            // CTE - no database prefix
            prev_appearance.table_name.clone()
        } else {
            // Regular table - use database.table format
            format!(
                "{}.{}",
                prev_appearance.database, prev_appearance.table_name
            )
        };

        let join = Join {
            table_name,
            table_alias: prev_appearance.rel_alias.clone(),
            joining_on: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(current_appearance.rel_alias.clone()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            current_appearance.column_name.clone(),
                        ),
                    }),
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(prev_appearance.rel_alias.clone()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            prev_appearance.column_name.clone(),
                        ),
                    }),
                ],
            }],
            join_type: JoinType::Inner, // Cross-branch is always INNER (required match)
            pre_filter: None,           // No pre-filter for cross-branch JOINs
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        };

        Self::push_join_if_not_duplicate(collected_graph_joins, join);

        crate::debug_print!(
            "       ✅ Generated: {} JOIN {} ON {}.{} = {}.{}",
            current_appearance.rel_alias,
            prev_appearance.rel_alias,
            current_appearance.rel_alias,
            current_appearance.column_name,
            prev_appearance.rel_alias,
            prev_appearance.column_name,
        );

        Ok(())
    }

    /// Find GraphRel in a logical plan (helper for CartesianProduct shared node processing).
    fn find_graph_rel_in_plan(plan: &LogicalPlan) -> Option<&GraphRel> {
        match plan {
            LogicalPlan::GraphRel(gr) => Some(gr),
            LogicalPlan::Projection(p) => Self::find_graph_rel_in_plan(p.input.as_ref()),
            LogicalPlan::Filter(f) => Self::find_graph_rel_in_plan(f.input.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema},
        query_planner::{
            logical_expr::Direction,
            logical_plan::{GraphNode, GraphRel, LogicalPlan},
            plan_ctx::{PlanCtx, TableCtx},
        },
    };
    use std::collections::HashMap;

    fn create_test_graph_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create Person node schema
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                database: "default".to_string(),
                table_name: "Person".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
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

        // Create Company node schema
        nodes.insert(
            "Company".to_string(),
            NodeSchema {
                database: "default".to_string(),
                table_name: "Company".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "founded".to_string()],
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

        // Create FOLLOWS relationship schema (edge list)
        relationships.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "default".to_string(),
                table_name: "FOLLOWS".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "since".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Person".to_string(),
                from_node_table: "Person".to_string(),
                to_node_table: "Person".to_string(),
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

        // Create WORKS_AT relationship schema (edge list)
        relationships.insert(
            "WORKS_AT".to_string(),
            RelationshipSchema {
                database: "default".to_string(),
                table_name: "WORKS_AT".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "position".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Company".to_string(),
                from_node_table: "Person".to_string(),
                to_node_table: "Company".to_string(),
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

        GraphSchema::build(1, "default".to_string(), nodes, relationships)
    }

    fn setup_plan_ctx_with_graph_entities() -> PlanCtx {
        let mut plan_ctx = PlanCtx::default();

        // Add person nodes
        plan_ctx.insert_table_ctx(
            "p1".to_string(),
            TableCtx::build(
                "p1".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p2".to_string(),
            TableCtx::build(
                "p2".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p3".to_string(),
            TableCtx::build(
                "p3".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        // Add company node
        plan_ctx.insert_table_ctx(
            "c1".to_string(),
            TableCtx::build(
                "c1".to_string(),
                Some(vec!["Company".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some(vec!["FOLLOWS".to_string()]),
                vec![],
                true,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "f2".to_string(),
            TableCtx::build(
                "f2".to_string(),
                Some(vec!["FOLLOWS".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        // Add works_at relationship
        plan_ctx.insert_table_ctx(
            "w1".to_string(),
            TableCtx::build(
                "w1".to_string(),
                Some(vec!["WORKS_AT".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        plan_ctx
    }

    fn create_scan_plan(table_alias: &str, table_name: &str) -> Arc<LogicalPlan> {
        // Use Empty since Scan is removed
        Arc::new(LogicalPlan::Empty)
    }

    fn create_graph_node(
        input: Arc<LogicalPlan>,
        alias: &str,
        is_denormalized: bool,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input,
            alias: alias.to_string(),
            label: None,
            is_denormalized,
            projected_columns: None,
        }))
    }

    fn create_graph_rel(
        left: Arc<LogicalPlan>,
        center: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        alias: &str,
        direction: Direction,
        left_connection: &str,
        right_connection: &str,
        labels: Option<Vec<String>>,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center,
            right,
            alias: alias.to_string(),
            direction,
            left_connection: left_connection.to_string(),
            right_connection: right_connection.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None, // Will be populated by filter pushdown
            labels,
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
        }))
    }

    #[test]
    fn test_no_graph_joins_when_no_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Create a plan with only a graph node (no relationships)
        let scan = create_scan_plan("p1", "person");
        let graph_node = create_graph_node(scan, "p1", false);

        let result = analyzer
            .analyze_with_graph_schema(graph_node.clone(), &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should not transform the plan since there are no graph relationships
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, graph_node);
            }
            _ => panic!("Expected no transformation for plan without relationships"),
        }
    }

    #[test]
    fn test_edge_list_same_node_type_outgoing_direction() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("f1").unwrap();

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
            Some(vec!["FOLLOWS".to_string()]),
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        println!("\n result: {:?}\n", result);

        // Should create graph joins
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Edge list optimization: Since neither node is referenced separately,
                        // PatternSchemaContext uses SingleTableScan strategy.
                        // This puts the edge table (FOLLOWS) in FROM clause with no additional JOINs.
                        assert_eq!(graph_joins.joins.len(), 1);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        // anchor_table is the relationship table (f1) used as FROM
                        assert_eq!(graph_joins.anchor_table, Some("f1".to_string()));

                        // Single join: relationship table (f1) with empty joining_on (FROM marker)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        // Empty joining_on indicates this is the FROM clause, not a JOIN
                        assert_eq!(rel_join.joining_on.len(), 0);
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_edge_list_different_node_types() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("w1").unwrap();

        // Create plan: (p1)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1", false);

        let graph_rel = create_graph_rel(
            p1_node,
            w1_scan,
            c1_node,
            "w1",
            Direction::Outgoing,
            "p1", // left_connection (p1 is the LEFT node)
            "c1", // right_connection (c1 is the RIGHT node)
            Some(vec!["WORKS_AT".to_string()]),
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for different node types
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Edge list optimization: p1 is referenced, c1 is not.
                        // SingleTableScan strategy puts w1 (edge table) in FROM clause.
                        assert_eq!(graph_joins.joins.len(), 1);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        // anchor_table is the relationship table (w1) used as FROM
                        assert_eq!(graph_joins.anchor_table, Some("w1".to_string()));

                        // Single join: w1 with empty joining_on (FROM marker)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.WORKS_AT");
                        assert_eq!(rel_join.table_alias, "w1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        // Empty joining_on indicates this is the FROM clause, not a JOIN
                        assert_eq!(rel_join.joining_on.len(), 0);
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    #[ignore] // Bitmap indexes not used in current schema - edge lists only (use_edge_list flag removed)
    fn test_bitmap_traversal() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // This test is obsolete - ClickGraph only uses edge lists
        // Bitmap traversal functionality has been removed

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some(vec!["FOLLOWS_outgoing".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
            Some(vec!["FOLLOWS".to_string()]),
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for bitmap traversal
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 1); // Simple relationship: only relationship join, start node is in FROM
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // (p1)-[f1:FOLLOWS]->(p2)
                        // For bitmap traversal, only relationship join is needed (start node in FROM)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS"); // Base table with database prefix
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.raw(), "to_id");
                                assert_eq!(right_prop.table_alias.0, "p2");
                                assert_eq!(right_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_standalone_relationship_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("f2").unwrap();

        // Create standalone relationship: (p3)-[f2:FOLLOWS]-(Empty)
        // This simulates a case where left node was already processed/removed
        let empty_left = Arc::new(LogicalPlan::Empty);
        let f2_scan = create_scan_plan("f2", "FOLLOWS");
        let p3_scan = create_scan_plan("p3", "Person");
        let p3_node = create_graph_node(p3_scan, "p3", false);

        let graph_rel = create_graph_rel(
            empty_left,
            f2_scan,
            p3_node,
            "f2",
            Direction::Outgoing,
            "p1", // left connection exists but left plan is Empty
            "p3",
            Some(vec!["FOLLOWS".to_string()]),
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Standalone relationship with Empty left node.
        // Expected: 3 joins (p1 as FROM with empty joining_on, f2, p3)
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        // Pattern: (p1)-[f2:FOLLOWS]->(p3) where left is Empty
                        // After reordering: f2, p3, p1 (order may vary due to optimization)
                        assert_eq!(graph_joins.joins.len(), 3);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // Check that all expected aliases are present (order may vary)
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();
                        assert!(join_aliases.contains(&&"f2".to_string()));
                        assert!(join_aliases.contains(&&"p3".to_string()));
                        assert!(join_aliases.contains(&&"p1".to_string()));

                        // Verify each join has correct structure
                        for join in &graph_joins.joins {
                            assert_eq!(join.join_type, JoinType::Inner);
                            // Joins may have empty or non-empty conditions depending on position
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_incoming_direction_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Update relationship label for incoming direction
        // plan_ctx.get_mut_table_ctx("f1").unwrap().set_labels(Some(vec!["FOLLOWS_incoming"]));
        plan_ctx.get_mut_table_ctx("f1").unwrap();

        // Create plan: (p2)<-[f1:FOLLOWS]-(p1)
        // This means p1 FOLLOWS p2 (arrow goes from p1 to p2)
        // After GraphRel construction normalization:
        //   - left_connection = p1 (FROM node, the source/follower)
        //   - right_connection = p2 (TO node, the target/followed)
        //   - direction = Incoming (preserved from pattern)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        // After construction normalization: left=FROM (p1), right=TO (p2)
        let graph_rel = create_graph_rel(
            p1_node, // left = FROM node (p1 is the follower/source)
            f1_scan,
            p2_node, // right = TO node (p2 is the followed/target)
            "f1",
            Direction::Incoming,
            "p1", // left_connection = FROM node
            "p2", // right_connection = TO node
            Some(vec!["FOLLOWS".to_string()]),
        );
        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create appropriate joins for incoming direction
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Edge list optimization: Neither p1 nor p2 is referenced separately.
                        // SingleTableScan strategy puts f1 (edge table) in FROM clause.
                        assert_eq!(graph_joins.joins.len(), 1);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        // anchor_table is the relationship table (f1) used as FROM
                        assert_eq!(graph_joins.anchor_table, Some("f1".to_string()));

                        // Single join: f1 with empty joining_on (FROM marker)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        // Empty joining_on indicates this is the FROM clause, not a JOIN
                        assert_eq!(rel_join.joining_on.len(), 0);
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_complex_nested_plan_with_multiple_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set relationships to use edge list
        plan_ctx.get_mut_table_ctx("f1").unwrap();
        plan_ctx.get_mut_table_ctx("w1").unwrap();

        // Create complex plan: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let first_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
            Some(vec!["FOLLOWS".to_string()]),
        );

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1", false);

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)

        let second_rel = create_graph_rel(
            c1_node,
            w1_scan,
            first_rel,
            "w1",
            Direction::Outgoing,
            "c1",
            "p2",
            Some(vec!["WORKS_AT".to_string()]),
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: second_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        "name".to_string(),
                    ),
                }),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        // In this case, c1 is the ending node, we are now joining in reverse order.
        // It means first we will join c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
        // So the tables in the order of joining will be c1, w1, p2, f1, p1.
        // FIX: Multi-hop patterns now correctly generate ALL node JOINs for proper chaining.
        // Previously, SingleTableScan optimization incorrectly removed node JOINs.

        // Should create joins for all relationships in the chain
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert!(graph_joins.joins.len() >= 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // Verify we have joins for both relationship aliases
                        let rel_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        // Should contain joins for both relationships
                        assert!(rel_aliases
                            .iter()
                            .any(|&alias| alias == "f1" || alias == "w1"));

                        // Multi-hop pattern: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                        // CORRECT: 5 joins - all nodes and edges for proper topological JOIN ordering
                        // c1 (anchor/FROM marker), w1 (edge), p2 (intermediate), f1 (edge), p1 (end)
                        println!("Actual joins len: {}", graph_joins.joins.len());
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();
                        println!("Join aliases: {:?}", join_aliases);
                        assert!(graph_joins.joins.len() == 5);

                        // Verify we have the expected join aliases: c1, w1, p2, f1, p1
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        println!("Join aliases found: {:?}", join_aliases);
                        assert!(join_aliases.contains(&&"c1".to_string())); // anchor node
                        assert!(join_aliases.contains(&&"w1".to_string())); // first edge
                        assert!(join_aliases.contains(&&"p2".to_string())); // intermediate node
                        assert!(join_aliases.contains(&&"f1".to_string())); // second edge
                        assert!(join_aliases.contains(&&"p1".to_string())); // end node

                        // Verify each join has basic structure (skip detailed checks due to optimization variations)
                        for join in &graph_joins.joins {
                            assert_eq!(join.join_type, JoinType::Inner);
                            assert!(!join.table_name.is_empty());
                            assert!(!join.table_alias.is_empty());
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    // ===== FK-Edge Pattern Tests =====

    fn create_self_referencing_fk_schema() -> GraphSchema {
        use crate::graph_catalog::expression_parser::PropertyValue;

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create Object node (filesystem objects - same table for all)
        nodes.insert(
            "Object".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "fs_objects".to_string(),
                column_names: vec![
                    "object_id".to_string(),
                    "name".to_string(),
                    "type".to_string(),
                    "parent_id".to_string(),
                ],
                primary_keys: "object_id".to_string(),
                node_id: NodeIdSchema::single("object_id".to_string(), "UInt64".to_string()),
                property_mappings: {
                    let mut props = HashMap::new();
                    props.insert(
                        "object_id".to_string(),
                        PropertyValue::Column("object_id".to_string()),
                    );
                    props.insert(
                        "name".to_string(),
                        PropertyValue::Column("name".to_string()),
                    );
                    props.insert(
                        "type".to_string(),
                        PropertyValue::Column("type".to_string()),
                    );
                    props
                },
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

        // Create PARENT relationship (self-referencing FK)
        // parent_id column on fs_objects points to object_id on same table
        relationships.insert(
            "PARENT".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "fs_objects".to_string(), // Same as node table!
                column_names: vec![],
                from_node: "Object".to_string(),
                to_node: "Object".to_string(), // Self-referencing
                from_node_table: "fs_objects".to_string(),
                to_node_table: "fs_objects".to_string(),
                from_id: "parent_id".to_string(), // FK column
                to_id: "object_id".to_string(),   // PK column
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
                is_fk_edge: true, // Self-referencing FK pattern
                constraints: None,
            },
        );

        GraphSchema::build(1, "test".to_string(), nodes, relationships)
    }

    fn create_non_self_referencing_fk_schema() -> GraphSchema {
        use crate::graph_catalog::expression_parser::PropertyValue;

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create Order node
        nodes.insert(
            "Order".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "orders".to_string(),
                column_names: vec![
                    "order_id".to_string(),
                    "customer_id".to_string(),
                    "total".to_string(),
                ],
                primary_keys: "order_id".to_string(),
                node_id: NodeIdSchema::single("order_id".to_string(), "UInt64".to_string()),
                property_mappings: {
                    let mut props = HashMap::new();
                    props.insert(
                        "order_id".to_string(),
                        PropertyValue::Column("order_id".to_string()),
                    );
                    props.insert(
                        "total".to_string(),
                        PropertyValue::Column("total".to_string()),
                    );
                    props
                },
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

        // Create Customer node
        nodes.insert(
            "Customer".to_string(),
            NodeSchema {
                database: "test".to_string(),
                table_name: "customers".to_string(),
                column_names: vec!["customer_id".to_string(), "name".to_string()],
                primary_keys: "customer_id".to_string(),
                node_id: NodeIdSchema::single("customer_id".to_string(), "UInt64".to_string()),
                property_mappings: {
                    let mut props = HashMap::new();
                    props.insert(
                        "customer_id".to_string(),
                        PropertyValue::Column("customer_id".to_string()),
                    );
                    props.insert(
                        "name".to_string(),
                        PropertyValue::Column("name".to_string()),
                    );
                    props
                },
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

        // Create PLACED_BY relationship (non-self-referencing FK)
        // customer_id column on orders points to customer_id on customers
        relationships.insert(
            "PLACED_BY".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "orders".to_string(), // Same as Order node table!
                column_names: vec![],
                from_node: "Order".to_string(),
                to_node: "Customer".to_string(), // Different table
                from_node_table: "orders".to_string(),
                to_node_table: "customers".to_string(),
                from_id: "order_id".to_string(),  // Order's PK
                to_id: "customer_id".to_string(), // FK pointing to Customer
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
                is_fk_edge: true, // FK-edge pattern (non-self-ref)
                constraints: None,
            },
        );

        GraphSchema::build(1, "test".to_string(), nodes, relationships)
    }

    #[test]
    fn test_fk_edge_pattern_self_referencing() {
        // Test self-referencing FK: (child:Object)-[:PARENT]->(parent:Object)
        let schema = create_self_referencing_fk_schema();

        // Verify schema detected FK pattern
        let rel_schema = schema.get_relationships_schemas().get("PARENT").unwrap();
        assert!(
            rel_schema.is_fk_edge,
            "PARENT relationship should be FK-edge pattern"
        );
        assert_eq!(rel_schema.from_node, "Object");
        assert_eq!(rel_schema.to_node, "Object");
        assert_eq!(rel_schema.from_id, "parent_id"); // FK column
        assert_eq!(rel_schema.to_id, "object_id"); // PK column
    }

    #[test]
    fn test_fk_edge_pattern_non_self_referencing() {
        // Test non-self-ref FK: (o:Order)-[:PLACED_BY]->(c:Customer)
        let schema = create_non_self_referencing_fk_schema();

        // Verify schema detected FK pattern
        let rel_schema = schema.get_relationships_schemas().get("PLACED_BY").unwrap();
        assert!(
            rel_schema.is_fk_edge,
            "PLACED_BY relationship should be FK-edge pattern"
        );
        assert_eq!(rel_schema.from_node, "Order");
        assert_eq!(rel_schema.to_node, "Customer");
        assert_eq!(rel_schema.from_id, "order_id"); // Order's PK
        assert_eq!(rel_schema.to_id, "customer_id"); // FK to Customer
    }

    #[test]
    fn test_standard_edge_is_not_fk_pattern() {
        // Verify standard edge tables are NOT marked as FK pattern
        let schema = create_test_graph_schema();

        let follows = schema.get_relationships_schemas().get("FOLLOWS").unwrap();
        assert!(!follows.is_fk_edge, "FOLLOWS should NOT be FK-edge pattern");

        let works_at = schema.get_relationships_schemas().get("WORKS_AT").unwrap();
        assert!(
            !works_at.is_fk_edge,
            "WORKS_AT should NOT be FK-edge pattern"
        );
    }

    // ========================================================================
    // Phase 4 Tests: Relationship Uniqueness Constraints
    // ========================================================================

    #[test]
    fn test_no_uniqueness_constraints_for_single_relationship() {
        // Single relationship pattern should not generate constraints
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();

        let metadata = PatternGraphMetadata {
            nodes: HashMap::new(),
            edges: vec![PatternEdgeInfo {
                alias: "r1".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "a".to_string(),
                to_node: "b".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            }],
        };

        let constraints =
            analyzer.generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

        assert_eq!(
            constraints.len(),
            0,
            "Single relationship should not generate constraints"
        );
    }

    #[test]
    fn test_uniqueness_constraints_for_two_relationships() {
        // Two-hop pattern should generate 1 constraint: r1 != r2
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();

        let metadata = PatternGraphMetadata {
            nodes: HashMap::new(),
            edges: vec![
                PatternEdgeInfo {
                    alias: "r1".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "a".to_string(),
                    to_node: "b".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
                PatternEdgeInfo {
                    alias: "r2".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "b".to_string(),
                    to_node: "c".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
            ],
        };

        let constraints =
            analyzer.generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

        assert_eq!(
            constraints.len(),
            1,
            "Two relationships should generate 1 constraint"
        );

        // Verify it's a composite constraint (from_id OR to_id inequality)
        match &constraints[0] {
            LogicalExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operator, Operator::Or, "Composite ID should use OR");
                assert_eq!(
                    op.operands.len(),
                    2,
                    "Should have 2 operands (from_id and to_id)"
                );
            }
            _ => panic!("Expected OperatorApplicationExp with OR"),
        }
    }

    #[test]
    fn test_uniqueness_constraints_for_three_relationships() {
        // Three-hop pattern should generate 3 constraints: r1!=r2, r1!=r3, r2!=r3
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();

        let metadata = PatternGraphMetadata {
            nodes: HashMap::new(),
            edges: vec![
                PatternEdgeInfo {
                    alias: "r1".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "a".to_string(),
                    to_node: "b".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
                PatternEdgeInfo {
                    alias: "r2".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "b".to_string(),
                    to_node: "c".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
                PatternEdgeInfo {
                    alias: "r3".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "c".to_string(),
                    to_node: "d".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
            ],
        };

        let constraints =
            analyzer.generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

        // Combinatorial: C(3,2) = 3 pairs
        assert_eq!(
            constraints.len(),
            3,
            "Three relationships should generate 3 pairwise constraints"
        );
    }

    #[test]
    fn test_skip_vlp_relationships_in_uniqueness() {
        // VLP relationships should be skipped in uniqueness constraint generation
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();

        let metadata = PatternGraphMetadata {
            nodes: HashMap::new(),
            edges: vec![
                PatternEdgeInfo {
                    alias: "r1".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "a".to_string(),
                    to_node: "b".to_string(),
                    is_referenced: true,
                    is_vlp: true, // VLP edge
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
                PatternEdgeInfo {
                    alias: "r2".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "b".to_string(),
                    to_node: "c".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
            ],
        };

        let constraints =
            analyzer.generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

        assert_eq!(constraints.len(), 0, "VLP relationships should be skipped");
    }

    #[test]
    fn test_uniqueness_constraints_with_different_relationship_types() {
        // Mixed relationship types should still generate constraints
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();

        let metadata = PatternGraphMetadata {
            nodes: HashMap::new(),
            edges: vec![
                PatternEdgeInfo {
                    alias: "f1".to_string(),
                    rel_types: vec!["FOLLOWS".to_string()],
                    from_node: "a".to_string(),
                    to_node: "b".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
                PatternEdgeInfo {
                    alias: "w1".to_string(),
                    rel_types: vec!["WORKS_AT".to_string()],
                    from_node: "b".to_string(),
                    to_node: "c".to_string(),
                    is_referenced: true,
                    is_vlp: false,
                    is_shortest_path: false,
                    direction: Direction::Outgoing,
                    is_optional: false,
                },
            ],
        };

        let constraints =
            analyzer.generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

        assert_eq!(
            constraints.len(),
            1,
            "Different relationship types should still generate constraints"
        );
    }
}
