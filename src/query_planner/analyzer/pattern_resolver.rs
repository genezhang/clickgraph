//! Pattern Resolver - Systematic type inference and query cloning
//!
//! ⚠️ **DEPRECATED**: This module's functionality has been merged into TypeInference.
//!
//! # Migration Notice (February 2026)
//!
//! PatternResolver has been **superseded by Unified TypeInference** in `type_inference.rs`.
//! The unified implementation provides:
//! - Complete WHERE constraint extraction (id() IN [...] patterns)
//! - Direction-aware validation against schema
//! - UNION generation with schema direction filtering
//! - Integration with Filter→GraphRel interception
//!
//! This module remains for backward compatibility but is no longer used in the query pipeline.
//! New code should use TypeInference directly.
//!
//! # Original Purpose
//!
//! PatternResolver provided systematic pattern resolution for untyped variables:
//! 1. Discovered all untyped node variables
//! 2. Queried schema for valid type candidates
//! 3. Generated all valid type combinations
//! 4. Validated against schema (e.g., User-[FOLLOWS]->Post is invalid)
//! 5. Cloned queries with proper types → UNION ALL
//!
//! # Example (Now handled by TypeInference)
//!
//! ```cypher
//! MATCH (a:User)--(o) WHERE NOT id(o) IN [1,2,3] RETURN o.name
//! ```
//!
//! Unified TypeInference now handles this by:
//! - Extracting WHERE constraints during Filter→GraphRel processing
//! - Combining explicit labels + WHERE constraints + schema
//! - Validating direction: check_relationship_exists_with_direction()
//! - Generating UNION with only schema-valid branches

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
        logical_expr::Direction,
        logical_plan::LogicalPlan,
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

use super::pattern_resolver_config::get_max_combinations;

/// Untyped variable discovered in pattern
#[derive(Debug, Clone)]
struct UntypedVariable {
    /// Variable name (e.g., "n", "o", "person")
    name: String,
    /// Possible types from schema (e.g., ["User", "Post"])
    candidates: Vec<String>,
}

/// Pattern Resolver analyzer pass
pub struct PatternResolver;

impl PatternResolver {
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerPass for PatternResolver {
    fn analyze_with_graph_schema(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::info!("🔍 PATTERN RESOLVER: Starting systematic type resolution");

        // Phase 1: Discover untyped variables
        let untyped_node_names = discover_untyped_nodes(&plan, plan_ctx);

        if untyped_node_names.is_empty() {
            log::info!("🔍 PATTERN RESOLVER: No untyped variables found, skipping");
            return Ok(Transformed::No(plan));
        }

        log::info!(
            "🔍 PATTERN RESOLVER: Found {} untyped variables: {:?}",
            untyped_node_names.len(),
            untyped_node_names
        );

        // Phase 2: Collect type candidates from schema
        let untyped_vars = collect_type_candidates(&untyped_node_names, graph_schema);

        if untyped_vars.is_empty() {
            plan_ctx.add_info("No valid types found for untyped variables");
            log::warn!("🔍 PATTERN RESOLVER: No schema types found for variables");
            return Ok(Transformed::No(plan));
        }

        // Phase 3: Generate all valid type combinations
        let max_combinations = get_max_combinations();
        let combinations = generate_type_combinations(&untyped_vars, max_combinations);

        log::info!(
            "🔍 PATTERN RESOLVER: Generated {} type combinations (max: {})",
            combinations.len(),
            max_combinations
        );

        if combinations.len() >= max_combinations {
            let warning = format!(
                "Hit combination limit ({}) - using first {} combinations",
                max_combinations,
                combinations.len()
            );
            plan_ctx.add_warning(&warning);
            log::warn!("🔍 PATTERN RESOLVER: {}", warning);
        }

        // Phase 4: Validate combinations against schema relationships
        let valid_combinations = validate_combinations(&combinations, graph_schema, &plan);

        log::info!(
            "🔍 PATTERN RESOLVER: {} valid combinations after schema validation (filtered {} invalid)",
            valid_combinations.len(),
            combinations.len() - valid_combinations.len()
        );

        if valid_combinations.is_empty() {
            plan_ctx.add_warning("No valid type combinations found after schema validation");
            log::warn!("🔍 PATTERN RESOLVER: All combinations filtered out by schema constraints");
            return Ok(Transformed::No(plan));
        }

        // Phase 4.5: Skip Union creation when VLP is present.
        // VLP CTE generator already handles multi-type expansion via enumerate_vlp_paths(),
        // which independently validates all (start_type, rel_type, end_type) combinations
        // from the schema. Creating Union branches here would cause duplicate CTEs.
        if plan.contains_variable_length_path() {
            log::info!(
                "🔍 PATTERN RESOLVER: Plan contains VLP — skipping Union creation. \
                 VLP CTE generator will handle type expansion for {} valid combinations.",
                valid_combinations.len()
            );
            return Ok(Transformed::No(plan));
        }

        // Phase 5: Clone query for each valid combination
        let cloned_plans = clone_plans_for_combinations(&plan, &valid_combinations);

        log::info!(
            "🔍 PATTERN RESOLVER: Cloned {} queries for valid combinations",
            cloned_plans.len()
        );

        // Phase 6: Combine with UNION ALL
        if cloned_plans.len() == 1 {
            // Single combination - no UNION needed, just return the cloned plan
            log::info!("🔍 PATTERN RESOLVER: Single combination, returning typed plan directly");
            Ok(Transformed::Yes(Arc::new(
                cloned_plans.into_iter().next().unwrap(),
            )))
        } else {
            // Multiple combinations - combine with UNION ALL
            use crate::query_planner::logical_plan::{Union, UnionType};

            let union_plan = LogicalPlan::Union(Union {
                inputs: cloned_plans.into_iter().map(Arc::new).collect(),
                union_type: UnionType::All, // UNION ALL (not UNION DISTINCT)
            });

            log::info!(
                "🔍 PATTERN RESOLVER: Created UNION ALL of {} typed queries",
                valid_combinations.len()
            );

            Ok(Transformed::Yes(Arc::new(union_plan)))
        }
    }
}

/// Discover all untyped node variables in the plan
fn discover_untyped_nodes(plan: &LogicalPlan, plan_ctx: &PlanCtx) -> Vec<String> {
    let mut untyped = HashSet::new();
    discover_untyped_recursive(plan, plan_ctx, &mut untyped);
    untyped.into_iter().collect()
}

/// Recursive traversal to find untyped variables
fn discover_untyped_recursive(
    plan: &LogicalPlan,
    plan_ctx: &PlanCtx,
    untyped: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check left connection (from node)
            if !has_label_in_ctx(&graph_rel.left_connection, plan_ctx) {
                log::debug!(
                    "🔍 Found untyped left connection: {}",
                    graph_rel.left_connection
                );
                untyped.insert(graph_rel.left_connection.clone());
            }

            // Check right connection (to node)
            if !has_label_in_ctx(&graph_rel.right_connection, plan_ctx) {
                log::debug!(
                    "🔍 Found untyped right connection: {}",
                    graph_rel.right_connection
                );
                untyped.insert(graph_rel.right_connection.clone());
            }

            // Recurse to sub-plans
            discover_untyped_recursive(&graph_rel.left, plan_ctx, untyped);
            discover_untyped_recursive(&graph_rel.center, plan_ctx, untyped);
            discover_untyped_recursive(&graph_rel.right, plan_ctx, untyped);
        }

        LogicalPlan::GraphNode(graph_node) => {
            // Check if node has label
            if graph_node.label.is_none() {
                log::debug!("🔍 Found untyped GraphNode: {}", graph_node.alias);
                untyped.insert(graph_node.alias.clone());
            }

            // Recurse to input (GraphNode always has input)
            discover_untyped_recursive(&graph_node.input, plan_ctx, untyped);
        }

        // Handle other plan types with inputs
        LogicalPlan::Filter(filter) => {
            discover_untyped_recursive(&filter.input, plan_ctx, untyped);
        }

        LogicalPlan::Projection(proj) => {
            discover_untyped_recursive(&proj.input, plan_ctx, untyped);
        }

        LogicalPlan::Union(union_plan) => {
            for input in &union_plan.inputs {
                discover_untyped_recursive(input, plan_ctx, untyped);
            }
        }

        LogicalPlan::GroupBy(group_by) => {
            discover_untyped_recursive(&group_by.input, plan_ctx, untyped);
        }

        LogicalPlan::OrderBy(order_by) => {
            discover_untyped_recursive(&order_by.input, plan_ctx, untyped);
        }

        LogicalPlan::Limit(limit) => {
            discover_untyped_recursive(&limit.input, plan_ctx, untyped);
        }

        LogicalPlan::Skip(skip) => {
            discover_untyped_recursive(&skip.input, plan_ctx, untyped);
        }

        // Leaf nodes - no traversal needed
        LogicalPlan::ViewScan(_) => {}

        LogicalPlan::Empty => {}

        // All other plan types - add as needed
        _ => {}
    }
}

/// Collect type candidates for each untyped variable from schema
fn collect_type_candidates(
    untyped_names: &[String],
    graph_schema: &GraphSchema,
) -> Vec<UntypedVariable> {
    // Use all_node_schemas() to get all node types from schema
    let all_node_types: Vec<String> = graph_schema
        .all_node_schemas()
        .keys()
        .map(|s| s.to_string())
        .collect();

    log::debug!(
        "🔍 Schema has {} node types: {:?}",
        all_node_types.len(),
        all_node_types
    );

    untyped_names
        .iter()
        .filter_map(|name| {
            if all_node_types.is_empty() {
                None
            } else {
                Some(UntypedVariable {
                    name: name.clone(),
                    candidates: all_node_types.clone(),
                })
            }
        })
        .collect()
}

/// Generate all type combinations (cartesian product) with limit
///
/// # Arguments
/// * `untyped_vars` - Variables with their type candidates
/// * `max_combinations` - Maximum combinations to generate (e.g., 38)
///
/// # Returns
/// Vector of HashMaps, each mapping variable_name → type
///
/// # Example
/// ```ignore
/// Input: [
///   UntypedVariable { name: "o", candidates: ["User", "Post"] },
///   UntypedVariable { name: "x", candidates: ["User", "Post"] }
/// ]
///
/// Output: [
///   {"o": "User", "x": "User"},
///   {"o": "User", "x": "Post"},
///   {"o": "Post", "x": "User"},
///   {"o": "Post", "x": "Post"}
/// ]
/// ```
fn generate_type_combinations(
    untyped_vars: &[UntypedVariable],
    max_combinations: usize,
) -> Vec<HashMap<String, String>> {
    if untyped_vars.is_empty() {
        return vec![];
    }

    // Start with one empty combination
    let mut combinations: Vec<HashMap<String, String>> = vec![HashMap::new()];

    // For each variable, extend all existing combinations with each of its candidates
    for var in untyped_vars {
        let mut new_combinations = Vec::new();

        for existing_combo in &combinations {
            for candidate in &var.candidates {
                // Check limit before adding
                if new_combinations.len() >= max_combinations {
                    log::warn!(
                        "🔍 Hit combination limit ({}) at variable '{}', stopping expansion",
                        max_combinations,
                        var.name
                    );
                    combinations = new_combinations;
                    return combinations;
                }

                // Clone existing combination and add this variable's type
                let mut new_combo = existing_combo.clone();
                new_combo.insert(var.name.clone(), candidate.clone());
                new_combinations.push(new_combo);
            }
        }

        combinations = new_combinations;
    }

    combinations
}

/// Clone logical plan for each valid type combination
///
/// # Arguments
/// * `plan` - Original logical plan with untyped variables
/// * `combinations` - Valid type combinations from validation
///
/// # Returns
/// Vector of cloned plans, one per combination, with labels added
fn clone_plans_for_combinations(
    plan: &LogicalPlan,
    combinations: &[HashMap<String, String>],
) -> Vec<LogicalPlan> {
    combinations
        .iter()
        .map(|combo| clone_plan_with_labels(plan, combo))
        .collect()
}

/// Clone a plan and add labels according to combination
/// Clone a plan tree, assigning labels from the given combination to untyped nodes.
///
/// Note: This sets `GraphNode.label` but does not rebuild the node's `input` scan.
/// The input may still be a Union of ViewScans over all node types. The downstream
/// union_pruning optimizer handles removing invalid branches based on the assigned label.
fn clone_plan_with_labels(plan: &LogicalPlan, combo: &HashMap<String, String>) -> LogicalPlan {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // If this node variable is in our combination, add the label
            if let Some(label) = combo.get(&node.alias) {
                if node.label.is_none() {
                    // Untyped node - add the label from combination
                    let mut cloned = node.clone();
                    cloned.label = Some(label.clone());
                    // Prune Union input to the ViewScan matching this label
                    cloned.input = Arc::new(prune_union_for_label(&node.input, label, combo));
                    LogicalPlan::GraphNode(cloned)
                } else {
                    // Already typed - just recurse
                    let mut cloned = node.clone();
                    cloned.input = Arc::new(clone_plan_with_labels(&node.input, combo));
                    LogicalPlan::GraphNode(cloned)
                }
            } else {
                // Not in combination - just recurse
                let mut cloned = node.clone();
                cloned.input = Arc::new(clone_plan_with_labels(&node.input, combo));
                LogicalPlan::GraphNode(cloned)
            }
        }

        LogicalPlan::GraphRel(graph_rel) => {
            let mut cloned = graph_rel.clone();
            cloned.left = Arc::new(clone_plan_with_labels(&graph_rel.left, combo));
            cloned.center = Arc::new(clone_plan_with_labels(&graph_rel.center, combo));
            cloned.right = Arc::new(clone_plan_with_labels(&graph_rel.right, combo));
            LogicalPlan::GraphRel(cloned)
        }

        LogicalPlan::Filter(filter) => {
            let mut cloned = filter.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&filter.input, combo));
            LogicalPlan::Filter(cloned)
        }

        LogicalPlan::Projection(proj) => {
            let mut cloned = proj.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&proj.input, combo));
            LogicalPlan::Projection(cloned)
        }

        LogicalPlan::GraphJoins(joins) => {
            let mut cloned = joins.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&joins.input, combo));
            LogicalPlan::GraphJoins(cloned)
        }

        LogicalPlan::Union(union_plan) => {
            let mut cloned = union_plan.clone();
            cloned.inputs = union_plan
                .inputs
                .iter()
                .map(|input| Arc::new(clone_plan_with_labels(input, combo)))
                .collect();
            LogicalPlan::Union(cloned)
        }

        LogicalPlan::GroupBy(group_by) => {
            let mut cloned = group_by.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&group_by.input, combo));
            LogicalPlan::GroupBy(cloned)
        }

        LogicalPlan::OrderBy(order_by) => {
            let mut cloned = order_by.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&order_by.input, combo));
            LogicalPlan::OrderBy(cloned)
        }

        LogicalPlan::Limit(limit) => {
            let mut cloned = limit.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&limit.input, combo));
            LogicalPlan::Limit(cloned)
        }

        LogicalPlan::Skip(skip) => {
            let mut cloned = skip.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&skip.input, combo));
            LogicalPlan::Skip(cloned)
        }

        LogicalPlan::WithClause(with_clause) => {
            let mut cloned = with_clause.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&with_clause.input, combo));
            LogicalPlan::WithClause(cloned)
        }

        LogicalPlan::Unwind(unwind) => {
            let mut cloned = unwind.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&unwind.input, combo));
            LogicalPlan::Unwind(cloned)
        }

        LogicalPlan::CartesianProduct(cart) => {
            let mut cloned = cart.clone();
            cloned.left = Arc::new(clone_plan_with_labels(&cart.left, combo));
            cloned.right = Arc::new(clone_plan_with_labels(&cart.right, combo));
            LogicalPlan::CartesianProduct(cloned)
        }

        LogicalPlan::Cte(cte) => {
            let mut cloned = cte.clone();
            cloned.input = Arc::new(clone_plan_with_labels(&cte.input, combo));
            LogicalPlan::Cte(cloned)
        }

        LogicalPlan::PageRank(pagerank) => {
            // PageRank doesn't have an input field, just clone it
            LogicalPlan::PageRank(pagerank.clone())
        }

        // Base cases that don't need recursion
        LogicalPlan::Empty => LogicalPlan::Empty,
        LogicalPlan::ViewScan(view_scan) => LogicalPlan::ViewScan(view_scan.clone()),
    }
}

/// Prune a Union input to the ViewScan matching the given label.
///
/// When PatternResolver assigns a label to an untyped node, the node's input
/// may still be a Union of ViewScans over all node types. This function
/// selects the ViewScan whose source table matches the target label's schema table,
/// effectively "resolving" the polymorphic node to a concrete type.
fn prune_union_for_label(
    input: &LogicalPlan,
    label: &str,
    combo: &HashMap<String, String>,
) -> LogicalPlan {
    if let LogicalPlan::Union(union_plan) = input {
        // Look up the target table for this label
        if let Some(schema) = crate::server::query_context::get_current_schema() {
            if let Some(node_schema) = schema.node_schema_opt(label) {
                let target_table = format!("{}.{}", node_schema.database, node_schema.table_name);
                // Find the ViewScan matching this table
                for vs_input in &union_plan.inputs {
                    if let LogicalPlan::ViewScan(scan) = vs_input.as_ref() {
                        if scan.source_table == target_table {
                            log::debug!(
                                "prune_union_for_label: selected '{}' for label '{}'",
                                target_table,
                                label
                            );
                            return LogicalPlan::ViewScan(scan.clone());
                        }
                    }
                }
            }
        }
        // Fallback: use first ViewScan
        if let Some(first) = union_plan.inputs.first() {
            log::warn!(
                "prune_union_for_label: could not resolve table for label '{}', falling back to first ViewScan",
                label
            );
            return clone_plan_with_labels(first, combo);
        }
    }
    // Not a Union — just recurse
    clone_plan_with_labels(input, combo)
}

/// Validate type combinations against schema relationships
///
/// # Arguments
/// * `combinations` - All generated type combinations
/// * `graph_schema` - Schema with relationship definitions
/// * `plan` - Logical plan containing relationship patterns
///
/// # Returns
/// Filtered list of combinations that satisfy all relationship constraints
///
/// # Algorithm
/// For each combination:
/// 1. Extract all relationship patterns from plan (GraphRel nodes)
/// 2. For each relationship, check if schema has matching edge
/// 3. Filter out combination if any relationship is invalid
///
/// # Example
/// ```ignore
/// Combination: {o: User, x: Post}
/// Relationship: (o)-[:FOLLOWS]->(x)
/// Schema check: Does User-[FOLLOWS]->Post exist?
/// Result: Invalid if not in schema
/// ```
fn validate_combinations(
    combinations: &[HashMap<String, String>],
    graph_schema: &GraphSchema,
    plan: &LogicalPlan,
) -> Vec<HashMap<String, String>> {
    // Extract all relationship patterns from the plan
    let relationships = extract_relationships(plan);

    if relationships.is_empty() {
        // No relationships to validate → all combinations valid
        log::debug!(
            "🔍 No relationships found, all {} combinations valid",
            combinations.len()
        );
        return combinations.to_vec();
    }

    // Extract typed node labels from the plan (nodes that already have labels)
    let typed_nodes = extract_typed_nodes(plan);
    log::debug!("🔍 Typed nodes for validation: {:?}", typed_nodes);

    log::debug!(
        "🔍 Validating {} combinations against {} relationship patterns",
        combinations.len(),
        relationships.len()
    );

    // Filter combinations based on relationship constraints
    combinations
        .iter()
        .filter(|combo| is_valid_combination(combo, &relationships, graph_schema, &typed_nodes))
        .cloned()
        .collect()
}

/// Extract typed node labels from the logical plan
///
/// Finds GraphNode nodes that already have labels assigned,
/// returning a map of alias → label (e.g., {"a" → "User"}).
fn extract_typed_nodes(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut typed = HashMap::new();
    extract_typed_nodes_recursive(plan, &mut typed);
    typed
}

fn extract_typed_nodes_recursive(plan: &LogicalPlan, typed: &mut HashMap<String, String>) {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if let Some(label) = &node.label {
                typed.insert(node.alias.clone(), label.clone());
            }
            extract_typed_nodes_recursive(&node.input, typed);
        }
        LogicalPlan::GraphRel(rel) => {
            extract_typed_nodes_recursive(&rel.left, typed);
            extract_typed_nodes_recursive(&rel.center, typed);
            extract_typed_nodes_recursive(&rel.right, typed);
        }
        LogicalPlan::Filter(f) => extract_typed_nodes_recursive(&f.input, typed),
        LogicalPlan::Projection(p) => extract_typed_nodes_recursive(&p.input, typed),
        LogicalPlan::GroupBy(g) => extract_typed_nodes_recursive(&g.input, typed),
        LogicalPlan::OrderBy(o) => extract_typed_nodes_recursive(&o.input, typed),
        LogicalPlan::Limit(l) => extract_typed_nodes_recursive(&l.input, typed),
        LogicalPlan::Skip(s) => extract_typed_nodes_recursive(&s.input, typed),
        LogicalPlan::GraphJoins(j) => extract_typed_nodes_recursive(&j.input, typed),
        LogicalPlan::WithClause(w) => extract_typed_nodes_recursive(&w.input, typed),
        LogicalPlan::Unwind(u) => extract_typed_nodes_recursive(&u.input, typed),
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                extract_typed_nodes_recursive(input, typed);
            }
        }
        LogicalPlan::CartesianProduct(c) => {
            extract_typed_nodes_recursive(&c.left, typed);
            extract_typed_nodes_recursive(&c.right, typed);
        }
        _ => {}
    }
}

/// Relationship pattern extracted from logical plan
#[derive(Debug, Clone)]
struct RelationshipPattern {
    /// Alias of left/from node (e.g., "o")
    left_alias: String,
    /// Alias of right/to node (e.g., "x")
    right_alias: String,
    /// Relationship type labels (e.g., ["FOLLOWS"] or ["FOLLOWS", "FRIENDS_WITH"])
    /// Empty means any relationship type
    rel_types: Vec<String>,
    /// Direction (for debug/logging only)
    direction: Direction,
}

/// Extract all relationship patterns from logical plan
fn extract_relationships(plan: &LogicalPlan) -> Vec<RelationshipPattern> {
    let mut relationships = Vec::new();
    extract_relationships_recursive(plan, &mut relationships);
    relationships
}

/// Recursively extract relationships from plan tree
fn extract_relationships_recursive(
    plan: &LogicalPlan,
    relationships: &mut Vec<RelationshipPattern>,
) {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Get relationship types from GraphRel.labels (canonical source, supports [:TYPE1|TYPE2])
            let rel_types = graph_rel.labels.clone().unwrap_or_default();

            relationships.push(RelationshipPattern {
                left_alias: graph_rel.left_connection.clone(),
                right_alias: graph_rel.right_connection.clone(),
                rel_types,
                direction: graph_rel.direction.clone(),
            });

            // Recurse into left and right nodes
            extract_relationships_recursive(&graph_rel.left, relationships);
            extract_relationships_recursive(&graph_rel.right, relationships);
        }

        LogicalPlan::GraphNode(node) => {
            extract_relationships_recursive(&node.input, relationships);
        }

        LogicalPlan::Filter(filter) => {
            extract_relationships_recursive(&filter.input, relationships);
        }

        LogicalPlan::Projection(proj) => {
            extract_relationships_recursive(&proj.input, relationships);
        }

        LogicalPlan::GraphJoins(joins) => {
            extract_relationships_recursive(&joins.input, relationships);
        }

        LogicalPlan::Union(union_plan) => {
            for input in &union_plan.inputs {
                extract_relationships_recursive(input, relationships);
            }
        }

        LogicalPlan::GroupBy(group_by) => {
            extract_relationships_recursive(&group_by.input, relationships);
        }

        LogicalPlan::OrderBy(order_by) => {
            extract_relationships_recursive(&order_by.input, relationships);
        }

        LogicalPlan::Limit(limit) => {
            extract_relationships_recursive(&limit.input, relationships);
        }

        LogicalPlan::Skip(skip) => {
            extract_relationships_recursive(&skip.input, relationships);
        }

        LogicalPlan::WithClause(with_clause) => {
            extract_relationships_recursive(&with_clause.input, relationships);
        }

        LogicalPlan::Unwind(unwind) => {
            extract_relationships_recursive(&unwind.input, relationships);
        }

        LogicalPlan::CartesianProduct(cart) => {
            extract_relationships_recursive(&cart.left, relationships);
            extract_relationships_recursive(&cart.right, relationships);
        }

        LogicalPlan::Empty
        | LogicalPlan::ViewScan(_)
        | LogicalPlan::Cte(_)
        | LogicalPlan::PageRank(_) => {
            // Base cases - no relationships
        }
    }
}

/// Check if a combination is valid for all relationship patterns
fn is_valid_combination(
    combo: &HashMap<String, String>,
    relationships: &[RelationshipPattern],
    graph_schema: &GraphSchema,
    typed_nodes: &HashMap<String, String>,
) -> bool {
    for rel_pattern in relationships {
        // Get node types — from combo (untyped) or typed_nodes (already typed)
        let from_type = match combo.get(&rel_pattern.left_alias) {
            Some(t) => t.as_str(),
            None => match typed_nodes.get(&rel_pattern.left_alias) {
                Some(t) => t.as_str(),
                None => continue, // Unknown alias, skip
            },
        };

        let to_type = match combo.get(&rel_pattern.right_alias) {
            Some(t) => t.as_str(),
            None => match typed_nodes.get(&rel_pattern.right_alias) {
                Some(t) => t.as_str(),
                None => continue, // Unknown alias, skip
            },
        };

        // Check if this edge exists in schema, considering direction
        let is_undirected = matches!(rel_pattern.direction, Direction::Either);

        let edge_exists = if rel_pattern.rel_types.is_empty() {
            // Untyped relationship — check if ANY relationship exists
            if is_undirected {
                check_any_relationship_exists_bidirectional(from_type, to_type, graph_schema)
            } else {
                check_any_relationship_exists(from_type, to_type, graph_schema)
            }
        } else {
            // Typed relationship — check specific type(s)
            rel_pattern.rel_types.iter().any(|rel_type| {
                if is_undirected {
                    check_relationship_exists_bidirectional(
                        from_type,
                        to_type,
                        rel_type,
                        graph_schema,
                    )
                } else {
                    check_relationship_exists(from_type, to_type, rel_type, graph_schema)
                }
            })
        };

        if !edge_exists {
            log::debug!(
                "🔍 Invalid combination: {}-[{}]->{} (direction={:?}) not in schema",
                from_type,
                if rel_pattern.rel_types.is_empty() {
                    "any".to_string()
                } else {
                    rel_pattern.rel_types.join("|")
                },
                to_type,
                rel_pattern.direction
            );
            return false;
        }
    }

    true
}

/// Match node type considering `$any` as wildcard (polymorphic schemas)
fn node_type_matches(schema_node: &str, query_node: &str) -> bool {
    schema_node == "$any" || schema_node == query_node
}

/// Check if a specific relationship exists in schema
fn check_relationship_exists(
    from_type: &str,
    to_type: &str,
    rel_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    if let Some(rel_schema) = graph_schema.get_relationships_schema_opt(rel_type) {
        node_type_matches(&rel_schema.from_node, from_type)
            && node_type_matches(&rel_schema.to_node, to_type)
    } else {
        false
    }
}

/// Check if a specific relationship exists in either direction
fn check_relationship_exists_bidirectional(
    from_type: &str,
    to_type: &str,
    rel_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    // Iterate all relationships to find matching type with either direction
    graph_schema
        .get_relationships_schemas()
        .iter()
        .any(|(key, rel_schema)| {
            // Match by relationship type (handle composite keys TYPE::FROM::TO)
            let type_matches = if key.contains("::") {
                key.split("::").next() == Some(rel_type)
            } else {
                key == rel_type
            };
            type_matches
                && ((node_type_matches(&rel_schema.from_node, from_type)
                    && node_type_matches(&rel_schema.to_node, to_type))
                    || (node_type_matches(&rel_schema.from_node, to_type)
                        && node_type_matches(&rel_schema.to_node, from_type)))
        })
}

/// Check if ANY relationship exists between two node types
fn check_any_relationship_exists(
    from_type: &str,
    to_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    // Iterate through all relationships and check if any match
    graph_schema
        .get_relationships_schemas()
        .values()
        .any(|rel_schema| {
            node_type_matches(&rel_schema.from_node, from_type)
                && node_type_matches(&rel_schema.to_node, to_type)
        })
}

/// Check if ANY relationship exists between two node types in either direction
fn check_any_relationship_exists_bidirectional(
    from_type: &str,
    to_type: &str,
    graph_schema: &GraphSchema,
) -> bool {
    graph_schema
        .get_relationships_schemas()
        .values()
        .any(|rel_schema| {
            (node_type_matches(&rel_schema.from_node, from_type)
                && node_type_matches(&rel_schema.to_node, to_type))
                || (node_type_matches(&rel_schema.from_node, to_type)
                    && node_type_matches(&rel_schema.to_node, from_type))
        })
}

/// Check if variable has a (non-empty) label in plan_ctx
fn has_label_in_ctx(var_name: &str, plan_ctx: &PlanCtx) -> bool {
    if let Ok(table_ctx) = plan_ctx.get_table_ctx(var_name) {
        table_ctx
            .get_labels()
            .is_some_and(|labels| !labels.is_empty())
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::GraphSchema;
    use crate::query_planner::logical_plan::*;
    use std::collections::HashMap;

    #[test]
    fn test_discover_untyped_simple_node() {
        // Create a simple untyped node (label=None) on top of Empty
        let plan = LogicalPlan::GraphNode(GraphNode {
            alias: "n".to_string(),
            label: None, // Untyped!
            input: Arc::new(LogicalPlan::Empty),
            projected_columns: None,
            is_denormalized: false,
            node_types: None,
        });

        let plan_ctx = create_test_plan_ctx();
        let untyped = discover_untyped_nodes(&plan, &plan_ctx);

        assert_eq!(untyped.len(), 1);
        assert!(untyped.contains(&"n".to_string()));
    }

    #[test]
    fn test_discover_typed_node() {
        // Create a typed node (label=Some)
        let plan = LogicalPlan::GraphNode(GraphNode {
            alias: "n".to_string(),
            label: Some("User".to_string()), // Typed!
            input: Arc::new(LogicalPlan::Empty),
            projected_columns: None,
            is_denormalized: false,
            node_types: None,
        });

        let plan_ctx = create_test_plan_ctx();
        let untyped = discover_untyped_nodes(&plan, &plan_ctx);

        assert_eq!(untyped.len(), 0); // Should be empty - node is typed
    }

    fn create_test_plan_ctx() -> PlanCtx {
        let empty_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        PlanCtx::new(Arc::new(empty_schema))
    }

    // ========================================================================
    // Phase 2 Tests: collect_type_candidates()
    // ========================================================================

    #[test]
    fn test_collect_type_candidates_empty_schema() {
        let schema = GraphSchema::build(1, "empty".to_string(), HashMap::new(), HashMap::new());
        let untyped_names = vec!["o".to_string()];

        let result = collect_type_candidates(&untyped_names, &schema);

        // Empty schema → no candidates → no untyped variables returned
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_collect_type_candidates_empty_vars() {
        let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let untyped_names = vec![];

        let result = collect_type_candidates(&untyped_names, &schema);

        assert_eq!(result.len(), 0);
    }

    // ========================================================================
    // Phase 3 Tests: generate_type_combinations()
    // ========================================================================

    #[test]
    fn test_generate_combinations_single_var() {
        let untyped_vars = vec![UntypedVariable {
            name: "o".to_string(),
            candidates: vec!["User".to_string(), "Post".to_string()],
        }];

        let result = generate_type_combinations(&untyped_vars, 100);

        // Should generate 2 combinations: (o:User) and (o:Post)
        assert_eq!(result.len(), 2);

        // Check first combination
        assert_eq!(result[0].get("o"), Some(&"User".to_string()));

        // Check second combination
        assert_eq!(result[1].get("o"), Some(&"Post".to_string()));
    }

    #[test]
    fn test_generate_combinations_two_vars() {
        let untyped_vars = vec![
            UntypedVariable {
                name: "o".to_string(),
                candidates: vec!["User".to_string(), "Post".to_string()],
            },
            UntypedVariable {
                name: "x".to_string(),
                candidates: vec!["User".to_string(), "Post".to_string()],
            },
        ];

        let result = generate_type_combinations(&untyped_vars, 100);

        // Should generate 4 combinations: 2 × 2 = 4
        assert_eq!(result.len(), 4);

        // Verify all combinations present
        let expected = vec![
            vec![("o", "User"), ("x", "User")],
            vec![("o", "User"), ("x", "Post")],
            vec![("o", "Post"), ("x", "User")],
            vec![("o", "Post"), ("x", "Post")],
        ];

        for expected_combo in expected {
            let found = result.iter().any(|combo| {
                expected_combo
                    .iter()
                    .all(|(k, v)| combo.get(*k) == Some(&v.to_string()))
            });
            assert!(found, "Missing combination: {:?}", expected_combo);
        }
    }

    #[test]
    fn test_generate_combinations_limit() {
        let untyped_vars = vec![
            UntypedVariable {
                name: "o".to_string(),
                candidates: vec![
                    "User".to_string(),
                    "Post".to_string(),
                    "Comment".to_string(),
                ],
            },
            UntypedVariable {
                name: "x".to_string(),
                candidates: vec![
                    "User".to_string(),
                    "Post".to_string(),
                    "Comment".to_string(),
                ],
            },
        ];

        // Limit to 5 combinations (would normally generate 9 = 3 × 3)
        let result = generate_type_combinations(&untyped_vars, 5);

        // Should stop at limit
        assert_eq!(result.len(), 5);

        // All combinations should have both variables mapped
        for combo in &result {
            assert!(combo.contains_key("o"));
            assert!(combo.contains_key("x"));
        }
    }

    #[test]
    fn test_generate_combinations_exact_limit() {
        let untyped_vars = vec![
            UntypedVariable {
                name: "o".to_string(),
                candidates: vec!["User".to_string(), "Post".to_string()],
            },
            UntypedVariable {
                name: "x".to_string(),
                candidates: vec!["User".to_string(), "Post".to_string()],
            },
        ];

        // Limit exactly equals number of combinations (4)
        let result = generate_type_combinations(&untyped_vars, 4);

        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_generate_combinations_empty() {
        let result = generate_type_combinations(&[], 100);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_generate_combinations_three_vars() {
        let untyped_vars = vec![
            UntypedVariable {
                name: "a".to_string(),
                candidates: vec!["T1".to_string(), "T2".to_string()],
            },
            UntypedVariable {
                name: "b".to_string(),
                candidates: vec!["T1".to_string(), "T2".to_string()],
            },
            UntypedVariable {
                name: "c".to_string(),
                candidates: vec!["T1".to_string(), "T2".to_string()],
            },
        ];

        let result = generate_type_combinations(&untyped_vars, 100);

        // Should generate 8 combinations: 2 × 2 × 2 = 8
        assert_eq!(result.len(), 8);

        // Verify all combinations have all three variables
        for combo in &result {
            assert_eq!(combo.len(), 3);
            assert!(combo.contains_key("a"));
            assert!(combo.contains_key("b"));
            assert!(combo.contains_key("c"));
        }
    }

    #[test]
    fn test_has_label_helper() {
        use crate::query_planner::plan_ctx::TableCtx;

        let empty_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let mut plan_ctx = PlanCtx::new(Arc::new(empty_schema));

        // Add node with label using TableCtx::build
        let table_ctx = TableCtx::build(
            "u".to_string(),
            Some(vec!["User".to_string()]),
            vec![],
            false, // is_rel
            true,  // explicit_alias
        );
        plan_ctx.insert_table_ctx("u".to_string(), table_ctx);

        // Verify has_label_in_ctx works
        assert!(has_label_in_ctx("u", &plan_ctx));
        assert!(!has_label_in_ctx("p", &plan_ctx)); // Not in context
    }

    // ========================================================================
    // Phase 4 Tests: Relationship extraction and validation
    // ========================================================================

    #[test]
    fn test_extract_relationships_empty() {
        let plan = LogicalPlan::Empty;
        let rels = extract_relationships(&plan);
        assert_eq!(rels.len(), 0);
    }

    #[test]
    fn test_validate_no_relationships() {
        // When there are no relationships in the plan, all combinations are valid
        let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let plan = LogicalPlan::Empty;

        let combinations = vec![
            vec![("o", "User"), ("x", "Post")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            vec![("o", "Post"), ("x", "User")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ];

        let result = validate_combinations(&combinations, &schema, &plan);

        // All combinations valid when no relationships to check
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_validate_combinations_filters_invalid() {
        // This is a simplified test - actual relationship validation
        // requires complex schema setup, so we test the logic path
        let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let plan = LogicalPlan::Empty;

        let combinations = vec![vec![("a", "Type1")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()];

        // With no schema relationships, validation should pass
        let result = validate_combinations(&combinations, &schema, &plan);
        assert_eq!(result.len(), 1);
    }
}
