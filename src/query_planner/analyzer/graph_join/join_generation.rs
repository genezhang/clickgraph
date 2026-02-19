//! Clean join generation: one generic algorithm, schema-driven decisions.
//!
//! ## Design
//!
//! Traditional (node-edge-node) is the base case: 3 tables, 2 JOINs.
//! All other JoinStrategy variants are optimizations that skip some JOINs.
//! The ordering algorithm is schema-independent topological sort.
//!
//! ## Algorithm
//!
//! ```text
//! for each (a)-[r]->(b):
//!     joins += generate_pattern_joins(strategy, a, r, b)
//! anchor = select_anchor(joins)
//! ordered = topo_sort_joins(joins, anchor)
//! ```

use std::collections::HashSet;

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::pattern_schema::{JoinStrategy, NodeAccessStrategy, NodePosition, PatternSchemaContext};
use crate::graph_catalog::graph_schema::RelationshipSchema;
use crate::query_planner::analyzer::errors::AnalyzerError;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{Join, JoinType};
use crate::query_planner::plan_ctx::PlanCtx;

use super::helpers::{self, JoinBuilder};

type AnalyzerResult<T> = Result<T, AnalyzerError>;

// =============================================================================
// Resolved table info passed by caller
// =============================================================================

/// All resolved names for one (a)-[r]->(b) pattern.
/// Caller resolves CTE vs base-table names before calling generate_pattern_joins.
pub struct ResolvedTables<'a> {
    pub left_alias: &'a str,
    pub left_table: &'a str,
    pub left_cte_name: &'a str,
    pub rel_alias: &'a str,
    pub rel_table: &'a str,
    pub rel_cte_name: &'a str,
    pub right_alias: &'a str,
    pub right_table: &'a str,
    pub right_cte_name: &'a str,
}

// =============================================================================
// Step 1: Generate joins for one pattern — schema decides WHAT joins
// =============================================================================

/// Generate joins for a single (a)-[r]->(b) pattern based on JoinStrategy.
///
/// Anchor-aware: if a node is already available (from a prior pattern), no FROM
/// marker is generated for it. The edge table anchors on whichever node is available.
///
/// Four cases for Traditional:
///   Neither available: FROM left, JOIN edge ON left, JOIN right ON edge
///   Left available:    JOIN edge ON left, JOIN right ON edge
///   Right available:   JOIN edge ON right, JOIN left ON edge
///   Both available:    JOIN edge ON left AND right
pub fn generate_pattern_joins(
    ctx: &PatternSchemaContext,
    t: &ResolvedTables,
    rel_schema: &RelationshipSchema,
    plan_ctx: &PlanCtx,
    pre_filter: Option<LogicalExpr>,
    already_available: &HashSet<String>,
) -> AnalyzerResult<Vec<Join>> {
    let joins = match &ctx.join_strategy {
        JoinStrategy::Traditional {
            left_join_col,
            right_join_col,
        } => {
            let left_id = own_table_id(&ctx.left_node, "Traditional left")?;
            let right_id = own_table_id(&ctx.right_node, "Traditional right")?;

            // Resolve columns through CTE mappings
            let r_left_id = helpers::resolve_identifier(&left_id, t.left_cte_name, plan_ctx);
            let r_left_join = helpers::resolve_identifier(left_join_col, t.rel_cte_name, plan_ctx);
            let r_right_id = helpers::resolve_identifier(&right_id, t.right_cte_name, plan_ctx);
            let r_right_join = helpers::resolve_identifier(right_join_col, t.rel_cte_name, plan_ctx);

            let left_avail = already_available.contains(t.left_alias);
            let right_avail = already_available.contains(t.right_alias);

            let edge_join = |extra_cond: bool| -> Join {
                let mut b = JoinBuilder::new(t.rel_table, t.rel_alias)
                    .pre_filter(pre_filter.clone())
                    .from_id(first_col(&rel_schema.from_id))
                    .to_id(first_col(&rel_schema.to_id));
                // Always add the left condition: edge.from_col = left.id
                b = b.add_identifier_condition(t.rel_alias, &r_left_join, t.left_alias, &r_left_id);
                if extra_cond {
                    // Also add right condition: edge.to_col = right.id
                    b = b.add_identifier_condition(t.rel_alias, &r_right_join, t.right_alias, &r_right_id);
                }
                b.build()
            };

            match (left_avail, right_avail) {
                // Neither available: first pattern — FROM left, JOIN edge, JOIN right
                (false, false) => vec![
                    JoinBuilder::from_marker(t.left_table, t.left_alias).build(),
                    edge_join(false),
                    JoinBuilder::new(t.right_table, t.right_alias)
                        .add_identifier_condition(t.right_alias, &r_right_id, t.rel_alias, &r_right_join)
                        .build(),
                ],
                // Left available: edge anchors on left, right via edge
                (true, false) => vec![
                    edge_join(false),
                    JoinBuilder::new(t.right_table, t.right_alias)
                        .add_identifier_condition(t.right_alias, &r_right_id, t.rel_alias, &r_right_join)
                        .build(),
                ],
                // Right available: edge anchors on right, left via edge
                (false, true) => vec![
                    JoinBuilder::new(t.rel_table, t.rel_alias)
                        .add_identifier_condition(t.rel_alias, &r_right_join, t.right_alias, &r_right_id)
                        .pre_filter(pre_filter.clone())
                        .from_id(first_col(&rel_schema.from_id))
                        .to_id(first_col(&rel_schema.to_id))
                        .build(),
                    JoinBuilder::new(t.left_table, t.left_alias)
                        .add_identifier_condition(t.left_alias, &r_left_id, t.rel_alias, &r_left_join)
                        .build(),
                ],
                // Both available: only the edge table, joining both sides
                (true, true) => vec![
                    edge_join(true),
                ],
            }
        }

        // Optimization: fully denormalized, 0 JOINs
        JoinStrategy::SingleTableScan { .. } => {
            if already_available.contains(t.rel_alias) {
                vec![] // Edge already available, nothing to add
            } else {
                vec![
                    JoinBuilder::from_marker(t.rel_table, t.rel_alias)
                        .pre_filter(pre_filter)
                        .from_id(first_col(&rel_schema.from_id))
                        .to_id(first_col(&rel_schema.to_id))
                        .build(),
                ]
            }
        }

        // Optimization: one node embedded in edge, 1 JOIN
        JoinStrategy::MixedAccess {
            joined_node,
            join_col,
        } => {
            let (node_alias, node_table, node_cte, node_strategy) = match joined_node {
                NodePosition::Left => (t.left_alias, t.left_table, t.left_cte_name, &ctx.left_node),
                NodePosition::Right => (t.right_alias, t.right_table, t.right_cte_name, &ctx.right_node),
            };
            let node_id = own_table_id(node_strategy, "MixedAccess joined node")?;
            let r_node_id = helpers::resolve_identifier(&node_id, node_cte, plan_ctx);
            let r_join_col = Identifier::Single(
                helpers::resolve_column(join_col, t.rel_cte_name, plan_ctx),
            );

            let edge_avail = already_available.contains(t.rel_alias);
            let node_avail = already_available.contains(node_alias);

            match (edge_avail, node_avail) {
                (false, false) => vec![
                    JoinBuilder::from_marker(t.rel_table, t.rel_alias)
                        .pre_filter(pre_filter)
                        .from_id(first_col(&rel_schema.from_id))
                        .to_id(first_col(&rel_schema.to_id))
                        .build(),
                    JoinBuilder::new(node_table, node_alias)
                        .add_identifier_condition(node_alias, &r_node_id, t.rel_alias, &r_join_col)
                        .build(),
                ],
                (true, false) => vec![
                    // Edge available, just join the node
                    JoinBuilder::new(node_table, node_alias)
                        .add_identifier_condition(node_alias, &r_node_id, t.rel_alias, &r_join_col)
                        .build(),
                ],
                (false, true) => vec![
                    // Node available, edge anchors on node
                    JoinBuilder::new(t.rel_table, t.rel_alias)
                        .add_identifier_condition(t.rel_alias, &r_join_col, node_alias, &r_node_id)
                        .pre_filter(pre_filter)
                        .from_id(first_col(&rel_schema.from_id))
                        .to_id(first_col(&rel_schema.to_id))
                        .build(),
                ],
                (true, true) => vec![], // Both available, nothing to add
            }
        }

        // Optimization: multi-hop denormalized, edge chains directly
        JoinStrategy::EdgeToEdge {
            prev_edge_alias,
            prev_edge_col,
            curr_edge_col,
        } => {
            vec![
                JoinBuilder::new(t.rel_table, t.rel_alias)
                    .add_condition(t.rel_alias, curr_edge_col, prev_edge_alias, prev_edge_col)
                    .pre_filter(pre_filter)
                    .from_id(first_col(&rel_schema.from_id))
                    .to_id(first_col(&rel_schema.to_id))
                    .build(),
            ]
        }

        // Optimization: same physical row, 0 JOINs
        JoinStrategy::CoupledSameRow { .. } => {
            vec![]
        }

        // Optimization: FK on node table, 1 JOIN
        //   join_side=Left  → right node IS the edge table, JOIN left
        //   join_side=Right → left node IS the edge table, JOIN right
        JoinStrategy::FkEdgeJoin {
            from_id,
            to_id,
            join_side,
            ..
        } => {
            match join_side {
                NodePosition::Left => {
                    // Right node IS the edge table (anchor). Left needs JOIN.
                    let left_id = own_table_id(&ctx.left_node, "FkEdgeJoin left")?;
                    let r_left_id = helpers::resolve_identifier(&left_id, t.left_cte_name, plan_ctx);
                    let r_from_id = Identifier::Single(
                        helpers::resolve_column(from_id, t.right_cte_name, plan_ctx),
                    );
                    let right_avail = already_available.contains(t.right_alias);
                    let left_avail = already_available.contains(t.left_alias);

                    match (right_avail, left_avail) {
                        (false, false) => vec![
                            JoinBuilder::from_marker(t.right_table, t.right_alias)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                            JoinBuilder::new(t.left_table, t.left_alias)
                                .add_identifier_condition(t.left_alias, &r_left_id, t.right_alias, &r_from_id)
                                .build(),
                        ],
                        (true, false) => vec![
                            JoinBuilder::new(t.left_table, t.left_alias)
                                .add_identifier_condition(t.left_alias, &r_left_id, t.right_alias, &r_from_id)
                                .build(),
                        ],
                        (false, true) => vec![
                            JoinBuilder::new(t.right_table, t.right_alias)
                                .add_identifier_condition(t.right_alias, &r_from_id, t.left_alias, &r_left_id)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                        ],
                        (true, true) => vec![],
                    }
                }
                NodePosition::Right => {
                    // Left node IS the edge table (anchor). Right needs JOIN.
                    let right_id = own_table_id(&ctx.right_node, "FkEdgeJoin right")?;
                    let r_right_id = helpers::resolve_identifier(&right_id, t.right_cte_name, plan_ctx);
                    let r_to_id = Identifier::Single(
                        helpers::resolve_column(to_id, t.left_cte_name, plan_ctx),
                    );
                    let left_avail = already_available.contains(t.left_alias);
                    let right_avail = already_available.contains(t.right_alias);

                    match (left_avail, right_avail) {
                        (false, false) => vec![
                            JoinBuilder::from_marker(t.left_table, t.left_alias)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                            JoinBuilder::new(t.right_table, t.right_alias)
                                .add_identifier_condition(t.right_alias, &r_right_id, t.left_alias, &r_to_id)
                                .build(),
                        ],
                        (true, false) => vec![
                            JoinBuilder::new(t.right_table, t.right_alias)
                                .add_identifier_condition(t.right_alias, &r_right_id, t.left_alias, &r_to_id)
                                .build(),
                        ],
                        (false, true) => vec![
                            JoinBuilder::new(t.left_table, t.left_alias)
                                .add_identifier_condition(t.left_alias, &r_to_id, t.right_alias, &r_right_id)
                                .from_id(from_id.clone())
                                .to_id(to_id.clone())
                                .build(),
                        ],
                        (true, true) => vec![],
                    }
                }
            }
        }
    };

    Ok(joins)
}

// =============================================================================
// Step 2: Collect + deduplicate across multiple patterns
// =============================================================================

/// Collect joins from a new pattern into the accumulated list.
/// Handles deduplication: when an alias is already present, its conditions
/// are redistributed to the join that references it (correlation).
pub fn collect_with_dedup(all_joins: &mut Vec<Join>, new_joins: Vec<Join>) {
    let existing_aliases: HashSet<String> = all_joins.iter().map(|j| j.table_alias.clone()).collect();

    for join in new_joins {
        if existing_aliases.contains(&join.table_alias) {
            // Alias already in the list. Two cases:
            // 1. FROM marker (no conditions) → just skip it
            // 2. Has conditions → redistribute them to the referencing join
            if !join.joining_on.is_empty() {
                redistribute_conditions(all_joins, &join);
            }
        } else {
            all_joins.push(join);
        }
    }
}

/// When a duplicate join has conditions (e.g., `right.id = edge.to_id`),
/// move those conditions to the OTHER join they reference.
///
/// Example: pattern 2 generates `JOIN b ON b.id = r2.to_id` but b already exists.
/// We find join `r2` and add `r2.to_id = b.id` to its conditions.
fn redistribute_conditions(joins: &mut [Join], duplicate: &Join) {
    for condition in &duplicate.joining_on {
        let refs = extract_aliases_from_condition(condition);
        // Find the "other" alias (not the duplicate's own alias)
        let target_alias = refs.iter().find(|a| *a != &duplicate.table_alias);
        if let Some(target) = target_alias {
            if let Some(target_join) = joins.iter_mut().find(|j| j.table_alias == **target) {
                target_join.joining_on.push(condition.clone());
                log::debug!(
                    "🔄 Redistributed condition from duplicate '{}' to '{}'",
                    duplicate.table_alias,
                    target
                );
            }
        }
    }
}

// =============================================================================
// Step 3: Select anchor + topological sort — schema-independent
// =============================================================================

/// Select the anchor (FROM) table from collected joins.
/// Prefers non-optional FROM markers (INNER), falls back to optional (LEFT).
pub fn select_anchor(joins: &[Join]) -> Option<String> {
    // Prefer non-optional FROM marker
    for join in joins {
        if join.joining_on.is_empty() && join.join_type != JoinType::Left {
            return Some(join.table_alias.clone());
        }
    }
    // Fall back to any FROM marker
    for join in joins {
        if join.joining_on.is_empty() {
            return Some(join.table_alias.clone());
        }
    }
    None
}

/// Topological sort of joins ensuring each JOIN only references already-available tables.
///
/// FROM markers (empty conditions) are placed first — they have no dependencies.
/// Then greedily picks joins whose dependencies are all satisfied.
/// Errors on unresolvable joins (circular dependency = upstream bug).
pub fn topo_sort_joins(
    joins: Vec<Join>,
    extra_available: &HashSet<String>,
) -> AnalyzerResult<Vec<Join>> {
    let mut available: HashSet<String> = extra_available.clone();
    let mut ordered: Vec<Join> = Vec::new();
    let mut remaining: Vec<Join> = Vec::new();

    // Phase 1: FROM markers first (no dependencies)
    for join in joins {
        if join.joining_on.is_empty() {
            available.insert(join.table_alias.clone());
            ordered.push(join);
        } else {
            remaining.push(join);
        }
    }

    // Phase 2: Greedy topological sort
    while !remaining.is_empty() {
        let before = remaining.len();
        let mut next_remaining = Vec::new();

        for join in remaining {
            let deps = extract_join_dependencies(&join);
            if deps.is_subset(&available) {
                available.insert(join.table_alias.clone());
                ordered.push(join);
            } else {
                next_remaining.push(join);
            }
        }

        remaining = next_remaining;
        if remaining.len() == before {
            // No progress — unresolvable dependencies
            let stuck: Vec<_> = remaining
                .iter()
                .map(|j| {
                    let deps = extract_join_dependencies(j);
                    let missing: Vec<_> = deps.difference(&available).cloned().collect();
                    format!("'{}' needs {:?}", j.table_alias, missing)
                })
                .collect();
            return Err(AnalyzerError::OptimizerError {
                message: format!("Unresolvable join dependencies: {}", stuck.join(", ")),
            });
        }
    }

    Ok(ordered)
}

// =============================================================================
// Step 4: Post-processing passes (applied once after sort)
// =============================================================================

/// Mark joins as LEFT JOIN for aliases in the optional set.
pub fn apply_optional_marking(joins: &mut [Join], optional_aliases: &HashSet<String>) {
    // Find the first optional alias — everything from there on should be LEFT JOIN
    let first_optional_idx = joins
        .iter()
        .position(|j| optional_aliases.contains(&j.table_alias));

    if let Some(start) = first_optional_idx {
        for join in &mut joins[start..] {
            if join.join_type == JoinType::Inner {
                join.join_type = JoinType::Left;
            }
        }
    }
}

// =============================================================================
// Step 5: VLP endpoint rewriting (post-pass before topo_sort)
// =============================================================================

/// Rewrite join conditions to reference VLP CTE endpoints instead of base node aliases.
///
/// When a node is accessed via a VLP CTE (e.g., `t.end_id` instead of `u2.user_id`),
/// join conditions that reference that node must be rewritten.
pub fn apply_vlp_rewrites(joins: &mut [Join], plan_ctx: &PlanCtx) {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{PropertyAccess, TableAlias};

    for join in joins.iter_mut() {
        for condition in &mut join.joining_on {
            for operand in &mut condition.operands {
                if let LogicalExpr::PropertyAccessExp(pa) = operand {
                    let alias = pa.table_alias.0.clone();
                    let col = match &pa.column {
                        PropertyValue::Column(c) => c.clone(),
                        _ => continue,
                    };
                    let (new_alias, new_col) = plan_ctx.get_vlp_join_reference(&alias, &col);
                    if new_alias != alias || new_col != col {
                        log::debug!(
                            "🔄 VLP rewrite: {}.{} → {}.{}",
                            alias, col, new_alias, new_col
                        );
                        *operand = LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(new_alias),
                            column: PropertyValue::Column(new_col),
                        });
                    }
                }
            }
        }
    }
}

// =============================================================================
// Step 6: Register denormalized aliases (side effects)
// =============================================================================

/// Register denormalized alias information in plan_ctx based on JoinStrategy.
/// This is the ONLY place side effects happen — separated from join generation.
pub fn register_denormalized_aliases(
    ctx: &PatternSchemaContext,
    left_alias: &str,
    rel_alias: &str,
    right_alias: &str,
    plan_ctx: &mut PlanCtx,
) {
    let rel_type = ctx.rel_types.first().cloned().unwrap_or_default();

    match &ctx.join_strategy {
        JoinStrategy::SingleTableScan { .. } => {
            // Both nodes embedded in edge table
            register_if_embedded(&ctx.left_node, left_alias, rel_alias, &rel_type, plan_ctx);
            register_if_embedded(&ctx.right_node, right_alias, rel_alias, &rel_type, plan_ctx);
        }
        JoinStrategy::MixedAccess { joined_node, .. } => {
            // One node is embedded
            let (embedded_strategy, embedded_alias) = match joined_node {
                NodePosition::Right => (&ctx.left_node, left_alias),
                NodePosition::Left => (&ctx.right_node, right_alias),
            };
            register_if_embedded(embedded_strategy, embedded_alias, rel_alias, &rel_type, plan_ctx);
        }
        JoinStrategy::EdgeToEdge { .. } => {
            // Both nodes embedded in edge
            register_if_embedded(&ctx.left_node, left_alias, rel_alias, &rel_type, plan_ctx);
            register_if_embedded(&ctx.right_node, right_alias, rel_alias, &rel_type, plan_ctx);
        }
        JoinStrategy::CoupledSameRow { unified_alias } => {
            // Both nodes embedded, but on the UNIFIED alias (not rel_alias)
            register_if_embedded(&ctx.left_node, left_alias, unified_alias, &rel_type, plan_ctx);
            register_if_embedded(&ctx.right_node, right_alias, unified_alias, &rel_type, plan_ctx);
        }
        JoinStrategy::FkEdgeJoin { join_side, .. } => {
            // Relationship is embedded on the anchor node table
            let anchor_alias = match join_side {
                NodePosition::Left => right_alias,   // Right is anchor
                NodePosition::Right => left_alias,   // Left is anchor
            };
            plan_ctx.register_denormalized_alias(
                rel_alias.to_string(),
                anchor_alias.to_string(),
                false,
                String::new(),
                rel_type,
            );
        }
        JoinStrategy::Traditional { .. } => {
            // No denormalized aliases for traditional pattern
        }
    }
}

/// Helper: register a node as denormalized if it's EmbeddedInEdge
fn register_if_embedded(
    strategy: &NodeAccessStrategy,
    node_alias: &str,
    edge_alias: &str,
    rel_type: &str,
    plan_ctx: &mut PlanCtx,
) {
    if let NodeAccessStrategy::EmbeddedInEdge { is_from_node, .. } = strategy {
        plan_ctx.register_denormalized_alias(
            node_alias.to_string(),
            edge_alias.to_string(),
            *is_from_node,
            String::new(),
            rel_type.to_string(),
        );
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Extract the ID column from a node that must be OwnTable.
fn own_table_id(strategy: &NodeAccessStrategy, context: &str) -> AnalyzerResult<Identifier> {
    match strategy {
        NodeAccessStrategy::OwnTable { id_column, .. } => Ok(id_column.clone()),
        _ => Err(AnalyzerError::OptimizerError {
            message: format!("{} requires OwnTable node, got {:?}", context, strategy),
        }),
    }
}

/// Get the first column name from an Identifier.
fn first_col(id: &Identifier) -> String {
    match id {
        Identifier::Single(col) => col.clone(),
        Identifier::Composite(cols) => cols.first().cloned().unwrap_or_default(),
    }
}

/// Extract all table aliases referenced in a join's ON conditions.
/// The join's own alias is excluded — these are its DEPENDENCIES.
fn extract_join_dependencies(join: &Join) -> HashSet<String> {
    let mut refs = HashSet::new();
    for condition in &join.joining_on {
        collect_aliases_from_expr_list(&condition.operands, &mut refs);
    }
    refs.remove(&join.table_alias);
    refs
}

/// Extract aliases from a single ON condition (for redistribution).
fn extract_aliases_from_condition(
    condition: &crate::query_planner::logical_expr::OperatorApplication,
) -> HashSet<String> {
    let mut refs = HashSet::new();
    collect_aliases_from_expr_list(&condition.operands, &mut refs);
    refs
}

/// Recursively collect table aliases from expressions.
fn collect_aliases_from_expr_list(exprs: &[LogicalExpr], refs: &mut HashSet<String>) {
    for expr in exprs {
        collect_aliases_from_expr(expr, refs);
    }
}

fn collect_aliases_from_expr(expr: &LogicalExpr, refs: &mut HashSet<String>) {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            refs.insert(pa.table_alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            collect_aliases_from_expr_list(&op.operands, refs);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topo_sort_linear_chain() {
        // a (FROM) → r (deps: a) → b (deps: r)
        let joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "user_id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "user_id", "r", "to_id")
                .build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(order, vec!["a", "r", "b"]);
    }

    #[test]
    fn test_topo_sort_out_of_order() {
        // Given in reverse: b, r, a(FROM)
        let joins = vec![
            JoinBuilder::new("users", "b")
                .add_condition("b", "user_id", "r", "to_id")
                .build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "user_id")
                .build(),
            JoinBuilder::from_marker("users", "a").build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(order, vec!["a", "r", "b"]);
    }

    #[test]
    fn test_topo_sort_diamond() {
        // a(FROM) → r1(deps:a) → b(deps:r1), a → r2(deps:a,b — redistributed)
        let joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "a", "id")
                .add_condition("r2", "to_id", "b", "id")
                .build(),
        ];

        let sorted = topo_sort_joins(joins, &HashSet::new()).unwrap();
        let order: Vec<&str> = sorted.iter().map(|j| j.table_alias.as_str()).collect();
        // r2 must come after both a and b
        let a_pos = order.iter().position(|&x| x == "a").unwrap();
        let b_pos = order.iter().position(|&x| x == "b").unwrap();
        let r2_pos = order.iter().position(|&x| x == "r2").unwrap();
        assert!(r2_pos > a_pos);
        assert!(r2_pos > b_pos);
    }

    #[test]
    fn test_topo_sort_unresolvable() {
        // Circular: r depends on b, b depends on r, no FROM
        let joins = vec![
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "b", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r", "to_id")
                .build(),
        ];

        let result = topo_sort_joins(joins, &HashSet::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_collect_with_dedup_shared_node() {
        // Pattern 1: FROM a, JOIN r1 ON r1.from=a.id, JOIN b ON b.id=r1.to
        // Pattern 2: FROM b (duplicate), JOIN r2 ON r2.from=b.id, JOIN c ON c.id=r2.to
        let mut all = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
        ];

        let new = vec![
            JoinBuilder::from_marker("users", "b").build(),   // duplicate
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "b", "id")
                .build(),
            JoinBuilder::new("users", "c")
                .add_condition("c", "id", "r2", "to_id")
                .build(),
        ];

        collect_with_dedup(&mut all, new);
        let aliases: Vec<&str> = all.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(aliases, vec!["a", "r1", "b", "r2", "c"]);

        // Verify b's duplicate FROM marker was skipped (no extra conditions on r2)
        let r2 = all.iter().find(|j| j.table_alias == "r2").unwrap();
        assert_eq!(r2.joining_on.len(), 1); // only r2.from=b.id
    }

    #[test]
    fn test_collect_with_dedup_both_sides_bound() {
        // Pattern 1: FROM a, JOIN r1, JOIN b
        // Pattern 2: FROM a (dup), JOIN r2 ON r2.from=a.id, JOIN b ON b.id=r2.to (dup)
        // b's condition should be redistributed to r2
        let mut all = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r1")
                .add_condition("r1", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r1", "to_id")
                .build(),
        ];

        let new = vec![
            JoinBuilder::from_marker("users", "a").build(),   // duplicate
            JoinBuilder::new("follows", "r2")
                .add_condition("r2", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")                    // duplicate with conditions
                .add_condition("b", "id", "r2", "to_id")
                .build(),
        ];

        collect_with_dedup(&mut all, new);
        let aliases: Vec<&str> = all.iter().map(|j| j.table_alias.as_str()).collect();
        assert_eq!(aliases, vec!["a", "r1", "b", "r2"]);

        // r2 should have 2 conditions: r2.from=a.id AND r2.to=b.id (redistributed)
        let r2 = all.iter().find(|j| j.table_alias == "r2").unwrap();
        assert_eq!(r2.joining_on.len(), 2);
    }

    #[test]
    fn test_select_anchor_prefers_inner() {
        let joins = vec![
            JoinBuilder::from_marker("t1", "a").join_type(JoinType::Left).build(),
            JoinBuilder::from_marker("t2", "b").join_type(JoinType::Inner).build(),
        ];
        assert_eq!(select_anchor(&joins), Some("b".to_string()));
    }

    #[test]
    fn test_apply_optional_marking() {
        let mut joins = vec![
            JoinBuilder::from_marker("users", "a").build(),
            JoinBuilder::new("follows", "r")
                .add_condition("r", "from_id", "a", "id")
                .build(),
            JoinBuilder::new("users", "b")
                .add_condition("b", "id", "r", "to_id")
                .build(),
        ];
        let optional: HashSet<String> = ["r", "b"].iter().map(|s| s.to_string()).collect();
        apply_optional_marking(&mut joins, &optional);

        assert_ne!(joins[0].join_type, JoinType::Left); // a stays Inner
        assert_eq!(joins[1].join_type, JoinType::Left); // r marked Left
        assert_eq!(joins[2].join_type, JoinType::Left); // b marked Left
    }
}
