//! Properties Builder Module
//!
//! This module handles extraction of property information from logical plans.
//! It resolves property mappings for nodes and relationships, handling
//! denormalized schemas and table alias resolution.

use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::plan_ctx::PlanCtx;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::plan_builder_utils::extract_sorted_properties;

/// Result type for properties builder operations
pub type PropertiesBuilderResult<T> = Result<T, RenderBuildError>;

/// `(properties, optional table alias override)` returned by property lookups.
/// `properties` is a list of `(cypher_name, column_name)` pairs; the optional alias is
/// `Some(rel_alias)` for denormalized nodes whose properties live on a relationship table.
pub type ResolvedProperties = (Vec<(String, String)>, Option<String>);

/// Trait for extracting property information from logical plans
pub trait PropertiesBuilder {
    /// Get all properties for an alias, returning both properties and the actual table alias to use.
    /// For denormalized nodes, the table alias is the relationship alias (not the node alias).
    /// Returns: (properties, actual_table_alias) where actual_table_alias is None to use the original alias
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> PropertiesBuilderResult<ResolvedProperties>;

    /// Get properties with PlanCtx for coupled edge alias resolution.
    /// For coupled edges, uses the unified_alias from PatternSchemaContext.
    fn get_properties_with_plan_ctx(
        &self,
        alias: &str,
        plan_ctx: Option<&PlanCtx>,
    ) -> PropertiesBuilderResult<ResolvedProperties>;
}

impl PropertiesBuilder for LogicalPlan {
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> PropertiesBuilderResult<ResolvedProperties> {
        match self {
            LogicalPlan::GraphNode(node) => {
                // Check if this node's alias matches
                if node.alias != alias {
                    return Ok((vec![], None));
                }

                // FAST PATH: Use pre-computed projected_columns if available
                // (populated by ProjectedColumnsResolver analyzer pass)
                if let Some(projected_cols) = &node.projected_columns {
                    log::info!("🔍 get_properties_with_table_alias: GraphNode '{}' using projected_columns ({} entries): {:?}",
                        alias, projected_cols.len(), projected_cols);
                    // projected_columns format: Vec<(property_name, qualified_column)>
                    // e.g., [("firstName", "p.first_name"), ("age", "p.age")]
                    // We need to return unqualified column names: ("firstName", "first_name")
                    let properties: Vec<(String, String)> = projected_cols
                        .iter()
                        .map(|(prop_name, qualified_col)| {
                            // Extract unqualified column: "p.first_name" -> "first_name"
                            // 🔧 FIX: Handle column names with multiple dots like "n.id.orig_h" -> "id.orig_h"
                            // Use splitn(2) to split only on the FIRST dot, keeping the rest intact
                            let unqualified = qualified_col
                                .split_once('.')
                                .map(|x| x.1)
                                .unwrap_or(qualified_col)
                                .to_string();
                            (prop_name.clone(), unqualified)
                        })
                        .collect();
                    return Ok((properties, None));
                }

                // FALLBACK: Compute from ViewScan (for nodes without projected_columns)
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    log::debug!("get_properties_with_table_alias: GraphNode '{}' has ViewScan, is_denormalized={}, from_node_properties={:?}, to_node_properties={:?}",
                        alias, crate::graph_catalog::pattern_schema::scan_denormalized_flag(scan),
                        scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()));
                    // For denormalized nodes with properties on the ViewScan (from standalone node query)
                    if crate::graph_catalog::pattern_schema::scan_denormalized_flag(scan) {
                        if let Some(from_props) = &scan.from_node_properties {
                            let properties = extract_sorted_properties(from_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} from_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None)); // Use original alias
                            }
                        }
                        if let Some(to_props) = &scan.to_node_properties {
                            let properties = extract_sorted_properties(to_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} to_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    } else {
                        // For non-denormalized nodes, properties come from the node table itself
                        // This happens when we have a standalone node query like MATCH (n) RETURN n
                        if let Some(from_props) = &scan.from_node_properties {
                            let properties = extract_sorted_properties(from_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} from_node_properties for non-denormalized '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                    // Standard nodes - try property_mapping first
                    let mut properties = extract_sorted_properties(&scan.property_mapping);

                    // ZEEK FIX: If property_mapping is empty, try from_node_properties (for coupled edge schemas)
                    if properties.is_empty() {
                        if let Some(from_props) = &scan.from_node_properties {
                            properties = extract_sorted_properties(from_props);
                        }
                        if properties.is_empty() {
                            if let Some(to_props) = &scan.to_node_properties {
                                properties = extract_sorted_properties(to_props);
                            }
                        }
                    }
                    return Ok((properties, None));
                } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                    // For denormalized polymorphic nodes, the input is a UNION of
                    // ViewScans, each with its own per-role property map (see
                    // `graph_catalog::pattern_schema::edge_side_node_properties`).
                    //
                    // #564: iterate ALL branches (first match wins), not just the FIRST —
                    // mirroring `cte_extraction::get_node_label_for_alias`'s identical
                    // Union case (same "first-match across all branches" shape, with the
                    // same documented caveat). Both helpers rest on the same implicit
                    // assumption — "every UNION branch shares the same alias schema" —
                    // true for every real fixture today (multi-table label, denormalized
                    // from/to dimension, and polymorphic UNIONs all carry the SAME
                    // properties for a given alias across branches), so this is a
                    // consistency/hardening fix, not a behavior change for any existing
                    // shape: it only matters if a future branch's FIRST ViewScan happens
                    // to expose fewer properties (e.g. an empty `WHERE false` placeholder
                    // arm) than a later branch — previously that starved the caller of
                    // real properties entirely; now it falls through to a branch that
                    // actually has them.
                    log::debug!(
                        "get_properties_with_table_alias: GraphNode '{}' has Union with {} inputs",
                        alias,
                        union_plan.inputs.len()
                    );
                    for branch_input in &union_plan.inputs {
                        if let LogicalPlan::ViewScan(scan) = branch_input.as_ref() {
                            // Try the from-role map first, then the to-role map.
                            for is_from_side in [true, false] {
                                if let Some(role_props) =
                                    crate::graph_catalog::pattern_schema::edge_side_node_properties(
                                        scan,
                                        is_from_side,
                                    )
                                {
                                    let properties = extract_sorted_properties(role_props);
                                    if !properties.is_empty() {
                                        log::debug!("get_properties_with_table_alias: Returning {} role-specific properties from UNION for '{}' (from_side={})", properties.len(), alias, is_from_side);
                                        return Ok((properties, None));
                                    }
                                }
                            }
                            // Fallback to property_mapping
                            let properties = extract_sorted_properties(&scan.property_mapping);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} property_mapping from UNION for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                }
                // If we reach here, no properties found for this GraphNode
                Ok((vec![], None))
            }
            LogicalPlan::GraphRel(rel) => {
                // Check if this relationship's alias matches
                if rel.alias == alias {
                    if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                        let mut properties = extract_sorted_properties(&scan.property_mapping);

                        // Add from_id and to_id columns for relationships
                        // These are required for RETURN r to expand correctly
                        // For composite IDs, expand into multiple columns
                        if let Some(ref from_id) = scan.from_id {
                            for (i, col) in from_id.columns().iter().enumerate() {
                                let key = if from_id.is_composite() {
                                    format!("from_id_{}", i + 1)
                                } else {
                                    "from_id".to_string()
                                };
                                properties.insert(i, (key, col.to_string()));
                            }
                        }
                        if let Some(ref to_id) = scan.to_id {
                            let offset = scan
                                .from_id
                                .as_ref()
                                .map(|id| id.columns().len())
                                .unwrap_or(0);
                            for (i, col) in to_id.columns().iter().enumerate() {
                                let key = if to_id.is_composite() {
                                    format!("to_id_{}", i + 1)
                                } else {
                                    "to_id".to_string()
                                };
                                properties.insert(offset + i, (key, col.to_string()));
                            }
                        }

                        return Ok((properties, None));
                    }
                }

                // For denormalized nodes, properties are in the relationship center's ViewScan
                // IMPORTANT: Direction affects which properties to use!
                // - Outgoing: left_connection → from_node_properties, right_connection → to_node_properties
                // - Incoming: left_connection → to_node_properties, right_connection → from_node_properties
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    crate::debug_println!("DEBUG GraphRel: alias='{}' checking left='{}', right='{}', rel_alias='{}', direction={:?}",
                        alias, rel.left_connection, rel.right_connection, rel.alias, rel.direction);
                    crate::debug_println!(
                        "DEBUG GraphRel: from_node_properties={:?}, to_node_properties={:?}",
                        scan.from_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>())
                    );

                    // Check if BOTH nodes are denormalized on this edge
                    let left_props_exist = scan.from_node_properties.is_some();
                    let right_props_exist = scan.to_node_properties.is_some();
                    let _both_nodes_denormalized = left_props_exist && right_props_exist;

                    // Check if alias matches left_connection
                    if alias == rel.left_connection {
                        log::info!("✅ Found left_connection match for '{}'", alias);
                        // left_connection is always FROM side (BidirectionalUnion swaps
                        // connections for Incoming, maintaining left=FROM invariant)
                        let props = &scan.from_node_properties;
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                log::info!(
                                    "🔍 VLP properties for '{}': {:?}",
                                    alias,
                                    properties.iter().map(|(k, _v)| k).collect::<Vec<_>>()
                                );
                                // 🔧 FIX: For OPTIONAL MATCH + VLP, if this is the anchor node (start node),
                                // use the ANCHOR TABLE's columns, not VLP CTE columns!
                                // The anchor node is in FROM clause, VLP CTE is LEFT JOINed.
                                // Detection: VLP + is_optional + left_connection (start node) matches this alias
                                log::info!("🔍 Checking OPTIONAL VLP: vlp={}, optional={}, left_connection='{}', alias='{}'", 
                                    rel.variable_length.is_some(), rel.is_optional.unwrap_or(false), rel.left_connection, alias);
                                if rel.variable_length.is_some()
                                    && rel.is_optional.unwrap_or(false)
                                    && rel.left_connection == alias
                                {
                                    log::info!(
                                        "🎯 OPTIONAL VLP: anchor node '{}' - fetching from ANCHOR GraphNode (not VLP CTE)",
                                        alias
                                    );
                                    // For anchor node: Get properties from the anchor GraphNode's ViewScan
                                    // NOT from the VLP denormalized properties (which have start_/end_ prefixes)
                                    if let LogicalPlan::GraphNode(anchor_node) = rel.left.as_ref() {
                                        if let LogicalPlan::ViewScan(anchor_scan) =
                                            anchor_node.input.as_ref()
                                        {
                                            // Get properties from the anchor table's ViewScan
                                            let anchor_properties = extract_sorted_properties(
                                                &anchor_scan.property_mapping,
                                            );
                                            log::info!(
                                                "✓ OPTIONAL VLP: Found {} properties from anchor table '{}': {:?}",
                                                anchor_properties.len(),
                                                anchor_scan.source_table,
                                                anchor_properties.iter().map(|(k, _)| k).collect::<Vec<_>>()
                                            );
                                            // Return None for table_alias so PropertyAccessExp uses the node's original alias (e.g., 'a')
                                            return Ok((anchor_properties, None));
                                        }
                                    }
                                    log::warn!("⚠️ OPTIONAL VLP: Could not find anchor GraphNode, falling through");
                                }
                                // 🔧 FIX: For VLP patterns, endpoint node properties should NOT use the relationship alias!
                                // VLP rewrite will handle mapping to CTE columns (t.start_city, t.end_city)
                                // For non-VLP denormalized edges, use relationship alias as before
                                if rel.variable_length.is_some() {
                                    log::info!(
                                        "🔍 VLP Pattern: left_connection '{}' properties will be resolved by VLP rewrite (not using rel.alias '{}')",
                                        alias, rel.alias
                                    );
                                    // Return None for table_alias so PropertyAccessExp keeps the original node alias
                                    // The VLP rewrite function will later map it to the correct CTE column
                                    return Ok((properties, None));
                                } else {
                                    // Non-VLP: For denormalized nodes, properties are stored on the edge table
                                    // The edge table is aliased as rel.alias in the FROM clause
                                    // BUT for coupled edges, check task-local context for unified_alias
                                    let edge_alias =
                                        crate::render_plan::get_denormalized_alias_mapping(alias)
                                            .unwrap_or_else(|| rel.alias.clone());
                                    return Ok((properties, Some(edge_alias)));
                                }
                            }
                        }
                    }
                    // Check if alias matches right_connection
                    if alias == rel.right_connection {
                        // right_connection is always TO side (BidirectionalUnion swaps
                        // connections for Incoming, maintaining right=TO invariant)
                        let props = &scan.to_node_properties;
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                // 🔧 FIX: For VLP patterns, endpoint node properties should NOT use the relationship alias!
                                // VLP rewrite will handle mapping to CTE columns (t.start_city, t.end_city)
                                // For non-VLP denormalized edges, use relationship alias as before
                                if rel.variable_length.is_some() {
                                    log::info!(
                                        "🔍 VLP Pattern: right_connection '{}' properties will be resolved by VLP rewrite (not using rel.alias '{}')",
                                        alias, rel.alias
                                    );
                                    // Return None for table_alias so PropertyAccessExp keeps the original node alias
                                    // The VLP rewrite function will later map it to the correct CTE column
                                    return Ok((properties, None));
                                } else {
                                    // Non-VLP: For fully denormalized edges (both nodes on edge), use relationship alias
                                    // because the edge table is aliased with rel.alias in the FROM clause
                                    // BUT for coupled edges, check task-local context for unified_alias
                                    let edge_alias =
                                        crate::render_plan::get_denormalized_alias_mapping(alias)
                                            .unwrap_or_else(|| rel.alias.clone());
                                    return Ok((properties, Some(edge_alias)));
                                }
                            }
                        }
                    }
                }

                // Check left and right branches
                // IMPORTANT: Only accept non-empty results to ensure we check all branches
                if let Ok(result) = rel.left.get_properties_with_table_alias(alias) {
                    if !result.0.is_empty() {
                        return Ok(result);
                    }
                }

                if let Ok(result) = rel.right.get_properties_with_table_alias(alias) {
                    if !result.0.is_empty() {
                        return Ok(result);
                    }
                }

                if let Ok(result) = rel.center.get_properties_with_table_alias(alias) {
                    if !result.0.is_empty() {
                        return Ok(result);
                    }
                }

                // If we reach here, no properties found in this GraphRel
                log::info!(
                    "   ⚠️ GraphRel: No properties found for alias '{}' in any branch",
                    alias
                );
                Ok((vec![], None))
            }
            LogicalPlan::Projection(proj) => proj.input.get_properties_with_table_alias(alias),
            LogicalPlan::Filter(filter) => filter.input.get_properties_with_table_alias(alias),
            LogicalPlan::GroupBy(gb) => gb.input.get_properties_with_table_alias(alias),
            LogicalPlan::GraphJoins(joins) => joins.input.get_properties_with_table_alias(alias),
            LogicalPlan::OrderBy(order) => order.input.get_properties_with_table_alias(alias),
            LogicalPlan::Skip(skip) => skip.input.get_properties_with_table_alias(alias),
            LogicalPlan::Limit(limit) => limit.input.get_properties_with_table_alias(alias),
            LogicalPlan::Union(union) => {
                // For UNION, check all branches and return the first successful result.
                // All branches should have the same schema, so any match is valid, even if it
                // currently has no properties (empty vector).
                if let Some(first_input) = union.inputs.first() {
                    if let Ok(result) = first_input.get_properties_with_table_alias(alias) {
                        return Ok(result);
                    }
                }
                Ok((vec![], None)) // No properties found in any branch
            }
            LogicalPlan::CartesianProduct(cp) => {
                // For CartesianProduct, search both branches and return the first match
                // This mirrors the UNION behavior but for exactly two inputs.
                // If the alias is not found in either branch, return no properties.
                // 🔧 CRITICAL FIX (Jan 24, 2026): Only return from left branch if properties found!
                // Previously, we returned immediately even with empty Vec, preventing right branch search.
                if let Ok((props, table_alias)) = cp.left.get_properties_with_table_alias(alias) {
                    if !props.is_empty() {
                        log::debug!("🔍 CartesianProduct: Found alias '{}' in LEFT branch with {} properties", alias, props.len());
                        return Ok((props, table_alias));
                    }
                }
                if let Ok((props, table_alias)) = cp.right.get_properties_with_table_alias(alias) {
                    if !props.is_empty() {
                        log::debug!("🔍 CartesianProduct: Found alias '{}' in RIGHT branch with {} properties", alias, props.len());
                        return Ok((props, table_alias));
                    }
                }
                log::debug!(
                    "🔍 CartesianProduct: Alias '{}' not found in either branch",
                    alias
                );
                Ok((vec![], None)) // No properties found
            }
            LogicalPlan::Unwind(unwind) => {
                // Delegate property resolution to the input of the UNWIND.
                // This ensures that aliases defined upstream can still be resolved
                // even when wrapped in an UNWIND operation.
                //
                // NOTE: Additional handling for tuple-valued properties produced by
                // the UNWIND expression can be added here if needed, but this
                // preserves the recursive behavior from the previous implementation.
                unwind.input.get_properties_with_table_alias(alias)
            }
            LogicalPlan::WithClause(wc) => {
                // ✅ FIX (Phase 6): Handle WITH clauses for variable renaming
                // When we have `MATCH (u:User) WITH u AS person`, we need to:
                // 1. Check if `alias` is in the exported_aliases
                // 2. If yes, find the corresponding source alias in items
                // 3. Delegate to input to get properties for source alias

                if wc.exported_aliases.contains(&alias.to_string()) {
                    // Find the source alias for this exported alias by looking at items
                    for item in &wc.items {
                        if let Some(col_alias) = &item.col_alias {
                            if col_alias.0 == alias {
                                // This is the item that produces this exported alias
                                // Try to extract the source alias
                                if let LogicalExpr::TableAlias(ta) = &item.expression {
                                    // Simple variable reference like WITH u AS person
                                    return wc.input.get_properties_with_table_alias(&ta.0);
                                }
                            }
                        }
                    }
                }

                // If not found in WITH, delegate to input
                wc.input.get_properties_with_table_alias(alias)
            }
            _ => Ok((vec![], None)), // No properties found
        }
    }

    fn get_properties_with_plan_ctx(
        &self,
        alias: &str,
        plan_ctx: Option<&PlanCtx>,
    ) -> PropertiesBuilderResult<ResolvedProperties> {
        // First, get properties using the standard method
        let (properties, initial_table_alias) = self.get_properties_with_table_alias(alias)?;

        // If no PlanCtx or no table alias override needed, return as-is
        let Some(ctx) = plan_ctx else {
            return Ok((properties, initial_table_alias));
        };

        // If properties exist and we have a table alias from denormalized node,
        // check PlanCtx for the correct edge_alias (for coupled edges)
        if !properties.is_empty() {
            if let Some(ref table_alias) = initial_table_alias {
                // Check if PlanCtx has a different edge_alias for this node
                // This happens for coupled edges where we need the unified_alias
                if let Some(strategy) = ctx.get_node_strategy(alias, None) {
                    if let Some(correct_alias) = strategy.property_source_alias() {
                        if correct_alias != table_alias {
                            log::info!(
                                "🔧 get_properties_with_plan_ctx: Coupled edge fix - alias '{}' using edge_alias '{}' (was '{}')",
                                alias, correct_alias, table_alias
                            );
                            return Ok((properties, Some(correct_alias.to_string())));
                        }
                    }
                }
            }
        }

        Ok((properties, initial_table_alias))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_plan::{GraphNode, Union, UnionType, ViewScan};
    use std::sync::Arc;

    fn empty_view_scan(source_table: &str) -> ViewScan {
        ViewScan::new(
            source_table.to_string(),
            None,
            std::collections::HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        )
    }

    /// #564 hardening: `get_properties_with_table_alias`'s `Union` case must
    /// find properties on a LATER branch when the FIRST branch's `ViewScan`
    /// has none — mirroring `cte_extraction::get_node_label_for_alias`'s
    /// identical "search every branch" Union case. Before this fix, the code
    /// only ever inspected `union_plan.inputs.first()`; a first branch with
    /// no properties (e.g. an empty placeholder arm) would starve the caller
    /// of properties a later branch legitimately has, even though no real
    /// fixture in the corpus currently produces this exact shape (a
    /// documented, not-currently-observed asymmetry — see #564).
    #[test]
    fn union_branch_property_lookup_checks_all_branches_not_just_first_564() {
        // First branch: no denormalized properties at all (simulates an
        // empty/placeholder arm).
        let first_branch = empty_view_scan("db.empty_arm");

        // Second branch: real denormalized from_node_properties.
        let mut from_props = std::collections::HashMap::new();
        from_props.insert(
            "code".to_string(),
            crate::graph_catalog::expression_parser::PropertyValue::Column(
                "origin_code".to_string(),
            ),
        );
        let mut second_branch = empty_view_scan("db.real_arm");
        second_branch.is_denormalized = true;
        second_branch.from_node_properties = Some(from_props);

        let union_plan = LogicalPlan::Union(Union {
            inputs: vec![
                Arc::new(LogicalPlan::ViewScan(Arc::new(first_branch))),
                Arc::new(LogicalPlan::ViewScan(Arc::new(second_branch))),
            ],
            union_type: UnionType::All,
            is_cypher_union: false,
        });

        let graph_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(union_plan),
            alias: "n".to_string(),
            label: Some("Airport".to_string()),
            is_denormalized: true,
            projected_columns: None,
            node_types: None,
        });

        let (properties, table_alias) = graph_node
            .get_properties_with_table_alias("n")
            .expect("lookup should succeed");

        assert_eq!(
            properties,
            vec![("code".to_string(), "origin_code".to_string())],
            "#564: must find the SECOND branch's properties when the first \
             branch has none, instead of stopping at the first branch"
        );
        assert_eq!(table_alias, None);
    }
}
