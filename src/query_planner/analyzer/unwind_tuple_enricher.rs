use crate::query_planner::logical_expr::{AggregateFnCall, LogicalExpr};
use crate::query_planner::logical_plan::{LogicalPlan, Unwind};
/// Analyzer pass to enrich Unwind nodes with tuple structure metadata
///
/// When UNWIND is used after collect(node), we need to track the tuple structure
/// so that property access like `user.name` can be mapped to the correct tuple index.
///
/// Example:
/// ```cypher
/// MATCH (u:User) WITH u, collect(u) as users
/// UNWIND users as user
/// RETURN user.name
/// ```
///
/// The collect(u) expands to groupArray(tuple(u.city, u.country, ..., u.user_id))
/// We need to track that tuple structure so `user.name` maps to `user.5` (index 5)
use std::sync::Arc;

/// Traverse the plan and enrich Unwind nodes with tuple_properties metadata
pub fn enrich_unwind_with_tuple_info(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    log::debug!(
        "🔍 UnwindTupleEnricher: Processing plan: {:?}",
        std::mem::discriminant(plan.as_ref())
    );
    match plan.as_ref() {
        LogicalPlan::Unwind(u) => {
            log::debug!(
                "🔍 UnwindTupleEnricher: Found Unwind node, alias={}, expression={:?}",
                u.alias,
                u.expression
            );
            // Check if this UNWIND references a collect() result
            let tuple_props = extract_tuple_properties_from_collect(&u.input, &u.expression);

            if tuple_props.is_some() {
                log::info!("✅ UnwindTupleEnricher: Successfully extracted tuple properties for alias '{}': {:?}", u.alias, tuple_props);
                // Create new Unwind node with tuple_properties set
                Arc::new(LogicalPlan::Unwind(Unwind {
                    input: enrich_unwind_with_tuple_info(u.input.clone()),
                    expression: u.expression.clone(),
                    alias: u.alias.clone(),
                    label: u.label.clone(),
                    tuple_properties: tuple_props,
                }))
            } else {
                log::warn!("⚠️  UnwindTupleEnricher: Could not find tuple properties for alias '{}', expression: {:?}", u.alias, u.expression);
                // No tuple info found, just recurse
                Arc::new(LogicalPlan::Unwind(Unwind {
                    input: enrich_unwind_with_tuple_info(u.input.clone()),
                    ..u.clone()
                }))
            }
        }
        LogicalPlan::WithClause(wc) => Arc::new(LogicalPlan::WithClause(
            crate::query_planner::logical_plan::WithClause {
            cte_name: None,
                input: enrich_unwind_with_tuple_info(wc.input.clone()),
                ..wc.clone()
            },
        )),
        LogicalPlan::Projection(p) => {
            log::debug!("🔍 UnwindTupleEnricher: Processing Projection, recursing into input");
            Arc::new(LogicalPlan::Projection(
                crate::query_planner::logical_plan::Projection {
                    input: enrich_unwind_with_tuple_info(p.input.clone()),
                    ..p.clone()
                },
            ))
        }
        LogicalPlan::Filter(f) => Arc::new(LogicalPlan::Filter(
            crate::query_planner::logical_plan::Filter {
                input: enrich_unwind_with_tuple_info(f.input.clone()),
                ..f.clone()
            },
        )),
        LogicalPlan::OrderBy(o) => Arc::new(LogicalPlan::OrderBy(
            crate::query_planner::logical_plan::OrderBy {
                input: enrich_unwind_with_tuple_info(o.input.clone()),
                ..o.clone()
            },
        )),
        LogicalPlan::Limit(l) => Arc::new(LogicalPlan::Limit(
            crate::query_planner::logical_plan::Limit {
                input: enrich_unwind_with_tuple_info(l.input.clone()),
                ..l.clone()
            },
        )),
        LogicalPlan::Skip(s) => Arc::new(LogicalPlan::Skip(
            crate::query_planner::logical_plan::Skip {
                input: enrich_unwind_with_tuple_info(s.input.clone()),
                ..s.clone()
            },
        )),
        LogicalPlan::GroupBy(g) => Arc::new(LogicalPlan::GroupBy(
            crate::query_planner::logical_plan::GroupBy {
                input: enrich_unwind_with_tuple_info(g.input.clone()),
                ..g.clone()
            },
        )),
        LogicalPlan::GraphNode(gn) => Arc::new(LogicalPlan::GraphNode(
            crate::query_planner::logical_plan::GraphNode {
                input: enrich_unwind_with_tuple_info(gn.input.clone()),
                ..gn.clone()
            },
        )),
        LogicalPlan::GraphRel(gr) => Arc::new(LogicalPlan::GraphRel(
            crate::query_planner::logical_plan::GraphRel {
                left: enrich_unwind_with_tuple_info(gr.left.clone()),
                ..gr.clone()
            },
        )),
        LogicalPlan::GraphJoins(gj) => Arc::new(LogicalPlan::GraphJoins(
            crate::query_planner::logical_plan::GraphJoins {
                input: enrich_unwind_with_tuple_info(gj.input.clone()),
                ..gj.clone()
            },
        )),
        LogicalPlan::Cte(cte) => {
            Arc::new(LogicalPlan::Cte(crate::query_planner::logical_plan::Cte {
                input: enrich_unwind_with_tuple_info(cte.input.clone()),
                ..cte.clone()
            }))
        }
        LogicalPlan::Union(u) => {
            let enriched_inputs: Vec<Arc<LogicalPlan>> = u
                .inputs
                .iter()
                .map(|input| enrich_unwind_with_tuple_info(input.clone()))
                .collect();
            Arc::new(LogicalPlan::Union(
                crate::query_planner::logical_plan::Union {
                    inputs: enriched_inputs,
                    ..u.clone()
                },
            ))
        }
        LogicalPlan::CartesianProduct(cp) => Arc::new(LogicalPlan::CartesianProduct(
            crate::query_planner::logical_plan::CartesianProduct {
                left: enrich_unwind_with_tuple_info(cp.left.clone()),
                right: enrich_unwind_with_tuple_info(cp.right.clone()),
                ..cp.clone()
            },
        )),
        // Leaf nodes - no children to recurse into
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => plan.clone(),
    }
}

/// Extract tuple property structure from a collect() expression in the plan
///
/// Searches for collect(alias) in the projection items of the input plan
/// and retrieves the property list that will be in the tuple
fn extract_tuple_properties_from_collect(
    plan: &Arc<LogicalPlan>,
    unwind_expr: &LogicalExpr,
) -> Option<Vec<(String, usize)>> {
    // The unwind_expr should reference a variable that was defined by collect()
    // Example: UNWIND users AS user -> unwind_expr is TableAlias("users")

    let unwind_var = match unwind_expr {
        LogicalExpr::TableAlias(alias) => {
            log::debug!(
                "🔍 UnwindTupleEnricher: UNWIND expression is TableAlias({})",
                alias.0
            );
            &alias.0
        }
        LogicalExpr::PropertyAccessExp(pa) => {
            log::debug!(
                "🔍 UnwindTupleEnricher: UNWIND expression is PropertyAccessExp({}.<prop>)",
                pa.table_alias.0
            );
            &pa.table_alias.0
        }
        _ => {
            log::warn!("⚠️  UnwindTupleEnricher: UNWIND expression is not TableAlias or PropertyAccessExp: {:?}", unwind_expr);
            return None;
        }
    };

    log::debug!(
        "🔍 UnwindTupleEnricher: Looking for collect() definition of '{}' in plan",
        unwind_var
    );
    // Search for this variable in the input plan's projection items
    let result = find_collect_tuple_structure(plan, unwind_var);
    if result.is_some() {
        log::info!(
            "✅ UnwindTupleEnricher: Found collect() definition for '{}'",
            unwind_var
        );
    } else {
        log::warn!(
            "⚠️  UnwindTupleEnricher: Could not find collect() definition for '{}'",
            unwind_var
        );
    }
    result
}

/// Search for collect(node) that created the array being unwound
/// Returns the tuple property structure with 1-based indices
fn find_collect_tuple_structure(
    plan: &Arc<LogicalPlan>,
    target_alias: &str,
) -> Option<Vec<(String, usize)>> {
    log::debug!(
        "🔍 find_collect_tuple_structure: Searching in plan type: {:?}, looking for '{}'",
        std::mem::discriminant(plan.as_ref()),
        target_alias
    );
    match plan.as_ref() {
        LogicalPlan::WithClause(wc) => {
            log::debug!(
                "🔍 find_collect_tuple_structure: Found WithClause, checking {} items",
                wc.items.len()
            );
            // Check if any items in WithClause is collect(node) with alias matching target
            for item in &wc.items {
                if let Some(col_alias) = &item.col_alias {
                    log::debug!(
                        "🔍 find_collect_tuple_structure:   Checking item with alias '{}'",
                        col_alias.0
                    );
                    if col_alias.0 == target_alias {
                        log::info!("✅ find_collect_tuple_structure: Found matching alias '{}', checking if it's collect()", target_alias);
                        // Check if this is a collect() aggregate
                        if let LogicalExpr::AggregateFnCall(AggregateFnCall { name, args }) =
                            &item.expression
                        {
                            log::debug!(
                                "🔍 find_collect_tuple_structure:   Expression is aggregate '{}'",
                                name
                            );
                            if name.to_lowercase() == "collect" && args.len() == 1 {
                                log::info!("✅ find_collect_tuple_structure: Found collect() with correct args");
                                if let LogicalExpr::TableAlias(alias) = &args[0] {
                                    log::info!("✅ find_collect_tuple_structure: collect({}) found! Getting properties", alias.0);
                                    // Found collect(node)! Get the properties for this node
                                    return get_tuple_property_mapping(plan, &alias.0);
                                }
                            }
                        }
                    }
                }
            }
            // Not in this WithClause, recurse
            log::debug!(
                "🔍 find_collect_tuple_structure: Not found in WithClause, recursing into input"
            );
            find_collect_tuple_structure(&wc.input, target_alias)
        }
        LogicalPlan::Projection(p) => {
            // Check if any projection item is collect(node) with alias matching target
            for item in &p.items {
                if let Some(col_alias) = &item.col_alias {
                    if col_alias.0 == target_alias {
                        // Check if this is a collect() aggregate
                        if let LogicalExpr::AggregateFnCall(AggregateFnCall { name, args }) =
                            &item.expression
                        {
                            if name.to_lowercase() == "collect" && args.len() == 1 {
                                if let LogicalExpr::TableAlias(alias) = &args[0] {
                                    // Found collect(node)! Get the properties for this node
                                    return get_tuple_property_mapping(plan, &alias.0);
                                }
                            }
                        }
                    }
                }
            }
            // Not in this projection, recurse
            find_collect_tuple_structure(&p.input, target_alias)
        }
        LogicalPlan::GroupBy(g) => find_collect_tuple_structure(&g.input, target_alias),
        LogicalPlan::Filter(f) => find_collect_tuple_structure(&f.input, target_alias),
        _ => None,
    }
}

/// Get the property-to-index mapping for a collected node
/// The order matches the order used in expand_collect_to_group_array
fn get_tuple_property_mapping(
    plan: &Arc<LogicalPlan>,
    node_alias: &str,
) -> Option<Vec<(String, usize)>> {
    log::debug!(
        "🔍 get_tuple_property_mapping: Getting properties for node_alias '{}'",
        node_alias
    );
    // Get properties from the node - same logic as get_properties_with_table_alias
    let properties = get_node_properties(plan, node_alias)?;

    log::info!(
        "✅ get_tuple_property_mapping: Found {} properties for '{}'",
        properties.len(),
        node_alias
    );
    // Create 1-based index mapping (tuple indices are 1-based in ClickHouse)
    // IMPORTANT: Use ClickHouse column names (col_name), not Cypher property names (prop_name)
    // because PropertyAccess expressions will have already been resolved to ClickHouse columns
    let mapping: Vec<(String, usize)> = properties
        .into_iter()
        .enumerate()
        .map(|(idx, (_prop_name, col_name))| {
            log::debug!(
                "🔍 get_tuple_property_mapping:   Column '{}' → index {}",
                col_name,
                idx + 1
            );
            (col_name, idx + 1)
        })
        .collect();

    log::info!(
        "✅ get_tuple_property_mapping: Created mapping with {} entries",
        mapping.len()
    );
    Some(mapping)
}

/// Simplified property extraction for a node alias
fn get_node_properties(plan: &Arc<LogicalPlan>, alias: &str) -> Option<Vec<(String, String)>> {
    log::debug!(
        "🔍 get_node_properties: Looking for alias '{}' in plan: {:?}",
        alias,
        std::mem::discriminant(plan.as_ref())
    );
    match plan.as_ref() {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            log::info!(
                "✅ get_node_properties: Found GraphNode with matching alias '{}'",
                alias
            );
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                log::debug!("🔍 get_node_properties: ViewScan found, extracting properties from property_mapping");
                let mut properties: Vec<(String, String)> = scan
                    .property_mapping
                    .iter()
                    .map(|(k, v)| (k.clone(), v.raw().to_string()))
                    .collect();
                properties.sort_by(|a, b| a.0.cmp(&b.0));
                return Some(properties);
            }
            None
        }
        LogicalPlan::WithClause(wc) => {
            log::debug!("🔍 get_node_properties: Recursing into WithClause input");
            get_node_properties(&wc.input, alias)
        }
        LogicalPlan::Projection(p) => get_node_properties(&p.input, alias),
        LogicalPlan::Filter(f) => get_node_properties(&f.input, alias),
        LogicalPlan::GroupBy(g) => get_node_properties(&g.input, alias),
        LogicalPlan::GraphNode(node) => get_node_properties(&node.input, alias),
        _ => {
            log::warn!(
                "⚠️  get_node_properties: No handler for plan type {:?}, returning None",
                std::mem::discriminant(plan.as_ref())
            );
            None
        }
    }
}
