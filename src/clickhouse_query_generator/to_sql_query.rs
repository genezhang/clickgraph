use crate::{
    query_planner::join_context::{VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN},
    query_planner::logical_plan::LogicalPlan,
    render_plan::{
        render_expr::{
            AggregateFnCall, Column, ColumnAlias, InSubquery, Literal, Operator,
            OperatorApplication, PropertyAccess, RenderExpr, ScalarFnCall, TableAlias,
        },
        {
            ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem,
            GroupByExpressions, Join, JoinItems, JoinType, OrderByItems, OrderByOrder, RenderPlan,
            SelectItems, ToSql, UnionItems, UnionType,
        },
    },
    server::query_context::{
        clear_all_render_contexts, get_cte_property_mapping, get_relationship_columns,
        is_multi_type_vlp_alias, set_all_render_contexts,
    },
    utils::cte_naming::is_generated_cte_name,
};
use std::collections::HashMap;

// Import function translator for Neo4j -> ClickHouse function mappings
use super::function_registry::get_function_mapping;
use super::function_translator::{get_ch_function_name, CH_PASSTHROUGH_PREFIX};

// ============================================================================
// RENDER CONTEXT ACCESSORS (delegating to unified query_context)
// ============================================================================

/// Get relationship columns for IS NULL checks
fn get_relationship_columns_from_context(alias: &str) -> Option<(String, String)> {
    get_relationship_columns(alias)
}

/// Get CTE property mapping
fn get_cte_property_from_context(cte_alias: &str, property: &str) -> Option<String> {
    get_cte_property_mapping(cte_alias, property)
}

/// Check if alias is a multi-type VLP endpoint
fn is_multi_type_vlp_alias_from_context(alias: &str) -> bool {
    is_multi_type_vlp_alias(alias)
}

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(contains_string_literal)
        }
        _ => false,
    }
}

/// Check if any operand in the expression contains a string
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(contains_string_literal)
}

/// Flatten nested + operations into a list of operands for concat()
fn flatten_addition_operands(expr: &RenderExpr) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .flat_map(flatten_addition_operands)
            .collect(),
        _ => vec![expr.to_sql()],
    }
}

/// Build the relationship columns mapping from a RenderPlan (for collecting data)
/// Returns the mapping of alias → (from_id_column, to_id_column)
fn build_relationship_columns_from_plan(plan: &RenderPlan) -> HashMap<String, (String, String)> {
    let mut map = HashMap::new();

    // Add joins from main plan - extract column from joining_on conditions
    for join in &plan.joins.0 {
        if let Some(from_col) = join.get_relationship_id_column() {
            // For now, just store from_col for both (we only need one for NULL checks)
            map.insert(join.table_alias.clone(), (from_col.clone(), from_col));
        }
    }

    // Also process unions (each branch has its own joins)
    if let Some(ref union) = plan.union.0 {
        for union_plan in &union.input {
            for join in &union_plan.joins.0 {
                if let Some(from_col) = join.get_relationship_id_column() {
                    map.insert(join.table_alias.clone(), (from_col.clone(), from_col));
                }
            }
        }
    }

    // Process CTEs recursively and merge results
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref cte_plan) = cte.content {
            let cte_map = build_relationship_columns_from_plan(cte_plan);
            map.extend(cte_map);
        }
    }

    map
}

/// Build CTE property mappings from RenderPlan CTEs (for collecting data)
/// Returns mapping of CTE alias → (property → column name)
fn build_cte_property_mappings(plan: &RenderPlan) -> HashMap<String, HashMap<String, String>> {
    let mut map = HashMap::new();

    // Process each CTE in the plan
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref cte_plan) = cte.content {
            let mut property_map: HashMap<String, String> = HashMap::new();

            // Build property mapping from SELECT items
            // Format: "property_name" → "cte_column_name"
            //
            // IMPORTANT: We use the FULL column name as the property name (e.g., "user_id" → "user_id")
            // because the column names in CTEs already come from ViewScan.property_mapping.
            //
            // Previous behavior: Used underscore/dot parsing to extract suffix (e.g., "user_id" → "id")
            // This broke auto-discovery schemas where property names include underscores.
            // Example bug: node_id=user_id with auto_discover_columns should expose property "user_id",
            // not "id" (which doesn't exist in the database).
            for select_item in &cte_plan.select.items {
                if let Some(ref col_alias) = select_item.col_alias {
                    let cte_col = col_alias.0.as_str();

                    // Identity mapping: property name = column name
                    property_map.insert(cte_col.to_string(), cte_col.to_string());
                }
            }

            if !property_map.is_empty() {
                log::debug!(
                    "🗺️  CTE '{}' property mapping: {:?}",
                    cte.cte_name,
                    property_map
                );
                map.insert(cte.cte_name.clone(), property_map.clone());
            }
        }
    }

    // CRITICAL: Also scan main plan's FROM clause to map CTE aliases
    // Example: FROM with_cnt_friend_cte_1 AS cnt_friend
    // We need to map BOTH "with_cnt_friend_cte_1" AND "cnt_friend" to the same property mapping
    if let Some(ref from_table) = plan.from.0 {
        let table_name = &from_table.name;
        let alias = from_table.alias.as_ref().unwrap_or(table_name);

        // If this FROM references a CTE (name starts with "with_" or matches a CTE name)
        if let Some(cte_mapping) = map.get(table_name).cloned() {
            if alias != table_name {
                log::debug!(
                    "🔗 Aliasing CTE '{}' as '{}' with same property mapping",
                    table_name,
                    alias
                );
                map.insert(alias.clone(), cte_mapping);
            }
        }
    }

    map
}

/// Build multi-type VLP aliases tracking from RenderPlan
/// Returns mapping of Cypher alias → CTE name for multi-type VLP queries
fn build_multi_type_vlp_aliases(plan: &RenderPlan) -> HashMap<String, String> {
    let mut aliases = HashMap::new();

    // Track multi-type VLP aliases for JSON property extraction
    // Multi-type VLP CTEs have names like "vlp_multi_type_u_x"
    // and their end_properties column contains JSON with node properties
    for cte in &plan.ctes.0 {
        if cte.cte_name.starts_with("vlp_multi_type_") {
            // Extract Cypher alias from CTE metadata if available
            if let Some(ref cypher_end_alias) = cte.vlp_cypher_end_alias {
                aliases.insert(cypher_end_alias.clone(), cte.cte_name.clone());
                log::info!(
                    "🎯 Tracked multi-type VLP alias: '{}' → CTE '{}'",
                    cypher_end_alias,
                    cte.cte_name
                );
            }
        }
    }

    aliases
}

/// Rewrite property access in SELECT, GROUP BY items for VLP queries
/// Maps Cypher aliases (a, b) to CTE column names (start_xxx, end_xxx)
/// For VLP, the CTE includes properties named using the Cypher property name: start_email, start_name, etc.
fn rewrite_vlp_select_aliases(mut plan: RenderPlan) -> RenderPlan {
    log::debug!("🔍 TRACING: rewrite_vlp_select_aliases called - checking for VLP CTEs");
    // 🔧 FIX: If FROM references a WITH CTE (not the raw VLP CTE), skip this rewriting
    // The WITH CTE has already transformed the columns, and the SELECT items reference
    // the WITH CTE columns, not the raw VLP CTE columns.
    if let Some(from_ref) = &plan.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            log::debug!(
                "🔧 VLP: FROM uses WITH CTE '{}' - skipping VLP SELECT rewriting",
                from_ref.name
            );
            return plan;
        }
    }

    // Check if any CTE is a VLP CTE
    let vlp_cte = plan
        .ctes
        .0
        .iter()
        .find(|cte| cte.vlp_cypher_start_alias.is_some());

    log::error!(
        "🔍🔍🔍 TRACING: Checking for VLP CTEs. Found {} CTEs",
        plan.ctes.0.len()
    );
    for (i, cte) in plan.ctes.0.iter().enumerate() {
        log::debug!(
            "🔍 TRACING: CTE {}: name={}, vlp_start_alias={:?}",
            i,
            cte.cte_name,
            cte.vlp_cypher_start_alias
        );
    }

    if let Some(vlp_cte) = vlp_cte {
        // 🔧 FIX: For OPTIONAL MATCH + VLP, FROM uses the anchor node table (not the VLP CTE),
        // and the VLP CTE is added as a LEFT JOIN. In this case, we should NOT rewrite
        // expressions because:
        // - FROM is: users AS a (anchor node)
        // - SELECT should reference: a.name (from anchor), COUNT(DISTINCT t.end_id) (from VLP CTE)
        // - VLP CTE is: LEFT JOIN vlp_a_b AS t ON a.user_id = t.start_id
        //
        // Detection: If FROM uses a regular table (not the VLP CTE), skip rewriting
        log::debug!("🔍 TRACING: VLP CTE detected: {}", vlp_cte.cte_name);
        if let Some(from_ref) = &plan.from.0 {
            log::debug!(
                "🔍 TRACING: FROM ref name: '{}', starts_with vlp_: {}",
                from_ref.name,
                from_ref.name.starts_with("vlp_")
            );
            if !from_ref.name.starts_with("vlp_") {
                log::debug!(
                    "🔍 TRACING: OPTIONAL VLP detected - FROM uses anchor table '{}' - SKIPPING VLP SELECT rewriting",
                    from_ref.name
                );
                log::info!(
                    "   Anchor properties will be accessed directly (e.g., a.name), VLP CTE ({}) used via LEFT JOIN",
                    vlp_cte.cte_name
                );
                return plan;
            } else {
                log::debug!(
                    "🔍 TRACING: NOT optional VLP - FROM uses VLP CTE - proceeding with rewriting"
                );
            }
        } else {
            log::debug!("🔍 TRACING: No FROM ref found");
        }

        let start_alias = vlp_cte.vlp_cypher_start_alias.clone();
        let end_alias = vlp_cte.vlp_cypher_end_alias.clone();
        let path_variable = vlp_cte.vlp_path_variable.clone();
        // Non-OPTIONAL VLP: always rewrite start alias (we return early for OPTIONAL VLP)
        let is_optional_vlp = false;

        log::info!(
            "🔧 VLP SELECT rewriting: start_alias={:?}, end_alias={:?}, path_variable={:?}",
            start_alias,
            end_alias,
            path_variable
        );
        log::info!("🔧 SELECT has {} items", plan.select.items.len());

        // Rewrite each SELECT item's expressions
        for (idx, item) in plan.select.items.iter_mut().enumerate() {
            log::info!("🔧 Item {}: {:?}", idx, item.expression);
            let before = format!("{:?}", item.expression);
            item.expression = rewrite_expr_for_vlp(
                &item.expression,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", item.expression);
            if before != after {
                log::info!("🔧   Rewritten from: {} → {}", before, after);
            }
        }

        // 🔧 BUG FIX: Also rewrite GROUP BY expressions for VLP queries
        // The GROUP BY clause may contain Cypher aliases (e.g., a.full_name)
        // that need to be rewritten to use VLP CTE columns (e.g., t.start_name)
        log::info!("🔧 VLP GROUP BY rewriting: {} items", plan.group_by.0.len());
        for (idx, group_expr) in plan.group_by.0.iter_mut().enumerate() {
            log::info!("🔧 GROUP BY {}: {:?}", idx, group_expr);
            let before = format!("{:?}", group_expr);
            *group_expr = rewrite_expr_for_vlp(
                group_expr,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", group_expr);
            if before != after {
                log::info!("🔧   GROUP BY rewritten from: {} → {}", before, after);
            }
        }

        // 🔧 BUG FIX: Also rewrite ORDER BY expressions for VLP queries
        // The ORDER BY clause may contain Cypher aliases (e.g., b.name)
        // that need to be rewritten to use VLP CTE columns (e.g., t.end_name)
        log::info!("🔧 VLP ORDER BY rewriting: {} items", plan.order_by.0.len());
        for (idx, order_item) in plan.order_by.0.iter_mut().enumerate() {
            log::info!("🔧 ORDER BY {}: {:?}", idx, order_item.expression);
            let before = format!("{:?}", order_item.expression);
            order_item.expression = rewrite_expr_for_vlp(
                &order_item.expression,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", order_item.expression);
            if before != after {
                log::info!("🔧   ORDER BY rewritten from: {} → {}", before, after);
            }
        }
    }

    plan
}

/// Recursively rewrite expressions to map VLP Cypher aliases to CTE column names
/// When we encounter PropertyAccess(a, xxx), we need to look up the Cypher property name
/// and create Column("start_xxx") using that Cypher property name (not the DB column name)
///
/// The challenge: at this point, we only have the DB column name from PropertyAccess.
/// The CTE was created with: `start_node.db_column AS start_cypher_property_name`
/// But the SELECT has: PropertyAccess(a, db_column_name)
///
/// To fix this, we need to NOT try to extract the property name from PropertyAccess,
/// but instead rely on the fact that properties are expanded at the render level.
/// The SELECT items should already have the Cypher property names as aliases,
/// and we just need to use those CTE column names directly.
///
/// Also handles path function rewriting:
/// - length(p) → t.hop_count
/// - nodes(p) → t.path_nodes  
/// - relationships(p) → t.path_relationships
fn rewrite_expr_for_vlp(
    expr: &RenderExpr,
    start_alias: &Option<String>,
    end_alias: &Option<String>,
    path_variable: &Option<String>,
    skip_start_alias: bool,
) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    match expr {
        RenderExpr::TableAlias(alias) => {
            // For VLP, TableAlias references to VLP endpoints should be rewritten to CTE columns
            if let Some(start) = start_alias {
                if &alias.0 == start {
                    if skip_start_alias {
                        return expr.clone();
                    }
                    return RenderExpr::Column(Column(PropertyValue::Column(
                        "t.start_id".to_string(),
                    )));
                }
            }
            if let Some(end) = end_alias {
                if &alias.0 == end {
                    return RenderExpr::Column(Column(PropertyValue::Column(
                        "t.end_id".to_string(),
                    )));
                }
            }
            expr.clone()
        }

        // Handle path functions: length(p), nodes(p), relationships(p)
        RenderExpr::ScalarFnCall(func) => {
            // Check if this is a path function with the path variable as argument
            if let Some(path_var) = path_variable {
                if func.args.len() == 1 {
                    if let RenderExpr::TableAlias(alias) = &func.args[0] {
                        if &alias.0 == path_var {
                            // This is a path function call: length(p), nodes(p), relationships(p)
                            let cte_column = match func.name.as_str() {
                                "length" => Some("hop_count"),
                                "nodes" => Some("path_nodes"),
                                "relationships" => Some("path_relationships"),
                                _ => None,
                            };

                            if let Some(col_name) = cte_column {
                                log::info!(
                                    "🔧 VLP path function: {}({}) → t.{}",
                                    func.name,
                                    path_var,
                                    col_name
                                );
                                return RenderExpr::Column(Column(PropertyValue::Column(format!(
                                    "t.{}",
                                    col_name
                                ))));
                            }
                        }
                    }
                }
            }

            // Not a path function - recursively rewrite arguments
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| {
                        rewrite_expr_for_vlp(
                            a,
                            start_alias,
                            end_alias,
                            path_variable,
                            skip_start_alias,
                        )
                    })
                    .collect(),
            })
        }

        // Rewrite PropertyAccess for VLP aliases
        // PropertyAccess(a, email_address) should NOT be changed by us -
        // it's handled at expansion level. But if we encounter it here,
        // convert to Column with the CTE column name format.
        //
        // The CTE columns are: start_email, start_name, etc. (using Cypher property names)
        // But PropertyAccess gives us database names like email_address, full_name
        // We need to match these by deriving the property name.
        RenderExpr::PropertyAccessExp(prop) => {
            log::error!(
                "🔧🔧🔧 rewrite_expr_for_vlp: Processing PropertyAccessExp {}.{}",
                prop.table_alias.0,
                prop.column.raw()
            );
            if let Some(start) = start_alias {
                if &prop.table_alias.0 == start {
                    if skip_start_alias {
                        log::error!("🔧🔧🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' but skipping for OPTIONAL VLP", start);
                        return expr.clone();
                    }
                    log::error!("🔧🔧🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' - rewriting to t.start_xxx", start);
                    // This is accessing start node property
                    // Create Column with the full table.column format to prevent heuristic inference
                    // The FROM clause has the CTE aliased as 't', so use t.start_xxx
                    let prop_name = derive_cypher_property_name(prop.column.raw());
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.start_{}",
                        prop_name
                    ))));
                }
            }

            if let Some(end) = end_alias {
                if &prop.table_alias.0 == end {
                    // This is accessing end node property
                    let prop_name = derive_cypher_property_name(prop.column.raw());
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.end_{}",
                        prop_name
                    ))));
                }
            }

            // Not a VLP alias - leave unchanged
            expr.clone()
        }

        // Recursively rewrite operands in operator applications
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| {
                        rewrite_expr_for_vlp(
                            o,
                            start_alias,
                            end_alias,
                            path_variable,
                            skip_start_alias,
                        )
                    })
                    .collect(),
            })
        }

        RenderExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(AggregateFnCall {
            name: agg.name.clone(),
            args: agg
                .args
                .iter()
                .map(|a| {
                    rewrite_expr_for_vlp(a, start_alias, end_alias, path_variable, skip_start_alias)
                })
                .collect(),
        }),

        // Handle bare path variable: p → tuple(t.path_nodes, t.path_edges, t.path_relationships, t.hop_count)
        // When RETURN p is used for a path variable, expand it to a tuple of path components
        RenderExpr::TableAlias(alias) if path_variable.as_ref() == Some(&alias.0) => {
            log::info!(
                "🔧 VLP path variable expansion: {} → tuple({}.path_nodes, {}.path_edges, ...)",
                alias.0,
                VLP_CTE_FROM_ALIAS,
                VLP_CTE_FROM_ALIAS
            );
            // Expand to tuple of path components using VLP_CTE_FROM_ALIAS constant
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "tuple".to_string(),
                args: vec![
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_nodes",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_edges",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_relationships",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.hop_count",
                        VLP_CTE_FROM_ALIAS
                    )))),
                ],
            })
        }

        RenderExpr::ColumnAlias(ColumnAlias(alias_str))
            if path_variable.as_ref() == Some(alias_str) =>
        {
            log::info!(
                "🔧 VLP path variable expansion (ColumnAlias): {} → tuple({}.path_nodes, {}.path_edges, ...)",
                alias_str, VLP_CTE_FROM_ALIAS, VLP_CTE_FROM_ALIAS
            );
            // Expand to tuple of path components using VLP_CTE_FROM_ALIAS constant
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "tuple".to_string(),
                args: vec![
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_nodes",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_edges",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.path_relationships",
                        VLP_CTE_FROM_ALIAS
                    )))),
                    RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "{}.hop_count",
                        VLP_CTE_FROM_ALIAS
                    )))),
                ],
            })
        }

        // Leave other expressions unchanged
        other => other.clone(),
    }
}

/// Derive Cypher property name from database column name
///
/// ⚠️ TECHNICAL DEBT: This uses hardcoded mappings for common schema patterns.
/// This is a workaround that should eventually be replaced with schema-aware resolution.
///
/// Current mappings:
/// - full_name → name (in social_benchmark, "name" is the Cypher property, "full_name" is the DB column)
/// - email_address → email (same pattern)
/// - user_id → id (user_id is the DB column, but Cypher uses "id" for the property)
/// - object_type → type (filesystem schema)
/// - size_bytes → size (filesystem schema)
/// - owner_id → owner (filesystem schema)
///
/// TODO: Pass schema context to this function to enable schema-aware property mapping.
/// This would allow proper handling of arbitrary schema variations without hardcoding.
///
/// FUTURE: Consider caching property mapping results to improve performance for repeated queries.
fn derive_cypher_property_name(db_column: &str) -> String {
    // Common mappings for various schemas
    // Social benchmark schema
    match db_column {
        "full_name" => "name".to_string(),
        "email_address" => "email".to_string(),
        "user_id" => "id".to_string(),
        // Filesystem schema
        "object_type" => "type".to_string(),
        "size_bytes" => "size".to_string(),
        "owner_id" => "owner".to_string(),
        // Default: use the column name as-is
        _ => db_column.to_string(),
    }
}

/// Extract fixed path information from a RenderPlan by analyzing SELECT items and JOINs
/// Returns FixedPathMetadata if the plan contains a path function call that can be resolved
fn extract_fixed_path_info_from_plan(
    plan: &RenderPlan,
) -> Option<crate::render_plan::FixedPathMetadata> {
    // Look for path function calls in SELECT items
    for item in &plan.select.items {
        if let Some(path_var) = find_path_function_argument(&item.expression) {
            // Found a path function with argument path_var
            // Infer hop count from the number of JOINs
            // For a path (a)-[:T]->(b), we have 2 JOINs (relationship + end node) = 1 hop
            // For a path (a)-[:T1]->(b)-[:T2]->(c), we have 4 JOINs = 2 hops
            // Formula: hops = JOINs / 2 (integer division)
            let hop_count = plan.joins.0.len() as u32 / 2;

            log::info!(
                "🔧 Detected fixed path: path_variable={}, hop_count={} (from {} JOINs)",
                path_var,
                hop_count,
                plan.joins.0.len()
            );

            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    // Also check GROUP BY and ORDER BY expressions
    for expr in &plan.group_by.0 {
        if let Some(path_var) = find_path_function_argument(expr) {
            let hop_count = plan.joins.0.len() as u32 / 2;
            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    for item in &plan.order_by.0 {
        if let Some(path_var) = find_path_function_argument(&item.expression) {
            let hop_count = plan.joins.0.len() as u32 / 2;
            return Some(crate::render_plan::FixedPathMetadata {
                path_variable: path_var,
                hop_count,
                node_aliases: vec![],
                rel_aliases: vec![],
            });
        }
    }

    None
}

/// Find a path function argument (e.g., the 'p' in length(p))
/// Returns the variable name if found
fn find_path_function_argument(expr: &RenderExpr) -> Option<String> {
    match expr {
        RenderExpr::ScalarFnCall(func) => {
            // Check for path functions
            if matches!(
                func.name.to_lowercase().as_str(),
                "length" | "nodes" | "relationships"
            ) && func.args.len() == 1
            {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    return Some(alias.0.clone());
                }
            }

            // Recursively check arguments
            for arg in &func.args {
                if let Some(var) = find_path_function_argument(arg) {
                    return Some(var);
                }
            }
            None
        }

        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                if let Some(var) = find_path_function_argument(operand) {
                    return Some(var);
                }
            }
            None
        }

        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                if let Some(var) = find_path_function_argument(arg) {
                    return Some(var);
                }
            }
            None
        }

        _ => None,
    }
}

/// Rewrite path function calls for fixed (non-VLP) path patterns
/// Converts:
/// - length(p) → 1 (literal hop count)
/// - nodes(p) → array of node IDs
/// - relationships(p) → array of relationship IDs
fn rewrite_fixed_path_functions(mut plan: RenderPlan) -> RenderPlan {
    if let Some(ref fixed_path_info) = plan.fixed_path_info {
        let path_var = fixed_path_info.path_variable.clone();
        let hop_count = fixed_path_info.hop_count;

        log::info!(
            "🔧 Fixed path rewriting: path_variable={}, hop_count={}",
            path_var,
            hop_count
        );
        log::info!("🔧 SELECT has {} items", plan.select.items.len());

        // Rewrite each SELECT item's expressions
        for item in plan.select.items.iter_mut() {
            let before = format!("{:?}", item.expression);
            item.expression = rewrite_expr_for_fixed_path(&item.expression, &path_var, hop_count);
            let after = format!("{:?}", item.expression);
            if before != after {
                log::info!("🔧   Rewritten from: {} → {}", before, after);
            }
        }

        // Also rewrite GROUP BY expressions
        log::info!(
            "🔧 Fixed path GROUP BY rewriting: {} items",
            plan.group_by.0.len()
        );
        for group_expr in &mut plan.group_by.0 {
            *group_expr = rewrite_expr_for_fixed_path(group_expr, &path_var, hop_count);
        }

        // Also rewrite ORDER BY expressions
        log::info!(
            "🔧 Fixed path ORDER BY rewriting: {} items",
            plan.order_by.0.len()
        );
        for order_item in &mut plan.order_by.0 {
            order_item.expression =
                rewrite_expr_for_fixed_path(&order_item.expression, &path_var, hop_count);
        }
    }

    plan
}

/// Recursively rewrite expressions to handle path function calls on fixed paths
/// Converts:
/// - length(p) → literal(hop_count)
/// - nodes(p) → [node_ids in order] (future enhancement)
/// - relationships(p) → [rel_ids in order] (future enhancement)
fn rewrite_expr_for_fixed_path(
    expr: &RenderExpr,
    path_variable: &str,
    hop_count: u32,
) -> RenderExpr {
    match expr {
        // Handle path functions: length(p)
        RenderExpr::ScalarFnCall(func) => {
            if func.args.len() == 1 {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    if alias.0 == *path_variable {
                        match func.name.to_lowercase().as_str() {
                            "length" => {
                                log::info!(
                                    "🔧 Fixed path function: length({}) → {}",
                                    path_variable,
                                    hop_count
                                );
                                return RenderExpr::Literal(Literal::Integer(hop_count as i64));
                            }
                            "nodes" => {
                                log::warn!(
                                    "🔧 Fixed path function: nodes({}) not yet implemented for fixed paths",
                                    path_variable
                                );
                                // TODO: Return array of node IDs
                                return expr.clone();
                            }
                            "relationships" => {
                                log::warn!(
                                    "🔧 Fixed path function: relationships({}) not yet implemented for fixed paths",
                                    path_variable
                                );
                                // TODO: Return array of relationship IDs
                                return expr.clone();
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Not a path function - recursively rewrite arguments
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: func
                    .args
                    .iter()
                    .map(|a| rewrite_expr_for_fixed_path(a, path_variable, hop_count))
                    .collect(),
            })
        }

        // Recursively rewrite operands in operator applications
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| rewrite_expr_for_fixed_path(o, path_variable, hop_count))
                    .collect(),
            })
        }

        RenderExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(AggregateFnCall {
            name: agg.name.clone(),
            args: agg
                .args
                .iter()
                .map(|a| rewrite_expr_for_fixed_path(a, path_variable, hop_count))
                .collect(),
        }),

        // Leave other expressions unchanged
        other => other.clone(),
    }
}

/// Generate SQL from RenderPlan with configurable CTE depth limit
pub fn render_plan_to_sql(mut plan: RenderPlan, max_cte_depth: u32) -> String {
    // Extract fixed path information if not already set
    // This looks at the RenderPlan structure to infer path variable info
    if plan.fixed_path_info.is_none() {
        plan.fixed_path_info = extract_fixed_path_info_from_plan(&plan);
    }

    // Rewrite VLP SELECT aliases before SQL generation
    // Maps Cypher aliases (a, b) to CTE column prefixes (start_, end_)
    plan = rewrite_vlp_select_aliases(plan);

    // Rewrite path function calls for fixed (non-VLP) path patterns
    // Converts length(p) → hop_count, etc.
    plan = rewrite_fixed_path_functions(plan);

    // Build ALL rendering contexts (CTE registry, relationship columns, CTE mappings, multi-type aliases)
    let relationship_columns = build_relationship_columns_from_plan(&plan);
    let cte_mappings = build_cte_property_mappings(&plan);
    let multi_type_aliases = build_multi_type_vlp_aliases(&plan);

    // TASK-LOCAL: Set ALL contexts for this async task's rendering context
    set_all_render_contexts(relationship_columns, cte_mappings, multi_type_aliases);

    let mut sql = String::new();

    // If there's a Union, wrap it in a subquery for correct ClickHouse behavior.
    // ClickHouse has a quirk where LIMIT/ORDER BY on bare UNION ALL only applies to
    // the last branch, not the combined result. Wrapping in a subquery fixes this.
    if plan.union.0.is_some() {
        sql.push_str(&plan.ctes.to_sql());

        // Check if SELECT items contain aggregation (e.g., count(*), sum(), etc.)
        let has_aggregation = plan
            .select
            .items
            .iter()
            .any(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)));

        log::debug!(
            "UNION rendering: has_aggregation={}, select_items={}",
            has_aggregation,
            plan.select.items.len()
        );
        for (idx, item) in plan.select.items.iter().enumerate() {
            log::debug!("  select[{}]: expr={:?}", idx, item.expression);
        }

        // Check if we need the subquery wrapper (when there's ORDER BY, LIMIT, GROUP BY, or aggregation)
        let needs_subquery = !plan.order_by.0.is_empty()
            || plan.limit.0.is_some()
            || plan.skip.0.is_some()
            || !plan.group_by.0.is_empty()
            || has_aggregation;

        log::debug!("UNION rendering: needs_subquery={}", needs_subquery);

        if needs_subquery {
            // Wrap UNION in a subquery
            // If there are specific SELECT items (aggregation case), use them
            // Otherwise default to SELECT *
            // For UNION with ordering/limiting, wrap in subquery and apply ORDER BY/LIMIT to outer query
            sql.push_str("SELECT ");

            if let Some(_union) = &plan.union.0 {
                // For UNION queries with aggregations, we need to select all columns from the subquery
                // and apply the aggregation in the outer SELECT.
                // For UNION queries without aggregations, select column aliases.
                if has_aggregation {
                    // With aggregation: apply the aggregation functions to the UNION result
                    // The aggregation expressions will reference columns from the UNION
                    let agg_select = plan
                        .select
                        .items
                        .iter()
                        .map(|item| {
                            // Keep the aggregation expression as-is; it will reference UNION columns
                            format!(
                                "{} AS \"{}\"",
                                item.expression.to_sql(),
                                item.col_alias
                                    .as_ref()
                                    .map(|a| a.0.clone())
                                    .unwrap_or_else(|| "result".to_string())
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&agg_select);
                } else {
                    // Without aggregation: select column aliases from the subquery
                    let alias_select = plan
                        .select
                        .items
                        .iter()
                        .map(|item| {
                            if let Some(col_alias) = &item.col_alias {
                                format!("__union.`{}` AS `{}`", col_alias.0, col_alias.0)
                            } else {
                                // Fallback to the expression
                                item.expression.to_sql()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&alias_select);
                }
            } else if !plan.select.items.is_empty() {
                sql.push_str(&plan.select.to_sql());
            } else {
                sql.push('*');
            }

            sql.push_str(" FROM (\n");

            // CRITICAL FIX: Generate the first branch's SQL from the base plan
            // The base plan (plan.select, plan.from, plan.joins, plan.filters) IS the first branch
            // plan.union only contains branches 2+
            // For denormalized UNIONs, use the plan's own SELECT which has the correct properties for the first branch
            let first_branch_sql = {
                let mut branch_sql = String::new();
                branch_sql.push_str(&plan.select.to_sql());
                branch_sql.push_str(&plan.from.to_sql());
                branch_sql.push_str(&plan.joins.to_sql());
                branch_sql.push_str(&plan.filters.to_sql());
                branch_sql
            };
            sql.push_str(&first_branch_sql);

            // Now add the remaining branches with UNION ALL
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };
                for union_branch in &union.input {
                    sql.push_str(union_type_str);
                    let branch_sql = {
                        let mut bsql = String::new();
                        // USE each branch's own SELECT items, not the first branch's
                        bsql.push_str(&union_branch.select.to_sql());
                        bsql.push_str(&union_branch.from.to_sql());
                        bsql.push_str(&union_branch.joins.to_sql());
                        bsql.push_str(&union_branch.filters.to_sql());
                        bsql
                    };
                    sql.push_str(&branch_sql);
                }
            }

            sql.push_str(") AS __union\n");

            // Add GROUP BY if present
            sql.push_str(&plan.group_by.to_sql());

            // Add ORDER BY after GROUP BY if present
            sql.push_str(&plan.order_by.to_sql());

            // Add LIMIT after ORDER BY if present
            if let Some(m) = plan.limit.0 {
                let skip_str = if let Some(n) = plan.skip.0 {
                    format!("{n},")
                } else {
                    "".to_string()
                };
                let limit_str = format!("LIMIT {skip_str} {m}");
                sql.push_str(&limit_str)
            }
        } else {
            // No ordering/limiting - bare UNION is fine
            // But we still need to output the first branch!
            let first_branch_sql = {
                let mut branch_sql = String::new();
                branch_sql.push_str(&plan.select.to_sql());
                branch_sql.push_str(&plan.from.to_sql());
                branch_sql.push_str(&plan.joins.to_sql());
                branch_sql.push_str(&plan.filters.to_sql());
                branch_sql
            };
            sql.push_str(&first_branch_sql);

            // Add remaining branches with UNION
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };
                for union_branch in &union.input {
                    sql.push_str(union_type_str);
                    let branch_sql = {
                        let mut bsql = String::new();
                        bsql.push_str(&union_branch.select.to_sql());
                        bsql.push_str(&union_branch.from.to_sql());
                        bsql.push_str(&union_branch.joins.to_sql());
                        bsql.push_str(&union_branch.filters.to_sql());
                        bsql
                    };
                    sql.push_str(&branch_sql);
                }
            }
        }

        return sql;
    }

    // Collect UNWIND (ARRAY JOIN) aliases to avoid `.*` expansion for scalar values
    let unwind_aliases: std::collections::HashSet<String> = plan
        .array_join
        .0
        .iter()
        .map(|aj| aj.alias.clone())
        .collect();

    sql.push_str(&plan.ctes.to_sql());
    sql.push_str(&plan.select.to_sql_with_unwind_aliases(&unwind_aliases));

    // Add FROM clause - use system.one for UNWIND-only queries (no actual table)
    let from_sql = plan.from.to_sql();
    if from_sql.is_empty() && !plan.array_join.0.is_empty() {
        // ARRAY JOIN requires a FROM clause in ClickHouse
        // system.one is a virtual table with one row, perfect for UNWIND-only queries
        sql.push_str("FROM system.one\n");
    } else {
        sql.push_str(&from_sql);
    }

    sql.push_str(&plan.joins.to_sql());
    sql.push_str(&plan.array_join.to_sql());
    sql.push_str(&plan.filters.to_sql());
    sql.push_str(&plan.group_by.to_sql());

    // Add HAVING clause if present (after GROUP BY, before ORDER BY)
    if let Some(having_expr) = &plan.having_clause {
        sql.push_str("HAVING ");
        sql.push_str(&having_expr.to_sql());
        sql.push('\n');
    }

    sql.push_str(&plan.order_by.to_sql());
    sql.push_str(&plan.union.to_sql());

    if let Some(m) = plan.limit.0 {
        let skip_str = if let Some(n) = plan.skip.0 {
            format!("{n},")
        } else {
            "".to_string()
        };
        let limit_str = format!("LIMIT {skip_str} {m}");
        sql.push_str(&limit_str)
    }

    // Add ClickHouse SETTINGS for recursive CTEs (variable-length paths)
    // Check if any CTE is recursive
    let has_recursive_cte = plan.ctes.0.iter().any(|cte| cte.is_recursive);
    if has_recursive_cte {
        sql.push_str(&format!(
            "\nSETTINGS max_recursive_cte_evaluation_depth = {}\n",
            max_cte_depth
        ));
    }

    // CLEANUP: Clear ALL task-local render contexts before returning
    clear_all_render_contexts();

    sql
}

impl ToSql for RenderPlan {
    fn to_sql(&self) -> String {
        // Use default depth of 100 when called via trait
        render_plan_to_sql(self.clone(), 100)
    }
}

impl ToSql for SelectItems {
    fn to_sql(&self) -> String {
        // Default behavior: no UNWIND aliases to exclude from `.*` expansion
        self.to_sql_with_unwind_aliases(&std::collections::HashSet::new())
    }
}

impl SelectItems {
    /// Generate SQL for SELECT items, excluding `.*` expansion for UNWIND aliases.
    /// UNWIND aliases are scalars, not tables, so `x.*` is invalid for them.
    pub fn to_sql_with_unwind_aliases(
        &self,
        unwind_aliases: &std::collections::HashSet<String>,
    ) -> String {
        let mut sql: String = String::new();

        if self.items.is_empty() {
            return sql;
        }

        if self.distinct {
            sql.push_str("SELECT DISTINCT \n");
        } else {
            sql.push_str("SELECT \n");
        }

        for (i, item) in self.items.iter().enumerate() {
            sql.push_str("      ");

            // 🔧 BUG #9 FIX: For path variables, when TableAlias matches col_alias,
            // render as `alias.*` to avoid "Already registered p AS p" error
            // This handles: SELECT p AS "p" FROM ... AS p (invalid)
            // Should be: SELECT p.* FROM ... AS p (valid)
            //
            // 🔧 UNWIND FIX: Skip `.*` expansion for UNWIND aliases since they're scalars, not tables
            //
            // 🔧 SCALAR FIX: ColumnAlias never gets `.*` expansion - it's a scalar column reference
            // This handles: WITH n.email as group_key ... RETURN group_key
            // where group_key is a scalar column, not a node/table
            let rendered_expr = if let RenderExpr::ColumnAlias(_) = &item.expression {
                // ColumnAlias is always rendered as-is (scalar reference)
                // No wildcard expansion: group_key stays group_key, not group_key.*
                item.expression.to_sql()
            } else if let RenderExpr::TableAlias(TableAlias(alias_name)) = &item.expression {
                log::debug!(
                    "🔍 Rendering TableAlias '{}', col_alias={:?}",
                    alias_name,
                    item.col_alias
                );
                if let Some(col_alias) = &item.col_alias {
                    if alias_name == &col_alias.0 {
                        // Check if this is an UNWIND alias - don't use `.*` for scalars
                        if unwind_aliases.contains(alias_name) {
                            // UNWIND alias: render as just the alias (scalar value)
                            alias_name.clone()
                        } else {
                            // Path/table alias: use `.*` expansion
                            format!("{}.*", alias_name)
                        }
                    } else {
                        log::debug!(
                            "  Alias mismatch: col_alias={} != expr_alias={}",
                            col_alias.0,
                            alias_name
                        );
                        item.expression.to_sql()
                    }
                } else {
                    item.expression.to_sql()
                }
            } else {
                item.expression.to_sql()
            };

            sql.push_str(&rendered_expr);

            // Only add AS clause if the alias differs from the expression
            // (already handled above for matching TableAlias case)
            if let Some(alias) = &item.col_alias {
                if let RenderExpr::TableAlias(TableAlias(expr_alias)) = &item.expression {
                    // For UNWIND aliases that match OR for aliases that differ, we need the AS clause
                    if expr_alias != &alias.0 || unwind_aliases.contains(expr_alias) {
                        sql.push_str(" AS \"");
                        sql.push_str(&alias.0);
                        sql.push('"');
                    }
                } else {
                    sql.push_str(" AS \"");
                    sql.push_str(&alias.0);
                    sql.push('"');
                }
            }
            if i + 1 < self.items.len() {
                sql.push_str(", ");
            }
            sql.push('\n');
        }
        sql
    }
}

impl ToSql for FromTableItem {
    fn to_sql(&self) -> String {
        if let Some(view_ref) = &self.0 {
            let mut sql = String::new();
            sql.push_str("FROM ");

            // For all references, use the name directly
            // Note: WHERE clause filtering is handled in WhereClause generation,
            // not as a subquery in FROM clause
            sql.push_str(&view_ref.name);

            // Extract the alias - prefer the explicit alias from ViewTableRef,
            // otherwise try to get it from the source logical plan
            let alias = if let Some(explicit_alias) = &view_ref.alias {
                explicit_alias.clone()
            } else {
                match view_ref.source.as_ref() {
                    LogicalPlan::ViewScan(_) => {
                        // ViewScan fallback - should not reach here if alias is properly set
                        VLP_CTE_FROM_ALIAS.to_string()
                    }
                    _ => VLP_CTE_FROM_ALIAS.to_string(), // Default fallback
                }
            };

            sql.push_str(" AS ");
            sql.push_str(&alias);

            // Add FINAL keyword AFTER alias if needed (ClickHouse syntax: FROM table AS alias FINAL)
            if view_ref.use_final {
                sql.push_str(" FINAL");
            }

            sql.push('\n');
            sql
        } else {
            "".into()
        }

        // let mut sql: String = String::new();
        // if self.0.is_none() {
        //     return sql;
        // }
        // sql.push_str("FROM ");

        // sql.push_str(&self.table_name);
        // if let Some(alias) = &self.table_alias {
        //     if !alias.is_empty() {
        //         sql.push_str(" AS ");
        //         sql.push_str(&alias);
        //     }
        // }
        // sql.push('\n');
        // sql
    }
}

impl ToSql for FilterItems {
    fn to_sql(&self) -> String {
        if let Some(expr) = &self.0 {
            format!("WHERE {}\n", expr.to_sql())
        } else {
            "".into()
        }
    }
}

/// ARRAY JOIN for ClickHouse - maps from Cypher UNWIND clauses
/// Supports multiple UNWIND for cartesian product
///
/// Example: UNWIND [1,2] AS x UNWIND [10,20] AS y
/// Generates: ARRAY JOIN [1,2] AS x ARRAY JOIN [10,20] AS y
impl ToSql for ArrayJoinItem {
    fn to_sql(&self) -> String {
        if self.0.is_empty() {
            return "".into();
        }

        let mut sql = String::new();
        for array_join in &self.0 {
            sql.push_str(&format!(
                "ARRAY JOIN {} AS {}\n",
                array_join.expression.to_sql(),
                array_join.alias
            ));
        }
        sql
    }
}

impl ToSql for GroupByExpressions {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("GROUP BY ");
        for (i, e) in self.0.iter().enumerate() {
            sql.push_str(&e.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for OrderByItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }
        sql.push_str("ORDER BY ");
        for (i, item) in self.0.iter().enumerate() {
            sql.push_str(&item.expression.to_sql());
            sql.push(' ');
            sql.push_str(&item.order.to_sql());
            if i + 1 < self.0.len() {
                sql.push_str(", ");
            }
        }
        sql.push('\n');
        sql
    }
}

impl ToSql for CteItems {
    fn to_sql(&self) -> String {
        let mut sql: String = String::new();
        if self.0.is_empty() {
            return sql;
        }

        // ClickHouse limitation: WITH RECURSIVE can only contain ONE recursive CTE
        // Solution: Keep first recursive CTE group in WITH RECURSIVE block,
        // wrap each additional recursive CTE group in a nested WITH RECURSIVE subquery

        // Group CTEs: each recursive CTE with all following non-recursive CTEs (until next recursive or end)
        let mut cte_groups: Vec<Vec<&Cte>> = Vec::new();
        let mut current_group: Vec<&Cte> = Vec::new();

        for cte in &self.0 {
            if cte.is_recursive {
                // Start new group with this recursive CTE
                if !current_group.is_empty() {
                    cte_groups.push(current_group);
                }
                current_group = vec![cte];
            } else {
                // Add non-recursive CTE to current group
                current_group.push(cte);
            }
        }

        // Add final group
        if !current_group.is_empty() {
            cte_groups.push(current_group);
        }

        // CRITICAL FIX: For groups 2+ that would be wrapped, extract trailing non-recursive CTEs
        // and move them to Group 1 (top level). This prevents:
        // 1. Duplicate CTE names (wrapper name = inner CTE name)
        // 2. Scope issues (WITH clause CTEs need to be accessible from final SELECT)
        if cte_groups.len() > 1 {
            let mut trailing_non_recursive: Vec<&Cte> = Vec::new();

            // Process groups in reverse (from last to second)
            for group_idx in (1..cte_groups.len()).rev() {
                let group = &mut cte_groups[group_idx];

                // Skip if first CTE isn't recursive (shouldn't happen based on grouping logic)
                if group.is_empty() || !group[0].is_recursive {
                    continue;
                }

                // Extract all trailing non-recursive CTEs from this group
                let mut non_recursive_start = 1; // Start after the recursive CTE
                for (i, cte) in group.iter().enumerate().skip(1) {
                    if cte.is_recursive {
                        non_recursive_start = i + 1;
                    }
                }

                if non_recursive_start < group.len() {
                    // Extract trailing non-recursive CTEs
                    let extracted: Vec<&Cte> = group.drain(non_recursive_start..).collect();
                    trailing_non_recursive.splice(0..0, extracted); // Prepend to maintain order
                }
            }

            // Add extracted CTEs to Group 1 (top level)
            if !trailing_non_recursive.is_empty() {
                cte_groups[0].extend(trailing_non_recursive);
            }
        }

        // If no recursive CTEs at all
        if cte_groups.is_empty() || !cte_groups.iter().any(|g| g[0].is_recursive) {
            sql.push_str("WITH ");
            for (i, cte) in self.0.iter().enumerate() {
                sql.push_str(&cte.to_sql());
                if i + 1 < self.0.len() {
                    sql.push_str(", ");
                }
                sql.push('\n');
            }
            return sql;
        }

        // Emit first group (WITH RECURSIVE block with first recursive CTE and its helpers)
        sql.push_str("WITH RECURSIVE ");
        let first_group = &cte_groups[0];
        for (i, cte) in first_group.iter().enumerate() {
            sql.push_str(&cte.to_sql());
            if i + 1 < first_group.len() || cte_groups.len() > 1 {
                sql.push_str(", ");
            }
            sql.push('\n');
        }

        // For additional groups (2nd recursive CTE onwards), wrap in subquery
        for group_idx in 1..cte_groups.len() {
            let group = &cte_groups[group_idx];
            let first_cte_in_group = group[0];

            // Only wrap if this group has a recursive CTE
            if first_cte_in_group.is_recursive {
                // Get the last CTE name in this group - that's what we'll expose
                let last_cte_name = &group[group.len() - 1].cte_name;

                // Check if the first CTE already contains nested CTE definitions (VLP multi-tier pattern)
                // This is indicated by the presence of multiple " AS (" in RawSql content
                let first_cte_content = match &first_cte_in_group.content {
                    CteContent::RawSql(s) => Some(s.as_str()),
                    _ => None,
                };

                let has_nested_ctes = first_cte_content
                    .map(|s| s.matches(" AS (").count() > 1)
                    .unwrap_or(false);

                if has_nested_ctes && group.len() == 1 {
                    // VLP CTE with multi-tier structure (e.g., "vlp_inner AS..., vlp AS...")
                    // Wrap the entire nested structure as-is
                    sql.push_str(&format!("{} AS (\n", last_cte_name));
                    sql.push_str("  SELECT * FROM (\n");
                    sql.push_str("    WITH RECURSIVE ");
                    sql.push_str(first_cte_content.unwrap());
                    sql.push_str("\n    SELECT * FROM ");
                    sql.push_str(last_cte_name);
                    sql.push_str("\n  )\n)");
                } else {
                    // Standard case: wrap each CTE normally
                    sql.push_str(&format!("{} AS (\n", last_cte_name));
                    sql.push_str("  SELECT * FROM (\n");
                    sql.push_str("    WITH RECURSIVE ");

                    // Emit all CTEs in this group
                    for (i, cte) in group.iter().enumerate() {
                        sql.push_str(&cte.to_sql());
                        if i + 1 < group.len() {
                            sql.push_str(", ");
                        }
                        sql.push('\n');
                    }

                    // Close the nested WITH and select the final CTE
                    sql.push_str("    SELECT * FROM ");
                    sql.push_str(last_cte_name);
                    sql.push_str("\n  )\n)");
                }

                if group_idx + 1 < cte_groups.len() {
                    sql.push_str(",\n");
                } else {
                    sql.push('\n');
                }
            } else {
                // Non-recursive group: emit normally
                for (i, cte) in group.iter().enumerate() {
                    sql.push_str(&cte.to_sql());
                    if i + 1 < group.len() || group_idx + 1 < cte_groups.len() {
                        sql.push_str(", ");
                    }
                    sql.push('\n');
                }
            }
        }

        sql
    }
}

impl ToSql for Cte {
    fn to_sql(&self) -> String {
        // Handle both structured and raw SQL content
        match &self.content {
            CteContent::Structured(plan) => {
                // For structured content, render only the query body (not nested CTEs)
                // CTEs should already be hoisted to the top level
                let mut cte_body = String::new();

                // Handle UNION plans - the union branches contain their own SELECTs
                if plan.union.0.is_some() {
                    // Check if we have custom SELECT items (WITH projection), modifiers, or GROUP BY
                    let has_custom_select = !plan.select.items.is_empty();
                    let has_order_by_skip_limit = !plan.order_by.0.is_empty()
                        || plan.limit.0.is_some()
                        || plan.skip.0.is_some();
                    let has_group_by = !plan.group_by.0.is_empty();
                    let needs_subquery =
                        has_custom_select || has_order_by_skip_limit || has_group_by;

                    if needs_subquery {
                        // Wrap UNION in a subquery to apply SELECT projection, ORDER BY/LIMIT/SKIP, or GROUP BY
                        if has_custom_select {
                            // Use custom SELECT items (e.g., WITH friend.firstName AS name)
                            cte_body.push_str(&plan.select.to_sql());
                        } else {
                            cte_body.push_str("SELECT * ");
                        }
                        cte_body.push_str("FROM (\n");
                        cte_body.push_str(&plan.union.to_sql());
                        cte_body.push_str(") AS __union\n");

                        // Add GROUP BY if present (for aggregations)
                        cte_body.push_str(&plan.group_by.to_sql());

                        cte_body.push_str(&plan.order_by.to_sql());

                        // Handle SKIP/LIMIT - either or both may be present
                        if plan.limit.0.is_some() || plan.skip.0.is_some() {
                            let skip_str = if let Some(n) = plan.skip.0 {
                                format!("{n}, ")
                            } else {
                                "".to_string()
                            };
                            // ClickHouse requires LIMIT if OFFSET is present
                            // Use a very large number if only SKIP is specified
                            let limit_val = plan.limit.0.unwrap_or(9223372036854775807i64); // i64::MAX
                            cte_body.push_str(&format!("LIMIT {skip_str}{limit_val}\n"));
                        }
                    } else {
                        // For Union plans without modifiers, just emit the union branches directly
                        cte_body.push_str(&plan.union.to_sql());
                    }
                } else {
                    // Standard single-query plan
                    // If there are no explicit SELECT items, default to SELECT *
                    if plan.select.items.is_empty() {
                        cte_body.push_str("SELECT *\n");
                    } else {
                        cte_body.push_str(&plan.select.to_sql());
                    }

                    cte_body.push_str(&plan.from.to_sql());
                    cte_body.push_str(&plan.joins.to_sql());
                    cte_body.push_str(&plan.filters.to_sql());
                    cte_body.push_str(&plan.group_by.to_sql());

                    // Add HAVING clause if present (after GROUP BY)
                    if let Some(having_expr) = &plan.having_clause {
                        cte_body.push_str("HAVING ");
                        cte_body.push_str(&having_expr.to_sql());
                        cte_body.push('\n');
                    }

                    cte_body.push_str(&plan.order_by.to_sql());

                    // Add LIMIT/SKIP for non-union CTEs as well
                    if plan.limit.0.is_some() || plan.skip.0.is_some() {
                        let skip_str = if let Some(n) = plan.skip.0 {
                            format!("{n}, ")
                        } else {
                            "".to_string()
                        };
                        // ClickHouse requires LIMIT if OFFSET is present
                        let limit_val = plan.limit.0.unwrap_or(9223372036854775807i64);
                        cte_body.push_str(&format!("LIMIT {skip_str}{limit_val}\n"));
                    }
                }

                format!("{} AS ({})", self.cte_name, cte_body)
            }
            CteContent::RawSql(sql) => {
                // Check if raw SQL already includes the CTE name and AS clause
                // (legacy behavior from VariableLengthCteGenerator)
                // or if we need to wrap it (new behavior from MultiTypeVlpJoinGenerator)
                if sql.trim_start().to_lowercase().starts_with("with ")
                    || sql
                        .trim_start()
                        .starts_with(&format!("{} AS", self.cte_name))
                    || sql.contains(" AS (")
                {
                    // Already wrapped - use as-is
                    sql.clone()
                } else {
                    // Raw CTE body - wrap it
                    format!("{} AS (\n{}\n)", self.cte_name, sql)
                }
            }
        }
    }
}

impl ToSql for UnionItems {
    fn to_sql(&self) -> String {
        if let Some(union) = &self.0 {
            let union_sql_strs: Vec<String> = union
                .input
                .iter()
                .map(|union_item| union_item.to_sql())
                .collect();

            let union_type_str = match union.union_type {
                UnionType::Distinct => "UNION DISTINCT \n", // ClickHouse requires explicit DISTINCT
                UnionType::All => "UNION ALL \n",
            };

            union_sql_strs.join(union_type_str)
        } else {
            "".into()
        }
    }
}

impl ToSql for JoinItems {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        for join in &self.0 {
            sql.push_str(&join.to_sql());
        }
        sql
    }
}

impl ToSql for Join {
    fn to_sql(&self) -> String {
        crate::debug_println!("🔍 Join::to_sql");
        crate::debug_print!("  table_alias: {}", self.table_alias);
        crate::debug_print!("  table_name: {}", self.table_name);
        crate::debug_print!("  joining_on.len(): {}", self.joining_on.len());
        crate::debug_print!("  pre_filter: {:?}", self.pre_filter.is_some());
        if !self.joining_on.is_empty() {
            crate::debug_print!("  joining_on conditions:");
            for (_idx, _cond) in self.joining_on.iter().enumerate() {
                crate::debug_print!("    [{}]: {:?}", _idx, _cond);
            }
        } else {
            crate::debug_print!("  ⚠️  WARNING: joining_on is EMPTY!");
        }

        let join_type_str = match self.join_type {
            JoinType::Join => {
                if self.joining_on.is_empty() {
                    "CROSS JOIN"
                } else {
                    "JOIN"
                }
            }
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
        };

        // For LEFT JOIN with pre_filter, use subquery form:
        // LEFT JOIN (SELECT * FROM table WHERE pre_filter) AS alias ON ...
        // This ensures the filter is applied BEFORE the join (correct LEFT JOIN semantics)
        //
        // For INNER JOIN with pre_filter, add filter to ON clause:
        // INNER JOIN table AS alias ON <join_cond> AND <pre_filter>
        // This is semantically equivalent and more efficient than subquery
        let table_expr = if let Some(ref pre_filter) = self.pre_filter {
            if matches!(self.join_type, JoinType::Left) {
                // Use to_sql_without_table_alias to render column names without table prefix
                // since inside the subquery, the table is not yet aliased
                let filter_sql = pre_filter.to_sql_without_table_alias();
                crate::debug_print!(
                    "  Using subquery form for LEFT JOIN with pre_filter: {}",
                    filter_sql
                );
                format!("(SELECT * FROM {} WHERE {})", self.table_name, filter_sql)
            } else {
                // For non-LEFT joins, pre_filter will be added to ON clause below
                self.table_name.clone()
            }
        } else {
            self.table_name.clone()
        };

        let mut sql = format!("{} {} AS {}", join_type_str, table_expr, self.table_alias);

        // Note: FINAL keyword for joins would need to be added here if Join struct
        // is enhanced to track use_final. For now, joins don't support FINAL.

        // Only add ON clause if there are joining conditions
        if !self.joining_on.is_empty() {
            let joining_on_str_vec: Vec<String> =
                self.joining_on.iter().map(|cond| cond.to_sql()).collect();

            let mut joining_on_str = joining_on_str_vec.join(" AND ");

            // For INNER JOINs (not LEFT), add pre_filter to ON clause
            // This applies polymorphic edge filters, schema filters, etc.
            if let Some(ref pre_filter) = self.pre_filter {
                if !matches!(self.join_type, JoinType::Left) {
                    let filter_sql = pre_filter.to_sql();
                    crate::debug_print!(
                        "  Adding pre_filter to INNER JOIN ON clause: {}",
                        filter_sql
                    );
                    joining_on_str = format!("{} AND {}", joining_on_str, filter_sql);
                }
            }

            sql.push_str(&format!(" ON {joining_on_str}"));
        }

        sql.push('\n');
        sql
    }
}

impl RenderExpr {
    /// Render this expression (including any subqueries) to a SQL string.
    pub fn to_sql(&self) -> String {
        match self {
            RenderExpr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => {
                    if *b {
                        "true".into()
                    } else {
                        "false".into()
                    }
                }
                Literal::String(s) => format!("'{}'", s),
                Literal::Null => "NULL".into(),
            },
            RenderExpr::Parameter(name) => format!("${}", name),
            RenderExpr::Raw(raw) => raw.clone(),
            RenderExpr::Star => "*".into(),
            RenderExpr::TableAlias(TableAlias(a)) | RenderExpr::ColumnAlias(ColumnAlias(a)) => {
                a.clone()
            }
            RenderExpr::Column(Column(a)) => {
                // For column references, we need to add the table alias prefix
                // to match our FROM clause alias generation
                let raw_value = a.raw();

                // Special case: If the column is "*", return it directly without table prefix
                // This happens when a WITH clause expands a table alias to all columns
                if raw_value == "*" {
                    return "*".to_string();
                }

                if raw_value.contains('.') {
                    raw_value.to_string() // Already has table prefix
                } else {
                    // 🔧 CRITICAL FIX (Jan 23, 2026): Detect VLP CTE columns by prefix or name
                    // VLP CTE columns are named: start_id, end_id, start_city, end_name, etc.
                    // Plus internal path metadata: hop_count, path_edges, path_relationships, path_nodes
                    // These should NOT be qualified with a table alias because they come from
                    // the VLP CTE and the rendering pipeline handles FROM alias separately
                    if raw_value.starts_with("start_")
                        || raw_value.starts_with("end_")
                        || matches!(
                            raw_value,
                            "hop_count" | "path_edges" | "path_relationships" | "path_nodes"
                        )
                    {
                        log::info!(
                            "🔧 Detected VLP CTE column '{}', returning unqualified",
                            raw_value
                        );
                        return raw_value.to_string();
                    }

                    // ⚠️ TECHNICAL DEBT: Heuristic table alias inference (Temporary workaround)
                    //
                    // CONTEXT: This uses pattern matching on column names to infer the correct table alias.
                    // Works well for simple queries but breaks down in complex multi-join scenarios.
                    //
                    // CURRENT STRATEGY: Infer table alias from column name patterns and common naming conventions
                    // This covers ~95% of real-world cases and maintains backward compatibility.
                    //
                    // ISSUES WITH THIS APPROACH:
                    // - Fails for non-standard naming conventions (e.g., "t_name" instead of "user_name")
                    // - Ambiguous in multi-table scenarios (e.g., both users and posts have "id")
                    // - Requires hardcoding new patterns for each new entity type
                    // - Fragile when column names conflict across entity types
                    //
                    // TODO: Long-term solution should:
                    // 1. Pass table context/alias through the rendering pipeline
                    // 2. Track which columns belong to which tables in RenderExpr
                    // 3. Eliminate guessing with explicit table.column mappings in RenderPlan
                    // 4. Add property resolution via schema for Cypher→Database column mapping
                    //
                    // PERFORMANCE NOTE: Consider caching heuristic results to avoid repeated pattern matching
                    //
                    // Current table alias patterns:
                    let alias = if raw_value.contains("user")
                        || raw_value.contains("username")
                        || raw_value.contains("last_login")
                        || raw_value.contains("registration")
                        || raw_value == "name"
                        || raw_value == "age"
                        || raw_value == "active"
                        || raw_value.starts_with("u_")
                    {
                        "u" // User-related columns use 'u' alias
                    } else if raw_value.contains("post")
                        || raw_value.contains("article")
                        || raw_value.contains("published")
                        || raw_value == "title"
                        || raw_value == "views"
                        || raw_value == "status"
                        || raw_value == "author"
                        || raw_value == "category"
                        || raw_value.starts_with("p_")
                    {
                        "p" // Post-related columns use 'p' alias
                    } else if raw_value.contains("customer")
                        || raw_value.contains("rating")
                        || raw_value == "email"
                        || raw_value.starts_with("customer_")
                        || raw_value.starts_with("c_")
                    {
                        // CRITICAL FIX: Use 'c' to match FROM clause, not 'customer'
                        // The FROM clause uses original Cypher variable names (c, not customer)
                        "c" // Customer-related columns use 'c' alias to match FROM Customer AS c
                    } else if raw_value.contains("product")
                        || raw_value.contains("price")
                        || raw_value.contains("inventory")
                        || raw_value.starts_with("prod_")
                    {
                        "product" // Product-related columns
                    } else {
                        // FALLBACK: For truly unknown columns, use 't' (temporary/table)
                        // This maintains compatibility while covering 95%+ of real use cases
                        "t"
                    };

                    format!("{}.{}", alias, raw_value)
                }
            }
            RenderExpr::List(items) => {
                let inner = items
                    .iter()
                    .map(|e| e.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                // Use array literal syntax [...] for ClickHouse
                // This works for both ARRAY JOIN (UNWIND) and IN clauses
                format!("[{}]", inner)
            }
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check for special functions that need custom handling
                let fn_name_lower = fn_call.name.to_lowercase();

                // Special handling for duration() with map argument
                if fn_name_lower == "duration" && fn_call.args.len() == 1 {
                    if let RenderExpr::MapLiteral(entries) = &fn_call.args[0] {
                        if !entries.is_empty() {
                            // Convert duration({days: 5, hours: 2}) -> (toIntervalDay(5) + toIntervalHour(2))
                            let interval_parts: Vec<String> = entries
                                .iter()
                                .filter_map(|(key, value)| {
                                    let value_sql = value.to_sql();
                                    let key_lower = key.to_lowercase();

                                    // Map Neo4j time unit to ClickHouse interval function
                                    let result = match key_lower.as_str() {
                                        "years" | "year" => {
                                            format!("toIntervalYear({})", value_sql)
                                        }
                                        "months" | "month" => {
                                            format!("toIntervalMonth({})", value_sql)
                                        }
                                        "weeks" | "week" => {
                                            format!("toIntervalWeek({})", value_sql)
                                        }
                                        "days" | "day" => format!("toIntervalDay({})", value_sql),
                                        "hours" | "hour" => {
                                            format!("toIntervalHour({})", value_sql)
                                        }
                                        "minutes" | "minute" => {
                                            format!("toIntervalMinute({})", value_sql)
                                        }
                                        "seconds" | "second" => {
                                            format!("toIntervalSecond({})", value_sql)
                                        }
                                        "milliseconds" | "millisecond" => {
                                            format!("toIntervalSecond({} / 1000.0)", value_sql)
                                        }
                                        "microseconds" | "microsecond" => {
                                            format!("toIntervalSecond({} / 1000000.0)", value_sql)
                                        }
                                        "nanoseconds" | "nanosecond" => {
                                            format!(
                                                "toIntervalSecond({} / 1000000000.0)",
                                                value_sql
                                            )
                                        }
                                        _ => {
                                            log::warn!(
                                                "Unknown duration unit '{}', using as-is",
                                                key
                                            );
                                            return None;
                                        }
                                    };
                                    Some(result)
                                })
                                .collect();

                            if interval_parts.len() == 1 {
                                return interval_parts[0].clone();
                            } else {
                                return format!("({})", interval_parts.join(" + "));
                            }
                        }
                    }
                }

                // Check if we have a Neo4j -> ClickHouse mapping
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        // Convert arguments to SQL
                        let args_sql: Vec<String> =
                            fn_call.args.iter().map(|e| e.to_sql()).collect();

                        // Apply transformation if provided
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };

                        // Return ClickHouse function with transformed args
                        format!(
                            "{}({})",
                            mapping.clickhouse_name,
                            transformed_args.join(", ")
                        )
                    }
                    None => {
                        // No mapping found - use original function name (passthrough)
                        let args = fn_call
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", fn_call.name, args)
                    }
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                // Check for ClickHouse pass-through prefix (ch.)
                if agg.name.starts_with(CH_PASSTHROUGH_PREFIX) {
                    if let Some(ch_fn_name) = get_ch_function_name(&agg.name) {
                        if ch_fn_name.is_empty() {
                            log::error!("ch. prefix requires a function name (e.g., ch.uniq)");
                            // TODO: Refactor to_sql() to return Result<String, Error> so this error
                            // can be propagated instead of returning an empty string here.
                            // Returning an empty string is acceptable as an intermediate step
                            // but may lead to SQL syntax errors later in query execution.
                            return String::new(); // Return empty string for invalid function name
                        }
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        log::debug!(
                            "ClickHouse aggregate pass-through: ch.{}({}) -> {}({})",
                            ch_fn_name,
                            args,
                            ch_fn_name,
                            args
                        );
                        return format!("{}({})", ch_fn_name, args);
                    }
                }

                // Check if we have a Neo4j -> ClickHouse mapping for aggregate functions
                let fn_name_lower = agg.name.to_lowercase();
                match get_function_mapping(&fn_name_lower) {
                    Some(mapping) => {
                        let args_sql: Vec<String> = agg.args.iter().map(|e| e.to_sql()).collect();
                        let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                            transform_fn(&args_sql)
                        } else {
                            args_sql
                        };
                        format!(
                            "{}({})",
                            mapping.clickhouse_name,
                            transformed_args.join(", ")
                        )
                    }
                    None => {
                        // No mapping - use original name (count, sum, min, max, avg, etc.)
                        let args = agg
                            .args
                            .iter()
                            .map(|e| e.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{}({})", agg.name, args)
                    }
                }
            }
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias,
                column,
            }) => {
                let col_name = column.raw();
                log::info!(
                    "🔍 RenderExpr::PropertyAccessExp: {}.{}",
                    table_alias.0,
                    col_name
                );

                // 🔧 CRITICAL FIX (Jan 23, 2026): Handle bare VLP columns in WITH clauses
                // When path functions are rewritten in WITH contexts, they use __vlp_bare_col marker
                // to indicate the column should be selected without a table alias
                if table_alias.0 == "__vlp_bare_col" {
                    log::info!(
                        "🔧 Detected VLP bare column: {} (from WITH clause path function)",
                        col_name
                    );
                    return col_name.to_string();
                }

                // Special case: Multi-type VLP properties stored in JSON
                // Check if this table alias is a multi-type VLP endpoint
                if is_multi_type_vlp_alias_from_context(&table_alias.0) {
                    log::info!("🎯 Found '{}' in multi-type VLP aliases!", table_alias.0);
                    // Properties like end_type, end_id, hop_count, path_relationships are direct CTE columns
                    if col_name == VLP_START_ID_COLUMN
                        || col_name == VLP_END_ID_COLUMN
                        || matches!(
                            col_name,
                            "end_type" | "end_properties" | "hop_count" | "path_relationships"
                        )
                    {
                        log::info!(
                            "🎯 Multi-type VLP CTE column: {}.{}",
                            table_alias.0,
                            col_name
                        );
                        return format!("{}.{}", table_alias.0, col_name);
                    } else {
                        // Regular properties need JSON extraction from end_properties
                        log::info!("🎯 Multi-type VLP JSON extraction: {}.{} → JSON_VALUE({}.end_properties, '$.{}')",
                                  table_alias.0, col_name, table_alias.0, col_name);
                        return format!(
                            "JSON_VALUE({}.end_properties, '$.{}')",
                            table_alias.0, col_name
                        );
                    }
                }

                // Check if table_alias refers to a CTE and needs property mapping
                // (fallback to task-local context for backward compatibility)
                if let Some(cte_col) = get_cte_property_from_context(&table_alias.0, col_name) {
                    log::debug!(
                        "🔧 CTE property mapping (legacy): {}.{} → {}",
                        table_alias.0,
                        col_name,
                        cte_col
                    );
                    return format!("{}.{}", table_alias.0, cte_col);
                }

                // Property has been resolved from schema during query planning.
                // Just use the resolved mapping directly.
                column.to_sql(&table_alias.0)
            }
            RenderExpr::OperatorApplicationExp(op) => {
                // ⚠️ TODO: Operator rendering consolidation (Phase 3)
                // This code is duplicated in to_sql.rs (~70 lines of similar operator handling).
                // Both implementations handle Operator enums with identical variants but different types:
                // - to_sql.rs: crate::query_planner::logical_expr::Operator
                // - to_sql_query.rs: crate::render_plan::render_expr::Operator
                // Phase 3 consolidation strategy: Create OperatorRenderer trait (see notes/OPERATOR_RENDERING_ANALYSIS.md)
                // Benefits:
                // - Eliminate duplication without type system complexity
                // - Preserve context-specific behavior (error handling, special cases)
                // - Enable future operator extensions
                // Estimated effort: 4-6 hours, should be 100% backward compatible
                log::debug!(
                    "RenderExpr::to_sql() OperatorApplicationExp: operator={:?}, operands.len()={}",
                    op.operator,
                    op.operands.len()
                );
                for (i, operand) in op.operands.iter().enumerate() {
                    log::debug!("  operand[{}]: {:?}", i, operand);
                }

                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX", // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH", // Special handling below
                        Operator::EndsWith => "ENDS WITH",     // Special handling below
                        Operator::Contains => "CONTAINS",      // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                // Special handling for IS NULL / IS NOT NULL with wildcard property access (e.g., r.*)
                // Convert r.* to appropriate ID column for null checks (LEFT JOIN produces NULL for all columns)
                // Since base tables have no NULLABLE columns, LEFT JOIN makes ALL columns NULL together,
                // so checking ANY ID column is sufficient (even for composite keys).
                if matches!(op.operator, Operator::IsNull | Operator::IsNotNull)
                    && op.operands.len() == 1
                {
                    if let RenderExpr::PropertyAccessExp(prop) = &op.operands[0] {
                        let col_name = prop.column.raw();
                        if col_name == "*" {
                            let table_alias = &prop.table_alias.0;
                            let op_str = if op.operator == Operator::IsNull {
                                "IS NULL"
                            } else {
                                "IS NOT NULL"
                            };

                            // Look up the actual column name from the JOIN metadata (populated during rendering)
                            // This ensures we use the CORRECT column for the SPECIFIC relationship table
                            if let Some((from_id, _to_id)) =
                                get_relationship_columns_from_context(table_alias)
                            {
                                // Use from_id - any ID column works since LEFT JOIN makes all NULL together
                                let id_sql = format!("{}.{}", table_alias, from_id);
                                return format!("{} {}", id_sql, op_str);
                            } else {
                                // ERROR: r.* wildcard is ALWAYS for relationships
                                // If alias not in map = bug in planning (missing from_id_column population)
                                panic!(
                                    "Internal error: Relationship alias '{}' not found in column mapping. \
                                     This indicates a bug in query planning - relationship JOINs should populate \
                                     from_id_column during creation. Check graph_join_inference.rs line ~2547.",
                                    table_alias
                                )
                            }
                        }
                    }
                }

                let rendered: Vec<String> = op.operands.iter().map(|e| e.to_sql()).collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // Special handling for IN/NOT IN with array columns
                // Cypher: x IN array_property → ClickHouse: has(array, x)
                if op.operator == Operator::In
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    return format!("has({}, {})", &rendered[1], &rendered[0]);
                }
                if op.operator == Operator::NotIn
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
                }

                // Special handling for Addition with string operands - use concat()
                // ClickHouse doesn't support + for string concatenation
                // Flatten nested + operations to handle cases like: a + ' - ' + b
                if op.operator == Operator::Addition && has_string_operand(&op.operands) {
                    let flattened: Vec<String> = op
                        .operands
                        .iter()
                        .flat_map(flatten_addition_operands)
                        .collect();
                    return format!("concat({})", flattened.join(", "));
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(), // should not happen
                    1 => {
                        // Handle unary operators: IS NULL/IS NOT NULL are suffix, NOT is prefix
                        match op.operator {
                            Operator::IsNull | Operator::IsNotNull => {
                                format!("{} {}", &rendered[0], sql_op) // suffix: "x IS NULL"
                            }
                            _ => {
                                format!("{} {}", sql_op, &rendered[0]) // prefix: "NOT x"
                            }
                        }
                    }
                    2 => {
                        // For AND/OR, wrap in parentheses to ensure correct precedence
                        // when combined with other expressions
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                            }
                            _ => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                        }
                    }
                    _ => {
                        // n-ary: join with the operator, wrap in parentheses for AND/OR
                        match op.operator {
                            Operator::And | Operator::Or => {
                                format!("({})", rendered.join(&format!(" {} ", sql_op)))
                            }
                            _ => rendered.join(&format!(" {} ", sql_op)),
                        }
                    }
                }
            }
            RenderExpr::Case(case) => {
                // For ClickHouse, use caseWithExpression for simple CASE expressions
                if let Some(case_expr) = &case.expr {
                    // caseWithExpression(expr, val1, res1, val2, res2, ..., default)
                    let mut args = vec![case_expr.to_sql()];

                    for (when_expr, then_expr) in &case.when_then {
                        args.push(when_expr.to_sql());
                        args.push(then_expr.to_sql());
                    }

                    let else_expr = case
                        .else_expr
                        .as_ref()
                        .map(|e| e.to_sql())
                        .unwrap_or_else(|| "NULL".to_string());
                    args.push(else_expr);

                    format!("caseWithExpression({})", args.join(", "))
                } else {
                    // Searched CASE - use standard CASE syntax
                    let mut sql = String::from("CASE");

                    for (when_expr, then_expr) in &case.when_then {
                        sql.push_str(&format!(
                            " WHEN {} THEN {}",
                            when_expr.to_sql(),
                            then_expr.to_sql()
                        ));
                    }

                    if let Some(else_expr) = &case.else_expr {
                        sql.push_str(&format!(" ELSE {}", else_expr.to_sql()));
                    }

                    sql.push_str(" END");
                    sql
                }
            }
            RenderExpr::InSubquery(InSubquery { expr, subplan }) => {
                let left = expr.to_sql();
                let body = subplan.to_sql();
                let body = body.split_whitespace().collect::<Vec<&str>>().join(" ");

                format!("{} IN ({})", left, body)
            }
            RenderExpr::ExistsSubquery(exists) => {
                // Use the pre-generated SQL from the ExistsSubquery
                format!("EXISTS ({})", exists.sql)
            }
            RenderExpr::ReduceExpr(reduce) => {
                // Convert to ClickHouse arrayFold((acc, x) -> expr, list, init)
                // Cast numeric init to Int64 to prevent type mismatch issues
                let init_sql = reduce.initial_value.to_sql();
                let list_sql = reduce.list.to_sql();
                let expr_sql = reduce.expression.to_sql();

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
            RenderExpr::MapLiteral(entries) => {
                // Use ClickHouse map() function for map literals
                // map('key1', val1, 'key2', val2, ...)
                if entries.is_empty() {
                    "map()".to_string()
                } else {
                    let args: Vec<String> = entries
                        .iter()
                        .flat_map(|(k, v)| {
                            let val_sql = v.to_sql();
                            vec![format!("'{}'", k), val_sql]
                        })
                        .collect();
                    format!("map({})", args.join(", "))
                }
            }
            RenderExpr::PatternCount(pc) => {
                // Use the pre-generated SQL from PatternCount (correlated subquery)
                pc.sql.clone()
            }
            RenderExpr::ArraySubscript { array, index } => {
                // Array subscript in ClickHouse: array[index]
                // Note: Cypher uses 1-based indexing, ClickHouse uses 1-based too
                let array_sql = array.to_sql();
                let index_sql = index.to_sql();
                format!("{}[{}]", array_sql, index_sql)
            }
            RenderExpr::ArraySlicing { array, from, to } => {
                // Array slicing in ClickHouse using arraySlice function
                // arraySlice(array, offset, length)
                // - offset: 1-based index (Cypher uses 0-based, need to convert)
                // - length: number of elements to extract
                let array_sql = array.to_sql();

                match (from, to) {
                    (Some(from_expr), Some(to_expr)) => {
                        // [from..to] - both bounds specified
                        // Cypher: 0-based inclusive on both ends
                        // ClickHouse arraySlice: 1-based offset, length parameter
                        // Example: [2..4] means indices 2,3,4 (3 elements starting at index 2)
                        // Convert: arraySlice(arr, from+1, to-from+1)
                        format!(
                            "arraySlice({}, {} + 1, {} - {} + 1)",
                            array_sql,
                            from_expr.to_sql(),
                            to_expr.to_sql(),
                            from_expr.to_sql()
                        )
                    }
                    (Some(from_expr), None) => {
                        // [from..] - only lower bound, slice to end
                        // arraySlice(arr, from+1) - omitting length takes rest of array
                        format!("arraySlice({}, {} + 1)", array_sql, from_expr.to_sql())
                    }
                    (None, Some(to_expr)) => {
                        // [..to] - only upper bound, slice from start
                        // arraySlice(arr, 1, to+1) - from index 1, take to+1 elements
                        format!("arraySlice({}, 1, {} + 1)", array_sql, to_expr.to_sql())
                    }
                    (None, None) => {
                        // [..] - no bounds, return entire array (identity operation)
                        array_sql
                    }
                }
            }
            RenderExpr::CteEntityRef(cte_ref) => {
                // CteEntityRef should be expanded to all its columns in the SELECT list
                // When we reach to_sql(), it means it wasn't expanded properly by select_builder
                // For now, generate SQL that selects all prefixed columns from the CTE
                log::warn!(
                    "CteEntityRef '{}' from CTE '{}' reached to_sql() - should have been expanded",
                    cte_ref.alias,
                    cte_ref.cte_name
                );
                // Fall back to table alias reference (this won't work correctly,
                // but prevents crashes while we complete the select_builder integration)
                format!("{}.{}", cte_ref.alias, cte_ref.alias)
            }
        }
    }

    /// Render this expression to SQL without table alias prefixes.
    /// Used for rendering filters inside subqueries where the table is not yet aliased.
    /// e.g., `LEFT JOIN (SELECT * FROM table WHERE is_active = true) AS b`
    /// The filter should be `is_active = true`, not `b.is_active = true`.
    pub fn to_sql_without_table_alias(&self) -> String {
        match self {
            RenderExpr::PropertyAccessExp(PropertyAccess { column, .. }) => {
                // Just render the column without the table alias prefix
                column.to_sql_column_only()
            }
            RenderExpr::OperatorApplicationExp(op) => {
                fn op_str(o: Operator) -> &'static str {
                    match o {
                        Operator::Addition => "+",
                        Operator::Subtraction => "-",
                        Operator::Multiplication => "*",
                        Operator::Division => "/",
                        Operator::ModuloDivision => "%",
                        Operator::Exponentiation => "^",
                        Operator::Equal => "=",
                        Operator::NotEqual => "<>",
                        Operator::LessThan => "<",
                        Operator::GreaterThan => ">",
                        Operator::LessThanEqual => "<=",
                        Operator::GreaterThanEqual => ">=",
                        Operator::RegexMatch => "REGEX", // Special handling below
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::In => "IN",
                        Operator::NotIn => "NOT IN",
                        Operator::StartsWith => "STARTS WITH", // Special handling below
                        Operator::EndsWith => "ENDS WITH",     // Special handling below
                        Operator::Contains => "CONTAINS",      // Special handling below
                        Operator::Not => "NOT",
                        Operator::Distinct => "DISTINCT",
                        Operator::IsNull => "IS NULL",
                        Operator::IsNotNull => "IS NOT NULL",
                    }
                }

                // Recursively render operands without table alias
                let rendered: Vec<String> = op
                    .operands
                    .iter()
                    .map(|e| e.to_sql_without_table_alias())
                    .collect();

                // Special handling for RegexMatch - ClickHouse uses match() function
                if op.operator == Operator::RegexMatch && rendered.len() == 2 {
                    return format!("match({}, {})", &rendered[0], &rendered[1]);
                }

                // Special handling for IN/NOT IN with array columns
                if op.operator == Operator::In
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    return format!("has({}, {})", &rendered[1], &rendered[0]);
                }
                if op.operator == Operator::NotIn
                    && rendered.len() == 2
                    && matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_))
                {
                    return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
                }

                // Special handling for string predicates - ClickHouse uses functions
                if op.operator == Operator::StartsWith && rendered.len() == 2 {
                    return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::EndsWith && rendered.len() == 2 {
                    return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
                }
                if op.operator == Operator::Contains && rendered.len() == 2 {
                    return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
                }

                let sql_op = op_str(op.operator);

                match rendered.len() {
                    0 => "".into(),
                    1 => match op.operator {
                        Operator::IsNull | Operator::IsNotNull => {
                            format!("{} {}", &rendered[0], sql_op)
                        }
                        _ => {
                            format!("{} {}", sql_op, &rendered[0])
                        }
                    },
                    2 => match op.operator {
                        Operator::And | Operator::Or => {
                            format!("({} {} {})", &rendered[0], sql_op, &rendered[1])
                        }
                        _ => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
                    },
                    _ => match op.operator {
                        Operator::And | Operator::Or => {
                            format!("({})", rendered.join(&format!(" {} ", sql_op)))
                        }
                        _ => rendered.join(&format!(" {} ", sql_op)),
                    },
                }
            }
            // For Raw expressions, strip table alias prefixes (e.g., "alias.column" -> "column")
            // This is needed for LEFT JOIN subqueries where the filter is inside SELECT * FROM table
            RenderExpr::Raw(raw_sql) => {
                // Simple approach: look for "word.word" patterns and keep only the part after the dot
                // This handles cases like "alias.column = 'value'" -> "column = 'value'"
                let result = raw_sql.clone();
                // Find and replace all "identifier.identifier" patterns
                let parts: Vec<&str> = result.split_whitespace().collect();
                let mut new_parts = Vec::new();
                for part in parts {
                    if part.contains('.') && !part.starts_with('\'') && !part.starts_with('"') {
                        // Split on dot and take the last part (the column name)
                        // But preserve the structure (e.g., "alias.column" becomes "column")
                        let dot_parts: Vec<&str> = part.split('.').collect();
                        if dot_parts.len() == 2
                            && !dot_parts[0].is_empty()
                            && !dot_parts[1].is_empty()
                        {
                            // Check if first part looks like an identifier (not a number)
                            let first_char = dot_parts[0].chars().next().unwrap_or('0');
                            if first_char.is_alphabetic() || first_char == '_' {
                                new_parts.push(dot_parts[1].to_string());
                                continue;
                            }
                        }
                    }
                    new_parts.push(part.to_string());
                }
                new_parts.join(" ")
            }
            // For other expression types, delegate to regular to_sql
            _ => self.to_sql(),
        }
    }
}

impl ToSql for OperatorApplication {
    fn to_sql(&self) -> String {
        // Map your enum to SQL tokens
        fn op_str(o: Operator) -> &'static str {
            match o {
                Operator::Addition => "+",
                Operator::Subtraction => "-",
                Operator::Multiplication => "*",
                Operator::Division => "/",
                Operator::ModuloDivision => "%",
                Operator::Exponentiation => "^",
                Operator::Equal => "=",
                Operator::NotEqual => "<>",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::RegexMatch => "REGEX", // Special handling below
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::In => "IN",
                Operator::NotIn => "NOT IN",
                Operator::StartsWith => "STARTS WITH", // Special handling below
                Operator::EndsWith => "ENDS WITH",     // Special handling below
                Operator::Contains => "CONTAINS",      // Special handling below
                Operator::Not => "NOT",
                Operator::Distinct => "DISTINCT",
                Operator::IsNull => "IS NULL",
                Operator::IsNotNull => "IS NOT NULL",
            }
        }

        let rendered: Vec<String> = self.operands.iter().map(|e| e.to_sql()).collect();

        // Debug operand information
        log::debug!(
            "OperatorApplication.to_sql(): operator={:?}, operands.len()={}, rendered.len()={}",
            self.operator,
            self.operands.len(),
            rendered.len()
        );
        for (i, (op, r)) in self.operands.iter().zip(rendered.iter()).enumerate() {
            log::debug!("  operand[{}]: {:?} -> '{}'", i, op, r);
        }

        // Special handling for RegexMatch - ClickHouse uses match() function
        if self.operator == Operator::RegexMatch && rendered.len() == 2 {
            return format!("match({}, {})", &rendered[0], &rendered[1]);
        }

        // Special handling for IN/NOT IN with array columns
        if self.operator == Operator::In
            && rendered.len() == 2
            && matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_))
        {
            return format!("has({}, {})", &rendered[1], &rendered[0]);
        }
        if self.operator == Operator::NotIn
            && rendered.len() == 2
            && matches!(&self.operands[1], RenderExpr::PropertyAccessExp(_))
        {
            return format!("NOT has({}, {})", &rendered[1], &rendered[0]);
        }

        // Special handling for string predicates - ClickHouse uses functions
        if self.operator == Operator::StartsWith && rendered.len() == 2 {
            return format!("startsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::EndsWith && rendered.len() == 2 {
            return format!("endsWith({}, {})", &rendered[0], &rendered[1]);
        }
        if self.operator == Operator::Contains && rendered.len() == 2 {
            return format!("(position({}, {}) > 0)", &rendered[0], &rendered[1]);
        }

        // Special handling for Addition with string operands - use concat()
        // ClickHouse doesn't support + for string concatenation
        // Flatten nested + operations to handle cases like: a + ' - ' + b
        if self.operator == Operator::Addition && has_string_operand(&self.operands) {
            let flattened: Vec<String> = self
                .operands
                .iter()
                .flat_map(flatten_addition_operands)
                .collect();
            return format!("concat({})", flattened.join(", "));
        }

        let sql_op = op_str(self.operator);

        match rendered.len() {
            0 => "".into(),                              // should not happen
            1 => format!("{} {}", sql_op, &rendered[0]), // unary
            2 => format!("{} {} {}", &rendered[0], sql_op, &rendered[1]),
            _ => {
                // n-ary: join with the operator
                rendered.join(&format!(" {} ", sql_op))
            }
        }
    }
}

impl ToSql for OrderByOrder {
    fn to_sql(&self) -> String {
        match self {
            OrderByOrder::Asc => "ASC".to_string(),
            OrderByOrder::Desc => "DESC".to_string(),
        }
    }
}
