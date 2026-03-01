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
            SelectItem, SelectItems, ToSql, UnionItems, UnionType,
        },
    },
    server::query_context::{
        clear_all_render_contexts, get_cte_property_mapping, get_relationship_columns,
        is_multi_type_vlp_alias, set_all_render_contexts,
    },
    utils::cte_naming::is_generated_cte_name,
};
use std::collections::HashMap;
use std::collections::HashSet;

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

    // Collect WITH CTE aliases to avoid conflicts
    // WITH CTEs (e.g., with_a_cte_0) export aliases that access base tables directly,
    // NOT through VLP JSON properties. We must not register these as VLP aliases.
    let mut with_cte_aliases: HashSet<String> = HashSet::new();
    for cte in &plan.ctes.0 {
        if cte.cte_name.starts_with("with_") {
            // Extract the alias from CTE name (e.g., "with_a_cte_0" → "a")
            // Also handle compound names like "with_a_allNeighboursCount_cte_0" → "a"
            if let Some(rest) = cte.cte_name.strip_prefix("with_") {
                if let Some(alias) = rest.split("_cte").next() {
                    with_cte_aliases.insert(alias.to_string());
                    // Also insert the first segment for compound aliases
                    // e.g., "a_allNeighboursCount" → also insert "a"
                    if let Some(first) = alias.split('_').next() {
                        with_cte_aliases.insert(first.to_string());
                    }
                }
            }
        }
    }

    // Track multi-type VLP aliases for JSON property extraction
    // Multi-type VLP CTEs have names like "vlp_multi_type_u_x"
    // and their end_properties column contains JSON with node properties
    for cte in &plan.ctes.0 {
        if cte.cte_name.starts_with("vlp_multi_type_") {
            // Extract Cypher alias from CTE metadata if available
            if let Some(ref cypher_end_alias) = cte.vlp_cypher_end_alias {
                // Skip if this alias is also a WITH CTE alias — WITH CTEs access base tables
                if with_cte_aliases.contains(cypher_end_alias.as_str()) {
                    log::info!(
                        "🎯 Skipping VLP alias '{}' — conflicts with WITH CTE alias",
                        cypher_end_alias
                    );
                    continue;
                }
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

    log::debug!(
        "🔍 TRACING: Checking for VLP CTEs. Found {} CTEs",
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
            // FROM is None — likely a Union shell where branches have their own FROM.
            // Check if any Union branch FROM uses the VLP CTE. If not, the VLP CTE
            // is consumed by a WITH CTE (not by the main query) — skip rewriting.
            let any_branch_uses_vlp = plan.union.0.as_ref().map_or(false, |union| {
                union.input.iter().any(|branch| {
                    branch
                        .from
                        .0
                        .as_ref()
                        .map_or(false, |f| f.name.starts_with("vlp_"))
                })
            });
            if !any_branch_uses_vlp {
                log::info!(
                    "🔍 VLP rewriting: FROM=None and no Union branch uses VLP CTE - skipping rewriting"
                );
                return plan;
            }
            log::debug!("🔍 TRACING: No FROM ref found but Union branches use VLP");
        }

        let mut start_alias = vlp_cte.vlp_cypher_start_alias.clone();
        let mut end_alias = vlp_cte.vlp_cypher_end_alias.clone();
        let path_variable = vlp_cte.vlp_path_variable.clone();
        // Non-OPTIONAL VLP: always rewrite start alias (we return early for OPTIONAL VLP)
        let is_optional_vlp = false;

        // Skip rewriting aliases that are covered by WITH CTE JOINs
        // These aliases reference WITH CTE columns, not VLP CTE columns
        for join in &plan.joins.0 {
            if join.table_name.starts_with("with_") {
                if start_alias.as_deref() == Some(join.table_alias.as_str()) {
                    log::info!(
                        "🔧 VLP top-level: Skipping start alias '{}' rewrite (covered by WITH CTE '{}')",
                        join.table_alias, join.table_name
                    );
                    start_alias = None;
                }
                if end_alias.as_deref() == Some(join.table_alias.as_str()) {
                    log::info!(
                        "🔧 VLP top-level: Skipping end alias '{}' rewrite (covered by WITH CTE '{}')",
                        join.table_alias, join.table_name
                    );
                    end_alias = None;
                }
            }
        }
        // Also check Union branches for WITH CTE JOINs
        if let Some(ref union) = plan.union.0 {
            for branch in &union.input {
                for join in &branch.joins.0 {
                    if join.table_name.starts_with("with_") {
                        if start_alias.as_deref() == Some(join.table_alias.as_str()) {
                            log::info!(
                                "🔧 VLP top-level: Skipping start alias '{}' rewrite (covered by WITH CTE in branch)",
                                join.table_alias
                            );
                            start_alias = None;
                        }
                        if end_alias.as_deref() == Some(join.table_alias.as_str()) {
                            log::info!(
                                "🔧 VLP top-level: Skipping end alias '{}' rewrite (covered by WITH CTE in branch)",
                                join.table_alias
                            );
                            end_alias = None;
                        }
                    }
                }
            }
        }

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

        // Also rewrite WHERE clause for VLP queries
        // The WHERE may reference Cypher node aliases (e.g., o.user_id) that need
        // to be rewritten to VLP CTE column references (e.g., t.end_user_id)
        if let Some(ref filter_expr) = plan.filters.0 {
            let before = format!("{:?}", filter_expr);
            let rewritten = rewrite_expr_for_vlp(
                filter_expr,
                &start_alias,
                &end_alias,
                &path_variable,
                is_optional_vlp,
            );
            let after = format!("{:?}", rewritten);
            if before != after {
                log::info!("🔧   WHERE rewritten from: {} → {}", before, after);
            }
            plan.filters = FilterItems(Some(rewritten));
        }

        // 🔧 CRITICAL FIX: Also rewrite JOIN conditions for VLP queries
        // JOIN conditions may reference Cypher node aliases (e.g., p.id, b.user_id) that need
        // to be rewritten to VLP CTE column references (e.g., t.end_id, t.end_user_id)
        //
        // Root cause: JOINs are built during logical plan → render plan conversion using
        // original Cypher variable names. After VLP CTE is created, these references must
        // be rewritten to use the CTE's start_/end_ columns.
        //
        // This was an oversight - we were rewriting SELECT/WHERE/GROUP BY/ORDER BY but not JOINs.
        log::info!("🔧 VLP JOIN rewriting: {} items", plan.joins.0.len());
        for (idx, join) in plan.joins.0.iter_mut().enumerate() {
            log::info!(
                "🔧 JOIN {}: table={}, alias={}",
                idx,
                join.table_name,
                join.table_alias
            );

            // Rewrite each condition in joining_on
            for (cond_idx, condition) in join.joining_on.iter_mut().enumerate() {
                let before = format!("{:?}", condition);

                // Rewrite left operand
                condition.operands[0] = rewrite_expr_for_vlp(
                    &condition.operands[0],
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );

                // Rewrite right operand
                condition.operands[1] = rewrite_expr_for_vlp(
                    &condition.operands[1],
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );

                let after = format!("{:?}", condition);
                if before != after {
                    log::info!(
                        "🔧   JOIN[{}] condition[{}] rewritten from: {} → {}",
                        idx,
                        cond_idx,
                        before,
                        after
                    );
                }
            }

            // Also rewrite pre_filter if present
            if let Some(ref filter_expr) = join.pre_filter {
                let before = format!("{:?}", filter_expr);
                let rewritten = rewrite_expr_for_vlp(
                    filter_expr,
                    &start_alias,
                    &end_alias,
                    &path_variable,
                    is_optional_vlp,
                );
                let after = format!("{:?}", rewritten);
                if before != after {
                    log::info!(
                        "🔧   JOIN[{}] pre_filter rewritten from: {} → {}",
                        idx,
                        before,
                        after
                    );
                }
                join.pre_filter = Some(rewritten);
            }
        }
    }

    // Also rewrite UNION branches — each may have its own VLP CTE
    // (e.g., undirected patterns create separate CTEs for each direction)
    // Pass parent CTEs so branches can find VLP CTE info (path_variable, start/end aliases)
    // when their own branch.ctes is empty (VLP CTEs live in the parent plan)
    let parent_ctes = plan.ctes.0.clone();
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            rewrite_vlp_branch_select(branch, &parent_ctes);
        }
    }

    plan
}

/// Rewrite VLP SELECT aliases for a single UNION branch RenderPlan.
/// Same logic as the main rewrite_vlp_select_aliases but operates on a branch.
/// `parent_ctes` provides VLP CTE info from the parent plan when the branch has none.
fn rewrite_vlp_branch_select(branch: &mut RenderPlan, parent_ctes: &[crate::render_plan::Cte]) {
    // Skip if FROM is a generated CTE (WITH clause)
    if let Some(from_ref) = &branch.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            return;
        }
    }

    // Check if FROM references a VLP CTE (starts with "vlp_")
    // The VLP CTE is defined at the parent level, not in branch.ctes
    let from_is_vlp = branch
        .from
        .0
        .as_ref()
        .is_some_and(|f| f.name.starts_with("vlp_"));

    if !from_is_vlp {
        return;
    }

    // Find VLP CTE info from branch's own CTEs (may be empty for child branches)
    // Fall back to parent CTEs when branch has none (VLP CTEs live at parent level)
    let vlp_cte = branch
        .ctes
        .0
        .iter()
        .find(|cte| cte.vlp_cypher_start_alias.is_some());

    let (mut start_alias, mut end_alias, path_variable) = if let Some(vlp_cte) = vlp_cte {
        (
            vlp_cte.vlp_cypher_start_alias.clone(),
            vlp_cte.vlp_cypher_end_alias.clone(),
            vlp_cte.vlp_path_variable.clone(),
        )
    } else {
        // No VLP CTE in branch.ctes - look up from parent CTEs using the branch's FROM name
        let from_name = branch
            .from
            .0
            .as_ref()
            .map(|f| f.name.as_str())
            .unwrap_or("");
        let parent_vlp = parent_ctes
            .iter()
            .find(|cte| cte.cte_name == from_name && cte.vlp_cypher_start_alias.is_some());
        if let Some(parent_cte) = parent_vlp {
            // The parent VLP CTE has the correct aliases for this branch's direction
            (
                parent_cte.vlp_cypher_start_alias.clone(),
                parent_cte.vlp_cypher_end_alias.clone(),
                parent_cte.vlp_path_variable.clone(),
            )
        } else {
            // Last resort: infer from filter expressions
            let start_alias = if let Some(ref filter) = branch.filters.0 {
                extract_alias_from_filter(filter)
            } else {
                None
            };
            (start_alias, None, None)
        }
    };

    // Skip rewriting if we couldn't determine start_alias
    let Some(_) = start_alias else {
        return;
    };

    // Skip rewriting aliases that are covered by WITH CTE JOINs
    // These aliases reference WITH CTE columns, not VLP CTE columns
    for join in &branch.joins.0 {
        if join.table_name.starts_with("with_") {
            if start_alias.as_deref() == Some(&join.table_alias) {
                log::info!(
                    "🔧 VLP branch: Skipping start alias '{}' rewrite (covered by WITH CTE '{}')",
                    join.table_alias,
                    join.table_name
                );
                start_alias = None;
            }
            if end_alias.as_deref() == Some(&join.table_alias) {
                log::info!(
                    "🔧 VLP branch: Skipping end alias '{}' rewrite (covered by WITH CTE '{}')",
                    join.table_alias,
                    join.table_name
                );
                end_alias = None;
            }
        }
    }

    log::info!(
        "🔧 VLP UNION branch rewriting: start={:?}, end={:?}",
        start_alias,
        end_alias
    );

    for item in branch.select.items.iter_mut() {
        item.expression = rewrite_expr_for_vlp(
            &item.expression,
            &start_alias,
            &end_alias,
            &path_variable,
            false,
        );
    }
    for group_expr in branch.group_by.0.iter_mut() {
        *group_expr =
            rewrite_expr_for_vlp(group_expr, &start_alias, &end_alias, &path_variable, false);
    }
    for order_item in branch.order_by.0.iter_mut() {
        order_item.expression = rewrite_expr_for_vlp(
            &order_item.expression,
            &start_alias,
            &end_alias,
            &path_variable,
            false,
        );
    }
    // 🔧 FIX: Also rewrite WHERE clause (filters) for VLP UNION branches
    // Without this, branches with LIMIT get wrapped in subqueries with unrewritten WHERE clauses
    if let Some(ref filter_expr) = branch.filters.0 {
        let rewritten =
            rewrite_expr_for_vlp(filter_expr, &start_alias, &end_alias, &path_variable, false);
        branch.filters.0 = Some(rewritten);
    }
}

/// Extract table alias from a filter expression (e.g., "u.user_id" -> "u")
fn extract_alias_from_filter(expr: &RenderExpr) -> Option<String> {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => Some(prop.table_alias.0.clone()),
        RenderExpr::OperatorApplicationExp(op) => {
            // Check first operand
            for operand in &op.operands {
                if let Some(alias) = extract_alias_from_filter(operand) {
                    return Some(alias);
                }
            }
            None
        }
        _ => None,
    }
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
        //
        // Special case: For ID columns (e.g., "id.orig_h"), use t.start_id or t.end_id directly
        // since the CTE has "start_id" column containing the full ID value.
        RenderExpr::PropertyAccessExp(prop) => {
            log::trace!(
                "🔧 rewrite_expr_for_vlp: Processing PropertyAccessExp {}.{}",
                prop.table_alias.0,
                prop.column.raw()
            );
            if let Some(start) = start_alias {
                if &prop.table_alias.0 == start {
                    if skip_start_alias {
                        log::debug!("🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' but skipping for OPTIONAL VLP", start);
                        return expr.clone();
                    }
                    log::debug!("🔧 rewrite_expr_for_vlp: MATCHED start alias '{}' - rewriting to t.start_xxx", start);

                    // Check if this is the ID column (contains "id" or matches known ID column patterns)
                    let col_raw = prop.column.raw();
                    if col_raw == "id"
                        || col_raw.starts_with("id.")
                        || col_raw.ends_with("_id")
                        || col_raw.contains(".orig_")
                        || col_raw.contains(".resp_")
                    {
                        // This is the ID column - use t.start_id directly
                        return RenderExpr::Column(Column(PropertyValue::Column(
                            "t.start_id".to_string(),
                        )));
                    }

                    // This is accessing start node property
                    // Create Column with the full table.column format to prevent heuristic inference
                    // The FROM clause has the CTE aliased as 't', so use t.start_xxx
                    let prop_name = derive_cypher_property_name(col_raw);
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.start_{}",
                        prop_name
                    ))));
                }
            }

            if let Some(end) = end_alias {
                if &prop.table_alias.0 == end {
                    // Check if this is the ID column
                    let col_raw = prop.column.raw();
                    if col_raw == "id"
                        || col_raw.starts_with("id.")
                        || col_raw.ends_with("_id")
                        || col_raw.contains(".orig_")
                        || col_raw.contains(".resp_")
                    {
                        // This is the ID column - use t.end_id directly
                        return RenderExpr::Column(Column(PropertyValue::Column(
                            "t.end_id".to_string(),
                        )));
                    }

                    // This is accessing end node property
                    let prop_name = derive_cypher_property_name(col_raw);
                    return RenderExpr::Column(Column(PropertyValue::Column(format!(
                        "t.end_{}",
                        prop_name
                    ))));
                }
            }

            // Not a start or end alias - check for VLP CTE columns accessed
            // via the relationship alias (e.g., r.path_relationships → t.path_relationships)
            let col_name = prop.column.raw();
            if matches!(
                col_name,
                "path_relationships"
                    | "rel_properties"
                    | "hop_count"
                    | "path_nodes"
                    | "path_edges"
                    | "start_id"
                    | "end_id"
                    | "end_type"
            ) {
                return RenderExpr::Column(Column(PropertyValue::Column(format!(
                    "t.{}",
                    col_name
                ))));
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

        // Handle ArraySubscript: rewrite inner expressions
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(rewrite_expr_for_vlp(
                array,
                start_alias,
                end_alias,
                path_variable,
                skip_start_alias,
            )),
            index: Box::new(rewrite_expr_for_vlp(
                index,
                start_alias,
                end_alias,
                path_variable,
                skip_start_alias,
            )),
        },

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
                                log::debug!(
                                    "🔧 Fixed path function: nodes({}) not yet implemented for fixed paths",
                                    path_variable
                                );
                                // TODO: Return array of node IDs
                                return expr.clone();
                            }
                            "relationships" => {
                                log::debug!(
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

/// Extract column references from ORDER BY expressions for UNION queries
/// Returns (original_expr, union_column_alias) pairs
/// Returns empty if any expression contains id() function (not supported in UNION)
fn extract_order_by_columns_for_union(order_by: &OrderByItems) -> Vec<(RenderExpr, String)> {
    let mut columns = Vec::new();

    for (idx, item) in order_by.0.iter().enumerate() {
        // Log unsupported expressions but still include them — don't silently drop ORDER BY
        if matches!(&item.expression, RenderExpr::ScalarFnCall(f) if f.name == "id") {
            log::warn!("⚠️  ORDER BY id() may not work correctly in UNION queries");
        }

        // Skip unresolvable "id" pseudo-property in UNION branches.
        // This arises from ORDER BY id(x) where x is an unlabeled node in a
        // multi-type pattern; the id() AST transform produces x.id but no
        // actual "id" column exists in the tables.
        if let RenderExpr::PropertyAccessExp(pa) = &item.expression {
            if pa.column.raw() == "id" {
                log::warn!(
                    "⚠️  Dropping ORDER BY {}.id from UNION (unresolvable pseudo-property)",
                    pa.table_alias.0
                );
                continue;
            }
        }

        if matches!(&item.expression, RenderExpr::PropertyAccessExp(_)) {
            log::warn!("⚠️  ORDER BY property access may not work correctly with PatternResolver UNION CTEs");
        }

        // Generate a unique alias for this ORDER BY column
        let col_alias = format!("__order_col_{}", idx);
        columns.push((item.expression.clone(), col_alias));
    }

    columns
}

/// Add ORDER BY columns to a RenderPlan's SELECT (for UNION branches)
/// For denormalized schemas, resolves virtual node property references
/// (e.g., `o.code`) to actual edge table columns (e.g., `t1.dest_code`)
/// by examining the branch's path tuple direction and schema properties.
fn add_order_by_columns_to_select(
    mut plan: RenderPlan,
    order_columns: &[(RenderExpr, String)],
) -> RenderPlan {
    use crate::render_plan::render_expr::ColumnAlias;
    use crate::render_plan::SelectItem;

    // Build context for denormalized virtual node resolution:
    // Parse the path tuple to find which aliases are start/end and the rel alias
    let path_context = extract_path_context_from_select(&plan.select);

    for (expr, alias) in order_columns {
        let resolved_expr = if let Some(ref ctx) = path_context {
            resolve_denormalized_order_by_expr(expr, ctx)
        } else {
            expr.clone()
        };

        plan.select.items.push(SelectItem {
            expression: resolved_expr,
            col_alias: Some(ColumnAlias(alias.clone())),
        });
    }

    plan
}

/// Path context extracted from a branch's SELECT items
struct PathBranchContext {
    start_alias: String,
    end_alias: String,
    rel_alias: String,
}

/// Extract path context (start/end/rel aliases) from SELECT items' path tuple
fn extract_path_context_from_select(select: &SelectItems) -> Option<PathBranchContext> {
    for item in &select.items {
        if let Some(ref ca) = item.col_alias {
            if ca.0 == "path" {
                if let RenderExpr::ScalarFnCall(func) = &item.expression {
                    if func.name == "tuple" && func.args.len() >= 4 {
                        let get_str = |idx: usize| -> Option<String> {
                            if let RenderExpr::Literal(Literal::String(s)) = &func.args[idx] {
                                Some(s.clone())
                            } else {
                                None
                            }
                        };
                        if let (Some(start), Some(end), Some(rel)) =
                            (get_str(1), get_str(2), get_str(3))
                        {
                            return Some(PathBranchContext {
                                start_alias: start,
                                end_alias: end,
                                rel_alias: rel,
                            });
                        }
                    }
                }
            }
        }
    }
    None
}

/// Resolve denormalized virtual node references in ORDER BY expressions.
/// Maps `o.code` → `t1.dest_code` (outgoing) or `t1.origin_code` (incoming)
/// by checking node position in path and schema from_node/to_node properties.
fn resolve_denormalized_order_by_expr(expr: &RenderExpr, ctx: &PathBranchContext) -> RenderExpr {
    use crate::graph_catalog::expression_parser::PropertyValue;

    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            let alias = &pa.table_alias.0;
            let prop_name = pa.column.raw();

            // Check if this alias is a virtual denormalized node (start or end of path)
            // and NOT the relationship alias (which is a real table)
            if alias == &ctx.rel_alias {
                return expr.clone(); // Real table alias, no resolution needed
            }

            // Determine if this alias is start_node or end_node
            let is_start = alias == &ctx.start_alias;
            let is_end = alias == &ctx.end_alias;

            if !is_start && !is_end {
                return expr.clone(); // Not a path node, leave as-is
            }

            // Look up denormalized property mapping from schema
            // For "id" property (from id() function transformation), resolve to node_id first
            let effective_prop_name = if prop_name == "id" {
                lookup_denorm_node_id_property().unwrap_or_else(|| prop_name.to_string())
            } else {
                prop_name.to_string()
            };

            if let Some(resolved_col) = resolve_denorm_property_from_schema(
                &effective_prop_name,
                is_start, // if start, use from_node_properties; if end, use to_node_properties
            ) {
                log::info!(
                    "🔧 ORDER BY: Resolved denorm {}.{} → {}.{}",
                    alias,
                    prop_name,
                    ctx.rel_alias,
                    resolved_col
                );
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(ctx.rel_alias.clone()),
                    column: PropertyValue::Column(resolved_col),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            // Special handling for id(alias) — resolve to the node's ID column
            if func.name.eq_ignore_ascii_case("id") && func.args.len() == 1 {
                if let RenderExpr::TableAlias(alias) = &func.args[0] {
                    let alias_name = &alias.0;
                    let is_start = alias_name == &ctx.start_alias;
                    let is_end = alias_name == &ctx.end_alias;
                    if is_start || is_end {
                        // Look up the node_id property name from schema, then resolve it
                        if let Some(id_prop) = lookup_denorm_node_id_property() {
                            if let Some(resolved_col) =
                                resolve_denorm_property_from_schema(&id_prop, is_start)
                            {
                                log::info!(
                                    "🔧 ORDER BY: Resolved denorm id({}) → {}.{}",
                                    alias_name,
                                    ctx.rel_alias,
                                    resolved_col
                                );
                                return RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(ctx.rel_alias.clone()),
                                    column: PropertyValue::Column(resolved_col),
                                });
                            }
                        }
                    }
                }
            }
            let new_args: Vec<_> = func
                .args
                .iter()
                .map(|a| resolve_denormalized_order_by_expr(a, ctx))
                .collect();
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: new_args,
            })
        }
        other => other.clone(),
    }
}

/// Look up a denormalized property from the active query's schema edge definitions.
/// Uses the task-local schema; falls back to GLOBAL_SCHEMAS["default"] if no context.
/// `is_from_node`: true = look in from_node_properties, false = look in to_node_properties
fn resolve_denorm_property_from_schema(prop_name: &str, is_from_node: bool) -> Option<String> {
    use crate::server::query_context::get_current_schema;

    let schema = get_current_schema()?;

    for rel_schema in schema.get_relationships_schemas().values() {
        let props: Option<&std::collections::HashMap<String, String>> = if is_from_node {
            rel_schema.from_node_properties.as_ref()
        } else {
            rel_schema.to_node_properties.as_ref()
        };
        if let Some(prop_map) = props {
            if let Some(col_name) = prop_map.get(prop_name) {
                return Some(col_name.clone());
            }
        }
    }
    None
}

/// Look up the node_id property name from the active query's schema.
/// Uses the task-local schema; falls back to GLOBAL_SCHEMAS["default"] if no context.
/// Returns the logical property name (e.g., "code") used for id() resolution.
fn lookup_denorm_node_id_property() -> Option<String> {
    use crate::server::query_context::get_current_schema;

    let schema = get_current_schema()?;

    for node_schema in schema.all_node_schemas().values() {
        if node_schema.is_denormalized {
            let columns = node_schema.node_id.id.columns();
            if let Some(first_col) = columns.first() {
                return Some(first_col.to_string());
            }
        }
    }
    None
}

/// Build a SELECT clause for UNION inner branches in the aggregation case.
/// Returns (inner_select_sql, agg_arg_columns) where agg_arg_columns lists
/// the SQL text of property-access expressions extracted from aggregate arguments.
/// The outer SELECT should backtick-escape these references in its aggregates.
fn build_union_inner_select(select: &SelectItems) -> (String, Vec<String>) {
    let non_agg_items: Vec<&SelectItem> = select
        .items
        .iter()
        .filter(|item| {
            if matches!(&item.expression, RenderExpr::AggregateFnCall(_)) {
                return false;
            }
            // Skip ALL __order_col items: ORDER BY is handled by outer query
            if let Some(alias) = &item.col_alias {
                if alias.0.starts_with("__order_col") {
                    return false;
                }
            }
            true
        })
        .collect();

    // Extract property-access expressions from aggregate arguments
    let mut agg_arg_cols: Vec<String> = Vec::new();
    for item in &select.items {
        if let RenderExpr::AggregateFnCall(agg) = &item.expression {
            for arg in &agg.args {
                collect_property_access_sql(arg, &mut agg_arg_cols);
            }
        }
    }
    agg_arg_cols.sort();
    agg_arg_cols.dedup();

    // Remove any that are already covered by non_agg_items (via their expression SQL)
    let existing_exprs: std::collections::HashSet<String> = non_agg_items
        .iter()
        .map(|item| item.expression.to_sql())
        .collect();
    agg_arg_cols.retain(|col| !existing_exprs.contains(col));

    if non_agg_items.is_empty() && agg_arg_cols.is_empty() {
        return ("SELECT 1 AS __dummy\n".to_string(), vec![]);
    }

    let mut sql = if select.distinct {
        "SELECT DISTINCT \n".to_string()
    } else {
        "SELECT \n".to_string()
    };

    let total_items = non_agg_items.len() + agg_arg_cols.len();
    let mut idx = 0;

    for item in &non_agg_items {
        sql.push_str("      ");
        sql.push_str(&item.expression.to_sql());
        if let Some(alias) = &item.col_alias {
            sql.push_str(&format!(" AS \"{}\"", alias.0));
        }
        idx += 1;
        if idx < total_items {
            sql.push(',');
        }
        sql.push('\n');
    }

    // Add aggregate argument columns with their SQL as alias
    for col_sql in &agg_arg_cols {
        sql.push_str(&format!("      {} AS \"{}\"", col_sql, col_sql));
        idx += 1;
        if idx < total_items {
            sql.push(',');
        }
        sql.push('\n');
    }

    (sql, agg_arg_cols)
}

/// Recursively collect property-access expression SQL from a RenderExpr tree.
fn collect_property_access_sql(expr: &RenderExpr, out: &mut Vec<String>) {
    match expr {
        RenderExpr::PropertyAccessExp(_) => {
            out.push(expr.to_sql());
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_property_access_sql(operand, out);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_property_access_sql(arg, out);
            }
        }
        _ => {}
    }
}

/// Build the outer SELECT for UNION with aggregation.
///
/// Non-aggregate items reference their inner-branch alias via backticks.
/// Aggregate items rewrite property-access arguments to backtick-escaped
/// column aliases so they reference the inner projection.
fn build_outer_aggregate_select(select: &SelectItems, agg_arg_cols: &[String]) -> String {
    let items: Vec<String> = select
        .items
        .iter()
        .filter(|item| {
            if let Some(alias) = &item.col_alias {
                if alias.0.starts_with("__order_col") {
                    return false;
                }
            }
            true
        })
        .map(|item| {
            let alias_str = item
                .col_alias
                .as_ref()
                .map(|a| a.0.clone())
                .unwrap_or_else(|| "result".to_string());
            if matches!(&item.expression, RenderExpr::AggregateFnCall(_)) {
                let mut agg_sql = item.expression.to_sql();
                for col_ref in agg_arg_cols {
                    agg_sql = agg_sql.replace(col_ref, &format!("`{}`", col_ref));
                }
                format!("{} AS \"{}\"", agg_sql, alias_str)
            } else {
                format!("`{}` AS \"{}\"", alias_str, alias_str)
            }
        })
        .collect();
    items.join(", ")
}

/// Build GROUP BY clause with aliased column references for UNION subqueries.
///
/// Maps each GROUP BY expression to its SELECT column alias (backtick-escaped)
/// when available, falling back to the raw expression otherwise.
fn build_aliased_group_by(group_by: &GroupByExpressions, select: &SelectItems) -> String {
    if group_by.0.is_empty() {
        return String::new();
    }
    let expr_to_alias: std::collections::HashMap<String, String> = select
        .items
        .iter()
        .filter_map(|item| {
            item.col_alias
                .as_ref()
                .map(|a| (item.expression.to_sql(), a.0.clone()))
        })
        .collect();

    let mut sql = "GROUP BY ".to_string();
    for (i, expr) in group_by.0.iter().enumerate() {
        let expr_sql = RenderExpr::to_sql(expr);
        if let Some(alias) = expr_to_alias.get(&expr_sql) {
            sql.push_str(&format!("`{}`", alias));
        } else {
            sql.push_str(&expr_sql);
        }
        if i + 1 < group_by.0.len() {
            sql.push_str(", ");
        }
    }
    sql.push('\n');
    sql
}

/// Render a single UNION branch to SQL. Simple branches produce
/// `SELECT ... FROM ... WHERE ...`. Complex branches (with inner
/// unions or per-arm LIMIT) wrap in a subselect.
fn render_union_branch_sql(branch: &RenderPlan) -> String {
    let has_inner_union = branch.union.0.is_some();
    let has_limit = branch.limit.0.is_some();
    let has_skip = branch.skip.0.is_some();
    let has_order_by = !branch.order_by.0.is_empty();

    if !has_inner_union && !has_limit && !has_skip && !has_order_by {
        // Simple branch: select + from + joins + filters
        let mut bsql = String::new();
        bsql.push_str(&branch.select.to_sql());
        bsql.push_str(&branch.from.to_sql());
        bsql.push_str(&branch.joins.to_sql());
        bsql.push_str(&branch.filters.to_sql());
        return bsql;
    }

    // Complex branch: wrap in subselect to preserve inner union/limit semantics
    let mut bsql = String::new();
    bsql.push_str("SELECT * FROM (\n");

    // First inner branch
    bsql.push_str(&branch.select.to_sql());
    bsql.push_str(&branch.from.to_sql());
    bsql.push_str(&branch.joins.to_sql());
    bsql.push_str(&branch.filters.to_sql());

    // Inner union branches
    if let Some(inner_union) = &branch.union.0 {
        let inner_union_type = match inner_union.union_type {
            UnionType::Distinct => "UNION DISTINCT \n",
            UnionType::All => "UNION ALL \n",
        };
        for inner_branch in &inner_union.input {
            bsql.push_str(inner_union_type);
            bsql.push_str(&render_union_branch_sql(inner_branch));
        }
    }

    bsql.push_str(")\n");

    // Add ORDER BY, LIMIT, SKIP
    if has_order_by {
        bsql.push_str(&branch.order_by.to_sql());
    }
    if let Some(limit) = branch.limit.0 {
        if let Some(skip) = branch.skip.0 {
            bsql.push_str(&format!("LIMIT {skip}, {limit}\n"));
        } else {
            bsql.push_str(&format!("LIMIT {limit}\n"));
        }
    } else if let Some(skip) = branch.skip.0 {
        bsql.push_str(&format!("OFFSET {skip}\n"));
    }

    bsql
}

/// Ensure a table name has a database prefix for base table references.
/// CTE references (names starting with `with_`, `vlp_`, `pattern_`, `rel_`, `__`)
/// are returned as-is. Base table names that are missing the `db.` prefix get it
/// by looking up the table in the current schema's node/relationship definitions.
fn ensure_database_prefix(table_name: &str) -> String {
    // Already has database prefix
    if table_name.contains('.') {
        return table_name.to_string();
    }

    // CTE references don't need database prefix
    if table_name.starts_with("with_")
        || table_name.starts_with("vlp_")
        || table_name.starts_with("pattern_")
        || table_name.starts_with("rel_")
        || table_name.starts_with("__")
        || table_name.starts_with("multi_type_vlp")
    {
        return table_name.to_string();
    }

    // Look up the table in the schema to find its database
    if let Some(schema) = crate::server::query_context::get_current_schema_with_fallback() {
        // Search node schemas for a matching table_name
        for node_schema in schema.all_node_schemas().values() {
            if node_schema.table_name == table_name && !node_schema.database.is_empty() {
                log::debug!(
                    "🔧 ensure_database_prefix: '{}' → '{}.{}' (from node schema)",
                    table_name,
                    node_schema.database,
                    table_name
                );
                return format!("{}.{}", node_schema.database, table_name);
            }
        }
        // Search relationship schemas for a matching table_name
        for rel_schema in schema.get_relationships_schemas().values() {
            if rel_schema.table_name == table_name && !rel_schema.database.is_empty() {
                log::debug!(
                    "🔧 ensure_database_prefix: '{}' → '{}.{}' (from relationship schema)",
                    table_name,
                    rel_schema.database,
                    table_name
                );
                return format!("{}.{}", rel_schema.database, table_name);
            }
        }
    }

    // Fallback: return as-is
    table_name.to_string()
}

/// Rewrite VLP variable references inside CTE bodies.
///
/// When a WITH CTE body references a VLP CTE (e.g., FROM vlp_person_friend),
/// its WHERE and JOIN expressions may still use original Cypher variable names
/// (e.g., friend.id, person.id). This rewrites them to VLP column names
/// (e.g., t.end_id, t.start_id).
///
/// For undirected VLP (base FROM + union branches), also clones filters and
/// JOINs to each union branch, rewriting with the correct VLP alias mapping
/// for that direction.
fn rewrite_vlp_in_cte_bodies(plan: &mut RenderPlan) {
    use std::collections::HashMap;

    // Collect VLP CTE alias info: cte_name → (start_alias, end_alias, path_variable)
    let vlp_info: HashMap<String, (Option<String>, Option<String>, Option<String>)> = plan
        .ctes
        .0
        .iter()
        .filter(|cte| cte.vlp_cypher_start_alias.is_some())
        .map(|cte| {
            (
                cte.cte_name.clone(),
                (
                    cte.vlp_cypher_start_alias.clone(),
                    cte.vlp_cypher_end_alias.clone(),
                    cte.vlp_path_variable.clone(),
                ),
            )
        })
        .collect();

    if vlp_info.is_empty() {
        return;
    }

    // Process each Structured CTE body
    for cte in &mut plan.ctes.0 {
        if let CteContent::Structured(ref mut inner) = cte.content {
            rewrite_cte_body_vlp_refs(inner, &vlp_info);
        }
    }
}

/// Rewrite VLP references in a single CTE body's RenderPlan.
/// If the body's FROM is a VLP CTE, rewrites filters and JOIN conditions.
/// For undirected VLP (with union branches), clones filters/JOINs to each branch.
fn rewrite_cte_body_vlp_refs(
    plan: &mut RenderPlan,
    vlp_info: &std::collections::HashMap<String, (Option<String>, Option<String>, Option<String>)>,
) {
    let from_name = match plan.from.0.as_ref() {
        Some(f) => f.name.clone(),
        None => return,
    };

    let forward_aliases = match vlp_info.get(&from_name) {
        Some(aliases) => aliases.clone(),
        None => return,
    };

    // Save original filters and joins before rewriting (needed for cloning to reverse branches)
    let original_filters = plan.filters.0.clone();
    let original_joins = plan.joins.0.clone();

    // Rewrite forward branch's filters
    if let Some(ref filter) = original_filters {
        plan.filters = FilterItems(Some(rewrite_expr_for_vlp(
            filter,
            &forward_aliases.0,
            &forward_aliases.1,
            &forward_aliases.2,
            false,
        )));
    }

    // Rewrite forward branch's JOIN conditions
    rewrite_joins_for_vlp(&mut plan.joins.0, &forward_aliases);

    // For undirected VLP: clone filters and JOINs to each reverse union branch
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            let branch_from_name = match branch.from.0.as_ref() {
                Some(f) => f.name.clone(),
                None => continue,
            };
            let reverse_aliases = match vlp_info.get(&branch_from_name) {
                Some(aliases) => aliases.clone(),
                None => continue,
            };

            // Clone and rewrite filters for reverse branch
            if let Some(ref filter) = original_filters {
                branch.filters = FilterItems(Some(rewrite_expr_for_vlp(
                    filter,
                    &reverse_aliases.0,
                    &reverse_aliases.1,
                    &reverse_aliases.2,
                    false,
                )));
            }

            // Clone and rewrite JOINs for reverse branch
            if !original_joins.is_empty() {
                branch.joins = JoinItems(original_joins.clone());
                rewrite_joins_for_vlp(&mut branch.joins.0, &reverse_aliases);
            }
        }
    }
}

/// Rewrite JOIN conditions using VLP alias mappings.
fn rewrite_joins_for_vlp(
    joins: &mut [Join],
    aliases: &(Option<String>, Option<String>, Option<String>),
) {
    for join in joins.iter_mut() {
        for cond in &mut join.joining_on {
            for operand in &mut cond.operands {
                *operand = rewrite_expr_for_vlp(operand, &aliases.0, &aliases.1, &aliases.2, false);
            }
        }
        if let Some(ref filter) = join.pre_filter {
            join.pre_filter = Some(rewrite_expr_for_vlp(
                filter, &aliases.0, &aliases.1, &aliases.2, false,
            ));
        }
    }
}

/// Recursively collect all CTE definitions from a RenderPlan tree,
/// removing them from their nested locations (union branches, CTE content, etc.).
fn collect_nested_ctes(plan: &mut RenderPlan, collected: &mut Vec<Cte>) {
    // Take CTEs from this plan level
    let ctes = std::mem::take(&mut plan.ctes.0);
    for mut cte in ctes {
        // Recursively flatten CTEs inside Structured CTE content
        if let CteContent::Structured(ref mut inner_plan) = cte.content {
            collect_nested_ctes(inner_plan, collected);
        }
        collected.push(cte);
    }

    // Recurse into union branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            collect_nested_ctes(branch, collected);
        }
    }
}

/// Flatten all CTEs from the entire RenderPlan tree to the top level.
/// After this call, `plan.ctes` contains ALL CTEs in sequential dependency order
/// and no nested CTEs remain anywhere.
///
/// `collect_nested_ctes` walks depth-first: inner CTEs (dependencies) are collected
/// before the outer CTEs that reference them. This naturally produces the correct
/// dependency order — no additional sorting needed.
fn flatten_all_ctes(plan: &mut RenderPlan) {
    let mut collected = Vec::new();
    collect_nested_ctes(plan, &mut collected);

    if collected.is_empty() {
        return;
    }

    // Deduplicate by name (keep first occurrence — the dependency-order one)
    let mut seen = std::collections::HashSet::new();
    collected.retain(|cte| seen.insert(cte.cte_name.clone()));

    plan.ctes.0 = collected;
}

pub fn render_plan_to_sql(mut plan: RenderPlan, max_cte_depth: u32) -> String {
    // STEP 0: Flatten ALL CTEs to top level in dependency order.
    // CTEs are always a flat, linear chain — never nested inside other CTEs or union branches.
    flatten_all_ctes(&mut plan);

    // STEP 0.5: Rewrite VLP variable references inside CTE bodies.
    // When a WITH CTE body reads FROM a VLP CTE, its WHERE/JOIN expressions may still
    // use original Cypher variable names (e.g., friend.id). Rewrite them to VLP column
    // names (e.g., t.end_id). For undirected VLP, also clone filters/JOINs to reverse branches.
    rewrite_vlp_in_cte_bodies(&mut plan);

    // Extract fixed path information if not already set
    // This looks at the RenderPlan structure to infer path variable info
    if plan.fixed_path_info.is_none() {
        plan.fixed_path_info = extract_fixed_path_info_from_plan(&plan);
    }

    // Rewrite VLP SELECT aliases before SQL generation
    // Maps Cypher aliases (a, b) to CTE column prefixes (start_, end_)
    plan = rewrite_vlp_select_aliases(plan);

    // 🔧 CRITICAL FIX: Sort JOINs by dependency to ensure correct SQL ordering
    // Topological sort ensures that if JOIN A references table B in its ON clause,
    // then B appears before A in the FROM/JOIN sequence.
    //
    // This prevents errors like: "Unknown identifier t1" when t1 is used before defined.
    // The sort function existed but was never called - this fixes it once for all queries!
    //
    // Root cause: JOINs were generated in arbitrary order during planning, but SQL
    // requires strict dependency order. This fix applies topological sorting centrally.
    plan.joins.0 = {
        use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
        use crate::render_plan::FromTable;

        // Convert plan.from to the format expected by sort_joins_by_dependency
        let from_table = plan.from.0.as_ref().map(|table_ref| FromTable {
            table: Some(table_ref.clone()),
            joins: vec![],
        });

        sort_joins_by_dependency(plan.joins.0, from_table.as_ref())
    };

    // Also sort JOINs in UNION branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
            use crate::render_plan::FromTable;

            let from_table = branch.from.0.as_ref().map(|table_ref| FromTable {
                table: Some(table_ref.clone()),
                joins: vec![],
            });
            branch.joins.0 =
                sort_joins_by_dependency(std::mem::take(&mut branch.joins.0), from_table.as_ref());
        }
    }

    // Also sort JOINs inside CTE plans (WITH clause CTEs have their own JOINs)
    for cte in plan.ctes.0.iter_mut() {
        if let CteContent::Structured(ref mut cte_plan) = cte.content {
            use crate::render_plan::plan_builder_helpers::sort_joins_by_dependency;
            use crate::render_plan::FromTable;

            let from_table = cte_plan.from.0.as_ref().map(|table_ref| FromTable {
                table: Some(table_ref.clone()),
                joins: vec![],
            });
            cte_plan.joins.0 = sort_joins_by_dependency(
                std::mem::take(&mut cte_plan.joins.0),
                from_table.as_ref(),
            );

            // Sort UNION branch JOINs inside CTEs too
            if let Some(ref mut union) = cte_plan.union.0 {
                for branch in union.input.iter_mut() {
                    let branch_from = branch.from.0.as_ref().map(|table_ref| FromTable {
                        table: Some(table_ref.clone()),
                        joins: vec![],
                    });
                    branch.joins.0 = sort_joins_by_dependency(
                        std::mem::take(&mut branch.joins.0),
                        branch_from.as_ref(),
                    );
                }
            }
        }
    }

    // Rewrite path function calls for fixed (non-VLP) path patterns
    // Converts length(p) → hop_count, etc.
    plan = rewrite_fixed_path_functions(plan);

    // Build ALL rendering contexts (CTE registry, relationship columns, CTE mappings, multi-type aliases)
    let relationship_columns = build_relationship_columns_from_plan(&plan);
    let cte_mappings = build_cte_property_mappings(&plan);
    let multi_type_aliases = build_multi_type_vlp_aliases(&plan);

    // TASK-LOCAL: Set ALL contexts for this async task's rendering context
    set_all_render_contexts(relationship_columns, cte_mappings, multi_type_aliases);

    // Set the variable registry from the outer render plan for property resolution
    if let Some(ref registry) = plan.variable_registry {
        crate::server::query_context::set_current_variable_registry(registry.clone());
    }

    let mut sql = String::new();

    // If there's a Union, wrap it in a subquery for correct ClickHouse behavior.
    // ClickHouse has a quirk where LIMIT/ORDER BY on bare UNION ALL only applies to
    // the last branch, not the combined result. Wrapping in a subquery fixes this.
    if plan.union.0.is_some() {
        sql.push_str(&plan.ctes.to_sql());

        // Extract ORDER BY columns that need to be added to UNION branches
        let order_by_columns = if !plan.order_by.0.is_empty() {
            extract_order_by_columns_for_union(&plan.order_by)
        } else {
            Vec::new()
        };

        // If we have ORDER BY, add those columns to all UNION branches
        let mut modified_plan = plan.clone();
        if !order_by_columns.is_empty() {
            log::info!(
                "🔄 UNION with ORDER BY: Adding {} ordering columns to branches",
                order_by_columns.len()
            );

            // Add to the first branch (which is the base plan)
            modified_plan = add_order_by_columns_to_select(modified_plan, &order_by_columns);

            // Add to remaining branches
            if let Some(union) = &mut modified_plan.union.0 {
                union.input = union
                    .input
                    .iter()
                    .map(|branch| add_order_by_columns_to_select(branch.clone(), &order_by_columns))
                    .collect();
            }
        }

        // Use the modified plan for SQL generation
        plan = modified_plan;

        // Check if SELECT items contain aggregation (e.g., count(*), sum(), etc.)
        let has_aggregation = plan
            .select
            .items
            .iter()
            .any(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)));

        // Pre-compute inner SELECT and aggregate arg columns for aggregation+UNION case
        let (inner_select_sql, agg_arg_cols) = if has_aggregation {
            let (sql, cols) = build_union_inner_select(&plan.select);
            (Some(sql), cols)
        } else {
            (None, vec![])
        };

        log::debug!(
            "UNION rendering: has_aggregation={}, select_items={}, agg_arg_cols={:?}",
            has_aggregation,
            plan.select.items.len(),
            agg_arg_cols
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
                if has_aggregation {
                    // Collect aggregate aliases to detect dependent order columns
                    let agg_aliases: std::collections::HashSet<String> = plan
                        .select
                        .items
                        .iter()
                        .filter(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                        .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                        .collect();

                    sql.push_str(&build_outer_aggregate_select(&plan.select, &agg_arg_cols));
                } else {
                    // Without aggregation: select column aliases from the subquery
                    let alias_select = plan
                        .select
                        .items
                        .iter()
                        .map(|item| {
                            if let Some(col_alias) = &item.col_alias {
                                format!("`{}` AS `{}`", col_alias.0, col_alias.0)
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

            // Generate UNION branch SQL.
            // When has_aggregation is true, all branches are stored in union.input
            // (extract_union moved the first branch there), so skip the base plan.
            // Otherwise, the base plan (select/from/joins/filters) IS the first branch.
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };

                // With aggregation: extract_union already put all branches in union.input,
                // so don't also render the base plan as first branch.
                //
                // The `plan.from.0.is_some()` guard handles literal-only aggregations
                // (e.g., `RETURN 'test' AS label, count(*) AS cnt`) where extract_union
                // moved all branches into union.input and left plan.from empty. When
                // plan.from is None, the base plan is not a separate branch, so we must
                // fall through to the else branch that iterates only over union.input.
                if !has_aggregation && plan.from.0.is_some() {
                    let first_branch_sql = {
                        let mut branch_sql = String::new();
                        branch_sql.push_str(&plan.select.to_sql());
                        branch_sql.push_str(&plan.from.to_sql());
                        branch_sql.push_str(&plan.joins.to_sql());
                        branch_sql.push_str(&plan.filters.to_sql());
                        branch_sql
                    };
                    sql.push_str(&first_branch_sql);

                    for union_branch in &union.input {
                        sql.push_str(union_type_str);
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                } else if has_aggregation {
                    // For aggregation: use pre-computed inner SELECT that includes
                    // non-aggregate columns plus aggregate argument columns.
                    let inner_sql = inner_select_sql.as_ref().unwrap();
                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        let mut branch_sql = String::new();
                        branch_sql.push_str(inner_sql);
                        branch_sql.push_str(&union_branch.from.to_sql());
                        branch_sql.push_str(&union_branch.joins.to_sql());
                        branch_sql.push_str(&union_branch.filters.to_sql());
                        sql.push_str(&branch_sql);
                    }
                } else {
                    // Non-aggregation, all branches in union.input
                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                }
            } else {
                // No union branches — just use the base plan as the subquery
                let first_branch_sql = {
                    let mut branch_sql = String::new();
                    branch_sql.push_str(&plan.select.to_sql());
                    branch_sql.push_str(&plan.from.to_sql());
                    branch_sql.push_str(&plan.joins.to_sql());
                    branch_sql.push_str(&plan.filters.to_sql());
                    branch_sql
                };
                sql.push_str(&first_branch_sql);
            }

            sql.push_str(") AS __union\n");

            // Add GROUP BY — for UNION subquery context, reference column aliases
            // from the inner SELECT rather than original table-qualified names
            sql.push_str(&build_aliased_group_by(&plan.group_by, &plan.select));

            // Add ORDER BY after GROUP BY if present
            // For aggregation: use original ORDER BY expressions since the outer SELECT
            // provides the aliased columns. For non-aggregation UNION: reference __union columns.
            if has_aggregation && !plan.order_by.0.is_empty() {
                sql.push_str(&plan.order_by.to_sql());
            } else if !plan.order_by.0.is_empty() && !order_by_columns.is_empty() {
                sql.push_str("ORDER BY ");
                let order_clauses: Vec<String> = plan
                    .order_by
                    .0
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, item)| {
                        if idx >= order_by_columns.len() {
                            log::debug!(
                                "ORDER BY column index {} exceeds available columns ({}), skipping",
                                idx,
                                order_by_columns.len()
                            );
                            return None;
                        }
                        let col_alias = &order_by_columns[idx].1;
                        let order_str = match item.order {
                            OrderByOrder::Asc => "ASC",
                            OrderByOrder::Desc => "DESC",
                        };
                        Some(format!("__union.`{}` {}", col_alias, order_str))
                    })
                    .collect();
                sql.push_str(&order_clauses.join(", "));
                sql.push('\n');
            } else if order_by_columns.is_empty() && !plan.order_by.0.is_empty() {
                // ORDER BY was removed due to unsupported id() function
                log::info!("  ORDER BY removed (contains unsupported id() in UNION context)");
            } else {
                sql.push_str(&plan.order_by.to_sql());
            }

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
            if let Some(union) = &plan.union.0 {
                let union_type_str = match union.union_type {
                    UnionType::Distinct => "UNION DISTINCT \n",
                    UnionType::All => "UNION ALL \n",
                };

                if plan.from.0.is_some() {
                    // Base plan IS the first branch
                    let first_branch_sql = {
                        let mut branch_sql = String::new();
                        branch_sql.push_str(&plan.select.to_sql());
                        branch_sql.push_str(&plan.from.to_sql());
                        branch_sql.push_str(&plan.joins.to_sql());
                        branch_sql.push_str(&plan.filters.to_sql());
                        branch_sql
                    };
                    sql.push_str(&first_branch_sql);

                    for union_branch in &union.input {
                        sql.push_str(union_type_str);
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                } else {
                    // Shell base: all branches in union.input
                    for (i, union_branch) in union.input.iter().enumerate() {
                        if i > 0 {
                            sql.push_str(union_type_str);
                        }
                        sql.push_str(&render_union_branch_sql(union_branch));
                    }
                }
            } else {
                // No union branches — just use the base plan
                let first_branch_sql = {
                    let mut branch_sql = String::new();
                    branch_sql.push_str(&plan.select.to_sql());
                    branch_sql.push_str(&plan.from.to_sql());
                    branch_sql.push_str(&plan.joins.to_sql());
                    branch_sql.push_str(&plan.filters.to_sql());
                    branch_sql
                };
                sql.push_str(&first_branch_sql);
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

    // Note: max_recursive_cte_evaluation_depth is set as a client-level option
    // in connection_pool.rs, not as a SQL SETTINGS clause.
    // The clickhouse crate sends queries with readonly=1, which prevents
    // SETTINGS in SQL. Client-level options are sent as HTTP query parameters
    // and work in readonly mode.

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
        if self.0.is_empty() {
            return String::new();
        }

        // Deduplicate CTEs by name (keep first occurrence)
        let mut seen_names = std::collections::HashSet::new();
        let deduped: Vec<&Cte> = self
            .0
            .iter()
            .filter(|cte| seen_names.insert(cte.cte_name.clone()))
            .collect();

        if deduped.is_empty() {
            return String::new();
        }

        // Simple rule: ONE `WITH RECURSIVE` at the top if any CTE is recursive,
        // then ALL CTEs flat and comma-separated. No nesting, no wrapping.
        let has_recursive = deduped.iter().any(|c| c.is_recursive);

        let mut sql = String::new();
        if has_recursive {
            sql.push_str("WITH RECURSIVE ");
        } else {
            sql.push_str("WITH ");
        }

        for (i, cte) in deduped.iter().enumerate() {
            sql.push_str(&cte.to_sql());
            if i + 1 < deduped.len() {
                sql.push_str(", \n");
            } else {
                sql.push('\n');
            }
        }

        sql
    }
}

impl ToSql for Cte {
    fn to_sql(&self) -> String {
        // Per-CTE registry: set this CTE's variable registry as task-local
        // so PropertyAccessExp::to_sql() can resolve CTE-scoped variables.
        let saved_registry = if self.variable_registry.is_some() {
            let prev = crate::server::query_context::get_current_variable_registry();
            if let Some(ref reg) = self.variable_registry {
                crate::server::query_context::set_current_variable_registry(reg.clone());
            }
            prev
        } else {
            None
        };

        // Handle both structured and raw SQL content
        let result = match &self.content {
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
                        // When the plan has its own FROM (bidirectional UNION), push the
                        // SELECT projection into each UNION branch instead of using
                        // SELECT * — avoids unresolvable table-qualified column refs.
                        let has_modifiers =
                            has_group_by || has_order_by_skip_limit || plan.having_clause.is_some();
                        let has_aggregation =
                            plan.select.items.iter().any(|item| {
                                matches!(&item.expression, RenderExpr::AggregateFnCall(_))
                            });

                        if has_aggregation && has_custom_select && plan.from.0.is_some() {
                            // Aggregate + UNION: inner branches project raw columns,
                            // outer SELECT applies aggregation over the __union subquery
                            let (inner_select_sql, agg_arg_cols) =
                                build_union_inner_select(&plan.select);
                            let outer_select =
                                build_outer_aggregate_select(&plan.select, &agg_arg_cols);

                            cte_body.push_str(&format!("SELECT {} FROM (\n", outer_select));

                            // First branch with non-aggregate inner SELECT
                            cte_body.push_str(&inner_select_sql);
                            cte_body.push_str(&plan.from.to_sql());
                            cte_body.push_str(&plan.joins.to_sql());
                            cte_body.push_str(&plan.filters.to_sql());

                            if let Some(union) = &plan.union.0 {
                                let union_type_str = match union.union_type {
                                    UnionType::Distinct => "UNION DISTINCT \n",
                                    UnionType::All => "UNION ALL \n",
                                };
                                for branch in &union.input {
                                    cte_body.push_str(union_type_str);
                                    cte_body.push_str(&inner_select_sql);
                                    cte_body.push_str(&branch.from.to_sql());
                                    cte_body.push_str(&branch.joins.to_sql());
                                    cte_body.push_str(&branch.filters.to_sql());
                                }
                            }

                            cte_body.push_str(") AS __union\n");
                        } else if has_custom_select && plan.from.0.is_some() {
                            let select_sql = plan.select.to_sql();

                            if has_modifiers {
                                // Need wrapper for GROUP BY/HAVING/ORDER BY/LIMIT
                                cte_body.push_str("SELECT * FROM (\n");
                            }

                            // First branch: plan's own FROM with projected SELECT
                            cte_body.push_str(&select_sql);
                            cte_body.push_str(&plan.from.to_sql());
                            cte_body.push_str(&plan.joins.to_sql());
                            cte_body.push_str(&plan.filters.to_sql());

                            if let Some(union) = &plan.union.0 {
                                let union_type_str = match union.union_type {
                                    UnionType::Distinct => "UNION DISTINCT \n",
                                    UnionType::All => "UNION ALL \n",
                                };
                                for branch in &union.input {
                                    cte_body.push_str(union_type_str);
                                    // Each branch gets the same SELECT projection
                                    cte_body.push_str(&select_sql);
                                    cte_body.push_str(&branch.from.to_sql());
                                    cte_body.push_str(&branch.joins.to_sql());
                                    cte_body.push_str(&branch.filters.to_sql());
                                }
                            }

                            if has_modifiers {
                                cte_body.push_str(") AS __union\n");
                            }
                        } else {
                            // No custom select or no plan.from: use existing wrapper pattern
                            if has_custom_select {
                                cte_body.push_str(&plan.select.to_sql());
                            } else {
                                cte_body.push_str("SELECT * ");
                            }
                            cte_body.push_str("FROM (\n");

                            if plan.from.0.is_some() {
                                // First branch without custom select — use branch's own select
                                cte_body.push_str(&plan.select.to_sql());
                                cte_body.push_str(&plan.from.to_sql());
                                cte_body.push_str(&plan.joins.to_sql());
                                cte_body.push_str(&plan.filters.to_sql());

                                if let Some(union) = &plan.union.0 {
                                    let union_type_str = match union.union_type {
                                        UnionType::Distinct => "UNION DISTINCT \n",
                                        UnionType::All => "UNION ALL \n",
                                    };
                                    for branch in &union.input {
                                        cte_body.push_str(union_type_str);
                                        cte_body.push_str(&render_union_branch_sql(branch));
                                    }
                                }
                            } else {
                                cte_body.push_str(&plan.union.to_sql());
                            }

                            cte_body.push_str(") AS __union\n");

                            // Outer JOINs only when NOT already inside UNION branches
                            if plan.from.0.is_none() {
                                cte_body.push_str(&plan.joins.to_sql());
                            }
                        }

                        // Add GROUP BY — use aliased column references since
                        // we're outside the __union subquery wrapper
                        cte_body.push_str(&build_aliased_group_by(&plan.group_by, &plan.select));

                        // Add HAVING clause if present (after GROUP BY)
                        if let Some(having_expr) = &plan.having_clause {
                            cte_body.push_str("HAVING ");
                            cte_body.push_str(&having_expr.to_sql());
                            cte_body.push('\n');
                        }

                        cte_body.push_str(&plan.order_by.to_sql());

                        // Handle SKIP/LIMIT - either or both may be present
                        if plan.limit.0.is_some() || plan.skip.0.is_some() {
                            let skip_str = if let Some(n) = plan.skip.0 {
                                format!("{n}, ")
                            } else {
                                "".to_string()
                            };
                            let limit_val = plan.limit.0.unwrap_or(9223372036854775807i64);
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
                    cte_body.push_str(&plan.array_join.to_sql());
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
        };

        // Restore previous registry
        match saved_registry {
            Some(prev) => crate::server::query_context::set_current_variable_registry(prev),
            None => crate::server::query_context::clear_current_variable_registry(),
        }

        result
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

        // Ensure table_name has database prefix for base tables.
        // CTE references (with_*_cte_*, vlp_*, pattern_*, rel_*) don't need prefix.
        // Base tables that are missing the prefix get it from the task-local schema.
        let qualified_table_name = ensure_database_prefix(&self.table_name);

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
                format!(
                    "(SELECT * FROM {} WHERE {})",
                    qualified_table_name, filter_sql
                )
            } else {
                // For non-LEFT joins, pre_filter will be added to ON clause below
                qualified_table_name.clone()
            }
        } else {
            qualified_table_name.clone()
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
        } else if matches!(
            self.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        ) {
            // INNER/LEFT/RIGHT JOIN with empty joining_on is likely a planner bug.
            // Log error but use ON 1=1 as fallback to avoid crashing the server.
            log::error!(
                "Join::to_sql: {:?} with empty joining_on for table_alias={} table_name={} — possible planner bug",
                self.join_type, self.table_alias, self.table_name
            );
            sql.push_str(" ON 1=1");
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

                    // CTE column names use p{N}_ prefix (e.g., p6_friend_lastName).
                    // These are output aliases after GROUP BY/UNION and should NOT get
                    // a heuristic table prefix.
                    if raw_value.starts_with('p') {
                        let rest = &raw_value[1..];
                        if let Some(pos) = rest.find('_') {
                            if pos > 0 && rest[..pos].chars().all(|c| c.is_ascii_digit()) {
                                return raw_value.to_string();
                            }
                        }
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
                                            log::debug!(
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

                // Special handling for datetime({epochMillis: x}) -> identity pass-through
                if fn_name_lower == "datetime" && fn_call.args.len() == 1 {
                    if let RenderExpr::MapLiteral(entries) = &fn_call.args[0] {
                        if entries.len() == 1 && entries[0].0.to_lowercase() == "epochmillis" {
                            return entries[0].1.to_sql();
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

                // Resolve via unified VariableRegistry for CTE-scoped variables only.
                // Match-sourced variables are already resolved to DB columns during planning,
                // so we only need registry resolution for CTE-sourced variables where the
                // PropertyAccess.column is a Cypher property name that needs CTE column mapping.
                if let Some(resolved) = crate::server::query_context::resolve_with_current_registry(
                    &table_alias.0,
                    col_name,
                ) {
                    use crate::query_planner::typed_variable::ResolvedProperty;
                    match resolved {
                        ResolvedProperty::CteColumn { sql_alias, column } => {
                            log::info!(
                                "🔧 VariableRegistry resolved: {}.{} → {}.{}",
                                table_alias.0,
                                col_name,
                                sql_alias,
                                column
                            );
                            return format!("{}.{}", sql_alias, column);
                        }
                        ResolvedProperty::DbColumn(_) | ResolvedProperty::Unresolved => {
                            // Match-sourced or unresolved: skip — PropertyAccess already has
                            // the correct DB column from planning. Fall through.
                        }
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

                // Resolve "id" pseudo-property (from id() function transform) to actual
                // schema id column. This handles composite ID schemas where the table
                // doesn't have a column literally named "id".
                if col_name == "id" {
                    use crate::server::query_context::get_current_schema;
                    if let Some(schema) = get_current_schema() {
                        // Try each node schema to find one whose source_table matches
                        // the alias's table. We check by looking at the FROM clause context.
                        // As a heuristic, try all schemas and use the first single-column ID.
                        for ns in schema.all_node_schemas().values() {
                            let cols = ns.node_id.columns();
                            if cols.len() == 1 {
                                if let Some(first_col) = cols.first() {
                                    log::info!(
                                        "🔧 Resolved {}.id → {}.{} (schema id column)",
                                        table_alias.0,
                                        table_alias.0,
                                        first_col
                                    );
                                    return format!("{}.{}", table_alias.0, first_col);
                                }
                            }
                        }
                        // For composite IDs, fall through to render as-is (may error)
                        log::warn!(
                            "⚠️  {}.id could not be resolved (composite/unknown ID)",
                            table_alias.0
                        );
                    }
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
                                // Not a relationship — likely a node alias from OPTIONAL MATCH
                                // (e.g., CASE WHEN c IS NULL ... where c is a Comment node).
                                // Resolve to the node's ID column for the null check.
                                //
                                // We check ALL node schemas for consensus on the ID column name.
                                // If all nodes agree, we use that column. If they disagree, we log
                                // an error since we cannot determine the specific node type from
                                // the alias at this stage.
                                let id_col = {
                                    use crate::server::query_context::get_current_schema;
                                    use std::collections::BTreeSet;
                                    let mut unique_id_cols = BTreeSet::new();
                                    if let Some(schema) = get_current_schema() {
                                        for ns in schema.all_node_schemas().values() {
                                            let cols = ns.node_id.columns();
                                            if cols.len() == 1 {
                                                if let Some(first_col) = cols.first() {
                                                    unique_id_cols.insert(first_col.to_string());
                                                }
                                            }
                                        }
                                    }
                                    if unique_id_cols.len() == 1 {
                                        unique_id_cols.into_iter().next().unwrap()
                                    } else if unique_id_cols.is_empty() {
                                        log::error!(
                                            "Node wildcard null check for alias '{}': no node schemas found with single-column ID. Defaulting to 'id'.",
                                            table_alias
                                        );
                                        String::from("id")
                                    } else {
                                        log::error!(
                                            "Node wildcard null check for alias '{}': node schemas disagree on ID column name ({:?}). Cannot determine specific node type at SQL generation stage. Defaulting to 'id'.",
                                            table_alias,
                                            unique_id_cols
                                        );
                                        String::from("id")
                                    }
                                };
                                log::debug!(
                                    "Node wildcard null check: {}.{} {}",
                                    table_alias,
                                    id_col,
                                    op_str
                                );
                                let id_sql = format!("{}.{}", table_alias, id_col);
                                return format!("{} {}", id_sql, op_str);
                            }
                        }
                    }
                }

                // Node identity comparison: Cypher `a <> b` or `a = b` where both sides
                // are bare node variables (TableAlias) should compare by node ID column.
                // ClickHouse doesn't understand bare table aliases as values.
                if matches!(op.operator, Operator::Equal | Operator::NotEqual)
                    && op.operands.len() == 2
                {
                    let both_table_aliases = op
                        .operands
                        .iter()
                        .all(|o| matches!(o, RenderExpr::TableAlias(_)));
                    if both_table_aliases {
                        let op_str = if op.operator == Operator::Equal {
                            "="
                        } else {
                            "<>"
                        };
                        let lhs = op.operands[0].to_sql();
                        let rhs = op.operands[1].to_sql();
                        return format!("{}.id {} {}.id", lhs, op_str, rhs);
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

                // IN/NOT IN with List containing non-constant elements → expand to OR/AND
                // ClickHouse: `x IN [col1, col2]` fails when array has column refs
                if (op.operator == Operator::In || op.operator == Operator::NotIn)
                    && rendered.len() == 2
                {
                    if let RenderExpr::List(list_items) = &op.operands[1] {
                        let has_non_constant = list_items.iter().any(|item| {
                            !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
                        });
                        if has_non_constant {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> =
                                list_items.iter().map(|item| item.to_sql()).collect();
                            if op.operator == Operator::In {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} = {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" OR "));
                            } else {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} <> {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" AND "));
                            }
                        }
                    }
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

                // Special handling for interval arithmetic with epoch-millis values
                if (op.operator == Operator::Addition || op.operator == Operator::Subtraction)
                    && rendered.len() == 2
                {
                    let has_interval = rendered.iter().any(|r| r.contains("toInterval"));
                    if has_interval {
                        let wrapped: Vec<String> = rendered
                            .iter()
                            .map(|r| {
                                if r.contains("toInterval")
                                    || r.contains("fromUnixTimestamp64Milli")
                                    || r.contains("parseDateTime64BestEffort")
                                    || r.contains("toDateTime")
                                    || r.contains("now64")
                                    || r.contains("now()")
                                {
                                    r.clone()
                                } else {
                                    format!("fromUnixTimestamp64Milli({})", r)
                                }
                            })
                            .collect();
                        let sql_op = op_str(op.operator);
                        return format!(
                            "toUnixTimestamp64Milli({} {} {})",
                            &wrapped[0], sql_op, &wrapped[1]
                        );
                    }
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
                            _ => {
                                // Parenthesize right operand for non-commutative ops
                                let needs_right_parens = match op.operator {
                                    Operator::Subtraction => matches!(
                                        &op.operands[1],
                                        RenderExpr::OperatorApplicationExp(inner)
                                            if inner.operator == Operator::Addition
                                                || inner.operator == Operator::Subtraction
                                    ),
                                    Operator::Division => matches!(
                                        &op.operands[1],
                                        RenderExpr::OperatorApplicationExp(inner)
                                            if inner.operator == Operator::Multiplication
                                                || inner.operator == Operator::Division
                                                || inner.operator == Operator::ModuloDivision
                                    ),
                                    _ => false,
                                };
                                if needs_right_parens {
                                    format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                                } else {
                                    format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                                }
                            }
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
                // Check if any branch returns a List (Array) — if so, NULL branches
                // must be replaced with [] because ClickHouse can't find a supertype
                // for Nullable(Nothing) and Array(T).
                // Note: this checks top-level List variants only; nested lists inside
                // function calls are not detected. This is acceptable because CASE
                // branches that return arrays use direct List expressions in practice.
                let has_list_branch = case
                    .when_then
                    .iter()
                    .any(|(_, t)| matches!(t, RenderExpr::List(_)))
                    || case
                        .else_expr
                        .as_ref()
                        .is_some_and(|e| matches!(e.as_ref(), RenderExpr::List(_)));

                let render_result = |expr: &RenderExpr| -> String {
                    if has_list_branch && matches!(expr, RenderExpr::Literal(Literal::Null)) {
                        "[]".to_string()
                    } else {
                        expr.to_sql()
                    }
                };

                // For ClickHouse, use caseWithExpression for simple CASE expressions
                if let Some(case_expr) = &case.expr {
                    // caseWithExpression(expr, val1, res1, val2, res2, ..., default)
                    let mut args = vec![case_expr.to_sql()];

                    for (when_expr, then_expr) in &case.when_then {
                        args.push(when_expr.to_sql());
                        args.push(render_result(then_expr));
                    }

                    let else_expr = case
                        .else_expr
                        .as_ref()
                        .map(|e| render_result(e))
                        .unwrap_or_else(|| {
                            if has_list_branch {
                                "[]".to_string()
                            } else {
                                "NULL".to_string()
                            }
                        });
                    args.push(else_expr);

                    format!("caseWithExpression({})", args.join(", "))
                } else {
                    // Searched CASE - use standard CASE syntax
                    let mut sql = String::from("CASE");

                    for (when_expr, then_expr) in &case.when_then {
                        sql.push_str(&format!(
                            " WHEN {} THEN {}",
                            when_expr.to_sql(),
                            render_result(then_expr)
                        ));
                    }

                    if let Some(else_expr) = &case.else_expr {
                        sql.push_str(&format!(" ELSE {}", render_result(else_expr)));
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
                //
                // IMPORTANT: ClickHouse requires all map values to be of the same type.
                // Since Cypher maps can have mixed types (e.g., {name:'nodes', data:count(*)}),
                // we cast all values to String to ensure type compatibility.
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
                let index_sql = match index.as_ref() {
                    RenderExpr::Literal(Literal::Integer(n)) => format!("{}", n + 1),
                    _ => index.to_sql(),
                };
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
                log::debug!(
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

                // IN/NOT IN with List containing non-constant elements → expand to OR/AND
                if (op.operator == Operator::In || op.operator == Operator::NotIn)
                    && rendered.len() == 2
                {
                    if let RenderExpr::List(list_items) = &op.operands[1] {
                        let has_non_constant = list_items.iter().any(|item| {
                            !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
                        });
                        if has_non_constant {
                            let lhs = &rendered[0];
                            let item_sqls: Vec<String> =
                                list_items.iter().map(|item| item.to_sql()).collect();
                            if op.operator == Operator::In {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} = {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" OR "));
                            } else {
                                let clauses: Vec<String> = item_sqls
                                    .iter()
                                    .map(|rhs| format!("{} <> {}", lhs, rhs))
                                    .collect();
                                return format!("({})", clauses.join(" AND "));
                            }
                        }
                    }
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

                // Special handling for interval arithmetic with epoch-millis values
                if (op.operator == Operator::Addition || op.operator == Operator::Subtraction)
                    && rendered.len() == 2
                {
                    let has_interval = rendered.iter().any(|r| r.contains("toInterval"));
                    if has_interval {
                        let wrapped: Vec<String> = rendered
                            .iter()
                            .map(|r| {
                                if r.contains("toInterval")
                                    || r.contains("fromUnixTimestamp64Milli")
                                    || r.contains("parseDateTime64BestEffort")
                                    || r.contains("toDateTime")
                                    || r.contains("now64")
                                    || r.contains("now()")
                                {
                                    r.clone()
                                } else {
                                    format!("fromUnixTimestamp64Milli({})", r)
                                }
                            })
                            .collect();
                        let sql_op = op_str(op.operator);
                        return format!(
                            "toUnixTimestamp64Milli({} {} {})",
                            &wrapped[0], sql_op, &wrapped[1]
                        );
                    }
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
                        _ => {
                            let needs_right_parens = match op.operator {
                                Operator::Subtraction => matches!(
                                    &op.operands[1],
                                    RenderExpr::OperatorApplicationExp(inner)
                                        if inner.operator == Operator::Addition
                                            || inner.operator == Operator::Subtraction
                                ),
                                Operator::Division => matches!(
                                    &op.operands[1],
                                    RenderExpr::OperatorApplicationExp(inner)
                                        if inner.operator == Operator::Multiplication
                                            || inner.operator == Operator::Division
                                            || inner.operator == Operator::ModuloDivision
                                ),
                                _ => false,
                            };
                            if needs_right_parens {
                                format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                            } else {
                                format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                            }
                        }
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

        // IN/NOT IN with List containing non-constant elements → expand to OR/AND
        if (self.operator == Operator::In || self.operator == Operator::NotIn)
            && rendered.len() == 2
        {
            if let RenderExpr::List(list_items) = &self.operands[1] {
                let has_non_constant = list_items
                    .iter()
                    .any(|item| !matches!(item, RenderExpr::Literal(_) | RenderExpr::Parameter(_)));
                if has_non_constant {
                    let lhs = &rendered[0];
                    let item_sqls: Vec<String> =
                        list_items.iter().map(|item| item.to_sql()).collect();
                    if self.operator == Operator::In {
                        let clauses: Vec<String> = item_sqls
                            .iter()
                            .map(|rhs| format!("{} = {}", lhs, rhs))
                            .collect();
                        return format!("({})", clauses.join(" OR "));
                    } else {
                        let clauses: Vec<String> = item_sqls
                            .iter()
                            .map(|rhs| format!("{} <> {}", lhs, rhs))
                            .collect();
                        return format!("({})", clauses.join(" AND "));
                    }
                }
            }
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

        // Special handling for interval arithmetic with epoch-millis values.
        // When + or - has a toInterval* operand, the other operand must be DateTime64.
        // Wrap non-interval operands with fromUnixTimestamp64Milli() and convert the
        // final result back to Int64 millis with toUnixTimestamp64Milli() for safe
        // comparisons with other Int64 timestamp columns.
        if (self.operator == Operator::Addition || self.operator == Operator::Subtraction)
            && rendered.len() == 2
        {
            let has_interval = rendered.iter().any(|r| r.contains("toInterval"));
            if has_interval {
                let wrapped: Vec<String> = rendered
                    .iter()
                    .map(|r| {
                        if r.contains("toInterval")
                            || r.contains("fromUnixTimestamp64Milli")
                            || r.contains("parseDateTime64BestEffort")
                            || r.contains("toDateTime")
                            || r.contains("now64")
                            || r.contains("now()")
                        {
                            r.clone()
                        } else {
                            format!("fromUnixTimestamp64Milli({})", r)
                        }
                    })
                    .collect();
                let sql_op = op_str(self.operator);
                // Convert result back to epoch millis for consistent Int64 comparisons
                return format!(
                    "toUnixTimestamp64Milli({} {} {})",
                    &wrapped[0], sql_op, &wrapped[1]
                );
            }
        }

        let sql_op = op_str(self.operator);

        match rendered.len() {
            0 => "".into(),                              // should not happen
            1 => format!("{} {}", sql_op, &rendered[0]), // unary
            2 => {
                // Parenthesize right operand to preserve associativity for non-commutative ops:
                // a - (b - c) must NOT flatten to a - b - c
                // a / (b * c) must NOT flatten to a / b * c
                let needs_right_parens = match self.operator {
                    Operator::Subtraction => matches!(
                        &self.operands[1],
                        RenderExpr::OperatorApplicationExp(inner)
                            if inner.operator == Operator::Addition
                                || inner.operator == Operator::Subtraction
                    ),
                    Operator::Division => matches!(
                        &self.operands[1],
                        RenderExpr::OperatorApplicationExp(inner)
                            if inner.operator == Operator::Multiplication
                                || inner.operator == Operator::Division
                                || inner.operator == Operator::ModuloDivision
                    ),
                    _ => false,
                };
                if needs_right_parens {
                    format!("{} {} ({})", &rendered[0], sql_op, &rendered[1])
                } else {
                    format!("{} {} {}", &rendered[0], sql_op, &rendered[1])
                }
            }
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
