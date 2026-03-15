//! Post-hoc Query Plan Optimizer
//!
//! Five optimization passes applied after join sorting, before SQL generation:
//!
//! 1. **Dead CTE elimination**: Removes CTEs that are never referenced by any
//!    later CTE or the outer query.
//!
//! 2. **VLP column pruning**: Removes unused property columns from recursive VLP CTEs.
//!    VLP CTEs carry all start/end node properties but outer queries typically only
//!    reference a few. Pruning reduces per-row overhead in recursive CTEs.
//!
//! 3. **CTE column pruning**: Removes unused carry-forward columns from structured
//!    CTEs. Chained CTEs (cte_1 → cte_2 → ... → outer) carry all node properties
//!    through the chain even when only a few are used in the final output. Works
//!    backwards from consumers to producers to identify minimum needed columns.
//!
//! 4. **Unreferenced join elimination**: Removes JOINs whose alias is completely
//!    unreferenced (e.g., spurious CROSS JOINs with `ON 1=1`).
//!
//! 5. **Bridge node elimination**: Removes node table JOINs that only serve as FK
//!    bridges between edge tables, rewriting downstream ON conditions to chain FKs
//!    directly. Critical for performance with `join_use_nulls=1`.
//!
//! 6. **Selective predicate FROM reordering**: When a WHERE filter has a constant
//!    equality predicate on an INNER JOIN table (e.g., `tag.name = 'value'`),
//!    promotes that table to FROM position so ClickHouse filters early instead of
//!    processing millions of rows through chained LEFT JOINs. Re-roots the join
//!    dependency tree and redistributes ON conditions along the path.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::expression_utils::references_alias;
use crate::render_plan::render_expr::{
    Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::view_table_ref::ViewTableRef;
use crate::render_plan::{CteContent, FromTableItem, Join, JoinType, RenderPlan};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Check if a table name is a generated CTE reference.
fn is_cte_table(table_name: &str) -> bool {
    table_name.starts_with("with_")
        || table_name.starts_with("vlp_")
        || table_name.starts_with("pc_")
        || table_name.starts_with("bidi_")
}

/// Check if an ON condition is a tautology (always true), e.g., `1 = 1`.
fn is_tautology_condition(cond: &OperatorApplication) -> bool {
    if cond.operator != Operator::Equal || cond.operands.len() != 2 {
        return false;
    }
    // Check for literal = literal where both sides are the same value
    if let (RenderExpr::Literal(l), RenderExpr::Literal(r)) = (&cond.operands[0], &cond.operands[1])
    {
        return l == r;
    }
    false
}

/// Upstream FK reference extracted from a bridge node's ON condition.
/// When a node JOIN has `ON node.id = edge.FK`, the upstream reference
/// is `edge.FK` (the alias and column from the other side of the equality).
struct UpstreamRef {
    alias: String,
    column: String,
}

/// Info about a bridge node candidate to be eliminated.
struct BridgeCandidate {
    /// Index into the joins vec
    join_idx: usize,
    /// The alias of the bridge node (e.g., "person2")
    alias: String,
    /// The ID column on the bridge node side (e.g., "id")
    id_column: String,
    /// The upstream FK reference (e.g., edge.PersonId)
    upstream: UpstreamRef,
}

/// Check if a CTE name is referenced in a RenderPlan (FROM, JOINs, subqueries).
fn is_cte_referenced_in_plan(plan: &RenderPlan, cte_name: &str) -> bool {
    // Check FROM
    if let Some(ref from) = plan.from.0 {
        if from.name == cte_name {
            return true;
        }
    }

    // Check JOINs
    for join in &plan.joins.0 {
        if join.table_name == cte_name {
            return true;
        }
    }

    // Check UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            if is_cte_referenced_in_plan(branch, cte_name) {
                return true;
            }
        }
    }

    // Check subqueries in expressions (EXISTS, IN subquery, pattern count)
    // These may reference CTEs indirectly. For safety, check all expressions
    // for the CTE name as a table alias reference.
    if is_alias_referenced_in_plan(plan, cte_name) {
        return true;
    }

    false
}

/// Remove CTEs that are never referenced by any later CTE or the outer query.
/// Iterates until no more dead CTEs are found (handles cascading dead references).
fn remove_dead_ctes(plan: &mut RenderPlan) {
    loop {
        let mut dead_indices = Vec::new();

        for (idx, cte) in plan.ctes.0.iter().enumerate() {
            let name = &cte.cte_name;

            // Check if referenced in the outer query
            if is_cte_referenced_in_plan(plan, name) {
                continue;
            }

            // Check if referenced by any OTHER CTE (later ones, since flattened in dependency order)
            let referenced_by_other_cte = plan.ctes.0.iter().enumerate().any(|(j, other_cte)| {
                if j == idx {
                    return false;
                }
                match &other_cte.content {
                    CteContent::Structured(inner_plan) => {
                        is_cte_referenced_in_plan(inner_plan, name)
                    }
                    CteContent::RawSql(sql) => sql.contains(name),
                }
            });

            if referenced_by_other_cte {
                continue;
            }

            log::debug!("Dead CTE elimination: removing {}", name);
            dead_indices.push(idx);
        }

        if dead_indices.is_empty() {
            break;
        }

        // Remove in reverse order
        for idx in dead_indices.iter().rev() {
            plan.ctes.0.remove(*idx);
        }
    }
}

/// Top-level entry point: run all post-hoc plan optimizations.
/// 1. Dead CTE elimination
/// 2. VLP column pruning
/// 3. CTE column pruning (removes unused carry-forward columns from chained CTEs)
/// 4. Unreferenced join elimination
/// 5. Bridge node join elimination
pub fn optimize_plan(plan: &mut RenderPlan) {
    remove_dead_ctes(plan);
    prune_vlp_columns(plan);
    prune_cte_columns(plan);
    let empty = HashSet::new();

    // Apply join optimization passes to main plan, UNION branches, and CTE bodies
    optimize_joins_in_plan(plan, &empty);

    // For UNION branches: collect aliases referenced in the parent plan's SELECT/WHERE/etc.
    // UNION branches often have empty SELECT items (populated later during SQL rendering),
    // so we must protect aliases the parent plan references.
    // Collect BEFORE taking mutable borrow of union.
    let parent_aliases = if plan.union.0.is_some() {
        collect_referenced_aliases(plan)
    } else {
        HashSet::new()
    };
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            optimize_joins_in_plan(branch, &parent_aliases);
        }
    }

    for cte in plan.ctes.0.iter_mut() {
        if let CteContent::Structured(ref mut cte_plan) = cte.content {
            optimize_joins_in_plan(cte_plan, &empty);

            let cte_parent_aliases = if cte_plan.union.0.is_some() {
                collect_referenced_aliases(cte_plan)
            } else {
                HashSet::new()
            };
            if let Some(ref mut union) = cte_plan.union.0 {
                for branch in union.input.iter_mut() {
                    optimize_joins_in_plan(branch, &cte_parent_aliases);
                }
            }
        }
    }
}

// ─── VLP Column Pruning ───────────────────────────────────────────────────────

/// Core columns in VLP CTEs that should never be pruned.
fn is_vlp_core_column(col: &str) -> bool {
    matches!(
        col,
        "start_id"
            | "end_id"
            | "hop_count"
            | "path_edges"
            | "path_relationships"
            | "path_nodes"
            | "total_weight"
            | "start_properties"
            | "end_properties"
            | "__rel_type__"
    )
}

/// Prune unused property columns from recursive VLP CTEs.
///
/// VLP CTEs carry ALL start/end node properties through recursion, but the outer
/// query typically only uses a few (e.g., start_id, end_id, start_creationDate).
/// Each unused column adds per-row overhead in every recursive iteration.
fn prune_vlp_columns(plan: &mut RenderPlan) {
    // Phase 1: Collect VLP CTE info and determine which columns to prune (immutable)
    let mut prune_list: Vec<(usize, HashSet<String>)> = Vec::new();

    for (i, cte) in plan.ctes.0.iter().enumerate() {
        if !cte.is_recursive || !cte.cte_name.starts_with("vlp_") {
            continue;
        }
        let sql = match &cte.content {
            CteContent::RawSql(s) => s,
            _ => continue,
        };

        // Extract all property column aliases defined in the VLP SQL
        let property_columns = extract_property_columns_from_vlp_sql(sql);
        if property_columns.is_empty() {
            continue;
        }

        // Find the alias(es) used to reference this VLP CTE in the outer plan
        let aliases = find_vlp_aliases_in_plan(plan, &cte.cte_name);

        // Collect all columns referenced externally (outer plan + other CTEs)
        let mut externally_used: HashSet<String> = HashSet::new();
        for alias in &aliases {
            collect_columns_for_alias_in_plan(plan, alias, &mut externally_used);
        }
        // Also check other CTE bodies for references to this VLP
        for other_cte in &plan.ctes.0 {
            if other_cte.cte_name == cte.cte_name {
                continue;
            }
            match &other_cte.content {
                CteContent::Structured(inner_plan) => {
                    let inner_aliases = find_vlp_aliases_in_plan(inner_plan, &cte.cte_name);
                    for alias in &inner_aliases {
                        collect_columns_for_alias_in_plan(inner_plan, alias, &mut externally_used);
                    }
                    // Also collect bare column references (ColumnAlias, Column, Raw)
                    // that match VLP column names — structured CTEs may reference
                    // VLP columns without a table alias prefix
                    if !inner_aliases.is_empty() {
                        collect_bare_vlp_column_refs_in_plan(
                            inner_plan,
                            &property_columns,
                            &mut externally_used,
                        );
                    }
                }
                CteContent::RawSql(other_sql) => {
                    // Scan raw SQL for column references via any known alias
                    for alias in &aliases {
                        collect_columns_from_raw_sql(other_sql, alias, &mut externally_used);
                    }
                    // Also scan for bare VLP column names in raw SQL
                    collect_bare_vlp_columns_from_raw_sql(
                        other_sql,
                        &property_columns,
                        &mut externally_used,
                    );
                }
            }
        }

        // Collect columns referenced internally in the VLP CTE (WHERE/JOIN conditions)
        let internally_used = collect_vlp_internal_refs(sql);

        // Determine unused columns
        let unused: HashSet<String> = property_columns
            .difference(&externally_used)
            .filter(|col| !internally_used.contains(*col))
            .cloned()
            .collect();

        if !unused.is_empty() {
            log::debug!(
                "VLP column pruning: {} — removing {}/{} property columns",
                cte.cte_name,
                unused.len(),
                property_columns.len()
            );
            prune_list.push((i, unused));
        }
    }

    // Phase 2: Apply pruning (mutable)
    for (idx, unused) in prune_list {
        if let CteContent::RawSql(ref mut sql) = plan.ctes.0[idx].content {
            *sql = prune_vlp_select_columns(sql, &unused);
        }
    }
}

// ─── CTE Column Pruning ─────────────────────────────────────────────────────

/// Prune unused columns from structured CTEs.
///
/// Chained CTEs carry ALL node properties through the chain (e.g., bi-14 has
/// 22 columns through 5 CTEs but only 5 are used in the final output).
/// This pass works backwards from consumers to producers, identifying which
/// columns are actually needed and removing the rest.
fn prune_cte_columns(plan: &mut RenderPlan) {
    if plan.ctes.0.is_empty() {
        return;
    }

    // Phase 1: Build CTE metadata (immutable scan)
    // For each structured CTE: what columns does it define?
    let mut cte_output_columns: HashMap<String, Vec<String>> = HashMap::new();
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref inner_plan) = cte.content {
            let cols: Vec<String> = inner_plan
                .select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|ca| ca.0.clone()))
                .collect();
            if !cols.is_empty() {
                cte_output_columns.insert(cte.cte_name.clone(), cols);
            }
        }
    }

    if cte_output_columns.is_empty() {
        return;
    }

    // Phase 2: Backward dataflow analysis to find truly needed columns.
    //
    // Start from the outer query, find what it needs from each CTE. Then process
    // CTEs from last to first: a CTE column is needed if:
    //   (a) it's directly referenced by the outer query, OR
    //   (b) it's referenced in a downstream CTE's non-carry-forward context
    //       (JOINs, WHERE, computed SELECT expressions, ORDER BY, etc.), OR
    //   (c) it's a carry-forward from upstream, AND the downstream CTE needs it
    //
    // This naturally handles chains: if cte_5 needs col X from cte_4, and cte_4
    // carries it from cte_3, then cte_3 needs X too. But if cte_5 doesn't need
    // col Y, neither does cte_4 or cte_3.

    // Build alias→upstream CTE mapping for each CTE body
    let mut cte_alias_map: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref p) = cte.content {
            let mut mappings = Vec::new();
            if let Some(ref from) = p.from.0 {
                if cte_output_columns.contains_key(&from.name) {
                    let alias = from.alias.as_ref().unwrap_or(&from.name).clone();
                    mappings.push((alias, from.name.clone()));
                }
            }
            for join in &p.joins.0 {
                if cte_output_columns.contains_key(&join.table_name) {
                    mappings.push((join.table_alias.clone(), join.table_name.clone()));
                }
            }
            if !mappings.is_empty() {
                cte_alias_map.insert(cte.cte_name.clone(), mappings);
            }
        }
    }

    // Initialize needed columns: start empty for all CTEs
    let mut needed_columns: HashMap<String, HashSet<String>> = HashMap::new();
    for cte_name in cte_output_columns.keys() {
        needed_columns.insert(cte_name.clone(), HashSet::new());
    }

    // Seed from outer query: find which CTE columns the outer query references
    for (cte_name, cols) in &cte_output_columns {
        let mut needed: HashSet<String> = HashSet::new();
        let col_set: HashSet<String> = cols.iter().cloned().collect();
        collect_cte_refs_in_plan(plan, cte_name, &col_set, &mut needed);
        if let Some(set) = needed_columns.get_mut(cte_name) {
            set.extend(needed);
        }
    }

    // Seed from self-references: columns referenced by the CTE's own
    // ORDER BY, HAVING, etc. via column alias (e.g., `ORDER BY numberOfMembers DESC`
    // references the CTE's own SELECT alias "numberOfMembers")
    for cte in &plan.ctes.0 {
        if let CteContent::Structured(ref inner_plan) = cte.content {
            if let Some(cols) = cte_output_columns.get(&cte.cte_name) {
                let col_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
                let mut self_refs: HashSet<String> = HashSet::new();
                collect_self_ref_aliases(inner_plan, &col_set, &mut self_refs);
                if let Some(set) = needed_columns.get_mut(&cte.cte_name) {
                    set.extend(self_refs);
                }
            }
        }
    }

    // Seed from raw SQL CTEs (conservative: mark all columns as needed)
    for other_cte in &plan.ctes.0 {
        if let CteContent::RawSql(sql) = &other_cte.content {
            for (cte_name, cols) in &cte_output_columns {
                if sql.contains(cte_name.as_str()) {
                    if let Some(set) = needed_columns.get_mut(cte_name) {
                        for col in cols {
                            set.insert(col.clone());
                        }
                    }
                }
            }
        }
    }

    // Process CTEs from last to first (backward propagation).
    // For each CTE that consumes an upstream CTE:
    //   1. Find which of its OWN output columns are needed (from consumers processed earlier)
    //   2. For each needed output column that is a carry-forward from upstream,
    //      mark the upstream column as needed
    //   3. For all internal references (JOINs, WHERE, computed expressions),
    //      mark those upstream columns as needed too
    let cte_names: Vec<String> = plan.ctes.0.iter().map(|c| c.cte_name.clone()).collect();

    // Iterate until stable (usually 1-2 passes for linear chains)
    loop {
        let mut changed = false;

        for cte_name in cte_names.iter().rev() {
            if !cte_output_columns.contains_key(cte_name) {
                continue;
            }

            let cte_plan = plan.ctes.0.iter().find(|c| c.cte_name == *cte_name);
            let inner_plan = match cte_plan {
                Some(cte) => match &cte.content {
                    CteContent::Structured(p) => p,
                    _ => continue,
                },
                None => continue,
            };

            let alias_mappings = match cte_alias_map.get(cte_name) {
                Some(m) => m.clone(),
                None => continue,
            };

            let my_needed = needed_columns.get(cte_name).cloned().unwrap_or_default();

            // For each upstream CTE alias, mark internally-used columns as needed
            for (alias, upstream_cte_name) in &alias_mappings {
                let mut internal_refs: HashSet<String> = HashSet::new();
                collect_cte_internal_column_refs(inner_plan, alias, &mut internal_refs);
                // Also collect bare column alias references matching upstream's columns
                if let Some(upstream_cols) = cte_output_columns.get(upstream_cte_name) {
                    let upstream_col_set: HashSet<String> = upstream_cols.iter().cloned().collect();
                    collect_bare_alias_refs_in_plan(
                        inner_plan,
                        &upstream_col_set,
                        &mut internal_refs,
                    );
                }
                if let Some(upstream_needed) = needed_columns.get_mut(upstream_cte_name) {
                    for col in &internal_refs {
                        if upstream_needed.insert(col.clone()) {
                            changed = true;
                        }
                    }
                }
            }

            // For each NEEDED carry-forward SELECT item, propagate to upstream
            for item in &inner_plan.select.items {
                let col_alias = match &item.col_alias {
                    Some(ca) => &ca.0,
                    None => continue,
                };
                if !my_needed.contains(col_alias) {
                    continue; // Not needed, don't propagate
                }
                if let RenderExpr::PropertyAccessExp(pa) = &item.expression {
                    let upstream_alias = &pa.table_alias.0;
                    let upstream_col = pa.column.raw().to_string();
                    for (alias, upstream_cte_name) in &alias_mappings {
                        if alias == upstream_alias {
                            if let Some(upstream_needed) = needed_columns.get_mut(upstream_cte_name)
                            {
                                if upstream_needed.insert(upstream_col.clone()) {
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        if !changed {
            break;
        }
    }

    // Phase 4: Apply pruning — remove SELECT items whose col_alias is not needed
    for cte in plan.ctes.0.iter_mut() {
        let needed = match needed_columns.get(&cte.cte_name) {
            Some(n) => n,
            None => continue,
        };

        if let CteContent::Structured(ref mut inner_plan) = cte.content {
            let original_count = inner_plan.select.items.len();
            let all_cols: Vec<String> = inner_plan
                .select
                .items
                .iter()
                .filter_map(|item| item.col_alias.as_ref().map(|ca| ca.0.clone()))
                .collect();
            inner_plan.select.items.retain(|item| {
                match &item.col_alias {
                    Some(ca) => needed.contains(&ca.0),
                    None => true, // Keep items without alias (computed columns, etc.)
                }
            });
            let pruned = original_count - inner_plan.select.items.len();
            if pruned > 0 {
                let removed: Vec<&String> =
                    all_cols.iter().filter(|c| !needed.contains(*c)).collect();
                log::debug!(
                    "CTE column pruning: {} — removed {}/{} columns: {:?}",
                    cte.cte_name,
                    pruned,
                    original_count,
                    removed
                );
            }
        }
    }
}

/// Collect all column references to a CTE in a plan (FROM + JOINs + UNION branches).
/// Also checks for references using the CTE name directly (e.g., `cte_name.col`)
/// and bare column alias references matching the CTE's known output columns.
fn collect_cte_refs_in_plan(
    plan: &RenderPlan,
    cte_name: &str,
    cte_columns: &HashSet<String>,
    needed: &mut HashSet<String>,
) {
    let mut found = false;
    let mut aliases_to_check: Vec<String> = Vec::new();

    // Check FROM
    if let Some(ref from) = plan.from.0 {
        if from.name == cte_name {
            let alias = from.alias.as_ref().unwrap_or(&from.name);
            aliases_to_check.push(alias.clone());
            found = true;
        }
    }
    // Check JOINs
    for join in &plan.joins.0 {
        if join.table_name == cte_name {
            aliases_to_check.push(join.table_alias.clone());
            found = true;
        }
    }
    // Always check the CTE name itself as a possible alias
    // (some expressions use `cte_name.col` directly)
    if !aliases_to_check.contains(&cte_name.to_string()) {
        aliases_to_check.push(cte_name.to_string());
    }

    for alias in &aliases_to_check {
        collect_columns_for_alias_in_plan(plan, alias, needed);
    }

    // If this plan references the CTE (via FROM or JOIN), also collect
    // bare column alias references — column names used without a table
    // alias prefix that match the CTE's known output columns.
    if found {
        let mut bare_refs: HashSet<String> = HashSet::new();
        collect_bare_alias_refs_in_plan(plan, cte_columns, &mut bare_refs);
        needed.extend(bare_refs);
    }

    // Check UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            collect_cte_refs_in_plan(branch, cte_name, cte_columns, needed);
        }
    }
}

/// Collect bare column alias references from all parts of a plan,
/// filtered to only include names that match known CTE output columns.
fn collect_bare_alias_refs_in_plan(
    plan: &RenderPlan,
    known_columns: &HashSet<String>,
    refs: &mut HashSet<String>,
) {
    for item in &plan.select.items {
        collect_bare_aliases_from_expr(&item.expression, known_columns, refs);
    }
    if let Some(ref filter) = plan.filters.0 {
        collect_bare_aliases_from_expr(filter, known_columns, refs);
    }
    for item in &plan.order_by.0 {
        collect_bare_aliases_from_expr(&item.expression, known_columns, refs);
    }
    for expr in &plan.group_by.0 {
        collect_bare_aliases_from_expr(expr, known_columns, refs);
    }
    if let Some(ref having) = plan.having_clause {
        collect_bare_aliases_from_expr(having, known_columns, refs);
    }
    for join in &plan.joins.0 {
        for cond in &join.joining_on {
            for operand in &cond.operands {
                collect_bare_aliases_from_expr(operand, known_columns, refs);
            }
        }
        if let Some(ref pf) = join.pre_filter {
            collect_bare_aliases_from_expr(pf, known_columns, refs);
        }
    }
    for aj in &plan.array_join.0 {
        collect_bare_aliases_from_expr(&aj.expression, known_columns, refs);
    }
    // UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            collect_bare_alias_refs_in_plan(branch, known_columns, refs);
        }
    }
}

/// Extract bare column alias names from an expression, filtered against known columns.
fn collect_bare_aliases_from_expr(
    expr: &RenderExpr,
    known_columns: &HashSet<String>,
    refs: &mut HashSet<String>,
) {
    match expr {
        RenderExpr::ColumnAlias(ca) => {
            if known_columns.contains(&ca.0) {
                refs.insert(ca.0.clone());
            }
        }
        RenderExpr::TableAlias(ta) => {
            if known_columns.contains(&ta.0) {
                refs.insert(ta.0.clone());
            }
        }
        RenderExpr::Column(col) => {
            let name = col.raw().to_string();
            if known_columns.contains(&name) {
                refs.insert(name);
            }
        }
        RenderExpr::Raw(raw) => {
            // Check for known column names in raw SQL (word boundary check)
            for col in known_columns {
                if raw.contains(col.as_str()) {
                    refs.insert(col.clone());
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_bare_aliases_from_expr(operand, known_columns, refs);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_bare_aliases_from_expr(arg, known_columns, refs);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_bare_aliases_from_expr(arg, known_columns, refs);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref e) = case.expr {
                collect_bare_aliases_from_expr(e, known_columns, refs);
            }
            for (when, then) in &case.when_then {
                collect_bare_aliases_from_expr(when, known_columns, refs);
                collect_bare_aliases_from_expr(then, known_columns, refs);
            }
            if let Some(ref e) = case.else_expr {
                collect_bare_aliases_from_expr(e, known_columns, refs);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                collect_bare_aliases_from_expr(item, known_columns, refs);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_bare_aliases_from_expr(array, known_columns, refs);
            collect_bare_aliases_from_expr(index, known_columns, refs);
        }
        RenderExpr::InSubquery(subq) => {
            collect_bare_aliases_from_expr(&subq.expr, known_columns, refs);
        }
        _ => {}
    }
}

/// Collect columns referenced in non-SELECT parts of a CTE body.
/// This finds columns used in JOINs, WHERE, ORDER BY, GROUP BY, HAVING —
/// columns that are needed for query logic, not just carry-forward.
fn collect_cte_internal_column_refs(plan: &RenderPlan, alias: &str, columns: &mut HashSet<String>) {
    // WHERE
    if let Some(ref filter) = plan.filters.0 {
        collect_columns_for_alias_in_expr(filter, alias, columns);
    }
    // ORDER BY
    for item in &plan.order_by.0 {
        collect_columns_for_alias_in_expr(&item.expression, alias, columns);
    }
    // GROUP BY
    for expr in &plan.group_by.0 {
        collect_columns_for_alias_in_expr(expr, alias, columns);
    }
    // HAVING
    if let Some(ref having) = plan.having_clause {
        collect_columns_for_alias_in_expr(having, alias, columns);
    }
    // JOINs (ON conditions and pre_filters)
    for join in &plan.joins.0 {
        for cond in &join.joining_on {
            for operand in &cond.operands {
                collect_columns_for_alias_in_expr(operand, alias, columns);
            }
        }
        if let Some(ref pf) = join.pre_filter {
            collect_columns_for_alias_in_expr(pf, alias, columns);
        }
    }
    // ARRAY JOIN
    for aj in &plan.array_join.0 {
        collect_columns_for_alias_in_expr(&aj.expression, alias, columns);
    }
    // Also check SELECT items that are NOT simple carry-forwards — computed
    // expressions that reference the upstream alias (e.g., `score + CASE WHEN ...`)
    for item in &plan.select.items {
        if let RenderExpr::PropertyAccessExp(pa) = &item.expression {
            if pa.table_alias.0 == alias {
                // Simple carry-forward: person1.p7_X AS "p7_X" — don't count this
                continue;
            }
        }
        // Computed expression — scan for alias references
        collect_columns_for_alias_in_expr(&item.expression, alias, columns);
    }
}

/// Collect column aliases from a CTE's own non-SELECT parts that reference its output columns.
/// For example, `ORDER BY numberOfMembers DESC` references the CTE's own "numberOfMembers" alias.
fn collect_self_ref_aliases(
    plan: &RenderPlan,
    output_columns: &HashSet<&str>,
    self_refs: &mut HashSet<String>,
) {
    // ORDER BY
    for item in &plan.order_by.0 {
        collect_alias_refs_in_expr(&item.expression, output_columns, self_refs);
    }
    // HAVING
    if let Some(ref having) = plan.having_clause {
        collect_alias_refs_in_expr(having, output_columns, self_refs);
    }
    // WHERE (can reference column aliases in some contexts)
    if let Some(ref filter) = plan.filters.0 {
        collect_alias_refs_in_expr(filter, output_columns, self_refs);
    }
    // GROUP BY
    for expr in &plan.group_by.0 {
        collect_alias_refs_in_expr(expr, output_columns, self_refs);
    }
}

/// Scan an expression for bare column alias references matching known output columns.
fn collect_alias_refs_in_expr(
    expr: &RenderExpr,
    output_columns: &HashSet<&str>,
    refs: &mut HashSet<String>,
) {
    match expr {
        RenderExpr::ColumnAlias(ca) => {
            if output_columns.contains(ca.0.as_str()) {
                refs.insert(ca.0.clone());
            }
        }
        RenderExpr::TableAlias(ta) => {
            if output_columns.contains(ta.0.as_str()) {
                refs.insert(ta.0.clone());
            }
        }
        RenderExpr::Column(col) => {
            let name = col.raw();
            if output_columns.contains(name) {
                refs.insert(name.to_string());
            }
        }
        RenderExpr::Raw(raw) => {
            // Check for bare column aliases in raw SQL
            for col in output_columns {
                if raw.contains(col) {
                    refs.insert(col.to_string());
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_alias_refs_in_expr(operand, output_columns, refs);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_alias_refs_in_expr(arg, output_columns, refs);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_alias_refs_in_expr(arg, output_columns, refs);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref e) = case.expr {
                collect_alias_refs_in_expr(e, output_columns, refs);
            }
            for (when, then) in &case.when_then {
                collect_alias_refs_in_expr(when, output_columns, refs);
                collect_alias_refs_in_expr(then, output_columns, refs);
            }
            if let Some(ref e) = case.else_expr {
                collect_alias_refs_in_expr(e, output_columns, refs);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                collect_alias_refs_in_expr(item, output_columns, refs);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_alias_refs_in_expr(array, output_columns, refs);
            collect_alias_refs_in_expr(index, output_columns, refs);
        }
        RenderExpr::InSubquery(subq) => {
            collect_alias_refs_in_expr(&subq.expr, output_columns, refs);
        }
        _ => {}
    }
}

/// Extract property column aliases (start_X, end_X) from VLP SQL.
/// Returns only non-core property columns that are candidates for pruning.
fn extract_property_columns_from_vlp_sql(sql: &str) -> HashSet<String> {
    let mut columns = HashSet::new();
    // Scan for " as start_XXX" or " as end_XXX" patterns
    let as_pat = " as ";
    let mut pos = 0;
    while let Some(idx) = sql[pos..].find(as_pat) {
        let start = pos + idx + as_pat.len();
        let end = sql[start..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| start + i)
            .unwrap_or(sql.len());
        let alias = &sql[start..end];
        if (alias.starts_with("start_") || alias.starts_with("end_"))
            && !is_vlp_core_column(alias)
            && alias.len() > 6
        // at least "start_X" (7 chars)
        {
            columns.insert(alias.to_string());
        }
        pos = end;
    }
    columns
}

/// Find all aliases used to reference a VLP CTE in a plan.
/// Checks FROM table name and JOIN table names.
fn find_vlp_aliases_in_plan(plan: &RenderPlan, vlp_name: &str) -> Vec<String> {
    let mut aliases = Vec::new();

    // Check FROM
    if let Some(ref from) = plan.from.0 {
        if from.name == vlp_name {
            if let Some(ref alias) = from.alias {
                aliases.push(alias.clone());
            } else {
                aliases.push(vlp_name.to_string());
            }
        }
    }

    // Check JOINs
    for join in &plan.joins.0 {
        if join.table_name == vlp_name {
            aliases.push(join.table_alias.clone());
        }
    }

    // Check UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            aliases.extend(find_vlp_aliases_in_plan(branch, vlp_name));
        }
    }

    aliases
}

/// Collect all column names referenced via PropertyAccessExp for a given alias.
fn collect_columns_for_alias_in_plan(
    plan: &RenderPlan,
    alias: &str,
    columns: &mut HashSet<String>,
) {
    // SELECT
    for item in &plan.select.items {
        collect_columns_for_alias_in_expr(&item.expression, alias, columns);
    }
    // WHERE
    if let Some(ref filter) = plan.filters.0 {
        collect_columns_for_alias_in_expr(filter, alias, columns);
    }
    // ORDER BY
    for item in &plan.order_by.0 {
        collect_columns_for_alias_in_expr(&item.expression, alias, columns);
    }
    // GROUP BY
    for expr in &plan.group_by.0 {
        collect_columns_for_alias_in_expr(expr, alias, columns);
    }
    // HAVING
    if let Some(ref having) = plan.having_clause {
        collect_columns_for_alias_in_expr(having, alias, columns);
    }
    // JOINs (ON conditions and pre_filters)
    for join in &plan.joins.0 {
        for cond in &join.joining_on {
            for operand in &cond.operands {
                collect_columns_for_alias_in_expr(operand, alias, columns);
            }
        }
        if let Some(ref pf) = join.pre_filter {
            collect_columns_for_alias_in_expr(pf, alias, columns);
        }
    }
    // ARRAY JOIN
    for aj in &plan.array_join.0 {
        collect_columns_for_alias_in_expr(&aj.expression, alias, columns);
    }
    // UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            collect_columns_for_alias_in_plan(branch, alias, columns);
        }
    }
}

/// Recursively collect column names from PropertyAccessExp matching a given alias.
fn collect_columns_for_alias_in_expr(
    expr: &RenderExpr,
    alias: &str,
    columns: &mut HashSet<String>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == alias {
                columns.insert(pa.column.raw().to_string());
            }
        }
        // Column expressions may contain "alias.column_name" as a raw string
        // (generated by VLP+WITH join condition builders)
        RenderExpr::Column(col) => {
            let raw = col.0.raw();
            if let Some(col_name) = raw.strip_prefix(&format!("{}.", alias)) {
                columns.insert(col_name.to_string());
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_columns_for_alias_in_expr(operand, alias, columns);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_columns_for_alias_in_expr(arg, alias, columns);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_columns_for_alias_in_expr(arg, alias, columns);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref e) = case.expr {
                collect_columns_for_alias_in_expr(e, alias, columns);
            }
            for (when, then) in &case.when_then {
                collect_columns_for_alias_in_expr(when, alias, columns);
                collect_columns_for_alias_in_expr(then, alias, columns);
            }
            if let Some(ref e) = case.else_expr {
                collect_columns_for_alias_in_expr(e, alias, columns);
            }
        }
        RenderExpr::InSubquery(subq) => {
            collect_columns_for_alias_in_expr(&subq.expr, alias, columns);
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_columns_for_alias_in_expr(array, alias, columns);
            collect_columns_for_alias_in_expr(index, alias, columns);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_columns_for_alias_in_expr(array, alias, columns);
            if let Some(ref f) = from {
                collect_columns_for_alias_in_expr(f, alias, columns);
            }
            if let Some(ref t) = to {
                collect_columns_for_alias_in_expr(t, alias, columns);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                collect_columns_for_alias_in_expr(item, alias, columns);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            collect_columns_for_alias_in_expr(&reduce.initial_value, alias, columns);
            collect_columns_for_alias_in_expr(&reduce.list, alias, columns);
            collect_columns_for_alias_in_expr(&reduce.expression, alias, columns);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                collect_columns_for_alias_in_expr(v, alias, columns);
            }
        }
        RenderExpr::Raw(raw) => {
            // Scan raw SQL for "alias.column" patterns where column is start_X/end_X
            let prefix = format!("{}.", alias);
            let mut pos = 0;
            while let Some(idx) = raw[pos..].find(&prefix) {
                let col_start = pos + idx + prefix.len();
                let col_end = raw[col_start..]
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .map(|i| col_start + i)
                    .unwrap_or(raw.len());
                let col = &raw[col_start..col_end];
                if col.starts_with("start_") || col.starts_with("end_") {
                    columns.insert(col.to_string());
                }
                pos = col_end;
            }
        }
        // Leaf nodes that don't contain column references
        _ => {}
    }
}

/// Collect column references from raw SQL text for a given alias.
fn collect_columns_from_raw_sql(sql: &str, alias: &str, columns: &mut HashSet<String>) {
    let prefix = format!("{}.", alias);
    let mut pos = 0;
    while let Some(idx) = sql[pos..].find(&prefix) {
        let col_start = pos + idx + prefix.len();
        let col_end = sql[col_start..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .map(|i| col_start + i)
            .unwrap_or(sql.len());
        let col = &sql[col_start..col_end];
        if (col.starts_with("start_") || col.starts_with("end_")) && !is_vlp_core_column(col) {
            columns.insert(col.to_string());
        }
        pos = col_end;
    }
}

/// Collect bare VLP column references (ColumnAlias, Column, Raw) from a structured plan.
/// Catches cases where structured CTEs reference VLP columns without a table alias prefix
/// (e.g., `end_birthday` instead of `t.end_birthday`).
fn collect_bare_vlp_column_refs_in_plan(
    plan: &RenderPlan,
    vlp_columns: &HashSet<String>,
    used: &mut HashSet<String>,
) {
    // Collect from all plan parts
    for item in &plan.select.items {
        collect_bare_vlp_refs_in_expr(&item.expression, vlp_columns, used);
    }
    if let Some(ref filter) = plan.filters.0 {
        collect_bare_vlp_refs_in_expr(filter, vlp_columns, used);
    }
    for item in &plan.order_by.0 {
        collect_bare_vlp_refs_in_expr(&item.expression, vlp_columns, used);
    }
    for expr in &plan.group_by.0 {
        collect_bare_vlp_refs_in_expr(expr, vlp_columns, used);
    }
    if let Some(ref having) = plan.having_clause {
        collect_bare_vlp_refs_in_expr(having, vlp_columns, used);
    }
    for join in &plan.joins.0 {
        for cond in &join.joining_on {
            for operand in &cond.operands {
                collect_bare_vlp_refs_in_expr(operand, vlp_columns, used);
            }
        }
        if let Some(ref pf) = join.pre_filter {
            collect_bare_vlp_refs_in_expr(pf, vlp_columns, used);
        }
    }
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            collect_bare_vlp_column_refs_in_plan(branch, vlp_columns, used);
        }
    }
}

/// Recursively scan an expression for bare column references matching VLP column names.
fn collect_bare_vlp_refs_in_expr(
    expr: &RenderExpr,
    vlp_columns: &HashSet<String>,
    used: &mut HashSet<String>,
) {
    match expr {
        RenderExpr::ColumnAlias(ca) => {
            if vlp_columns.contains(&ca.0) {
                used.insert(ca.0.clone());
            }
        }
        RenderExpr::Column(col) => {
            let col_name = col.raw().to_string();
            if vlp_columns.contains(&col_name) {
                used.insert(col_name);
            }
        }
        RenderExpr::Raw(raw) => {
            // Scan raw SQL for bare VLP column names
            for col in vlp_columns {
                if raw.contains(col.as_str()) {
                    used.insert(col.clone());
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_bare_vlp_refs_in_expr(operand, vlp_columns, used);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_bare_vlp_refs_in_expr(arg, vlp_columns, used);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_bare_vlp_refs_in_expr(arg, vlp_columns, used);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref e) = case.expr {
                collect_bare_vlp_refs_in_expr(e, vlp_columns, used);
            }
            for (when, then) in &case.when_then {
                collect_bare_vlp_refs_in_expr(when, vlp_columns, used);
                collect_bare_vlp_refs_in_expr(then, vlp_columns, used);
            }
            if let Some(ref e) = case.else_expr {
                collect_bare_vlp_refs_in_expr(e, vlp_columns, used);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                collect_bare_vlp_refs_in_expr(item, vlp_columns, used);
            }
        }
        RenderExpr::InSubquery(subq) => {
            collect_bare_vlp_refs_in_expr(&subq.expr, vlp_columns, used);
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_bare_vlp_refs_in_expr(array, vlp_columns, used);
            collect_bare_vlp_refs_in_expr(index, vlp_columns, used);
        }
        _ => {}
    }
}

/// Scan raw SQL for bare VLP column names (without alias prefix).
fn collect_bare_vlp_columns_from_raw_sql(
    sql: &str,
    vlp_columns: &HashSet<String>,
    used: &mut HashSet<String>,
) {
    for col in vlp_columns {
        if sql.contains(col.as_str()) {
            used.insert(col.clone());
        }
    }
}

/// Collect property columns referenced in VLP CTE's own FROM/JOIN/WHERE clauses.
/// These are internal references (e.g., edge constraint filters, wrapper CTE WHEREs)
/// that must be preserved.
fn collect_vlp_internal_refs(sql: &str) -> HashSet<String> {
    let mut refs = HashSet::new();

    // Scan non-column-definition regions of the SQL.
    let mut in_non_def_region = false;
    for line in sql.lines() {
        let trimmed = line.trim();

        // Always scan WHERE portions — even on SELECT lines (wrapper CTEs have
        // "SELECT * FROM vlp_inner WHERE end_col = ..." on a single line)
        if let Some(where_pos) = trimmed.find("WHERE ") {
            scan_line_for_vlp_column_refs(&trimmed[where_pos..], &mut refs);
        }

        // Detect transitions between definition and non-definition regions
        if trimmed.starts_with("SELECT") {
            in_non_def_region = false;
            continue;
        }
        if trimmed.starts_with("FROM ")
            || trimmed.starts_with("JOIN ")
            || trimmed.starts_with("WHERE ")
            || trimmed.starts_with("ORDER ")
            || trimmed.starts_with("HAVING ")
            || trimmed.starts_with("GROUP ")
        {
            in_non_def_region = true;
        }

        if !in_non_def_region {
            continue;
        }

        // Scan this line for property column references
        scan_line_for_vlp_column_refs(trimmed, &mut refs);
    }

    refs
}

/// Scan a text line for start_X / end_X column references.
fn scan_line_for_vlp_column_refs(text: &str, refs: &mut HashSet<String>) {
    for prefix in &["start_", "end_"] {
        let mut pos = 0;
        while let Some(idx) = text[pos..].find(prefix) {
            let abs = pos + idx;
            // Word boundary check (not preceded by alphanumeric or _)
            if abs > 0 {
                let prev = text.as_bytes()[abs - 1];
                if prev.is_ascii_alphanumeric() || prev == b'_' {
                    pos = abs + 1;
                    continue;
                }
            }
            let end = text[abs..]
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .map(|i| abs + i)
                .unwrap_or(text.len());
            let col = &text[abs..end];
            if !is_vlp_core_column(col) && col.len() > prefix.len() {
                refs.insert(col.to_string());
            }
            pos = end;
        }
    }
}

/// Remove unused property columns from VLP CTE SQL.
/// Handles both base case and recursive case (split by UNION ALL).
fn prune_vlp_select_columns(sql: &str, unused: &HashSet<String>) -> String {
    if unused.is_empty() {
        return sql.to_string();
    }

    // Split at UNION ALL boundaries to process base and recursive cases independently
    let union_sep = "\n    UNION ALL\n";
    let parts: Vec<&str> = sql.split(union_sep).collect();

    let pruned: Vec<String> = parts
        .into_iter()
        .map(|part| prune_columns_in_select_block(part, unused))
        .collect();

    pruned.join(union_sep)
}

/// Prune unused columns from a single SELECT...FROM block.
fn prune_columns_in_select_block(block: &str, unused: &HashSet<String>) -> String {
    // Find SELECT keyword
    let select_pos = match block.find("SELECT") {
        Some(pos) => pos,
        None => return block.to_string(),
    };
    let after_select = select_pos + "SELECT".len();

    // Find FROM keyword (on its own line with indentation)
    // Try multiple FROM patterns since indentation may vary
    let from_pos = block[after_select..]
        .find("\n    FROM ")
        .or_else(|| block[after_select..].find("\n        FROM "))
        .or_else(|| block[after_select..].find("\nFROM "))
        .map(|pos| after_select + pos);

    let from_pos = match from_pos {
        Some(pos) => pos,
        None => return block.to_string(),
    };

    let prefix = &block[..after_select];
    let columns_str = &block[after_select..from_pos];
    let suffix = &block[from_pos..];

    // Split columns by ",\n" delimiter
    let columns: Vec<&str> = columns_str.split(",\n").collect();

    // Filter: keep columns that don't define any unused alias
    let kept: Vec<&str> = columns
        .into_iter()
        .filter(|col| {
            let trimmed = col.trim();
            !unused.iter().any(|u| column_defines_alias(trimmed, u))
        })
        .collect();

    if kept.is_empty() {
        // Safety: don't remove ALL columns
        return block.to_string();
    }

    format!("{}{}{}", prefix, kept.join(",\n"), suffix)
}

/// Check if a column definition text defines a specific alias.
fn column_defines_alias(col_trimmed: &str, alias: &str) -> bool {
    // Pattern: "... as alias_name" at end of definition
    let as_pat = format!(" as {}", alias);
    if col_trimmed.ends_with(&as_pat) {
        return true;
    }
    // Carry-forward without explicit alias: "vp.alias_name" (standalone)
    col_trimmed == format!("vp.{}", alias)
}

// ─── Join Optimizations ──────────────────────────────────────────────────────

/// Apply all join optimizations to a single plan.
fn optimize_joins_in_plan(plan: &mut RenderPlan, protected_aliases: &HashSet<String>) {
    remove_unreferenced_joins(plan, protected_aliases);
    eliminate_bridge_nodes_in_plan(plan, protected_aliases);
    // Anchor selection (select_anchor in join_generation.rs) handles inline property
    // filters ({name: $tag}). This post-hoc pass catches WHERE clause filters that
    // aren't inline — both are needed for defense-in-depth.
    reorder_from_for_selective_predicate(plan);
}

/// Collect all table aliases referenced in a plan's SELECT, WHERE, ORDER BY, GROUP BY, HAVING.
/// Used to build a "protected aliases" set for UNION branch optimization.
fn collect_referenced_aliases(plan: &RenderPlan) -> HashSet<String> {
    let mut aliases = HashSet::new();
    for item in &plan.select.items {
        collect_aliases_from_expr(&item.expression, &mut aliases);
    }
    if let Some(ref filter) = plan.filters.0 {
        collect_aliases_from_expr(filter, &mut aliases);
    }
    for item in &plan.order_by.0 {
        collect_aliases_from_expr(&item.expression, &mut aliases);
    }
    for expr in &plan.group_by.0 {
        collect_aliases_from_expr(expr, &mut aliases);
    }
    if let Some(ref having) = plan.having_clause {
        collect_aliases_from_expr(having, &mut aliases);
    }
    for aj in &plan.array_join.0 {
        collect_aliases_from_expr(&aj.expression, &mut aliases);
    }
    aliases
}

/// Recursively collect all table aliases referenced in a RenderExpr.
fn collect_aliases_from_expr(expr: &RenderExpr, aliases: &mut HashSet<String>) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_expr(operand, aliases);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_aliases_from_expr(arg, aliases);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_aliases_from_expr(arg, aliases);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref e) = case.expr {
                collect_aliases_from_expr(e, aliases);
            }
            for (when, then) in &case.when_then {
                collect_aliases_from_expr(when, aliases);
                collect_aliases_from_expr(then, aliases);
            }
            if let Some(ref else_expr) = case.else_expr {
                collect_aliases_from_expr(else_expr, aliases);
            }
        }
        RenderExpr::List(exprs) => {
            for e in exprs {
                collect_aliases_from_expr(e, aliases);
            }
        }
        RenderExpr::InSubquery(sub) => {
            collect_aliases_from_expr(&sub.expr, aliases);
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_aliases_from_expr(array, aliases);
            collect_aliases_from_expr(index, aliases);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_aliases_from_expr(array, aliases);
            if let Some(f) = from {
                collect_aliases_from_expr(f, aliases);
            }
            if let Some(t) = to {
                collect_aliases_from_expr(t, aliases);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            collect_aliases_from_expr(&reduce.initial_value, aliases);
            collect_aliases_from_expr(&reduce.list, aliases);
            collect_aliases_from_expr(&reduce.expression, aliases);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                collect_aliases_from_expr(v, aliases);
            }
        }
        RenderExpr::TableAlias(ta) => {
            aliases.insert(ta.0.clone());
        }
        RenderExpr::ColumnAlias(ca) => {
            aliases.insert(ca.0.clone());
        }
        RenderExpr::ExistsSubquery(es) => {
            // Extract "alias." patterns from pre-rendered SQL
            extract_aliases_from_sql(&es.sql, aliases);
        }
        RenderExpr::PatternCount(pc) => {
            // Extract "alias." patterns from pre-rendered SQL
            extract_aliases_from_sql(&pc.sql, aliases);
        }
        RenderExpr::Raw(raw) => {
            extract_aliases_from_sql(raw, aliases);
        }
        _ => {}
    }
}

/// Extract "alias." patterns from a SQL string.
fn extract_aliases_from_sql(sql: &str, aliases: &mut HashSet<String>) {
    for word in sql.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.') {
        if let Some(alias) = word.strip_suffix('.') {
            if !alias.is_empty() {
                aliases.insert(alias.to_string());
            }
        } else if word.contains('.') {
            if let Some(alias) = word.split('.').next() {
                if !alias.is_empty() {
                    aliases.insert(alias.to_string());
                }
            }
        }
    }
}

/// Remove JOINs whose alias is completely unreferenced in the plan.
///
/// Only removes JOINs that are semantically safe to eliminate:
/// - LEFT JOINs: removing an unreferenced LEFT JOIN never changes row cardinality
/// - CROSS JOINs (ON 1=1): these are typically spurious joins added by orphan alias
///   resolution; removing them eliminates unwanted row multiplication
///
/// INNER JOINs are NOT removed because they can filter rows (if the ON condition
/// eliminates non-matching rows), which would change Cypher bag semantics.
fn remove_unreferenced_joins(plan: &mut RenderPlan, protected_aliases: &HashSet<String>) {
    // Collect indices to remove (in reverse order for safe removal)
    let mut to_remove = Vec::new();

    for (idx, join) in plan.joins.0.iter().enumerate().rev() {
        let alias = &join.table_alias;

        // Only remove LEFT JOINs and CROSS JOINs (ON 1=1)
        let is_safe_to_remove = match join.join_type {
            JoinType::Left => true,
            // JoinType::Join with ON 1=1 is a CROSS JOIN — safe to remove when unreferenced
            JoinType::Join => {
                join.joining_on.len() == 1 && is_tautology_condition(&join.joining_on[0])
            }
            _ => false,
        };
        if !is_safe_to_remove {
            continue;
        }

        // Never remove edge tables — they provide the traversal.
        // This also protects synthetic LEFT JOINs from OPTIONAL MATCH on
        // denormalized schemas (plan_builder.rs sets from_id_column as a
        // preservation marker when count(r) resolves to count(*) and the
        // alias appears unreferenced).
        if join.from_id_column.is_some() || join.to_id_column.is_some() {
            continue;
        }

        // Never remove VLP joins
        if join.graph_rel.is_some() {
            continue;
        }

        // Never remove CTE joins
        if is_cte_table(&join.table_name) {
            continue;
        }

        // Never remove joins in fixed path metadata
        if let Some(ref fpm) = plan.fixed_path_info {
            if fpm.node_aliases.contains(alias) {
                continue;
            }
        }

        // Keep if alias is protected by the parent plan (UNION branch optimization)
        if protected_aliases.contains(alias) {
            continue;
        }

        // Keep if alias is referenced in SELECT/WHERE/ORDER BY/GROUP BY/HAVING/ARRAY JOIN
        if is_alias_referenced_in_plan(plan, alias) {
            continue;
        }

        // Keep if any OTHER join's ON condition or pre_filter references this alias
        // (unlike bridge elimination, we can't rewrite those here)
        let alias_used_in_other_joins = plan.joins.0.iter().enumerate().any(|(i, other)| {
            if i == idx {
                return false;
            }
            for cond in &other.joining_on {
                for operand in &cond.operands {
                    if references_alias(operand, alias) {
                        return true;
                    }
                }
            }
            if let Some(ref pf) = other.pre_filter {
                if references_alias(pf, alias) {
                    return true;
                }
            }
            false
        });

        if alias_used_in_other_joins {
            continue;
        }

        log::debug!(
            "Unreferenced join elimination: removing {} ({})",
            alias,
            join.table_name
        );
        to_remove.push(idx);
    }

    // Remove in reverse index order (already reversed from the loop)
    for idx in &to_remove {
        plan.joins.0.remove(*idx);
    }
}

/// Eliminate bridge nodes within a single plan's join list.
/// Iterates until no more eliminations are found (handles chained bridges).
fn eliminate_bridge_nodes_in_plan(plan: &mut RenderPlan, protected_aliases: &HashSet<String>) {
    loop {
        let candidates = find_bridge_candidates(plan, protected_aliases);
        if candidates.is_empty() {
            break;
        }

        // Pass 2: Rewrite all references to eliminated nodes
        for candidate in &candidates {
            rewrite_joins_for_bridge(
                &mut plan.joins.0,
                &candidate.alias,
                &candidate.id_column,
                &candidate.upstream,
            );
            rewrite_plan_exprs_for_bridge(
                plan,
                &candidate.alias,
                &candidate.id_column,
                &candidate.upstream,
            );
        }

        // Pass 3: Remove eliminated JOINs (indices are in reverse order from find_bridge_candidates)
        for candidate in &candidates {
            plan.joins.0.remove(candidate.join_idx);
        }

        log::debug!(
            "Bridge node elimination: removed {} bridge JOINs",
            candidates.len()
        );
    }
}

/// Pass 1: Identify bridge node candidates by iterating joins in reverse.
/// Returns candidates sorted by descending index (safe for sequential removal).
fn find_bridge_candidates(
    plan: &RenderPlan,
    protected_aliases: &HashSet<String>,
) -> Vec<BridgeCandidate> {
    let mut candidates = Vec::new();

    for (idx, join) in plan.joins.0.iter().enumerate().rev() {
        // Guard: must not be an edge table (edge tables have from_id/to_id columns)
        if join.from_id_column.is_some() || join.to_id_column.is_some() {
            continue;
        }

        // Guard: must not be a VLP join
        if join.graph_rel.is_some() {
            continue;
        }

        // Guard: must not have a pre-filter (schema/view filter)
        if join.pre_filter.is_some() {
            continue;
        }

        // Guard: must have exactly one ON condition with Operator::Equal
        if join.joining_on.len() != 1 {
            continue;
        }
        let condition = &join.joining_on[0];
        if condition.operator != Operator::Equal || condition.operands.len() != 2 {
            continue;
        }

        // Guard: both operands must be PropertyAccessExp
        let (left, right) = (&condition.operands[0], &condition.operands[1]);
        let (left_pa, right_pa) = match (left, right) {
            (RenderExpr::PropertyAccessExp(l), RenderExpr::PropertyAccessExp(r)) => (l, r),
            _ => continue,
        };

        // Guard: table_name must not be a CTE reference
        if is_cte_table(&join.table_name) {
            continue;
        }

        // Guard: alias must not be in FixedPathMetadata.node_aliases
        if let Some(ref fpm) = plan.fixed_path_info {
            if fpm.node_aliases.contains(&join.table_alias) {
                continue;
            }
        }

        // Determine which side is "self" (the node being joined) and which is "upstream"
        let (id_column, upstream) = if left_pa.table_alias.0 == join.table_alias {
            // Pattern: node.id = edge.FK
            (
                left_pa.column.raw().to_string(),
                UpstreamRef {
                    alias: right_pa.table_alias.0.clone(),
                    column: right_pa.column.raw().to_string(),
                },
            )
        } else if right_pa.table_alias.0 == join.table_alias {
            // Pattern: edge.FK = node.id
            (
                right_pa.column.raw().to_string(),
                UpstreamRef {
                    alias: left_pa.table_alias.0.clone(),
                    column: left_pa.column.raw().to_string(),
                },
            )
        } else {
            // Neither operand references this join's alias — skip
            continue;
        };

        let alias = &join.table_alias;

        // Guard: alias must not be protected by parent plan (UNION branch optimization)
        if protected_aliases.contains(alias) {
            continue;
        }

        // Guard: alias must not be referenced via non-ID columns in any plan part.
        // If only the ID column is referenced (SELECT, WHERE, ORDER BY, etc.),
        // those references can be rewritten to the upstream FK — still a valid bridge.
        // But correlated refs in pre-rendered SQL strings (ExistsSubquery, PatternCount)
        // cannot be structurally rewritten, so any such reference blocks elimination.
        if has_non_id_ref_in_plan(plan, alias, &id_column) {
            continue;
        }

        // Guard: alias must not appear as unresolved bare TableAlias/ColumnAlias.
        // These are bare variable references that rewrite_bare_variables couldn't resolve.
        // While we CAN rewrite them, the context they appear in (e.g., `forum1 <> forum2`)
        // may have other unresolved aliases that stay as-is, producing invalid SQL.
        if has_unresolved_bare_ref_in_plan(plan, alias) {
            continue;
        }

        // Guard: other JOINs must only reference this alias via the id_column.
        // Pass 2 only rewrites `alias.id_column` → upstream. If another JOIN
        // references `alias.other_column`, that reference would become dangling.
        let has_non_id_ref = plan.joins.0.iter().enumerate().any(|(i, other)| {
            if i == idx {
                return false;
            }
            for cond in &other.joining_on {
                for operand in &cond.operands {
                    if has_non_id_column_ref(operand, alias, &id_column) {
                        return true;
                    }
                }
            }
            if let Some(ref pf) = other.pre_filter {
                if has_non_id_column_ref(pf, alias, &id_column) {
                    return true;
                }
            }
            false
        });
        if has_non_id_ref {
            continue;
        }

        candidates.push(BridgeCandidate {
            join_idx: idx,
            alias: alias.clone(),
            id_column,
            upstream,
        });
    }

    candidates
}

/// Check if an expression contains unresolved TableAlias/ColumnAlias for the given alias.
/// These are bare variable references that `rewrite_bare_variables` couldn't resolve.
/// Bridge elimination rewrites them, but if the SAME expression contains OTHER unresolved
/// bare aliases (e.g., `forum1 <> forum2` where both are TableAlias), rewriting only one
/// produces inconsistent SQL. We block elimination in this case.
fn has_unresolved_bare_ref(expr: &RenderExpr, alias: &str) -> bool {
    match expr {
        RenderExpr::TableAlias(ta) => ta.0 == alias,
        RenderExpr::ColumnAlias(ca) => ca.0 == alias,
        RenderExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|o| has_unresolved_bare_ref(o, alias)),
        RenderExpr::ScalarFnCall(func) => {
            func.args.iter().any(|a| has_unresolved_bare_ref(a, alias))
        }
        RenderExpr::AggregateFnCall(agg) => {
            agg.args.iter().any(|a| has_unresolved_bare_ref(a, alias))
        }
        RenderExpr::Case(case) => {
            case.when_then.iter().any(|(w, t)| {
                has_unresolved_bare_ref(w, alias) || has_unresolved_bare_ref(t, alias)
            }) || case
                .else_expr
                .as_ref()
                .is_some_and(|e| has_unresolved_bare_ref(e, alias))
        }
        RenderExpr::List(items) => items.iter().any(|i| has_unresolved_bare_ref(i, alias)),
        RenderExpr::InSubquery(subq) => has_unresolved_bare_ref(&subq.expr, alias),
        _ => false,
    }
}

/// Check if an alias has unresolved bare references in plan expressions.
fn has_unresolved_bare_ref_in_plan(plan: &RenderPlan, alias: &str) -> bool {
    for item in &plan.select.items {
        if has_unresolved_bare_ref(&item.expression, alias) {
            return true;
        }
    }
    if let Some(ref filter) = plan.filters.0 {
        if has_unresolved_bare_ref(filter, alias) {
            return true;
        }
    }
    for item in &plan.order_by.0 {
        if has_unresolved_bare_ref(&item.expression, alias) {
            return true;
        }
    }
    for expr in &plan.group_by.0 {
        if has_unresolved_bare_ref(expr, alias) {
            return true;
        }
    }
    if let Some(ref having) = plan.having_clause {
        if has_unresolved_bare_ref(having, alias) {
            return true;
        }
    }
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            for item in &branch.select.items {
                if has_unresolved_bare_ref(&item.expression, alias) {
                    return true;
                }
            }
            if let Some(ref filter) = branch.filters.0 {
                if has_unresolved_bare_ref(filter, alias) {
                    return true;
                }
            }
            for item in &branch.order_by.0 {
                if has_unresolved_bare_ref(&item.expression, alias) {
                    return true;
                }
            }
            for expr in &branch.group_by.0 {
                if has_unresolved_bare_ref(expr, alias) {
                    return true;
                }
            }
            if let Some(ref having) = branch.having_clause {
                if has_unresolved_bare_ref(having, alias) {
                    return true;
                }
            }
        }
    }
    false
}

/// Check if an alias is referenced with a non-ID column in plan expressions
/// (SELECT, WHERE, ORDER BY, GROUP BY, HAVING, ARRAY JOIN, UNION branches).
/// Returns true if ANY reference uses a column other than `id_column`, OR if
/// the alias appears in pre-rendered correlated SQL (ExistsSubquery/PatternCount)
/// which cannot be structurally rewritten.
fn has_non_id_ref_in_plan(plan: &RenderPlan, alias: &str, id_column: &str) -> bool {
    // Check SELECT items
    for item in &plan.select.items {
        if has_non_id_column_ref(&item.expression, alias, id_column)
            || expr_has_correlated_ref(&item.expression, alias)
        {
            return true;
        }
    }

    // Check WHERE clause
    if let Some(ref filter) = plan.filters.0 {
        if has_non_id_column_ref(filter, alias, id_column) || expr_has_correlated_ref(filter, alias)
        {
            return true;
        }
    }

    // Check ORDER BY
    for item in &plan.order_by.0 {
        if has_non_id_column_ref(&item.expression, alias, id_column)
            || expr_has_correlated_ref(&item.expression, alias)
        {
            return true;
        }
    }

    // Check GROUP BY
    for expr in &plan.group_by.0 {
        if has_non_id_column_ref(expr, alias, id_column) || expr_has_correlated_ref(expr, alias) {
            return true;
        }
    }

    // Check HAVING
    if let Some(ref having) = plan.having_clause {
        if has_non_id_column_ref(having, alias, id_column) || expr_has_correlated_ref(having, alias)
        {
            return true;
        }
    }

    // Check UNION branches
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            for item in &branch.select.items {
                if has_non_id_column_ref(&item.expression, alias, id_column)
                    || expr_has_correlated_ref(&item.expression, alias)
                {
                    return true;
                }
            }
            if let Some(ref filter) = branch.filters.0 {
                if has_non_id_column_ref(filter, alias, id_column)
                    || expr_has_correlated_ref(filter, alias)
                {
                    return true;
                }
            }
            for item in &branch.order_by.0 {
                if has_non_id_column_ref(&item.expression, alias, id_column)
                    || expr_has_correlated_ref(&item.expression, alias)
                {
                    return true;
                }
            }
            for expr in &branch.group_by.0 {
                if has_non_id_column_ref(expr, alias, id_column)
                    || expr_has_correlated_ref(expr, alias)
                {
                    return true;
                }
            }
        }
    }

    // Check ARRAY JOIN expressions
    for aj in &plan.array_join.0 {
        if has_non_id_column_ref(&aj.expression, alias, id_column)
            || expr_has_correlated_ref(&aj.expression, alias)
        {
            return true;
        }
    }

    false
}

/// Check if an alias is referenced in non-rewritable parts of the plan
/// (SELECT, WHERE, ORDER BY, GROUP BY, HAVING, ARRAY JOIN).
/// JOIN ON conditions and pre_filters are NOT checked because Pass 2 rewrites them.
fn is_alias_referenced_in_plan(plan: &RenderPlan, alias: &str) -> bool {
    // Check SELECT items
    for item in &plan.select.items {
        if references_alias(&item.expression, alias)
            || expr_has_correlated_ref(&item.expression, alias)
        {
            return true;
        }
    }

    // Check WHERE clause
    if let Some(ref filter) = plan.filters.0 {
        if references_alias(filter, alias) || expr_has_correlated_ref(filter, alias) {
            return true;
        }
    }

    // Check ORDER BY
    for item in &plan.order_by.0 {
        if references_alias(&item.expression, alias)
            || expr_has_correlated_ref(&item.expression, alias)
        {
            return true;
        }
    }

    // Check GROUP BY
    for expr in &plan.group_by.0 {
        if references_alias(expr, alias) || expr_has_correlated_ref(expr, alias) {
            return true;
        }
    }

    // Check HAVING
    if let Some(ref having) = plan.having_clause {
        if references_alias(having, alias) || expr_has_correlated_ref(having, alias) {
            return true;
        }
    }

    // Note: We do NOT check other JOINs' ON conditions or pre_filters here.
    // Those are rewritten in Pass 2 (rewrite_joins_for_bridge), so references
    // to the bridge alias in ON conditions are expected and handled.

    // Check UNION branches (SELECT, WHERE, ORDER BY, GROUP BY only — not JOIN ON conditions)
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            for item in &branch.select.items {
                if references_alias(&item.expression, alias)
                    || expr_has_correlated_ref(&item.expression, alias)
                {
                    return true;
                }
            }
            if let Some(ref filter) = branch.filters.0 {
                if references_alias(filter, alias) || expr_has_correlated_ref(filter, alias) {
                    return true;
                }
            }
            for item in &branch.order_by.0 {
                if references_alias(&item.expression, alias)
                    || expr_has_correlated_ref(&item.expression, alias)
                {
                    return true;
                }
            }
            for expr in &branch.group_by.0 {
                if references_alias(expr, alias) || expr_has_correlated_ref(expr, alias) {
                    return true;
                }
            }
        }
    }

    // Check ARRAY JOIN expressions
    for aj in &plan.array_join.0 {
        if references_alias(&aj.expression, alias) || expr_has_correlated_ref(&aj.expression, alias)
        {
            return true;
        }
    }

    false
}

/// Check if an expression tree contains ExistsSubquery or PatternCount nodes
/// whose embedded SQL references the given alias. These carry pre-rendered correlated
/// SQL strings that `references_alias()` doesn't inspect.
fn expr_has_correlated_ref(expr: &RenderExpr, alias: &str) -> bool {
    let alias_dot = format!("{}.", alias);
    match expr {
        RenderExpr::ExistsSubquery(es) => es.sql.contains(&alias_dot),
        RenderExpr::PatternCount(pc) => pc.sql.contains(&alias_dot),
        RenderExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|o| expr_has_correlated_ref(o, alias)),
        RenderExpr::ScalarFnCall(func) => {
            func.args.iter().any(|a| expr_has_correlated_ref(a, alias))
        }
        RenderExpr::AggregateFnCall(agg) => {
            agg.args.iter().any(|a| expr_has_correlated_ref(a, alias))
        }
        RenderExpr::Case(case) => {
            case.expr
                .as_ref()
                .is_some_and(|e| expr_has_correlated_ref(e, alias))
                || case.when_then.iter().any(|(w, t)| {
                    expr_has_correlated_ref(w, alias) || expr_has_correlated_ref(t, alias)
                })
                || case
                    .else_expr
                    .as_ref()
                    .is_some_and(|e| expr_has_correlated_ref(e, alias))
        }
        RenderExpr::List(items) => items.iter().any(|i| expr_has_correlated_ref(i, alias)),
        RenderExpr::ArraySubscript { array, index } => {
            expr_has_correlated_ref(array, alias) || expr_has_correlated_ref(index, alias)
        }
        RenderExpr::InSubquery(subq) => expr_has_correlated_ref(&subq.expr, alias),
        _ => false,
    }
}

/// Check if an expression references an alias with a column OTHER than the given id_column.
/// Used to guard bridge elimination: if other JOINs reference non-ID columns on the
/// candidate, those references can't be rewritten, so elimination is unsafe.
fn has_non_id_column_ref(expr: &RenderExpr, alias: &str, id_column: &str) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            pa.table_alias.0 == alias && pa.column.raw() != id_column
        }
        RenderExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|o| has_non_id_column_ref(o, alias, id_column)),
        RenderExpr::ScalarFnCall(func) => func
            .args
            .iter()
            .any(|a| has_non_id_column_ref(a, alias, id_column)),
        RenderExpr::AggregateFnCall(agg) => agg
            .args
            .iter()
            .any(|a| has_non_id_column_ref(a, alias, id_column)),
        RenderExpr::Case(case) => {
            case.when_then.iter().any(|(w, t)| {
                has_non_id_column_ref(w, alias, id_column)
                    || has_non_id_column_ref(t, alias, id_column)
            }) || case
                .else_expr
                .as_ref()
                .is_some_and(|e| has_non_id_column_ref(e, alias, id_column))
        }
        RenderExpr::List(items) => items
            .iter()
            .any(|i| has_non_id_column_ref(i, alias, id_column)),
        RenderExpr::InSubquery(subq) => has_non_id_column_ref(&subq.expr, alias, id_column),
        // Raw SQL strings may contain alias references we can't structurally analyze.
        // Conservatively treat any mention of the alias as a non-ID reference.
        RenderExpr::Raw(sql) => sql.contains(&format!("{}.", alias)),
        RenderExpr::ExistsSubquery(es) => es.sql.contains(&format!("{}.", alias)),
        RenderExpr::PatternCount(pc) => pc.sql.contains(&format!("{}.", alias)),
        _ => false,
    }
}

/// Rewrite plan expressions (SELECT, WHERE, ORDER BY, GROUP BY, HAVING, UNION branches)
/// to replace bridge node ID references with upstream FK references.
fn rewrite_plan_exprs_for_bridge(
    plan: &mut RenderPlan,
    eliminated_alias: &str,
    eliminated_id_col: &str,
    upstream: &UpstreamRef,
) {
    // Rewrite SELECT items
    for item in plan.select.items.iter_mut() {
        rewrite_bridge_in_expr(
            &mut item.expression,
            eliminated_alias,
            eliminated_id_col,
            upstream,
        );
    }

    // Rewrite WHERE clause
    if let Some(ref mut filter) = plan.filters.0 {
        rewrite_bridge_in_expr(filter, eliminated_alias, eliminated_id_col, upstream);
    }

    // Rewrite ORDER BY
    for item in plan.order_by.0.iter_mut() {
        rewrite_bridge_in_expr(
            &mut item.expression,
            eliminated_alias,
            eliminated_id_col,
            upstream,
        );
    }

    // Rewrite GROUP BY
    for expr in plan.group_by.0.iter_mut() {
        rewrite_bridge_in_expr(expr, eliminated_alias, eliminated_id_col, upstream);
    }

    // Rewrite HAVING
    if let Some(ref mut having) = plan.having_clause {
        rewrite_bridge_in_expr(having, eliminated_alias, eliminated_id_col, upstream);
    }

    // Rewrite ARRAY JOIN
    for aj in plan.array_join.0.iter_mut() {
        rewrite_bridge_in_expr(
            &mut aj.expression,
            eliminated_alias,
            eliminated_id_col,
            upstream,
        );
    }

    // Rewrite UNION branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            for item in branch.select.items.iter_mut() {
                rewrite_bridge_in_expr(
                    &mut item.expression,
                    eliminated_alias,
                    eliminated_id_col,
                    upstream,
                );
            }
            if let Some(ref mut filter) = branch.filters.0 {
                rewrite_bridge_in_expr(filter, eliminated_alias, eliminated_id_col, upstream);
            }
            for item in branch.order_by.0.iter_mut() {
                rewrite_bridge_in_expr(
                    &mut item.expression,
                    eliminated_alias,
                    eliminated_id_col,
                    upstream,
                );
            }
            for expr in branch.group_by.0.iter_mut() {
                rewrite_bridge_in_expr(expr, eliminated_alias, eliminated_id_col, upstream);
            }
            if let Some(ref mut having) = branch.having_clause {
                rewrite_bridge_in_expr(having, eliminated_alias, eliminated_id_col, upstream);
            }
            for aj in branch.array_join.0.iter_mut() {
                rewrite_bridge_in_expr(
                    &mut aj.expression,
                    eliminated_alias,
                    eliminated_id_col,
                    upstream,
                );
            }
            // Also rewrite UNION branch JOINs
            rewrite_joins_for_bridge(
                &mut branch.joins.0,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
        }
    }
}

/// Pass 2: Rewrite downstream JOIN ON conditions to bypass an eliminated bridge node.
/// Replaces `PropertyAccessExp(eliminated_alias, id_col)` with `PropertyAccessExp(upstream_alias, upstream_col)`.
fn rewrite_joins_for_bridge(
    joins: &mut [Join],
    eliminated_alias: &str,
    eliminated_id_col: &str,
    upstream: &UpstreamRef,
) {
    for join in joins.iter_mut() {
        // Rewrite ON conditions
        for cond in join.joining_on.iter_mut() {
            for operand in cond.operands.iter_mut() {
                rewrite_bridge_in_expr(operand, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        // Rewrite pre_filter if present
        if let Some(ref mut pre_filter) = join.pre_filter {
            rewrite_bridge_in_expr(pre_filter, eliminated_alias, eliminated_id_col, upstream);
        }
    }
}

/// Rewrite a single expression: replace references to the eliminated bridge node's
/// ID column with the upstream FK reference.
fn rewrite_bridge_in_expr(
    expr: &mut RenderExpr,
    eliminated_alias: &str,
    eliminated_id_col: &str,
    upstream: &UpstreamRef,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == eliminated_alias && pa.column.raw() == eliminated_id_col {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(upstream.alias.clone()),
                    column: PropertyValue::Column(upstream.column.clone()),
                });
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                rewrite_bridge_in_expr(operand, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in func.args.iter_mut() {
                rewrite_bridge_in_expr(arg, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in agg.args.iter_mut() {
                rewrite_bridge_in_expr(arg, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref mut e) = case.expr {
                rewrite_bridge_in_expr(e, eliminated_alias, eliminated_id_col, upstream);
            }
            for (when, then) in case.when_then.iter_mut() {
                rewrite_bridge_in_expr(when, eliminated_alias, eliminated_id_col, upstream);
                rewrite_bridge_in_expr(then, eliminated_alias, eliminated_id_col, upstream);
            }
            if let Some(ref mut e) = case.else_expr {
                rewrite_bridge_in_expr(e, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::InSubquery(subq) => {
            rewrite_bridge_in_expr(
                &mut subq.expr,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
        }
        RenderExpr::ArraySubscript { array, index } => {
            rewrite_bridge_in_expr(array, eliminated_alias, eliminated_id_col, upstream);
            rewrite_bridge_in_expr(index, eliminated_alias, eliminated_id_col, upstream);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            rewrite_bridge_in_expr(array, eliminated_alias, eliminated_id_col, upstream);
            if let Some(ref mut f) = from {
                rewrite_bridge_in_expr(f, eliminated_alias, eliminated_id_col, upstream);
            }
            if let Some(ref mut t) = to {
                rewrite_bridge_in_expr(t, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::List(items) => {
            for item in items.iter_mut() {
                rewrite_bridge_in_expr(item, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            rewrite_bridge_in_expr(
                &mut reduce.initial_value,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
            rewrite_bridge_in_expr(
                &mut reduce.list,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
            rewrite_bridge_in_expr(
                &mut reduce.expression,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, value) in entries.iter_mut() {
                rewrite_bridge_in_expr(value, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        // TableAlias represents a bare node reference (resolves to node.id).
        // If it matches the eliminated alias, rewrite to upstream FK.
        RenderExpr::TableAlias(ta) => {
            if ta.0 == eliminated_alias {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(upstream.alias.clone()),
                    column: PropertyValue::Column(upstream.column.clone()),
                });
            }
        }
        // ColumnAlias can also represent a bare node reference in some contexts.
        RenderExpr::ColumnAlias(ca) => {
            if ca.0 == eliminated_alias {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(upstream.alias.clone()),
                    column: PropertyValue::Column(upstream.column.clone()),
                });
            }
        }
        // Leaf nodes — no rewriting needed
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::Column(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_)
        | RenderExpr::Parameter(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::Literal;
    use crate::render_plan::render_expr::{ColumnAlias, Operator};
    use crate::render_plan::*;

    /// Helper to create a simple PropertyAccessExp
    fn prop(alias: &str, col: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(col.to_string()),
        })
    }

    /// Helper to create an equality ON condition
    fn eq_on(left: RenderExpr, right: RenderExpr) -> OperatorApplication {
        OperatorApplication {
            operator: Operator::Equal,
            operands: vec![left, right],
        }
    }

    /// Helper to create a minimal RenderPlan with given joins and select items
    fn make_plan(joins: Vec<Join>, select_exprs: Vec<RenderExpr>) -> RenderPlan {
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: select_exprs
                    .into_iter()
                    .map(|e| SelectItem {
                        expression: e,
                        col_alias: None,
                    })
                    .collect(),
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(joins),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: None,
            order_by: OrderByItems(vec![]),
            skip: SkipItem(None),
            limit: LimitItem(None),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        }
    }

    /// Helper to create a node join (no from_id/to_id)
    fn node_join(alias: &str, table: &str, on: Vec<OperatorApplication>) -> Join {
        Join {
            table_name: table.to_string(),
            table_alias: alias.to_string(),
            joining_on: on,
            join_type: JoinType::Left,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        }
    }

    /// Helper to create an edge join (has from_id/to_id)
    fn edge_join(
        alias: &str,
        table: &str,
        on: Vec<OperatorApplication>,
        from_id: &str,
        to_id: &str,
    ) -> Join {
        Join {
            table_name: table.to_string(),
            table_alias: alias.to_string(),
            joining_on: on,
            join_type: JoinType::Left,
            pre_filter: None,
            from_id_column: Some(from_id.to_string()),
            to_id_column: Some(to_id.to_string()),
            graph_rel: None,
        }
    }

    #[test]
    fn test_simple_bridge_elimination() {
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person2   ON person2.id = t3.PersonId         <- BRIDGE
        //   edge2(t4) ON t4.PersonId = person2.id
        //
        // After elimination:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   edge2(t4) ON t4.PersonId = t3.PersonId        <- rewritten

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
            edge_join(
                "t4",
                "Message_hasCreator_Person",
                vec![eq_on(prop("t4", "PersonId"), prop("person2", "id"))],
                "PersonId",
                "MessageId",
            ),
        ];

        // SELECT only references t3 and t4, not person2
        let select = vec![prop("t3", "PersonId"), prop("t4", "MessageId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // person2 should be eliminated
        assert_eq!(plan.joins.0.len(), 2);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
        assert_eq!(plan.joins.0[1].table_alias, "t4");

        // t4's ON condition should now reference t3.PersonId instead of person2.id
        let t4_on = &plan.joins.0[1].joining_on[0];
        if let RenderExpr::PropertyAccessExp(ref pa) = t4_on.operands[1] {
            assert_eq!(pa.table_alias.0, "t3");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected PropertyAccessExp after bridge elimination");
        }
    }

    #[test]
    fn test_no_elimination_when_referenced_in_select() {
        // person2 is referenced in SELECT — should NOT be eliminated
        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
        ];

        let select = vec![prop("person2", "name")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // person2 should NOT be eliminated
        assert_eq!(plan.joins.0.len(), 2);
    }

    #[test]
    fn test_no_elimination_for_edge_tables() {
        // Edge tables (with from_id/to_id) should never be eliminated
        let joins = vec![edge_join(
            "t3",
            "Person_likes_Message",
            vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
            "PersonId",
            "MessageId",
        )];

        let select = vec![prop("message1", "id")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_chained_bridge_elimination() {
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person2   ON person2.id = t3.PersonId         <- BRIDGE
        //   edge2(t4) ON t4.PersonId = person2.id
        //   message2  ON message2.id = t4.MessageId       <- BRIDGE
        //   edge3     ON edge3.MessageId = message2.id
        //
        // After elimination (iterative):
        //   edge1(t3)  ON t3.MessageId = message1.id
        //   edge2(t4)  ON t4.PersonId = t3.PersonId
        //   edge3      ON edge3.MessageId = t4.MessageId

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
            edge_join(
                "t4",
                "Message_hasCreator_Person",
                vec![eq_on(prop("t4", "PersonId"), prop("person2", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "message2",
                "Message",
                vec![eq_on(prop("message2", "id"), prop("t4", "MessageId"))],
            ),
            edge_join(
                "edge3",
                "Person_likes_Message",
                vec![eq_on(prop("edge3", "MessageId"), prop("message2", "id"))],
                "PersonId",
                "MessageId",
            ),
        ];

        let select = vec![prop("edge3", "PersonId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // Both person2 and message2 should be eliminated
        assert_eq!(plan.joins.0.len(), 3);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
        assert_eq!(plan.joins.0[1].table_alias, "t4");
        assert_eq!(plan.joins.0[2].table_alias, "edge3");

        // t4 should reference t3.PersonId
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.joins.0[1].joining_on[0].operands[1] {
            assert_eq!(pa.table_alias.0, "t3");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected rewritten t4 ON condition");
        }

        // edge3 should reference t4.MessageId
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.joins.0[2].joining_on[0].operands[1] {
            assert_eq!(pa.table_alias.0, "t4");
            assert_eq!(pa.column.raw(), "MessageId");
        } else {
            panic!("Expected rewritten edge3 ON condition");
        }
    }

    #[test]
    fn test_no_elimination_with_pre_filter() {
        // Node join with pre_filter (schema filter) should NOT be eliminated
        let joins = vec![node_join(
            "person2",
            "Person",
            vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
        )];

        let mut plan = make_plan(joins, vec![]);
        plan.joins.0[0].pre_filter = Some(RenderExpr::Raw("type = 'Person'".to_string()));

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // Should NOT be eliminated due to pre_filter
        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_no_elimination_for_cte_tables() {
        // Joins referencing CTE tables (with_ prefix) should NOT be eliminated
        let joins = vec![node_join(
            "w1",
            "with_cte_1",
            vec![eq_on(prop("w1", "id"), prop("t3", "PersonId"))],
        )];

        let mut plan = make_plan(joins, vec![]);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_unreferenced_tail_node_elimination() {
        // A node at the end of a chain that's completely unreferenced should be eliminated
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person3   ON person3.id = t3.PersonId     <- UNREFERENCED
        //
        // After: person3 removed entirely

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person3",
                "Person",
                vec![eq_on(prop("person3", "id"), prop("t3", "PersonId"))],
            ),
        ];

        let select = vec![prop("t3", "PersonId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // person3 should be eliminated (unreferenced)
        assert_eq!(plan.joins.0.len(), 1);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
    }

    #[test]
    fn test_id_only_bridge_elimination_with_select_rewrite() {
        // Bridge node referenced only via ID column in SELECT/GROUP BY/ORDER BY.
        // Pattern (bi-6):
        //   edge(t2) ON t2.MessageId = message1.id
        //   person1  ON person1.id = t2.PersonId   <- BRIDGE (ID-only ref in SELECT)
        //
        // SELECT person1.id, GROUP BY person1.id, ORDER BY person1.id
        // → should rewrite to t2.PersonId and eliminate person1

        let joins = vec![
            edge_join(
                "t2",
                "Message_hasCreator_Person",
                vec![eq_on(prop("t2", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person1",
                "Person",
                vec![eq_on(prop("person1", "id"), prop("t2", "PersonId"))],
            ),
        ];

        let select = vec![prop("person1", "id")];
        let mut plan = make_plan(joins, select);
        plan.group_by.0.push(prop("person1", "id"));
        plan.order_by.0.push(OrderByItem {
            expression: prop("person1", "id"),
            order: OrderByOrder::Asc,
        });

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // person1 should be eliminated
        assert_eq!(plan.joins.0.len(), 1, "person1 should be eliminated");
        assert_eq!(plan.joins.0[0].table_alias, "t2");

        // SELECT should be rewritten: person1.id → t2.PersonId
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.select.items[0].expression {
            assert_eq!(pa.table_alias.0, "t2");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected rewritten SELECT expression");
        }

        // GROUP BY should be rewritten
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.group_by.0[0] {
            assert_eq!(pa.table_alias.0, "t2");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected rewritten GROUP BY expression");
        }

        // ORDER BY should be rewritten
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.order_by.0[0].expression {
            assert_eq!(pa.table_alias.0, "t2");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected rewritten ORDER BY expression");
        }
    }

    #[test]
    fn test_no_id_bridge_elimination_when_non_id_column_referenced() {
        // Node referenced via non-ID column (e.g., tag.name) should NOT be eliminated
        let joins = vec![
            edge_join(
                "t1",
                "Message_hasTag_Tag",
                vec![eq_on(prop("t1", "MessageId"), prop("message1", "id"))],
                "TagId",
                "MessageId",
            ),
            node_join(
                "tag",
                "Tag",
                vec![eq_on(prop("tag", "id"), prop("t1", "TagId"))],
            ),
        ];

        let select = vec![prop("message1", "id")];
        let mut plan = make_plan(joins, select);
        // WHERE tag.name = 'Databases' — non-ID column reference
        plan.filters = FilterItems(Some(RenderExpr::OperatorApplicationExp(
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    prop("tag", "name"),
                    RenderExpr::Literal(Literal::String("Databases".to_string())),
                ],
            },
        )));

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // tag should NOT be eliminated (referenced by non-ID column)
        assert_eq!(plan.joins.0.len(), 2, "tag should NOT be eliminated");
    }

    #[test]
    fn test_no_bridge_elimination_with_unresolved_bare_ref() {
        // Node referenced as unresolved TableAlias should NOT be eliminated
        let joins = vec![node_join(
            "forum1",
            "Forum",
            vec![eq_on(prop("forum1", "id"), prop("t3", "ForumId"))],
        )];

        let mut plan = make_plan(joins, vec![]);
        // WHERE forum1 <> forum2 — both unresolved TableAlias
        plan.filters = FilterItems(Some(RenderExpr::OperatorApplicationExp(
            OperatorApplication {
                operator: Operator::NotEqual,
                operands: vec![
                    RenderExpr::TableAlias(TableAlias("forum1".to_string())),
                    RenderExpr::TableAlias(TableAlias("forum2".to_string())),
                ],
            },
        )));

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // forum1 should NOT be eliminated (unresolved bare ref)
        assert_eq!(
            plan.joins.0.len(),
            1,
            "forum1 should NOT be eliminated due to unresolved bare ref"
        );
    }

    #[test]
    fn test_no_bridge_elimination_with_raw_schema_filter_in_where() {
        // Node referenced via Raw expression in WHERE (schema filter) should NOT be eliminated
        let joins = vec![node_join(
            "p",
            "Place",
            vec![eq_on(prop("p", "id"), prop("t1", "CityId"))],
        )];

        let select = vec![prop("p", "id")];
        let mut plan = make_plan(joins, select);
        // WHERE (p.type = 'City') — as Raw expression (how schema filters are stored)
        plan.filters = FilterItems(Some(RenderExpr::Raw("(p.type = 'City')".to_string())));

        eliminate_bridge_nodes_in_plan(&mut plan, &HashSet::new());

        // p should NOT be eliminated (Raw expression references alias)
        assert_eq!(
            plan.joins.0.len(),
            1,
            "p should NOT be eliminated due to Raw filter reference"
        );
    }

    // ─── Dead CTE Elimination Tests ─────────────────────────────────────────

    /// Helper to create a ViewTableRef for test purposes
    fn test_view_ref(name: &str, alias: Option<&str>) -> ViewTableRef {
        use crate::query_planner::logical_plan::LogicalPlan;
        ViewTableRef {
            source: std::sync::Arc::new(LogicalPlan::Empty),
            name: name.to_string(),
            alias: alias.map(|s| s.to_string()),
            use_final: false,
        }
    }

    /// Helper to create a CTE with given name and a structured plan
    fn make_cte(name: &str, select_exprs: Vec<(RenderExpr, &str)>) -> Cte {
        let items = select_exprs
            .into_iter()
            .map(|(expr, alias)| SelectItem {
                expression: expr,
                col_alias: Some(ColumnAlias(alias.to_string())),
            })
            .collect();
        Cte {
            cte_name: name.to_string(),
            content: CteContent::Structured(Box::new(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                array_join: ArrayJoinItem(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
                fixed_path_info: None,
                is_multi_label_scan: false,
                variable_registry: None,
            })),
            is_recursive: false,
            vlp_start_alias: None,
            vlp_end_alias: None,
            vlp_start_table: None,
            vlp_end_table: None,
            vlp_cypher_start_alias: None,
            vlp_cypher_end_alias: None,
            vlp_start_id_col: None,
            vlp_end_id_col: None,
            vlp_path_variable: None,
            columns: vec![],
            from_alias: None,
            outer_where_filters: None,
            with_exported_aliases: vec![],
            variable_registry: None,
        }
    }

    /// Helper to set FROM on a CTE's inner plan
    fn set_cte_from(cte: &mut Cte, table_name: &str, alias: &str) {
        if let CteContent::Structured(ref mut plan) = cte.content {
            plan.from = FromTableItem(Some(test_view_ref(table_name, Some(alias))));
        }
    }

    #[test]
    fn test_dead_cte_elimination() {
        // cte_1 is referenced by outer query, cte_2 is not → cte_2 removed
        let mut plan = make_plan(vec![], vec![prop("cte1_alias", "col_a")]);
        let mut cte1 = make_cte("with_cte_1", vec![(prop("x", "a"), "col_a")]);
        set_cte_from(&mut cte1, "some_table", "x");
        let mut cte2 = make_cte("with_cte_2", vec![(prop("y", "b"), "col_b")]);
        set_cte_from(&mut cte2, "other_table", "y");
        plan.ctes = CteItems(vec![cte1, cte2]);
        plan.from = FromTableItem(Some(test_view_ref("with_cte_1", Some("cte1_alias"))));

        remove_dead_ctes(&mut plan);

        assert_eq!(plan.ctes.0.len(), 1);
        assert_eq!(plan.ctes.0[0].cte_name, "with_cte_1");
    }

    #[test]
    fn test_dead_cte_keeps_transitively_referenced() {
        // cte_1 is referenced by cte_2, cte_2 is referenced by outer → both kept
        let mut plan = make_plan(vec![], vec![prop("cte2_alias", "col_b")]);
        let mut cte1 = make_cte("with_cte_1", vec![(prop("x", "a"), "col_a")]);
        set_cte_from(&mut cte1, "some_table", "x");
        let mut cte2 = make_cte("with_cte_2", vec![(prop("c1", "col_a"), "col_b")]);
        set_cte_from(&mut cte2, "with_cte_1", "c1");
        plan.ctes = CteItems(vec![cte1, cte2]);
        plan.from = FromTableItem(Some(test_view_ref("with_cte_2", Some("cte2_alias"))));

        remove_dead_ctes(&mut plan);

        assert_eq!(plan.ctes.0.len(), 2);
    }

    // ─── CTE Column Pruning Tests ───────────────────────────────────────────

    #[test]
    fn test_cte_column_pruning_removes_unused() {
        // CTE has columns col_a, col_b, col_c; outer query only uses col_a
        let mut plan = make_plan(vec![], vec![prop("c1", "col_a")]);
        let mut cte = make_cte(
            "with_test_cte_1",
            vec![
                (prop("x", "a"), "col_a"),
                (prop("x", "b"), "col_b"),
                (prop("x", "c"), "col_c"),
            ],
        );
        set_cte_from(&mut cte, "some_table", "x");
        plan.ctes = CteItems(vec![cte]);
        plan.from = FromTableItem(Some(test_view_ref("with_test_cte_1", Some("c1"))));

        prune_cte_columns(&mut plan);

        if let CteContent::Structured(ref inner) = plan.ctes.0[0].content {
            assert_eq!(inner.select.items.len(), 1);
            assert_eq!(inner.select.items[0].col_alias.as_ref().unwrap().0, "col_a");
        } else {
            panic!("Expected Structured CTE");
        }
    }

    #[test]
    fn test_cte_column_pruning_preserves_self_refs() {
        // CTE has col_a, col_b; HAVING references col_b → both kept
        let mut plan = make_plan(vec![], vec![prop("c1", "col_a")]);
        let mut cte = make_cte(
            "with_test_cte_1",
            vec![(prop("x", "a"), "col_a"), (prop("x", "b"), "col_b")],
        );
        set_cte_from(&mut cte, "some_table", "x");
        // Add HAVING that references col_b
        if let CteContent::Structured(ref mut inner) = cte.content {
            inner.having_clause = Some(RenderExpr::ColumnAlias(ColumnAlias("col_b".to_string())));
        }
        plan.ctes = CteItems(vec![cte]);
        plan.from = FromTableItem(Some(test_view_ref("with_test_cte_1", Some("c1"))));

        prune_cte_columns(&mut plan);

        if let CteContent::Structured(ref inner) = plan.ctes.0[0].content {
            assert_eq!(inner.select.items.len(), 2);
        } else {
            panic!("Expected Structured CTE");
        }
    }

    #[test]
    fn test_cte_column_pruning_backward_propagation() {
        // Chain: cte_1 → cte_2 → outer. Outer uses col_a from cte_2, which
        // carry-forwards col_a from cte_1. col_b should be pruned from both.
        let mut plan = make_plan(vec![], vec![prop("c2", "col_a")]);

        let mut cte1 = make_cte(
            "with_cte_1",
            vec![(prop("x", "a"), "col_a"), (prop("x", "b"), "col_b")],
        );
        set_cte_from(&mut cte1, "some_table", "x");

        let mut cte2 = make_cte(
            "with_cte_2",
            vec![
                (prop("c1", "col_a"), "col_a"),
                (prop("c1", "col_b"), "col_b"),
            ],
        );
        set_cte_from(&mut cte2, "with_cte_1", "c1");

        plan.ctes = CteItems(vec![cte1, cte2]);
        plan.from = FromTableItem(Some(test_view_ref("with_cte_2", Some("c2"))));

        prune_cte_columns(&mut plan);

        // Both CTEs should have col_b pruned
        for cte in &plan.ctes.0 {
            if let CteContent::Structured(ref inner) = cte.content {
                assert_eq!(
                    inner.select.items.len(),
                    1,
                    "CTE {} should have 1 column after pruning",
                    cte.cte_name
                );
                assert_eq!(inner.select.items[0].col_alias.as_ref().unwrap().0, "col_a");
            }
        }
    }

    /// Regression test: UNION branches with empty select.items must not have
    /// JOINs removed when those JOINs are referenced by the parent plan's SELECT.
    ///
    /// Scenario: parent SELECT references person2.id, UNION branches have empty
    /// select (populated later during SQL rendering) but contain a LEFT JOIN for
    /// person2. Without protection, remove_unreferenced_joins would remove it.
    #[test]
    fn test_union_branch_preserves_parent_referenced_joins() {
        // Parent plan references person2.id in SELECT
        let parent_select = vec![prop("person2", "id")];

        // UNION branch: empty SELECT, but has JOINs including person2
        let branch_joins = vec![
            edge_join(
                "t1",
                "likes",
                vec![eq_on(prop("person", "id"), prop("t1", "PersonId"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t1", "PersonId"))],
            ),
        ];
        let branch = make_plan(branch_joins, vec![]); // empty SELECT

        let mut plan = make_plan(vec![], parent_select);
        plan.union = UnionItems(Some(Union {
            input: vec![branch],
            union_type: crate::render_plan::UnionType::All,
        }));

        optimize_plan(&mut plan);

        // person2 JOIN must survive in the branch
        let branch = &plan.union.0.as_ref().unwrap().input[0];
        let aliases: Vec<&str> = branch
            .joins
            .0
            .iter()
            .map(|j| j.table_alias.as_str())
            .collect();
        assert!(
            aliases.contains(&"person2"),
            "person2 JOIN should be preserved (parent SELECT references it), got: {:?}",
            aliases
        );
    }

    /// Test that collect_aliases_from_expr handles TableAlias, ExistsSubquery,
    /// and PatternCount variants correctly.
    #[test]
    fn test_collect_aliases_covers_all_expr_variants() {
        let mut aliases = HashSet::new();

        // TableAlias
        collect_aliases_from_expr(
            &RenderExpr::TableAlias(TableAlias("node1".to_string())),
            &mut aliases,
        );
        assert!(aliases.contains("node1"));

        // ColumnAlias
        collect_aliases_from_expr(
            &RenderExpr::ColumnAlias(ColumnAlias("col1".to_string())),
            &mut aliases,
        );
        assert!(aliases.contains("col1"));

        // ExistsSubquery with alias.column pattern
        collect_aliases_from_expr(
            &RenderExpr::ExistsSubquery(render_expr::ExistsSubquery {
                sql: "EXISTS (SELECT 1 FROM t WHERE friend.id = t.PersonId)".to_string(),
            }),
            &mut aliases,
        );
        assert!(
            aliases.contains("friend"),
            "ExistsSubquery should extract 'friend' from SQL"
        );

        // PatternCount with alias.column pattern
        collect_aliases_from_expr(
            &RenderExpr::PatternCount(render_expr::PatternCount {
                sql: "(SELECT COUNT(*) FROM r WHERE person.id = r.Id)".to_string(),
            }),
            &mut aliases,
        );
        assert!(
            aliases.contains("person"),
            "PatternCount should extract 'person' from SQL"
        );
    }

    // =========================================================================
    // Pass 6: Selective predicate FROM reordering tests
    // =========================================================================

    /// Helper: make a FROM item
    fn make_from(table: &str, alias: &str) -> FromTableItem {
        FromTableItem(Some(ViewTableRef {
            source: Arc::new(LogicalPlan::Empty),
            name: table.to_string(),
            alias: Some(alias.to_string()),
            use_final: false,
        }))
    }

    /// Helper: make an INNER JOIN (no edge columns)
    fn inner_join(alias: &str, table: &str, on: Vec<OperatorApplication>) -> Join {
        Join {
            table_name: table.to_string(),
            table_alias: alias.to_string(),
            joining_on: on,
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        }
    }

    /// Helper: make a WHERE filter
    fn make_filter(expr: RenderExpr) -> FilterItems {
        FilterItems(Some(expr))
    }

    #[test]
    fn test_selective_predicate_reorder_promotes_filtered_join_to_from() {
        // FROM message → INNER JOIN edge ON edge.MessageId = message.id
        //              → INNER JOIN tag ON tag.id = edge.TagId
        // WHERE tag.name = 'Databases'
        //
        // Expected: FROM tag → INNER JOIN edge ON tag.id = edge.TagId
        //                     → INNER JOIN message ON edge.MessageId = message.id

        let mut plan = make_plan(vec![], vec![prop("tag", "name")]);
        plan.from = make_from("Message", "message");
        plan.joins = JoinItems(vec![
            inner_join(
                "edge",
                "Message_hasTag_Tag",
                vec![eq_on(prop("edge", "MessageId"), prop("message", "id"))],
            ),
            inner_join(
                "tag",
                "Tag",
                vec![eq_on(prop("tag", "id"), prop("edge", "TagId"))],
            ),
        ]);
        plan.filters = make_filter(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                prop("tag", "name"),
                RenderExpr::Literal(Literal::String("Databases".to_string())),
            ],
        }));

        reorder_from_for_selective_predicate(&mut plan);

        // tag should now be FROM
        let from = plan.from.0.as_ref().unwrap();
        assert_eq!(from.alias.as_deref(), Some("tag"));

        // message should now be a JOIN
        let join_aliases: Vec<&str> = plan
            .joins
            .0
            .iter()
            .map(|j| j.table_alias.as_str())
            .collect();
        assert!(
            join_aliases.contains(&"message"),
            "old FROM should become a JOIN, got: {:?}",
            join_aliases
        );

        // All joins should have ON conditions (no orphans)
        for join in &plan.joins.0 {
            assert!(
                !join.joining_on.is_empty(),
                "JOIN {} should have ON conditions",
                join.table_alias
            );
        }
    }

    #[test]
    fn test_selective_predicate_skips_when_from_already_filtered() {
        // FROM tag WHERE tag.name = 'Databases' → INNER JOIN edge
        // Should NOT reorder (FROM already has the selective predicate)
        let mut plan = make_plan(vec![], vec![prop("tag", "name")]);
        plan.from = make_from("Tag", "tag");
        plan.joins = JoinItems(vec![inner_join(
            "edge",
            "Message_hasTag_Tag",
            vec![eq_on(prop("edge", "TagId"), prop("tag", "id"))],
        )]);
        plan.filters = make_filter(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                prop("tag", "name"),
                RenderExpr::Literal(Literal::String("Databases".to_string())),
            ],
        }));

        reorder_from_for_selective_predicate(&mut plan);

        // FROM should still be tag
        let from = plan.from.0.as_ref().unwrap();
        assert_eq!(from.alias.as_deref(), Some("tag"));
    }

    #[test]
    fn test_selective_predicate_skips_left_join_path() {
        // FROM message → LEFT JOIN tag ON ... WHERE tag.name = 'Databases'
        // Should NOT reorder (can't promote through LEFT JOINs)
        let mut plan = make_plan(vec![], vec![prop("tag", "name")]);
        plan.from = make_from("Message", "message");
        plan.joins = JoinItems(vec![node_join(
            "tag",
            "Tag",
            vec![eq_on(prop("tag", "id"), prop("message", "tagId"))],
        )]);
        plan.filters = make_filter(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                prop("tag", "name"),
                RenderExpr::Literal(Literal::String("Databases".to_string())),
            ],
        }));

        reorder_from_for_selective_predicate(&mut plan);

        // FROM should still be message (LEFT JOIN blocks reordering)
        let from = plan.from.0.as_ref().unwrap();
        assert_eq!(from.alias.as_deref(), Some("message"));
    }

    #[test]
    fn test_selective_predicate_range_predicate_fallback() {
        // FROM message → INNER JOIN person ON ... WHERE person.age > 30
        // Range predicate should also trigger reordering (Pass 2 fallback)
        let mut plan = make_plan(vec![], vec![prop("person", "age")]);
        plan.from = make_from("Message", "message");
        plan.joins = JoinItems(vec![inner_join(
            "person",
            "Person",
            vec![eq_on(prop("person", "id"), prop("message", "personId"))],
        )]);
        plan.filters = make_filter(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                prop("person", "age"),
                RenderExpr::Literal(Literal::Integer(30)),
            ],
        }));

        reorder_from_for_selective_predicate(&mut plan);

        // person should now be FROM (range predicate triggered)
        let from = plan.from.0.as_ref().unwrap();
        assert_eq!(from.alias.as_deref(), Some("person"));
    }

    #[test]
    fn test_is_non_table_reference_recursive() {
        // `lower(person.name)` should NOT be considered a constant
        let expr = RenderExpr::ScalarFnCall(render_expr::ScalarFnCall {
            name: "lower".to_string(),
            args: vec![prop("person", "name")],
        });
        assert!(
            !is_non_table_reference(&expr),
            "lower(person.name) should not be treated as constant"
        );

        // A literal should be considered constant
        let lit = RenderExpr::Literal(Literal::String("hello".to_string()));
        assert!(
            is_non_table_reference(&lit),
            "string literal should be treated as constant"
        );

        // A parameter should be considered constant
        let param = RenderExpr::Parameter("$tag".to_string());
        assert!(
            is_non_table_reference(&param),
            "parameter should be treated as constant"
        );
    }
}

// =============================================================================
// Pass 6: Selective predicate FROM reordering
// =============================================================================

/// Reorder FROM for selective WHERE predicates.
///
/// When a WHERE filter has a constant equality predicate on an INNER JOIN table
/// (e.g., `tag.name = 'value'`), promoting that table to FROM position allows
/// ClickHouse to filter early and dramatically reduce intermediate join sizes.
///
/// The algorithm re-roots the join dependency tree: it finds the path from the
/// selective table to the current FROM, redistributes ON conditions along that
/// path, and re-sorts all JOINs by dependency.
///
/// Example (bi-6):
///   Before: FROM Message (3M rows) → ... → INNER JOIN Tag WHERE tag.name = '...'
///   After:  FROM Tag (1 row) → ... → INNER JOIN Message
///   Result: 53s → 0.78s (68x speedup)
fn reorder_from_for_selective_predicate(plan: &mut RenderPlan) {
    // --- Guards ---
    let from_ref = match plan.from.0.as_ref() {
        Some(vtr) => vtr,
        None => return,
    };
    let from_alias = from_ref
        .alias
        .clone()
        .unwrap_or_else(|| from_ref.name.clone());
    let from_name = from_ref.name.clone();

    // Don't reorder if FROM uses FINAL (ReplacingMergeTree)
    if from_ref.use_final {
        return;
    }
    // Don't reorder if UNION present
    if plan.union.0.is_some() {
        return;
    }
    // Don't reorder if no joins
    if plan.joins.0.is_empty() {
        return;
    }
    // Don't reorder if FROM is a CTE
    if is_cte_table(&from_name) {
        return;
    }

    // --- Step 1: Find selective INNER JOIN alias ---
    let target_alias = match find_selective_join_alias(plan, &from_alias) {
        Some(a) => a,
        None => return,
    };

    // --- Step 2: Find path from target to old FROM in join graph ---
    let path = match find_join_path(&plan.joins.0, &target_alias, &from_alias) {
        Some(p) => p,
        None => return,
    };

    // Guard: all joins on the path must be INNER (not LEFT/RIGHT)
    for alias in &path {
        if *alias == from_alias {
            continue; // Old FROM is not a join
        }
        if let Some(j) = plan.joins.0.iter().find(|j| j.table_alias == *alias) {
            if !matches!(j.join_type, JoinType::Inner) {
                return;
            }
            if is_cte_table(&j.table_name) {
                return;
            }
        }
    }

    // --- Step 3: Collect edge conditions along the path ---
    let mut edge_conditions: Vec<Vec<OperatorApplication>> = Vec::new();
    for i in 0..path.len() - 1 {
        let conds = find_connecting_conditions(&plan.joins.0, &path[i], &path[i + 1]);
        if conds.is_empty() {
            return; // No connection found — can't reorder safely
        }
        edge_conditions.push(conds);
    }

    // --- Step 4: Redistribute ON conditions along the path ---
    // Remove path-edge conditions from their current owners
    for i in 0..path.len() - 1 {
        let a = &path[i].clone();
        let b = &path[i + 1].clone();
        for join in plan.joins.0.iter_mut() {
            if join.table_alias == *a || join.table_alias == *b {
                join.joining_on
                    .retain(|c| !condition_connects_aliases(c, a, b));
            }
        }
    }

    // Add new ON conditions: each path node gets the edge connecting to its parent
    for i in 1..path.len() {
        let alias = &path[i];
        let conditions = &edge_conditions[i - 1];

        if *alias == from_alias {
            // Old FROM becomes an INNER JOIN with the path-edge ON condition
            let new_join = Join {
                table_name: from_name.clone(),
                table_alias: from_alias.clone(),
                joining_on: conditions.clone(),
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            };
            plan.joins.0.push(new_join);
        } else if let Some(join) = plan.joins.0.iter_mut().find(|j| j.table_alias == *alias) {
            for cond in conditions {
                join.joining_on.push(cond.clone());
            }
        }
    }

    // --- Step 5: Remove target from JOINs and make it FROM ---
    let target_idx = match plan
        .joins
        .0
        .iter()
        .position(|j| j.table_alias == target_alias)
    {
        Some(idx) => idx,
        None => return,
    };
    let target_join = plan.joins.0.remove(target_idx);

    // Compute FROM name with database prefix (match the old FROM's prefix)
    let new_from_name = if target_join.table_name.contains('.') {
        target_join.table_name.clone()
    } else if let Some(dot_pos) = from_name.find('.') {
        format!("{}.{}", &from_name[..dot_pos], target_join.table_name)
    } else {
        target_join.table_name.clone()
    };

    plan.from = FromTableItem(Some(ViewTableRef {
        source: Arc::new(LogicalPlan::Empty),
        name: new_from_name,
        alias: Some(target_alias.clone()),
        use_final: false,
    }));

    // --- Step 6: Re-sort JOINs by dependency ---
    reorder_joins_by_dependency(plan, &target_alias);

    log::debug!(
        "Selective predicate FROM reorder: {} promoted to FROM (was {})",
        target_alias,
        from_alias
    );
}

/// Find an INNER JOIN alias that has a constant predicate in WHERE.
/// Prefers equality predicates (most selective), then falls back to any
/// comparison/IN predicate with a constant operand.
/// Returns None if no such alias exists or the FROM already has one.
fn find_selective_join_alias(plan: &RenderPlan, from_alias: &str) -> Option<String> {
    let filter = plan.filters.0.as_ref()?;

    // Collect INNER JOIN aliases (excluding CTEs)
    let inner_aliases: HashSet<String> = plan
        .joins
        .0
        .iter()
        .filter(|j| matches!(j.join_type, JoinType::Inner))
        .filter(|j| !is_cte_table(&j.table_name))
        .map(|j| j.table_alias.clone())
        .collect();

    if inner_aliases.is_empty() {
        return None;
    }

    // Decompose AND-connected predicates
    let predicates = decompose_and_predicates(filter);

    // Check if FROM already has a constant predicate (no benefit to reordering)
    if predicates
        .iter()
        .any(|p| get_constant_predicate_alias(p).is_some_and(|a| a == from_alias))
    {
        return None;
    }

    // Pass 1: Prefer equality predicates (most selective)
    for pred in &predicates {
        if let Some(alias) = get_constant_eq_alias(pred) {
            if inner_aliases.contains(&alias) {
                return Some(alias);
            }
        }
    }

    // Pass 2: Fall back to any constant predicate (range, IN, STARTS WITH, etc.)
    for pred in &predicates {
        if let Some(alias) = get_constant_predicate_alias(pred) {
            if inner_aliases.contains(&alias) {
                return Some(alias);
            }
        }
    }

    None
}

/// Decompose AND-connected predicates into a flat list.
fn decompose_and_predicates(expr: &RenderExpr) -> Vec<&RenderExpr> {
    if let RenderExpr::OperatorApplicationExp(op) = expr {
        if op.operator == Operator::And {
            let mut result = Vec::new();
            for operand in &op.operands {
                result.extend(decompose_and_predicates(operand));
            }
            return result;
        }
    }
    vec![expr]
}

/// Extract the table alias from a constant equality predicate.
/// Matches patterns like `alias.col = 'literal'` or `alias.col = $param`.
fn get_constant_eq_alias(expr: &RenderExpr) -> Option<String> {
    if let RenderExpr::OperatorApplicationExp(op) = expr {
        if op.operator == Operator::Equal && op.operands.len() == 2 {
            let (left, right) = (&op.operands[0], &op.operands[1]);

            if let RenderExpr::PropertyAccessExp(prop) = left {
                if is_non_table_reference(right) {
                    return Some(prop.table_alias.0.clone());
                }
            }
            if let RenderExpr::PropertyAccessExp(prop) = right {
                if is_non_table_reference(left) {
                    return Some(prop.table_alias.0.clone());
                }
            }
        }
    }
    None
}

/// Operators that indicate a filtering predicate (not arithmetic or logical connectors).
const FILTER_OPERATORS: &[Operator] = &[
    Operator::Equal,
    Operator::NotEqual,
    Operator::LessThan,
    Operator::GreaterThan,
    Operator::LessThanEqual,
    Operator::GreaterThanEqual,
    Operator::In,
    Operator::NotIn,
    Operator::StartsWith,
    Operator::EndsWith,
    Operator::Contains,
    Operator::RegexMatch,
];

/// Extract the table alias from any constant predicate (equality, range, IN, etc.).
/// Matches patterns like `alias.col < $param`, `alias.col IN [...]`,
/// `alias.col STARTS WITH 'prefix'`, etc.
fn get_constant_predicate_alias(expr: &RenderExpr) -> Option<String> {
    if let RenderExpr::OperatorApplicationExp(op) = expr {
        if FILTER_OPERATORS.contains(&op.operator) && op.operands.len() == 2 {
            let (left, right) = (&op.operands[0], &op.operands[1]);

            if let RenderExpr::PropertyAccessExp(prop) = left {
                if is_non_table_reference(right) {
                    return Some(prop.table_alias.0.clone());
                }
            }
            if let RenderExpr::PropertyAccessExp(prop) = right {
                if is_non_table_reference(left) {
                    return Some(prop.table_alias.0.clone());
                }
            }
        }
    }
    None
}

/// Check if an expression contains no table/alias references at any depth
/// (i.e., is a constant-like value: literal, parameter, or function of constants).
fn is_non_table_reference(expr: &RenderExpr) -> bool {
    let mut aliases = HashSet::new();
    collect_aliases_from_expr(expr, &mut aliases);
    aliases.is_empty()
}

/// BFS to find the shortest path between two aliases in the join dependency graph.
/// Returns the path as a vec of aliases: [start, ..., end].
fn find_join_path(joins: &[Join], start: &str, end: &str) -> Option<Vec<String>> {
    // Build adjacency list from ON conditions
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    for join in joins {
        for cond in &join.joining_on {
            let aliases = get_condition_aliases(cond);
            let alias_vec: Vec<&String> = aliases.iter().collect();
            for i in 0..alias_vec.len() {
                for j in (i + 1)..alias_vec.len() {
                    adj.entry(alias_vec[i].clone())
                        .or_default()
                        .push(alias_vec[j].clone());
                    adj.entry(alias_vec[j].clone())
                        .or_default()
                        .push(alias_vec[i].clone());
                }
            }
        }
    }

    // BFS
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut parent: HashMap<String, String> = HashMap::new();

    visited.insert(start.to_string());
    queue.push_back(start.to_string());

    while let Some(current) = queue.pop_front() {
        if current == end {
            // Reconstruct path
            let mut path = Vec::new();
            let mut node = end.to_string();
            while node != start {
                path.push(node.clone());
                node = parent[&node].clone();
            }
            path.push(start.to_string());
            path.reverse();
            return Some(path);
        }

        if let Some(neighbors) = adj.get(&current) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    parent.insert(neighbor.clone(), current.clone());
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    None
}

/// Extract all table aliases referenced in an ON condition.
fn get_condition_aliases(cond: &OperatorApplication) -> HashSet<String> {
    let mut aliases = HashSet::new();
    for operand in &cond.operands {
        collect_aliases_from_expr(operand, &mut aliases);
    }
    aliases
}

/// Find ON conditions that connect two specific aliases.
fn find_connecting_conditions(joins: &[Join], a: &str, b: &str) -> Vec<OperatorApplication> {
    let mut result = Vec::new();
    for join in joins {
        for cond in &join.joining_on {
            if condition_connects_aliases(cond, a, b) {
                result.push(cond.clone());
            }
        }
    }
    result
}

/// Check if an ON condition references both aliases.
fn condition_connects_aliases(cond: &OperatorApplication, a: &str, b: &str) -> bool {
    let aliases = get_condition_aliases(cond);
    aliases.contains(a) && aliases.contains(b)
}

/// Re-sort JOINs by dependency after FROM reordering.
/// INNER JOINs are sorted first, then LEFT JOINs, each in topological order.
fn reorder_joins_by_dependency(plan: &mut RenderPlan, from_alias: &str) {
    let mut inner_joins: Vec<Join> = Vec::new();
    let mut left_joins: Vec<Join> = Vec::new();

    for join in plan.joins.0.drain(..) {
        match join.join_type {
            JoinType::Left | JoinType::Right => left_joins.push(join),
            _ => inner_joins.push(join),
        }
    }

    // Topological sort for INNER JOINs
    let mut available: HashSet<String> = HashSet::new();
    available.insert(from_alias.to_string());

    let mut sorted_inner = topo_sort_joins(inner_joins, &mut available);
    let sorted_left = topo_sort_joins(left_joins, &mut available);

    sorted_inner.extend(sorted_left);
    plan.joins.0 = sorted_inner;
}

/// Topological sort a list of joins given the set of currently available aliases.
/// Updates `available` as joins are sorted.
fn topo_sort_joins(mut remaining: Vec<Join>, available: &mut HashSet<String>) -> Vec<Join> {
    let mut sorted = Vec::new();

    while !remaining.is_empty() {
        let mut found = false;
        for i in 0..remaining.len() {
            let deps = get_join_dependencies(&remaining[i]);
            if deps.iter().all(|d| available.contains(d)) {
                let join = remaining.remove(i);
                available.insert(join.table_alias.clone());
                sorted.push(join);
                found = true;
                break;
            }
        }
        if !found {
            // Can't resolve dependencies — append remaining as-is
            sorted.extend(remaining);
            break;
        }
    }

    sorted
}

/// Extract table aliases that a join depends on (from ON conditions, excluding self).
fn get_join_dependencies(join: &Join) -> HashSet<String> {
    let mut deps = HashSet::new();
    for cond in &join.joining_on {
        for operand in &cond.operands {
            collect_aliases_from_expr(operand, &mut deps);
        }
    }
    deps.remove(&join.table_alias);
    deps
}
