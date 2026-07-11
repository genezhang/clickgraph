//! GROUP BY clause builder for logical plans
//!
//! This module provides the `GroupByBuilder` trait and its implementation for extracting
//! GROUP BY clauses from logical query plans. The builder handles:
//!
//! - Recursive extraction through the plan tree
//! - Optimization: Using only ID columns instead of all node properties
//! - Table alias expansion and property mapping
//! - Denormalized edge patterns where node properties are in edge table
//! - Wildcard column handling (e.g., `a.*`)
//!
//! ## Architecture
//!
//! The trait-based design allows:
//! - Separation of GROUP BY logic from the main plan builder
//! - Clean delegation pattern for plan traversal
//! - Explicit handling of all LogicalPlan variants
//!
//! ## Key Optimization
//!
//! When a node alias appears in GROUP BY (e.g., `GROUP BY a` or `GROUP BY a.*`),
//! instead of grouping by all node properties (8+ columns), we only group by the
//! ID column. This is sound because all other properties are functionally dependent
//! on the ID. This significantly improves query performance.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::{Direction, LogicalExpr};
use crate::query_planner::logical_plan::{GroupBy, LogicalPlan};
use std::collections::HashSet;

use super::errors::RenderBuildError;
use super::plan_builder::RenderPlanBuilder;
use super::plan_builder_helpers::apply_property_mapping_to_expr;
use super::render_expr::{PropertyAccess, RenderExpr, TableAlias};

/// Result type for GROUP BY builder operations
pub type GroupByBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting GROUP BY clauses from logical plans
///
/// Implemented by `LogicalPlan` to enable recursive GROUP BY extraction
/// through the plan tree with proper delegation to child plans.
pub trait GroupByBuilder {
    /// Extract GROUP BY expressions from this plan node
    ///
    /// Returns a vector of `RenderExpr` representing the GROUP BY clause.
    /// Returns an empty vector if no GROUP BY is found.
    ///
    /// # Behavior by Plan Type
    ///
    /// - **GroupBy**: Processes expressions, applies ID column optimization
    /// - **Pass-through plans** (Limit, Skip, OrderBy, Projection, Filter, etc.):
    ///   Delegates to input plan
    /// - **GraphRel**: Tries left, then center, then right inputs
    /// - **Others**: Returns empty vector (no GROUP BY)
    fn extract_group_by(&self) -> GroupByBuilderResult<Vec<RenderExpr>>;
}

impl GroupByBuilder for LogicalPlan {
    fn extract_group_by(&self) -> GroupByBuilderResult<Vec<RenderExpr>> {
        log::info!(
            "🔧 GROUP BY: extract_group_by() called for plan type {:?}",
            std::mem::discriminant(self)
        );

        let group_by = match &self {
            // Pass-through plans - delegate to input
            LogicalPlan::Limit(limit) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&limit.input)?
            }
            LogicalPlan::Skip(skip) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&skip.input)?
            }
            LogicalPlan::OrderBy(order_by) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&order_by.input)?
            }
            LogicalPlan::Projection(projection) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&projection.input)?
            }
            LogicalPlan::Filter(filter) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&filter.input)?
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&graph_joins.input)?
            }
            LogicalPlan::GraphNode(node) => {
                <LogicalPlan as GroupByBuilder>::extract_group_by(&node.input)?
            }

            // GraphRel - try left, center, right in order
            LogicalPlan::GraphRel(rel) => {
                // For relationships, try left first, then center, then right
                <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.left)
                    .or_else(|_| <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.center))
                    .or_else(|_| <LogicalPlan as GroupByBuilder>::extract_group_by(&rel.right))?
            }

            // GroupBy - main processing logic
            LogicalPlan::GroupBy(group_by) => process_group_by_expressions(group_by)?,

            // All other plans have no GROUP BY
            _ => vec![],
        };

        Ok(group_by)
    }
}

/// Process GROUP BY expressions with optimization and property mapping
///
/// This function handles the core GROUP BY logic:
/// 1. Expands table aliases to their properties
/// 2. Applies ID column optimization for node aliases
/// 3. Handles wildcard columns (e.g., `a.*`)
/// 4. Manages denormalized edge patterns
/// 5. Converts logical expressions to render expressions
fn process_group_by_expressions(group_by: &GroupBy) -> GroupByBuilderResult<Vec<RenderExpr>> {
    log::info!(
        "🔧 GROUP BY: Found GroupBy plan, processing {} expressions",
        group_by.expressions.len()
    );

    let mut result: Vec<RenderExpr> = Vec::new();
    let mut seen_group_by_aliases: HashSet<String> = HashSet::new();

    for expr in &group_by.expressions {
        // Case 1: TableAlias - expand to ID column only (optimization)
        if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) = expr {
            if handle_table_alias_group_by(
                &group_by.input,
                &alias.0,
                &mut result,
                &mut seen_group_by_aliases,
            )? {
                continue; // Successfully handled
            }
        }

        // Case 1b (#484): id()/elementId() scalar call - route through the same
        // pattern_union-aware / schema-driven ID resolution the SELECT path uses
        // (`select_builder.rs` Case 5), instead of falling through to Case 3's
        // generic conversion, which hits the function-registry `toInt64(0)`
        // placeholder mapping. That placeholder is fine in SELECT position
        // (Bolt/result-transformer compute the real id from element_id
        // metadata there) but is fatal in GROUP BY: every row hashes to the
        // same constant key and all groups silently collapse into one.
        if let Some(resolved) = resolve_id_function_for_group_order(&group_by.input, expr) {
            result.push(resolved);
            continue;
        }

        // Case 2: PropertyAccessExp with wildcard "*" - expand to ID column only
        if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(prop_access) =
            expr
        {
            if prop_access.column.raw() == "*"
                && handle_wildcard_group_by(
                    &group_by.input,
                    prop_access,
                    &mut result,
                    &mut seen_group_by_aliases,
                )?
            {
                continue; // Successfully handled
            }
        }

        // Case 3: Regular expression - convert and apply property mapping
        let mut render_expr: RenderExpr = expr.clone().try_into()?;
        apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
        result.push(render_expr);
    }

    Ok(result)
}

/// Resolve the full set of node-identity columns to use as GROUP BY keys for a
/// node alias, via the task-local graph schema (NOT raw ViewScan flags).
///
/// For a **composite-id** node (e.g. Account keyed by `(bank_id, account_number)`)
/// the single-column `ViewScan.id_column` path only emits the FIRST id column,
/// silently collapsing distinct nodes that share that first component (issue #457).
/// This resolves the node's label from the plan and asks the schema for the
/// complete `node_id.columns()` set so every identity column becomes a GROUP BY key.
///
/// Returns `Some(cols)` only for composite ids (the case that needs expansion) —
/// gated purely on `node_id.is_composite()`, a schema-catalog property, so no raw
/// pattern-axis flag is branched on. Returns `None` for single-column ids,
/// denormalized/virtual nodes (always single-column here), VLP endpoints,
/// CTE-backed aliases, or when the schema/label cannot be resolved — leaving the
/// established single-column `find_id_column_for_alias` path untouched.
///
/// The returned columns mirror how `ViewScan.id_column` is derived for
/// non-denormalized nodes (`node_id.columns()` used directly as DB column names),
/// so the first element is always identical to the previous single-column key.
///
/// NOTE — deliberate multiplication of the consumer (refactor plan §1.4 disease):
/// the whole-node GROUP BY id-optimization exists in FOUR places today —
/// `handle_table_alias_group_by` / `handle_wildcard_group_by` here, plus their
/// near-verbatim duplicates in `plan_builder_utils.rs`: `extract_group_by`'s
/// GroupBy arm AND `expand_table_alias_to_group_by_id_only` (the WITH→CTE
/// render path — the copy that actually fires for `WITH a, count(..)` shapes).
/// ALL of them must call this helper so composite ids behave identically on
/// both sides of a WITH barrier; the eventual Phase-2 dedup should collapse
/// the `plan_builder_utils.rs` copies onto this module.
/// Resolve `id(alias)` / `elementId(alias)` scalar-function calls in GROUP BY /
/// ORDER BY position to the underlying ID column expression (#484).
///
/// Without this, GROUP BY/ORDER BY over `id()`/`elementId()` fall through to the
/// generic function-registry placeholder mapping (`id` -> `toInt64(0)`, see
/// `sql_generator/emitters/clickhouse/function_registry.rs`), which is a
/// harmless placeholder in SELECT position (the Bolt layer / result
/// transformer compute the real id from element_id metadata there) but is a
/// SILENT WRONG RESULTS bug in GROUP BY (every row hashes to the same
/// constant key, so all groups collapse into one) and a no-op in ORDER BY.
///
/// Mirrors the pattern_union-aware resolution `select_builder.rs` Case 5
/// already applies to SELECT items:
/// 1. Pattern-union endpoint (#466/#468 deferred-UNION `pattern_combinations`):
///    use the CTE's label-agnostic `start_id`/`end_id` columns via
///    `pattern_union_endpoint_role` — a single label's id column is NULL on
///    every other label's branch, which would silently drop/miscount rows.
/// 2. Plain path: resolve the id column the same way the established
///    `handle_table_alias_group_by` GROUP BY optimization does —
///    `find_id_column_for_alias` (walks the plan to the ViewScan's
///    schema-derived `id_column`) plus `get_properties_with_table_alias` for
///    denormalized edge-embedded node resolution.
///
/// Returns `None` when `expr` isn't an `id()`/`elementId()` call over a bare
/// alias (or `alias.*`), or when resolution fails — callers should fall back
/// to their existing generic conversion path unchanged.
pub(super) fn resolve_id_function_for_group_order(
    input: &LogicalPlan,
    expr: &LogicalExpr,
) -> Option<RenderExpr> {
    let LogicalExpr::ScalarFnCall(fn_call) = expr else {
        return None;
    };
    if !(fn_call.name.eq_ignore_ascii_case("id") || fn_call.name.eq_ignore_ascii_case("elementid"))
        || fn_call.args.len() != 1
    {
        return None;
    }
    let is_element_id = fn_call.name.eq_ignore_ascii_case("elementid");
    let alias = match &fn_call.args[0] {
        LogicalExpr::TableAlias(a) => a.0.clone(),
        LogicalExpr::PropertyAccessExp(p) if p.column.raw() == "*" => p.table_alias.0.clone(),
        _ => return None,
    };

    // Pattern-union endpoint: use the CTE's label-agnostic start_id/end_id,
    // exactly as select_builder.rs Case 5 does for SELECT items.
    if let Some((rel_alias, is_left)) = input.pattern_union_endpoint_role(&alias) {
        let (id_col, type_col) = if is_left {
            ("start_id", "start_type")
        } else {
            ("end_id", "end_type")
        };
        return Some(if is_element_id {
            RenderExpr::Raw(format!(
                "concat({rel_alias}.{type_col}, ':', {rel_alias}.{id_col}, '-')"
            ))
        } else {
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias),
                column: PropertyValue::Column(id_col.to_string()),
            })
        });
    }

    if is_element_id {
        // Non-endpoint elementId(): no established GROUP BY/ORDER BY
        // resolution pre-existed here either (fell into the same placeholder
        // as id()). Keep scope narrow to the pattern_union fix and leave
        // plain elementId() untouched, matching select_builder's Case 5
        // scoping (see its "Non-endpoint elementId()" comment).
        return None;
    }

    // #484 review follow-up (BLOCKING finding): a multi-label alias that
    // renders via a RAW per-label `UNION ALL` (one full branch per candidate
    // label, collapsed under an outer `__union`/top-level `Union` alias —
    // whether reached bare, `MATCH (n) RETURN id(n), count(*)`, or through a
    // DIRECTED GraphRel chain, `MATCH (a)-[:REL]->(item) RETURN id(item),
    // count(*)`) must NOT go through the "plain path" below.
    // `find_id_column_for_alias` always returns the FIRST candidate label's
    // id column (e.g. `post_id`/`fs_id`), which is only valid for a
    // single-label node or a GraphRel endpoint that renders through a real
    // addressable CTE/table alias in the CURRENT scope (a
    // `multi_type_vlp_joins`/`bidirectional_union` CTE or a `pattern_union_*`
    // CTE) — for a raw per-label union, `GROUP BY alias.post_id` in the OUTER
    // query references a table alias that only exists INSIDE the union
    // branches, not in the outer scope (ClickHouse Code 47
    // UNKNOWN_IDENTIFIER).
    //
    // The distinguishing signal is NOT "has a GraphRel connection at all" —
    // an earlier version of this guard used exactly that (`no GraphRel
    // connection at all` == unsafe) and missed the directed-chain case above,
    // because a directed GraphRel connection is just as much a raw union as
    // no connection at all; only an UNDIRECTED (`Either`/`was_undirected`)
    // GraphRel endpoint collapses into the single-alias
    // `multi_type_vlp_joins`/`bidirectional_union` CTE. Reuse #483's
    // `graph_rel_connection_role` discriminator (already used by
    // `projection_tagging.rs`'s `count(DISTINCT alias)` rewrite for this
    // exact axis) instead of re-deriving it: `Some(_)` means the safe,
    // single-alias-CTE shape; `None` means either no connection or a
    // DIRECTED connection, both of which need the raw-union check below.
    // (The `pattern_union_*` CTE shape is already excluded earlier via the
    // `pattern_union_endpoint_role` check above, before this function runs.)
    //
    // A correct fix needs the multi-label discriminator (`tuple`/`coalesce`
    // over each label's id column, à la #467's `count(DISTINCT n)`) to be
    // introduced early enough that `PropertyRequirementsAnalyzer` projects
    // the per-label columns into each UNION branch under the
    // `alias.<col>`-quoted-alias convention #467's aggregate rewrite relies
    // on — that convention is wired specifically into the aggregate-wrapper
    // render path, not a general mechanism a bare GROUP BY/ORDER BY key can
    // reuse without materially more plumbing than fits this fix. Until that
    // follow-up lands, leave this shape on the PRE-existing
    // (wrong-but-non-fatal) `toInt64(0)` placeholder rather than regressing
    // it into a hard runtime failure: bail out here so the fallthrough
    // generic-conversion path (Case 3 in both call sites) takes over, exactly
    // as it did before this whole `id()` resolution existed.
    if renders_via_raw_label_union(input, &alias) {
        return None;
    }

    // Plain path: same resolution as `handle_table_alias_group_by`.
    let id_col = input.find_id_column_for_alias(&alias).ok()?;
    let table_alias_to_use = match input.get_properties_with_table_alias(&alias) {
        Ok((props, Some(actual_alias))) if !props.is_empty() => actual_alias,
        _ => alias.clone(),
    };
    Some(RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(table_alias_to_use),
        column: PropertyValue::Column(id_col),
    }))
}

/// Does `alias` render via a RAW per-label `UNION ALL` (multiple physical
/// per-label branches collapsed under one outer `__union`/top-level `Union`
/// alias) rather than through a single addressable CTE/table alias in the
/// current SQL scope?
///
/// Two shapes reach this function:
/// - A bare multi-label `MATCH (n)` with NO GraphRel connecting it to
///   anything (`generate_union_for_untyped_nodes` in `type_inference.rs`
///   clones a whole `GraphNode` subtree per candidate label).
/// - A multi-label alias reached through a DIRECTED GraphRel chain (e.g.
///   `(folder)-[:CONTAINS]->(item)` with `item` unlabeled): the same
///   per-label cloning happens, but this time it clones the whole
///   `GraphRel` subtree per label (#467's target shape), so `alias` DOES
///   appear as a GraphRel connection — just not the safe kind (see below).
///
/// Excluded (returns `false`, i.e. safe to use the "plain path"):
/// - Single-label nodes, and denormalized/polymorphic nodes whose per-label
///   `Union` is nested INSIDE a single `GraphNode.input` (one address-able
///   alias in the outer scope) rather than wrapping the whole subtree.
/// - An UNDIRECTED (`Either`/`was_undirected`) GraphRel endpoint: per #483's
///   `graph_rel_connection_role`, that shape collapses into a single
///   `multi_type_vlp_joins`/`bidirectional_union` CTE exposing one
///   `start_id`/`end_id` regardless of label count — a real addressable
///   alias, not a raw union. This is the exact discriminator
///   `projection_tagging.rs`'s `count(DISTINCT alias)` rewrite already uses
///   to tell the two directed-vs-undirected multi-label-endpoint shapes
///   apart (see its #483 doc comment) — reused here rather than
///   re-derived, since a directed GraphRel connection is just as much a raw
///   union as no connection at all.
fn renders_via_raw_label_union(plan: &LogicalPlan, alias: &str) -> bool {
    // Short-circuit: an UNDIRECTED GraphRel endpoint is the one GraphRel
    // shape that renders through a real single-alias CTE, not a raw union.
    if plan.graph_rel_connection_role(alias).is_some() {
        return false;
    }
    match plan {
        // A bare multi-label `MATCH (n)` surfaces as a TOP-LEVEL
        // `LogicalPlan::Union` wrapping full per-label branches — one
        // GraphNode per candidate label, all sharing `alias`
        // (`generate_union_for_untyped_nodes` in type_inference.rs clones
        // the whole subtree per label combination) — NOT a Union nested
        // inside a single GraphNode.input (that nested shape is the
        // denormalized/polymorphic pattern, a different, already-safe case
        // `find_id_column_for_alias` handles directly). Detect it by
        // counting how many branches resolve `alias` to a GraphNode.
        LogicalPlan::Union(u) => {
            let matching_branches = u
                .inputs
                .iter()
                .filter(|i| plan_contains_graphnode_alias(i, alias))
                .count();
            matching_branches > 1
                || u.inputs
                    .iter()
                    .any(|i| renders_via_raw_label_union(i, alias))
        }
        LogicalPlan::GraphNode(node) => renders_via_raw_label_union(&node.input, alias),
        LogicalPlan::GraphRel(rel) => {
            renders_via_raw_label_union(&rel.left, alias)
                || renders_via_raw_label_union(&rel.center, alias)
                || renders_via_raw_label_union(&rel.right, alias)
        }
        LogicalPlan::Filter(f) => renders_via_raw_label_union(&f.input, alias),
        LogicalPlan::Projection(p) => renders_via_raw_label_union(&p.input, alias),
        LogicalPlan::GraphJoins(gj) => renders_via_raw_label_union(&gj.input, alias),
        LogicalPlan::GroupBy(gb) => renders_via_raw_label_union(&gb.input, alias),
        LogicalPlan::OrderBy(ob) => renders_via_raw_label_union(&ob.input, alias),
        LogicalPlan::Skip(s) => renders_via_raw_label_union(&s.input, alias),
        LogicalPlan::Limit(l) => renders_via_raw_label_union(&l.input, alias),
        LogicalPlan::Cte(cte) => renders_via_raw_label_union(&cte.input, alias),
        LogicalPlan::WithClause(wc) => renders_via_raw_label_union(&wc.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            renders_via_raw_label_union(&cp.left, alias)
                || renders_via_raw_label_union(&cp.right, alias)
        }
        _ => false,
    }
}

/// Does this subtree resolve `alias` to a `GraphNode` at all (used to count
/// how many Union branches represent the same aliased node under a
/// different label)?
fn plan_contains_graphnode_alias(plan: &LogicalPlan, alias: &str) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => true,
        LogicalPlan::GraphNode(node) => plan_contains_graphnode_alias(&node.input, alias),
        LogicalPlan::GraphRel(rel) => {
            plan_contains_graphnode_alias(&rel.left, alias)
                || plan_contains_graphnode_alias(&rel.center, alias)
                || plan_contains_graphnode_alias(&rel.right, alias)
        }
        LogicalPlan::Filter(f) => plan_contains_graphnode_alias(&f.input, alias),
        LogicalPlan::Projection(p) => plan_contains_graphnode_alias(&p.input, alias),
        LogicalPlan::GraphJoins(gj) => plan_contains_graphnode_alias(&gj.input, alias),
        LogicalPlan::GroupBy(gb) => plan_contains_graphnode_alias(&gb.input, alias),
        LogicalPlan::OrderBy(ob) => plan_contains_graphnode_alias(&ob.input, alias),
        LogicalPlan::Skip(s) => plan_contains_graphnode_alias(&s.input, alias),
        LogicalPlan::Limit(l) => plan_contains_graphnode_alias(&l.input, alias),
        LogicalPlan::Cte(cte) => plan_contains_graphnode_alias(&cte.input, alias),
        LogicalPlan::WithClause(wc) => plan_contains_graphnode_alias(&wc.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            plan_contains_graphnode_alias(&cp.left, alias)
                || plan_contains_graphnode_alias(&cp.right, alias)
        }
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|i| plan_contains_graphnode_alias(i, alias)),
        _ => false,
    }
}

pub(super) fn composite_id_group_by_columns(
    input: &LogicalPlan,
    alias: &str,
) -> Option<Vec<String>> {
    let schema = crate::server::query_context::get_current_schema_with_fallback()?;
    let label = super::cte_extraction::get_node_label_for_alias(alias, input)?;
    let node_schema = schema.node_schema(&label).ok()?;

    // Only composite ids need multi-column expansion. Gated purely on the
    // schema-catalog `is_composite()` property — no raw pattern-axis flag. In
    // this engine denormalized/virtual nodes are always single-column, so
    // `is_composite()` already excludes them and they keep their existing
    // DB-column resolution via the single-column `find_id_column_for_alias` path.
    if !node_schema.node_id.is_composite() {
        return None;
    }

    Some(
        node_schema
            .node_id
            .columns()
            .iter()
            .map(|c| c.to_string())
            .collect(),
    )
}

/// Push one GROUP BY key per composite-id column for `table_alias`.
/// The caller dedups at the alias level (via `seen_aliases`) before calling this.
/// Shared by all three whole-node GROUP BY sites (see `composite_id_group_by_columns`).
pub(super) fn push_composite_id_group_by(
    result: &mut Vec<RenderExpr>,
    table_alias: &str,
    id_columns: &[String],
) {
    for id_col in id_columns {
        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias.to_string()),
            column: PropertyValue::Column(id_col.clone()),
        }));
    }
}

/// Handle GROUP BY for table alias expressions (e.g., `GROUP BY a`)
///
/// Applies the ID column optimization: instead of grouping by all node properties,
/// we only group by the ID column(s) since all other properties are functionally
/// dependent on them. For composite-id nodes, ALL identity columns are emitted
/// (issue #457) so distinct nodes are never collapsed.
///
/// Returns `true` if the alias was successfully handled, `false` otherwise.
fn handle_table_alias_group_by(
    input: &LogicalPlan,
    alias: &str,
    result: &mut Vec<RenderExpr>,
    seen_aliases: &mut HashSet<String>,
) -> GroupByBuilderResult<bool> {
    // Get properties for this alias
    let (properties, actual_table_alias) = match input.get_properties_with_table_alias(alias) {
        Ok(info) => info,
        Err(_) => return Ok(false), // Cannot resolve - let caller handle
    };

    if properties.is_empty() {
        return Ok(false); // No properties - not a node alias
    }

    let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());

    // Skip if we've already added this alias (avoid duplicates)
    if seen_aliases.contains(&table_alias_to_use) {
        return Ok(true); // Already handled
    }
    seen_aliases.insert(table_alias_to_use.clone());

    // Composite-id nodes: emit EVERY identity column as a GROUP BY key so distinct
    // nodes sharing a first id component are not silently merged (issue #457).
    if let Some(id_columns) = composite_id_group_by_columns(input, alias) {
        log::debug!(
            "🔧 GROUP BY optimization: Using {} composite ID columns {:?} for alias '{}'",
            id_columns.len(),
            id_columns,
            table_alias_to_use
        );
        push_composite_id_group_by(result, &table_alias_to_use, &id_columns);
        return Ok(true);
    }

    // Single-column id: get the ID column from the schema (via ViewScan.id_column)
    let id_col = input.find_id_column_for_alias(alias).unwrap_or_else(|_| {
        log::warn!(
            "⚠️ Could not find ID column for alias '{}', using fallback",
            alias
        );
        "id".to_string()
    });

    log::debug!(
        "🔧 GROUP BY optimization: Using ID column '{}' from schema instead of {} properties for alias '{}'",
        id_col,
        properties.len(),
        table_alias_to_use
    );

    result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(table_alias_to_use.clone()),
        column: PropertyValue::Column(id_col),
    }));

    Ok(true)
}

/// Handle GROUP BY for wildcard property access (e.g., `GROUP BY a.*`)
///
/// Applies the same ID column optimization as table aliases, but also handles
/// denormalized edge patterns where node properties are stored in the edge table.
///
/// Returns `true` if successfully handled, `false` otherwise.
fn handle_wildcard_group_by(
    input: &LogicalPlan,
    prop_access: &crate::query_planner::logical_expr::PropertyAccess,
    result: &mut Vec<RenderExpr>,
    seen_aliases: &mut HashSet<String>,
) -> GroupByBuilderResult<bool> {
    // Get properties for this alias
    let (properties, actual_table_alias) =
        match input.get_properties_with_table_alias(&prop_access.table_alias.0) {
            Ok(info) => info,
            Err(_) => return Ok(false), // Cannot resolve - let caller handle
        };

    let table_alias_to_use =
        actual_table_alias.unwrap_or_else(|| prop_access.table_alias.0.clone());

    // Skip if we've already added this alias (avoid duplicates)
    if seen_aliases.contains(&table_alias_to_use) {
        return Ok(true); // Already handled
    }
    seen_aliases.insert(table_alias_to_use.clone());

    // Case A: Denormalized edge pattern - find node properties in relationship
    if let Some((_, table_alias)) =
        find_node_properties_for_rel_alias(input, &prop_access.table_alias.0)
    {
        // Found denormalized node properties - get ID from schema (MUST succeed)
        let id_col = input
            .find_id_column_for_alias(&prop_access.table_alias.0)
            .map_err(|e| {
                RenderBuildError::InvalidRenderPlan(format!(
                    "Cannot find ID column for denormalized alias '{}': {}",
                    prop_access.table_alias.0, e
                ))
            })?;

        log::debug!(
            "🔧 GROUP BY optimization: Using ID column '{}' from schema for denormalized alias '{}'",
            id_col,
            table_alias
        );

        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias.clone()),
            column: PropertyValue::Column(id_col),
        }));

        return Ok(true);
    }

    // Case B: Regular node alias - use ID column(s)
    if !properties.is_empty() {
        // Composite-id nodes: emit EVERY identity column (issue #457).
        if let Some(id_columns) = composite_id_group_by_columns(input, &prop_access.table_alias.0) {
            log::debug!(
                "🔧 GROUP BY optimization: Using {} composite ID columns {:?} for alias '{}'",
                id_columns.len(),
                id_columns,
                table_alias_to_use
            );
            push_composite_id_group_by(result, &table_alias_to_use, &id_columns);
            return Ok(true);
        }

        let id_col = input
            .find_id_column_for_alias(&prop_access.table_alias.0)
            .map_err(|e| {
                RenderBuildError::InvalidRenderPlan(format!(
                    "Cannot find ID column for alias '{}': {}",
                    prop_access.table_alias.0, e
                ))
            })?;

        log::debug!(
            "🔧 GROUP BY optimization: Using ID column '{}' instead of {} properties for alias '{}'",
            id_col,
            properties.len(),
            table_alias_to_use
        );

        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias_to_use.clone()),
            column: PropertyValue::Column(id_col),
        }));

        return Ok(true);
    }

    Ok(false) // Could not handle - let caller handle
}

/// Find node properties when the alias is a relationship alias with "*" column
///
/// For denormalized schemas, the node alias gets remapped to the relationship alias,
/// so we need to look up which node this represents and get its properties.
///
/// Returns `Some((properties, table_alias))` if found, `None` otherwise.
fn find_node_properties_for_rel_alias(
    plan: &LogicalPlan,
    rel_alias: &str,
) -> Option<(Vec<(String, String)>, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.alias == rel_alias => {
            // This relationship matches - get the left node's properties (most common case)
            // Left node is typically the one being grouped in WITH clause
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // Check direction to determine which properties to use
                let is_incoming = rel.direction == Direction::Incoming;
                let props = if is_incoming {
                    &scan.to_node_properties
                } else {
                    &scan.from_node_properties
                };

                if let Some(node_props) = props {
                    let properties: Vec<(String, String)> = node_props
                        .iter()
                        .map(|(prop_name, prop_value)| {
                            (prop_name.clone(), prop_value.raw().to_string())
                        })
                        .collect();
                    if !properties.is_empty() {
                        // Return properties and the actual table alias to use
                        return Some((properties, rel.alias.clone()));
                    }
                }
            }
            None
        }
        LogicalPlan::GraphRel(rel) => {
            // Not this relationship - search children recursively
            if let Some(result) = find_node_properties_for_rel_alias(&rel.left, rel_alias) {
                return Some(result);
            }
            if let Some(result) = find_node_properties_for_rel_alias(&rel.center, rel_alias) {
                return Some(result);
            }
            find_node_properties_for_rel_alias(&rel.right, rel_alias)
        }
        // Pass-through plans - search input
        LogicalPlan::Projection(proj) => find_node_properties_for_rel_alias(&proj.input, rel_alias),
        LogicalPlan::Filter(filter) => find_node_properties_for_rel_alias(&filter.input, rel_alias),
        LogicalPlan::GroupBy(gb) => find_node_properties_for_rel_alias(&gb.input, rel_alias),
        LogicalPlan::GraphJoins(joins) => {
            find_node_properties_for_rel_alias(&joins.input, rel_alias)
        }
        LogicalPlan::OrderBy(order) => find_node_properties_for_rel_alias(&order.input, rel_alias),
        LogicalPlan::Skip(skip) => find_node_properties_for_rel_alias(&skip.input, rel_alias),
        LogicalPlan::Limit(limit) => find_node_properties_for_rel_alias(&limit.input, rel_alias),
        _ => None,
    }
}
