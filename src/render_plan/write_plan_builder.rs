//! Write-render plan builder: `LogicalPlan::Create / SetProperties / Delete /
//! Remove` → `WriteRenderPlan`.
//!
//! ## Scope (v1, Phase 2)
//!
//! - `CREATE (a:Label {props...})` — single-node INSERT with literal property
//!   values.
//! - `SET a.prop = expr` — UPDATE on the target alias's table.
//! - `REMOVE a.prop` — UPDATE setting `prop = NULL`.
//! - `DELETE a` / `DETACH DELETE a` — DELETE on the target alias's node table,
//!   with rel-table cleanups in front for `DETACH DELETE`.
//!
//! ## Deferred for follow-up
//!
//! - `CREATE (a)-[:R]->(b)` between aliases bound by a preceding `MATCH`.
//!   Requires the executor (Phase 3) to first read endpoint IDs and then issue
//!   the rel INSERT — Phase 2 errors out cleanly with a clear message.
//! - SET / DELETE / REMOVE inside chained WITH clauses. Plan / executor
//!   coordination needed for cross-CTE alias resolution.
//!
//! All rejected forms produce a typed error so the caller can surface a
//! clear message.

use std::sync::Arc;

use thiserror::Error;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema};
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{
    Create, CreatePattern, CreateRel, Delete, Filter, GraphJoins, LogicalPlan, Remove, SetItem,
    SetProperties, WriteProperty,
};

use super::errors::RenderBuildError;
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{ColumnAlias, RenderExpr, TableAlias};
use super::write_render::{DeleteOp, InsertOp, RowSource, UpdateOp, WriteRenderPlan};
use super::{RenderPlan, SelectItem, SelectItems};

#[derive(Debug, Error, Clone, PartialEq)]
pub enum WriteRenderError {
    #[error("{0}")]
    Build(String),

    #[error(transparent)]
    Render(#[from] RenderBuildError),
}

impl From<String> for WriteRenderError {
    fn from(s: String) -> Self {
        WriteRenderError::Build(s)
    }
}

/// Top-level entry point: take a `LogicalPlan` and produce a
/// `WriteRenderPlan` if the root is a write variant.
///
/// Returns `Ok(None)` for read-only plans so the caller can fall through to
/// the regular read render path.
pub fn build_write_plan(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> Result<Option<WriteRenderPlan>, WriteRenderError> {
    match plan {
        LogicalPlan::Create(c) => Ok(Some(build_create(c, schema)?)),
        LogicalPlan::SetProperties(sp) => Ok(Some(build_set(sp, schema)?)),
        LogicalPlan::Delete(d) => Ok(Some(build_delete(d, schema)?)),
        LogicalPlan::Remove(r) => Ok(Some(build_remove(r, schema)?)),
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

fn build_create(
    create: &Create,
    schema: &GraphSchema,
) -> Result<WriteRenderPlan, WriteRenderError> {
    let mut ops: Vec<WriteRenderPlan> = Vec::new();
    for pattern in &create.patterns {
        match pattern {
            CreatePattern::Node(node) => {
                let node_schema = schema.node_schema_opt(&node.label).ok_or_else(|| {
                    WriteRenderError::Build(format!("CREATE: unknown node label `{}`", node.label))
                })?;
                ops.push(WriteRenderPlan::Insert(build_node_insert(
                    node.label.as_str(),
                    &node.properties,
                    node_schema,
                )?));
            }
            CreatePattern::Rel(rel) => {
                // CREATE rel between aliases requires the executor to resolve
                // endpoint IDs at runtime — Phase 3 work. Phase 2 emits a
                // clear, actionable error.
                return Err(WriteRenderError::Build(format!(
                    "CREATE relationship `{}`: writing edges between bound \
                     aliases is not supported in this build of ClickGraph yet. \
                     Use the embedded `Connection::create_edge(...)` API for now \
                     (or wait for Phase 3 of the embedded-writes plan).",
                    rel_descriptor(rel)
                )));
            }
        }
    }
    Ok(unwrap_singleton(ops))
}

fn build_node_insert(
    label: &str,
    properties: &[WriteProperty],
    node_schema: &NodeSchema,
) -> Result<InsertOp, WriteRenderError> {
    let mut columns: Vec<String> = Vec::with_capacity(properties.len());
    let mut row: Vec<RenderExpr> = Vec::with_capacity(properties.len());

    for prop in properties {
        let column = resolve_node_property_column(node_schema, &prop.key).ok_or_else(|| {
            WriteRenderError::Build(format!(
                "CREATE node `{}`: property `{}` cannot be mapped to a writable column",
                label, prop.key
            ))
        })?;
        let value = render_value(&prop.value)?;
        columns.push(column);
        row.push(value);
    }

    Ok(InsertOp {
        database: node_schema.database.clone(),
        table: node_schema.table_name.clone(),
        columns,
        rows: vec![row],
    })
}

fn resolve_node_property_column(node_schema: &NodeSchema, key: &str) -> Option<String> {
    if let Some(prop_value) = node_schema.property_mappings.get(key) {
        match prop_value {
            PropertyValue::Column(c) => return Some(c.clone()),
            // Expression-mapped properties are not writable; skip silently
            // and let the caller decide whether that's an error.
            PropertyValue::Expression(_) => return None,
        }
    }
    if node_schema.column_names.iter().any(|c| c == key) {
        return Some(key.to_string());
    }
    None
}

fn rel_descriptor(rel: &CreateRel) -> String {
    format!(
        "({})-[:{}]->({})",
        rel.start_alias, rel.rel_type, rel.end_alias
    )
}

// ---------------------------------------------------------------------------
// SET / REMOVE
// ---------------------------------------------------------------------------

fn build_set(
    sp: &SetProperties,
    schema: &GraphSchema,
) -> Result<WriteRenderPlan, WriteRenderError> {
    // Group SET items by target alias so each alias produces one UpdateOp
    // (or, for Phase 5e multi-label aliases, one UpdateOp per resolved
    // label).
    let grouped = group_assignments_by_alias(&sp.items);
    let mut ops: Vec<WriteRenderPlan> = Vec::with_capacity(grouped.len());

    for (alias, items) in grouped {
        let labels = find_all_alias_labels(&alias, &sp.input);
        if labels.is_empty() {
            return Err(WriteRenderError::Build(format!(
                "SET: alias `{}` is not bound by a preceding MATCH (or its label cannot be \
                 resolved at this stage)",
                alias
            )));
        }

        // Phase 5e: when an untyped MATCH expands to a Union over multiple
        // node tables, emit one UPDATE per label table, sourcing each
        // UPDATE from a label-scoped slice of the read pipeline. Single-
        // label aliases keep the original input untouched.
        for label in &labels {
            let scoped_input = if labels.len() > 1 {
                slice_plan_to_label(&alias, label, &sp.input).ok_or_else(|| {
                    WriteRenderError::Build(format!(
                        "SET: untyped target `{}` resolves to multiple labels {:?} but \
                         the read pipeline could not be sliced for label `{}`.",
                        alias, labels, label
                    ))
                })?
            } else {
                sp.input.clone()
            };
            ops.push(build_set_for_label(
                &alias,
                label,
                &items,
                &scoped_input,
                schema,
            )?);
        }
    }

    Ok(unwrap_singleton(ops))
}

/// Build the single UPDATE for an (alias, label) pair against `input`,
/// which must bind `alias` to exactly `label`.
fn build_set_for_label(
    alias: &str,
    label: &str,
    items: &[SetItem],
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<WriteRenderPlan, WriteRenderError> {
    let node_schema = schema
        .node_schema_opt(label)
        .ok_or_else(|| WriteRenderError::Build(format!("SET: unknown node label `{}`", label)))?;

    // Per Cypher semantics, repeated assignments to the same property
    // are last-wins. ClickHouse rejects duplicate column assignments in
    // an UPDATE list, so collapse before emitting.
    let mut assignments: Vec<(String, RenderExpr)> = Vec::with_capacity(items.len());
    let mut col_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for item in items {
        let column =
            resolve_node_property_column(node_schema, &item.property).ok_or_else(|| {
                WriteRenderError::Build(format!(
                    "SET: property `{}.{}` cannot be mapped to a writable column",
                    alias, item.property
                ))
            })?;
        let value = render_value(&item.value)?;
        if let Some(&i) = col_index.get(&column) {
            assignments[i].1 = value;
        } else {
            col_index.insert(column.clone(), assignments.len());
            assignments.push((column, value));
        }
    }

    let id_column = node_id_column_or_error(node_schema, "SET", alias)?;
    let source = build_id_source(alias, &id_column, input, schema)?;

    Ok(WriteRenderPlan::Update(UpdateOp {
        database: node_schema.database.clone(),
        table: node_schema.table_name.clone(),
        assignments,
        id_column,
        source,
    }))
}

fn build_remove(rem: &Remove, schema: &GraphSchema) -> Result<WriteRenderPlan, WriteRenderError> {
    // REMOVE → SET property = NULL.
    let null_items: Vec<SetItem> = rem
        .items
        .iter()
        .map(|item| SetItem {
            target_alias: item.target_alias.clone(),
            property: item.property.clone(),
            value: LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::Null),
        })
        .collect();
    let synthetic = SetProperties {
        input: rem.input.clone(),
        items: null_items,
    };
    build_set(&synthetic, schema)
}

fn group_assignments_by_alias(items: &[SetItem]) -> Vec<(String, Vec<SetItem>)> {
    let mut order: Vec<String> = Vec::new();
    let mut map: std::collections::HashMap<String, Vec<SetItem>> = std::collections::HashMap::new();
    for item in items {
        if !map.contains_key(&item.target_alias) {
            order.push(item.target_alias.clone());
        }
        map.entry(item.target_alias.clone())
            .or_default()
            .push(item.clone());
    }
    order
        .into_iter()
        .map(|alias| {
            let v = map.remove(&alias).unwrap();
            (alias, v)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// DELETE / DETACH DELETE
// ---------------------------------------------------------------------------

fn build_delete(del: &Delete, schema: &GraphSchema) -> Result<WriteRenderPlan, WriteRenderError> {
    let mut ops: Vec<WriteRenderPlan> = Vec::new();

    for alias in &del.targets {
        // First try as a node alias.
        let labels = find_all_alias_labels(alias, &del.input);
        if !labels.is_empty() {
            // Phase 5e: untyped MATCH (n) expands to a Union over every
            // node table, binding `n` to multiple labels at once. Fan out
            // to one DELETE per label, slicing the read pipeline so each
            // per-table source query reads only that label's branch.
            for label in &labels {
                let scoped_input = if labels.len() > 1 {
                    slice_plan_to_label(alias, label, &del.input).ok_or_else(|| {
                        WriteRenderError::Build(format!(
                            "DELETE: untyped target `{}` resolves to multiple labels {:?} \
                             but the read pipeline could not be sliced for label `{}`. \
                             This is typically a planner shape we don't yet handle in the \
                             multi-label fan-out — file a bug with the offending Cypher.",
                            alias, labels, label
                        ))
                    })?
                } else {
                    del.input.clone()
                };
                ops.extend(build_delete_for_label(
                    alias,
                    label,
                    del.detach,
                    &scoped_input,
                    schema,
                )?);
            }
            continue;
        }

        // Relationship-alias DELETE is deferred. Keying the DELETE by
        // `from_id` alone would over-delete (multiple edges share the same
        // source node), and an `(from_id, to_id)` tuple-IN form requires
        // render-plan support that lands in a follow-up. Reject cleanly.
        if find_alias_rel_type(alias, &del.input).is_some() {
            return Err(WriteRenderError::Build(format!(
                "DELETE on relationship alias `{}` is not supported in this build of \
                 ClickGraph yet. Use `Connection::delete_edges(...)` for now (or wait \
                 for the tuple-IN form to land in a follow-up).",
                alias
            )));
        }

        return Err(WriteRenderError::Build(format!(
            "DELETE target `{}` is not bound by a preceding MATCH",
            alias
        )));
    }

    if ops.is_empty() {
        return Err(WriteRenderError::Build(
            "DELETE produced no operations".to_string(),
        ));
    }
    Ok(unwrap_singleton(ops))
}

/// Emit the DELETE ops for a single (alias, label) pair against `input`,
/// which must bind `alias` to exactly `label`. Used by the multi-label
/// fan-out and by the existing single-label path with the same code.
fn build_delete_for_label(
    alias: &str,
    label: &str,
    detach: bool,
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Vec<WriteRenderPlan>, WriteRenderError> {
    let mut ops: Vec<WriteRenderPlan> = Vec::new();

    let node_schema = schema.node_schema_opt(label).ok_or_else(|| {
        WriteRenderError::Build(format!("DELETE: unknown node label `{}`", label))
    })?;
    let id_column = node_id_column_or_error(node_schema, "DELETE", alias)?;

    if detach {
        ops.extend(build_detach_rel_deletes(
            alias, label, &id_column, input, schema,
        )?);
    }

    let source = build_id_source(alias, &id_column, input, schema)?;
    ops.push(WriteRenderPlan::Delete(DeleteOp {
        database: node_schema.database.clone(),
        table: node_schema.table_name.clone(),
        id_column,
        source,
    }));
    Ok(ops)
}

/// For DETACH DELETE: enumerate every relationship table that references
/// `node_label`, and emit one DELETE per side that touches it.
///
/// `node_id_column` is the resolved primary-key column on `node_label`'s
/// table — it threads through into the rel-cleanup subqueries so they
/// project the right column from the read pipeline. (Previously this was
/// hard-coded to `"id"`, which broke for schemas where the PK has a
/// different column name like `user_id`.)
fn build_detach_rel_deletes(
    alias: &str,
    node_label: &str,
    node_id_column: &str,
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<Vec<WriteRenderPlan>, WriteRenderError> {
    let mut deletes: Vec<WriteRenderPlan> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();

    for rel_schema in schema.get_relationships_schemas().values() {
        if rel_schema.is_fk_edge {
            // FK-edge writes are out of scope for v1; ignore for DETACH so we
            // don't accidentally try to delete from a node table via an FK
            // column. Standard edge tables only.
            continue;
        }

        let touches_from = rel_schema.from_node == node_label;
        let touches_to = rel_schema.to_node == node_label;
        if !touches_from && !touches_to {
            continue;
        }

        // Multiple polymorphic variants of the same rel type can share a
        // physical table. Dedupe by (database, table) so we emit at most
        // one DELETE per side per physical table.
        let key = (rel_schema.database.clone(), rel_schema.table_name.clone());
        if !seen.insert(key.clone()) {
            continue;
        }

        if touches_from {
            let from_col = rel_schema
                .from_id
                .as_single()
                .map_err(|_| {
                    WriteRenderError::Build(format!(
                        "DETACH DELETE: composite from_id on `{}` is not supported in v1",
                        rel_schema.table_name
                    ))
                })?
                .to_string();
            deletes.push(WriteRenderPlan::Delete(DeleteOp {
                database: rel_schema.database.clone(),
                table: rel_schema.table_name.clone(),
                id_column: from_col,
                source: build_id_source(alias, node_id_column, input, schema)?,
            }));
        }
        if touches_to {
            let to_col = rel_schema
                .to_id
                .as_single()
                .map_err(|_| {
                    WriteRenderError::Build(format!(
                        "DETACH DELETE: composite to_id on `{}` is not supported in v1",
                        rel_schema.table_name
                    ))
                })?
                .to_string();
            deletes.push(WriteRenderPlan::Delete(DeleteOp {
                database: rel_schema.database.clone(),
                table: rel_schema.table_name.clone(),
                id_column: to_col,
                source: build_id_source(alias, node_id_column, input, schema)?,
            }));
        }
    }
    Ok(deletes)
}

// ---------------------------------------------------------------------------
// ID source helpers
// ---------------------------------------------------------------------------

/// Build the WHERE-IN source for an UPDATE/DELETE: a subquery rendered from
/// the read pipeline, with its SELECT list overridden to project just
/// `<alias>.<id_column>`.
fn build_id_source(
    alias: &str,
    id_column: &str,
    input: &Arc<LogicalPlan>,
    schema: &GraphSchema,
) -> Result<RowSource, WriteRenderError> {
    let mut render_plan = input.to_render_plan(schema)?;
    override_select_to_id(&mut render_plan, alias, id_column);
    Ok(RowSource::Subquery(Box::new(render_plan)))
}

fn override_select_to_id(plan: &mut RenderPlan, alias: &str, id_column: &str) {
    plan.select = SelectItems {
        items: vec![SelectItem {
            expression: RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: PropertyValue::Column(id_column.to_string()),
            }),
            col_alias: Some(ColumnAlias(id_column.to_string())),
        }],
        distinct: true,
    };
    // Writes don't care about ordering / pagination in the inner subquery.
    plan.order_by.0.clear();
    plan.skip.0 = None;
    plan.limit.0 = None;
}

// ---------------------------------------------------------------------------
// Schema / alias lookups (kept local to this module to minimise coupling)
// ---------------------------------------------------------------------------

fn node_id_column_or_error(
    node_schema: &NodeSchema,
    clause: &str,
    alias: &str,
) -> Result<String, WriteRenderError> {
    node_schema
        .node_id
        .column_or_error()
        .map(|s| s.to_string())
        .map_err(|e| {
            WriteRenderError::Build(format!(
                "{} on `{}`: composite primary keys are not supported in v1 ({})",
                clause, alias, e
            ))
        })
}

/// Return *every* label bound to `alias` in `plan`, sorted and deduped.
///
/// Phase 5e: an untyped Cypher MATCH like `MATCH (n) DELETE n` expands
/// the planner's `n` GraphNode into a `LogicalPlan::Union` with one
/// branch per node table — each branch binds `alias = n` to a different
/// label. The single-label `find_alias_label` returns whichever label
/// the depth-first walk reaches first, so the write pipeline used to
/// emit a DELETE/UPDATE for just that one table; this helper enumerates
/// all of them so the caller can fan out across the union.
fn find_all_alias_labels(alias: &str, plan: &LogicalPlan) -> Vec<String> {
    let mut out: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    collect_alias_labels(alias, plan, &mut out);
    out.into_iter().collect()
}

fn collect_alias_labels(
    alias: &str,
    plan: &LogicalPlan,
    out: &mut std::collections::BTreeSet<String>,
) {
    match plan {
        LogicalPlan::GraphNode(n) if n.alias == alias => {
            if let Some(l) = &n.label {
                out.insert(l.clone());
            }
            collect_alias_labels(alias, &n.input, out);
        }
        LogicalPlan::GraphNode(n) => collect_alias_labels(alias, &n.input, out),
        LogicalPlan::GraphRel(r) => {
            collect_alias_labels(alias, &r.left, out);
            collect_alias_labels(alias, &r.center, out);
            collect_alias_labels(alias, &r.right, out);
        }
        LogicalPlan::Filter(f) => collect_alias_labels(alias, &f.input, out),
        LogicalPlan::Projection(p) => collect_alias_labels(alias, &p.input, out),
        LogicalPlan::GroupBy(gb) => collect_alias_labels(alias, &gb.input, out),
        LogicalPlan::OrderBy(ob) => collect_alias_labels(alias, &ob.input, out),
        LogicalPlan::Skip(s) => collect_alias_labels(alias, &s.input, out),
        LogicalPlan::Limit(l) => collect_alias_labels(alias, &l.input, out),
        LogicalPlan::Cte(c) => collect_alias_labels(alias, &c.input, out),
        LogicalPlan::GraphJoins(gj) => collect_alias_labels(alias, &gj.input, out),
        LogicalPlan::Unwind(u) => collect_alias_labels(alias, &u.input, out),
        LogicalPlan::Union(u) => {
            for b in &u.inputs {
                collect_alias_labels(alias, b, out);
            }
        }
        LogicalPlan::CartesianProduct(cp) => {
            collect_alias_labels(alias, &cp.left, out);
            collect_alias_labels(alias, &cp.right, out);
        }
        LogicalPlan::WithClause(wc) => collect_alias_labels(alias, &wc.input, out),
        LogicalPlan::Create(c) => collect_alias_labels(alias, &c.input, out),
        LogicalPlan::SetProperties(sp) => collect_alias_labels(alias, &sp.input, out),
        LogicalPlan::Delete(d) => collect_alias_labels(alias, &d.input, out),
        LogicalPlan::Remove(r) => collect_alias_labels(alias, &r.input, out),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => {}
    }
}

/// Walk `plan` and return a copy with every `LogicalPlan::Union` replaced
/// by the single branch that binds `alias` to `target_label` only.
/// Returns `None` if no such branch exists, or if the plan shape between
/// the root and the Union contains a wrapper this function doesn't yet
/// rebuild — in which case the caller surfaces a clear "could not be
/// sliced" error instead of generating wrong SQL silently.
///
/// Phase 5e calls this for every label resolved by `find_all_alias_labels`
/// to scope each per-label DELETE/UPDATE's source query to that label's
/// read pipeline.
///
/// **Wrappers preserved above the Union (v1):** `Filter` (so any WHERE
/// from the untyped MATCH still constrains the slice) and `GraphJoins`
/// (so the joins-and-anchor metadata threads through). Other wrappers
/// — `Projection`, `OrderBy`, `Skip`, `Limit`, `GroupBy`, `WithClause`,
/// `CartesianProduct` — are intentionally not rebuilt; if they appear
/// above the Union, slicing returns `None`. (Note that even when slicing
/// succeeds, `override_select_to_id` later strips `order_by` / `skip` /
/// `limit` from the rendered subquery, so those clauses on the read
/// side don't propagate into the source-of-IDs subquery anyway —
/// extending this function to rebuild them would be cosmetic only.)
/// Multi-label fan-out under those shapes is a follow-up.
fn slice_plan_to_label(
    alias: &str,
    target_label: &str,
    plan: &Arc<LogicalPlan>,
) -> Option<Arc<LogicalPlan>> {
    match plan.as_ref() {
        LogicalPlan::Union(u) => {
            // Pick the branch whose only label binding for `alias` is
            // `target_label`. If a branch carries multiple labels we
            // can't safely scope it, so skip and rely on a deeper match.
            for branch in &u.inputs {
                let branch_labels = find_all_alias_labels(alias, branch);
                if branch_labels.len() == 1 && branch_labels[0] == target_label {
                    return Some(branch.clone());
                }
            }
            // Fall back: the target label might be inside a nested Union.
            for branch in &u.inputs {
                if let Some(b) = slice_plan_to_label(alias, target_label, branch) {
                    return Some(b);
                }
            }
            None
        }
        // Read-pipeline wrappers: recurse and rebuild around the sliced input.
        LogicalPlan::Filter(f) => {
            slice_plan_to_label(alias, target_label, &f.input).map(|new_input| {
                Arc::new(LogicalPlan::Filter(Filter {
                    input: new_input,
                    predicate: f.predicate.clone(),
                }))
            })
        }
        LogicalPlan::GraphJoins(gj) => {
            slice_plan_to_label(alias, target_label, &gj.input).map(|new_input| {
                Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                    input: new_input,
                    joins: gj.joins.clone(),
                    optional_aliases: gj.optional_aliases.clone(),
                    anchor_table: gj.anchor_table.clone(),
                    cte_references: gj.cte_references.clone(),
                    correlation_predicates: gj.correlation_predicates.clone(),
                }))
            })
        }
        // Below the Union, leaf shapes that already bind a single label
        // are returned as-is. Any other shape (any wrapper not listed
        // in this function's doc comment) returns `None` so the caller
        // surfaces a clear "could not be sliced" error rather than
        // generating wrong SQL silently.
        _ => {
            let labels = find_all_alias_labels(alias, plan);
            if labels.len() == 1 && labels[0] == target_label {
                Some(plan.clone())
            } else {
                None
            }
        }
    }
}

fn find_alias_label(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(n) if n.alias == alias => n.label.clone(),
        LogicalPlan::GraphNode(n) => find_alias_label(alias, &n.input),
        LogicalPlan::GraphRel(r) => find_alias_label(alias, &r.left)
            .or_else(|| find_alias_label(alias, &r.center))
            .or_else(|| find_alias_label(alias, &r.right)),
        LogicalPlan::Filter(f) => find_alias_label(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_label(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_label(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_label(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_label(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_label(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_label(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_label(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_label(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().find_map(|p| find_alias_label(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            find_alias_label(alias, &cp.left).or_else(|| find_alias_label(alias, &cp.right))
        }
        LogicalPlan::WithClause(wc) => find_alias_label(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_label(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_label(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_label(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_label(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

fn find_alias_rel_type(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(r) if r.alias == alias => {
            r.labels.as_ref().and_then(|l| l.first().cloned())
        }
        LogicalPlan::GraphRel(r) => find_alias_rel_type(alias, &r.left)
            .or_else(|| find_alias_rel_type(alias, &r.center))
            .or_else(|| find_alias_rel_type(alias, &r.right)),
        LogicalPlan::GraphNode(n) => find_alias_rel_type(alias, &n.input),
        LogicalPlan::Filter(f) => find_alias_rel_type(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_rel_type(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_rel_type(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_rel_type(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_rel_type(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_rel_type(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_rel_type(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_rel_type(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().find_map(|p| find_alias_rel_type(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            find_alias_rel_type(alias, &cp.left).or_else(|| find_alias_rel_type(alias, &cp.right))
        }
        LogicalPlan::WithClause(wc) => find_alias_rel_type(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_rel_type(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_rel_type(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_rel_type(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Value rendering
// ---------------------------------------------------------------------------

fn render_value(expr: &LogicalExpr) -> Result<RenderExpr, WriteRenderError> {
    RenderExpr::try_from(expr.clone()).map_err(WriteRenderError::Render)
}

fn unwrap_singleton(mut ops: Vec<WriteRenderPlan>) -> WriteRenderPlan {
    if ops.len() == 1 {
        ops.pop().unwrap()
    } else {
        WriteRenderPlan::Sequence(ops)
    }
}
