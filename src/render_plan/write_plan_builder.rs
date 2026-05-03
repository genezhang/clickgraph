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

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::{
    Create, CreatePattern, CreateRel, Delete, LogicalPlan, Remove, SetItem, SetProperties,
    WriteProperty,
};

use super::errors::RenderBuildError;
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{ColumnAlias, Literal as RenderLiteral, RenderExpr, TableAlias};
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
    // Group SET items by target alias so each alias produces one UpdateOp.
    let grouped = group_assignments_by_alias(&sp.items);
    let mut ops: Vec<WriteRenderPlan> = Vec::with_capacity(grouped.len());

    for (alias, items) in grouped {
        let label = find_alias_label(&alias, &sp.input).ok_or_else(|| {
            WriteRenderError::Build(format!(
                "SET: alias `{}` is not bound by a preceding MATCH (or its label cannot be \
                 resolved at this stage)",
                alias
            ))
        })?;
        let node_schema = schema.node_schema_opt(&label).ok_or_else(|| {
            WriteRenderError::Build(format!("SET: unknown node label `{}`", label))
        })?;

        let mut assignments: Vec<(String, RenderExpr)> = Vec::with_capacity(items.len());
        for item in &items {
            let column =
                resolve_node_property_column(node_schema, &item.property).ok_or_else(|| {
                    WriteRenderError::Build(format!(
                        "SET: property `{}.{}` cannot be mapped to a writable column",
                        alias, item.property
                    ))
                })?;
            assignments.push((column, render_value(&item.value)?));
        }

        let id_column = node_id_column_or_error(node_schema, "SET", &alias)?;
        let source = build_id_source(&alias, &id_column, &sp.input, schema)?;

        ops.push(WriteRenderPlan::Update(UpdateOp {
            database: node_schema.database.clone(),
            table: node_schema.table_name.clone(),
            assignments,
            id_column,
            source,
        }));
    }

    Ok(unwrap_singleton(ops))
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
        if let Some(label) = find_alias_label(alias, &del.input) {
            let node_schema = schema.node_schema_opt(&label).ok_or_else(|| {
                WriteRenderError::Build(format!("DELETE: unknown node label `{}`", label))
            })?;

            if del.detach {
                ops.extend(build_detach_rel_deletes(alias, &label, &del.input, schema)?);
            }

            let id_column = node_id_column_or_error(node_schema, "DELETE", alias)?;
            let source = build_id_source(alias, &id_column, &del.input, schema)?;
            ops.push(WriteRenderPlan::Delete(DeleteOp {
                database: node_schema.database.clone(),
                table: node_schema.table_name.clone(),
                id_column,
                source,
            }));
            continue;
        }

        // Otherwise try as a relationship alias.
        if let Some(rel_type) = find_alias_rel_type(alias, &del.input) {
            let rel_schema = find_rel_schema(schema, &rel_type).ok_or_else(|| {
                WriteRenderError::Build(format!("DELETE: unknown relationship type `{}`", rel_type))
            })?;
            // Use from_id as the discriminator column for the IN list. We
            // identify rows via (from_id, to_id) pairs in practice — for v1
            // we use from_id only and document this in the SQL generator.
            let id_column = rel_schema
                .from_id
                .as_single()
                .map_err(|_| {
                    WriteRenderError::Build(format!(
                        "DELETE on rel `{}`: composite from_id is not supported in v1",
                        alias
                    ))
                })?
                .to_string();
            let source = build_rel_id_source(alias, &id_column, &del.input, schema)?;
            ops.push(WriteRenderPlan::Delete(DeleteOp {
                database: rel_schema.database.clone(),
                table: rel_schema.table_name.clone(),
                id_column,
                source,
            }));
            continue;
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

/// For DETACH DELETE: enumerate every relationship table that references
/// `node_label`, and emit one DELETE per side that touches it.
fn build_detach_rel_deletes(
    alias: &str,
    node_label: &str,
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
                source: build_id_source(alias, "id", input, schema)?,
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
                source: build_id_source(alias, "id", input, schema)?,
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

fn build_rel_id_source(
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

fn find_rel_schema<'a>(schema: &'a GraphSchema, rel_type: &str) -> Option<&'a RelationshipSchema> {
    // Prefer single-key (covers most schemas) then fall back to the composite
    // index to handle polymorphic relationship variants.
    if let Some(s) = schema.get_relationships_schema_opt(rel_type) {
        return Some(s);
    }
    schema
        .get_relationships_schemas()
        .iter()
        .find(|(k, _)| k.starts_with(&format!("{}::", rel_type)))
        .map(|(_, v)| v)
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

// ---------------------------------------------------------------------------
// Tag selectors so this file's `use` block isn't flagged as dead in the
// scenario where these types aren't directly referenced after the typestate
// roundtrip lands. Keeping them re-exported keeps callers stable.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn _phantom_anchor()
where
    RenderLiteral: Sized,
    InsertOp: Serialize,
    DeleteOp: for<'de> Deserialize<'de>,
{
}
