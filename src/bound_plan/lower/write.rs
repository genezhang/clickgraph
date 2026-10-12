//! Lowering for the embedded write path (S13, `docs/design/EXPLICIT_SCOPE.md`):
//! the rows a write statement's operations apply to, and its SET values.
//!
//! A statement's updating clauses apply to the records its read part
//! produces (`BoundWriteStatement::read`). An UPDATE / DELETE of a node table
//! reads the identities of the node in those records ([`lower_identities`]),
//! the read part lowered as a read's relation; a SET value is an expression
//! of the node's own row ([`lower_write_value`]), its properties read from the
//! table the UPDATE writes, qualified by the table's name.

use std::collections::HashMap;

use crate::bound_plan::expr::{referenced_names, subquery_vars};
use crate::bound_plan::types::{BoundStatement, VarId};
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema};
use crate::query_planner::logical_expr::LogicalExpr;
use crate::render_plan::{CteItems, RenderPlan};

use super::{
    constraint_reads, demand, parse_var, select, unsupported, At, Body, LowerError, LowerOptions,
    Query, Scan,
};

/// The distinct identities of a node in a read part's records: one column
/// per identity column (`__cg_id{i}`); with `labeled`, the first is the
/// node's label (a node of several possible labels).
#[derive(Debug, Clone)]
pub struct IdentityRows {
    pub plan: RenderPlan,
    pub labeled: bool,
    pub columns: Vec<String>,
}

/// The identities of node `var` in the records of `stmt`'s read part
/// (`stmt.plan`, no projection). `None` when the node matches nothing.
pub fn lower_identities(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
) -> Result<Option<IdentityRows>, LowerError> {
    match schema.lowering_view() {
        Some(read) => identities_over(stmt, &read, options, var),
        None => identities_over(stmt, schema, options, var),
    }
}

fn identities_over(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
) -> Result<Option<IdentityRows>, LowerError> {
    let constraints = constraint_reads(stmt, schema);
    let demand = demand(stmt, &constraints);
    let unstable = std::cell::RefCell::new(std::collections::BTreeSet::new());
    let query = Query {
        stmt,
        schema,
        options,
        demand: &demand,
        as_values: std::collections::BTreeSet::new(),
        unstable: &unstable,
    };
    let mut ctes = Vec::new();
    let mut l = query.lowerer(&mut ctes);
    l.relation(&stmt.plan)?;
    l.finish_relation();
    let Some(ids) = l.identity(var)? else {
        return Ok(None);
    };
    let labeled = matches!(l.scans.get(&var), Some(Scan::Labels { .. }));
    let mut body = Body {
        distinct: true,
        ..Body::default()
    };
    let mut columns = Vec::new();
    for (i, id) in ids.into_iter().enumerate() {
        let name = format!("__cg_id{i}");
        body.select.push(select(id, &name));
        columns.push(name);
    }
    let mut plan = l.render(body);
    plan.ctes = CteItems(std::mem::take(&mut l.ctes));
    Ok(Some(IdentityRows {
        plan,
        labeled,
        columns,
    }))
}

/// A SET value of node `var` of `label`, as an UPDATE of its table computes
/// it for each row: its properties read from the row, qualified by `table`
/// (the table's name). A value that reads another variable or a subquery is
/// not lowered: one table's UPDATE reads only its own rows.
#[allow(clippy::too_many_arguments)]
pub fn lower_write_value(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
    label: &str,
    node: &NodeSchema,
    table: &str,
    value: &LogicalExpr,
) -> Result<crate::render_plan::render_expr::RenderExpr, LowerError> {
    if !subquery_vars(value).is_empty() {
        return unsupported("a SET value that reads a pattern");
    }
    if referenced_names(value, false)
        .iter()
        .any(|n| parse_var(n).is_some_and(|v| v != var))
    {
        return unsupported("a SET value that reads another variable");
    }
    let no_demand = HashMap::new();
    let unstable = std::cell::RefCell::new(std::collections::BTreeSet::new());
    let query = Query {
        stmt,
        schema,
        options,
        demand: &no_demand,
        as_values: std::collections::BTreeSet::new(),
        unstable: &unstable,
    };
    let mut ctes = Vec::new();
    let mut l = query.lowerer(&mut ctes);
    l.scans.insert(
        var,
        Scan::Node {
            schema: node,
            label: label.to_string(),
            at: At::Table(table.to_string()),
        },
    );
    let lowered = l.expr(value, &HashMap::new())?;
    if !l.ctes.is_empty() || !l.joins.is_empty() {
        return unsupported("a SET value that reads more than its node's row");
    }
    Ok(lowered)
}
