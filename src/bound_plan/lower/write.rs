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
/// (`stmt.plan`, no projection): each once with `distinct`, else one per
/// record. `None` when the node matches nothing.
pub fn lower_identities(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
    distinct: bool,
) -> Result<Option<IdentityRows>, LowerError> {
    match schema.lowering_view() {
        Some(read) => identities_over(stmt, &read, options, var, distinct),
        None => identities_over(stmt, schema, options, var, distinct),
    }
}

fn identities_over(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
    distinct: bool,
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
    l.finish_relation()?;
    let Some(ids) = l.identity(var)? else {
        return Ok(None);
    };
    let labeled = matches!(l.scans.get(&var), Some(Scan::Labels { .. }));
    let mut body = Body {
        distinct,
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

/// The values `exprs` in each record of `stmt`'s read part, one row per
/// record (columns `__cg_v{i}`), with each value's SQL (over a read part of
/// no MATCH, a constant's: an INSERT's `VALUES`). A value no property can
/// hold (a map, a node, a relationship, a path, a list of them or holding
/// a NULL: Neo4j's TypeError) is `Err(Ok(reason))`.
pub fn lower_records(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    exprs: &[LogicalExpr],
) -> Result<(RenderPlan, Vec<String>), Result<String, LowerError>> {
    match schema.lowering_view() {
        Some(read) => records_over(stmt, &read, options, exprs),
        None => records_over(stmt, schema, options, exprs),
    }
}

fn records_over(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    exprs: &[LogicalExpr],
) -> Result<(RenderPlan, Vec<String>), Result<String, LowerError>> {
    if exprs.iter().any(|e| !subquery_vars(e).is_empty()) {
        return Err(unsupported("a CREATE value that reads a pattern"));
    }
    // The values read the read part's variables: demanded as a RETURN's.
    let mut extra = constraint_reads(stmt, schema);
    extra.extend(exprs.iter().cloned());
    let demand = demand(stmt, &extra);
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
    l.relation(&stmt.plan).map_err(Err)?;
    l.finish_relation().map_err(Err)?;
    for e in exprs {
        if let Some(reason) = unstorable(&l, stmt, e) {
            return Err(Ok(format!("a property value that is {reason}")));
        }
    }
    let mut body = Body::default();
    let mut sqls = Vec::new();
    for (i, e) in exprs.iter().enumerate() {
        let value = l.expr(e, &HashMap::new()).map_err(Err)?;
        sqls.push(
            crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain(
                &value,
            ),
        );
        body.select.push(select(value, &format!("__cg_v{i}")));
    }
    let mut plan = l.render(body);
    plan.ctes = CteItems(std::mem::take(&mut l.ctes));
    Ok((plan, sqls))
}

/// What no property can hold that `e` is (Neo4j's TypeError), if it is.
fn unstorable(
    l: &super::Lowerer<'_>,
    stmt: &BoundStatement,
    e: &LogicalExpr,
) -> Option<&'static str> {
    use super::unwind::Kind;
    use crate::bound_plan::types::BindingKind;
    use crate::query_planner::logical_expr::{Literal, TableAlias};
    if let LogicalExpr::TableAlias(TableAlias(name)) = e {
        if let Some(v) = parse_var(name) {
            if !matches!(stmt.binding(v).kind, BindingKind::Value) {
                return Some("a node, relationship or path");
            }
        }
    }
    if let LogicalExpr::List(items) = e {
        if items
            .iter()
            .any(|i| matches!(i, LogicalExpr::Literal(Literal::Null)))
        {
            return Some("a list holding a NULL");
        }
    }
    fn of(k: &Kind, top: bool) -> Option<&'static str> {
        match k {
            Kind::Map(_) => Some("a map"),
            Kind::Element(_) => Some("a node or relationship"),
            Kind::List(inner) if top => of(inner, false),
            Kind::List(_) => Some("a list of lists"),
            _ => None,
        }
    }
    of(&l.kind(e), true)
}

/// The properties `stmt`'s read part reads of each variable: its patterns'
/// property maps, its WHERE, WITH and UNWIND expressions, and the edge
/// constraints of its relationships.
pub fn read_properties(
    stmt: &BoundStatement,
    schema: &GraphSchema,
) -> HashMap<VarId, std::collections::BTreeSet<String>> {
    let read = |schema: &GraphSchema| demand(stmt, &constraint_reads(stmt, schema));
    match schema.lowering_view() {
        Some(view) => read(&view),
        None => read(schema),
    }
}

/// A SET value of node `var` of `label`, as an UPDATE of its table computes
/// it for each row (SQL): its properties read from the row, by their bare
/// columns (a lightweight UPDATE has no alias, and chdb's resolves no
/// column qualified by the table's name). A value that reads another
/// variable or a subquery is not lowered: one table's UPDATE reads only its
/// own rows.
#[allow(clippy::too_many_arguments)]
pub fn lower_write_value(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    var: VarId,
    label: &str,
    node: &NodeSchema,
    value: &LogicalExpr,
) -> Result<String, LowerError> {
    if !subquery_vars(value).is_empty() {
        return unsupported("a SET value that reads a pattern");
    }
    let local = |v: VarId| {
        matches!(
            stmt.binding(v).source,
            crate::bound_plan::types::BindingSource::Local
        )
    };
    if referenced_names(value, false)
        .iter()
        .any(|n| parse_var(n).is_some_and(|v| v != var && !local(v)))
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
            at: At::Table(ROW.to_string()),
        },
    );
    let lowered = l.expr(value, &HashMap::new())?;
    if !l.ctes.is_empty() || !l.joins.is_empty() {
        return unsupported("a SET value that reads more than its node's row");
    }
    Ok(unqualified(
        &crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain(
            &lowered,
        ),
        ROW,
    ))
}

/// The alias a SET value reads its row's columns at ([`lower_write_value`]).
const ROW: &str = "__cg_row";

/// `sql` without the qualifier `alias.` of its columns, outside its string
/// literals (`'…'`, a quote doubled or escaped) and quoted names (`` `…` ``,
/// `"…"`).
fn unqualified(sql: &str, alias: &str) -> String {
    let qualifier = format!("{alias}.");
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(c) = rest.chars().next() {
        if matches!(c, '\'' | '`' | '"') {
            // Through the closing quote: a backslash escapes the next
            // character, a doubled quote is one.
            let mut end = 1;
            let bytes = rest.as_bytes();
            while end < bytes.len() {
                match bytes[end] {
                    b'\\' => end += 2,
                    b if b == c as u8 && bytes.get(end + 1) == Some(&(c as u8)) => end += 2,
                    b if b == c as u8 => {
                        end += 1;
                        break;
                    }
                    _ => end += 1,
                }
            }
            let end = end.min(rest.len());
            out.push_str(&rest[..end]);
            rest = &rest[end..];
            continue;
        }
        let word_start = out
            .chars()
            .last()
            .is_none_or(|p| !(p.is_alphanumeric() || p == '_' || p == '.'));
        if word_start && rest.starts_with(&qualifier) {
            rest = &rest[qualifier.len()..];
            continue;
        }
        out.push(c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn unqualified_skips_literals_and_quoted_names() {
        assert_eq!(
            super::unqualified("__cg_row.age + 1", "__cg_row"),
            "age + 1"
        );
        assert_eq!(
            super::unqualified(
                "concat(__cg_row.name, '__cg_row.x', 'it''s __cg_row.y', 'a\\'__cg_row.z')",
                "__cg_row"
            ),
            "concat(name, '__cg_row.x', 'it''s __cg_row.y', 'a\\'__cg_row.z')"
        );
        assert_eq!(
            super::unqualified("x__cg_row.age + `__cg_row.q`", "__cg_row"),
            "x__cg_row.age + `__cg_row.q`"
        );
    }
}
