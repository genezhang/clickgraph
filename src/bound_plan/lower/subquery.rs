//! Subquery expressions (P-4c S9, `docs/design/EXPLICIT_SCOPE.md` §4.12):
//! `EXISTS { … }`, a pattern predicate, `exists(pattern)`, `size(pattern)`
//! and pattern comprehensions.
//!
//! A subquery is applied to the current rows before an expression reading it
//! is lowered ([`Lowerer::apply_subqueries`]): its matches `Q`, as an
//! OPTIONAL MATCH's (§4.9), grouped by the correlation columns into a count
//! or a list, LEFT JOIN the rows on the correlation. One group per
//! correlation key, so every row joins at most one: the rows are neither
//! multiplied nor dropped, and keep their order. Its value is then a column
//! of the joined group (`exists`: the count is not 0; a row that joins none
//! has count 0 and the empty list). Nothing is a correlated subquery in SQL
//! (ClickHouse cannot read an outer table two levels down, §4.12).
//!
//! `Q` reads the correlation as an OPTIONAL MATCH does:
//! * anchored ([`Lowerer::anchor_correlation`]): the shared nodes from their
//!   own tables, `Q` being the pattern's matches in the whole graph, grouped
//!   by them;
//! * otherwise from a **drive** `D`, the distinct correlation columns of the
//!   current rows. Unlike an OPTIONAL MATCH's, `D` reads a copy of the rows
//!   so far ([`Lowerer::snapshot`]) and the current segment stays open, so a
//!   subquery can be applied in the middle of a MATCH (its WHERE), inside an
//!   OPTIONAL MATCH's `Q` and inside another subquery's `Q`. Every column of
//!   `D` (identities, endpoints, properties, values) is a correlation column,
//!   joined NULL-safely: a variable only the WHERE reads can be NULL and
//!   still decide it, and a relationship identified by its ends is told
//!   from a parallel one by its properties.
//!
//! A NULL shared node or relationship is in no match (`Q`'s MATCH requires it
//! not NULL), so its row gets false / 0 / `[]`, as in Neo4j.
//!
//! A pattern predicate of a list comprehension's element is a members
//! subquery (S11o9, `SubqueryKind::Members`): the nodes in the element's
//! place, looked for from the elements of the comprehension's list when it
//! has one ([`Lowerer::members_drive`]), which `x IN` reads.
//!
//! Not lowered yet: a subquery reading a path, a variable-length
//! relationship's list or a carried list of nodes; a pattern comprehension
//! of nodes, relationships, paths or maps; a subquery in the ORDER BY / WHERE
//! of a projection that reads a projected name (a WITH's WHERE that reads
//! only projected names is bound as a `Filter` after it); a shortestPath's
//! WHERE conjunct that reads both a subquery and the path.

use std::collections::{BTreeSet, HashMap};

use crate::graph_catalog::graph_schema::NodeSchema;
use crate::query_planner::logical_expr::{self as lx, LogicalExpr};
use crate::render_plan::render_expr::{
    AggregateFnCall, Literal, Operator, OperatorApplication, RenderExpr, ScalarFnCall,
};
use crate::render_plan::{ArrayJoin, Cte, CteContent, JoinType, RenderPlan};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::current_function_mapper;

use super::elements::{slot, Elem};
use super::unwind::Kind;
use super::{
    col_at, conjuncts, empty_plan, eq, is_constant, join, parse_var, property_refs, reads_only,
    referenced_names, select, table_aliases, table_ref, unsupported, At, Body, Correlated,
    LowerError, Lowerer, Scan, Segment,
};
use crate::bound_plan::expr::{subquery_var, subquery_vars};
use crate::bound_plan::types::{BindingKind, BoundOp, BoundPattern, Subquery, SubqueryKind, VarId};

/// The column of a subquery's grouped matches holding its count or list.
const VALUE: &str = "__value";

/// The name of a subquery's grouped matches' CTE, before its number.
const SUBQUERY_CTE: &str = "subquery_s";

/// `a` NULL-safely equal to `b`, as one join condition (the dialect's),
/// conjoined with `true` (a conjunct of the ON is an operator application).
fn null_safe_equal(a: &RenderExpr, b: &RenderExpr) -> OperatorApplication {
    OperatorApplication {
        operator: Operator::And,
        operands: vec![
            RenderExpr::Raw(
                current_function_mapper()
                    .null_safe_equal(&render_expr_to_sql_plain(a), &render_expr_to_sql_plain(b)),
            ),
            RenderExpr::Literal(Literal::Boolean(true)),
        ],
    }
}

/// The expression reads no subquery.
pub(super) fn subquery_free(e: &LogicalExpr) -> bool {
    subquery_vars(e).is_empty()
}

/// The conjuncts of a bound predicate (`a AND b AND c`).
pub(super) fn logical_conjuncts(e: &LogicalExpr) -> Vec<&LogicalExpr> {
    match e {
        LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op)
            if op.operator == lx::Operator::And =>
        {
            op.operands.iter().flat_map(logical_conjuncts).collect()
        }
        other => vec![other],
    }
}

/// The conjunction of `exprs` (`None` for none).
pub(super) fn logical_and(mut exprs: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(LogicalExpr::OperatorApplicationExp(
            lx::OperatorApplication {
                operator: lx::Operator::And,
                operands: exprs,
            },
        )),
    }
}

impl<'s> Lowerer<'s> {
    fn subquery_of(&self, v: VarId) -> Result<&'s Subquery, LowerError> {
        match self.subqueries.iter().find(|s| s.var == v) {
            Some(s) => Ok(s),
            None => unsupported(format!("internal: {v} is not a subquery")),
        }
    }

    /// The expressions of the subqueries `exprs` read, and of those they
    /// read: what a clause reads includes what its subqueries read (a
    /// property of a correlated node, say).
    pub(super) fn nested_subquery_exprs(&self, exprs: &[&LogicalExpr]) -> Vec<&'s LogicalExpr> {
        let mut out: Vec<&'s LogicalExpr> = Vec::new();
        let mut todo: Vec<VarId> = exprs.iter().flat_map(|e| subquery_vars(e)).collect();
        let mut seen: Vec<VarId> = Vec::new();
        while let Some(v) = todo.pop() {
            if seen.contains(&v) {
                continue;
            }
            seen.push(v);
            let Ok(sq) = self.subquery_of(v) else {
                continue;
            };
            let BoundOp::Match {
                pattern, predicate, ..
            } = &sq.plan
            else {
                continue;
            };
            let mut own: Vec<&'s LogicalExpr> = predicate.iter().collect();
            if let SubqueryKind::Collect(p) = &sq.kind {
                own.push(p);
            }
            for part in &pattern.parts {
                own.extend(
                    part.nodes
                        .iter()
                        .flat_map(|n| n.props.iter().map(|(_, e)| e)),
                );
                own.extend(
                    part.rels
                        .iter()
                        .flat_map(|r| r.props.iter().map(|(_, e)| e)),
                );
            }
            todo.extend(own.iter().flat_map(|e| subquery_vars(e)));
            out.extend(own);
        }
        out
    }

    /// Whether every subquery `e` reads can be applied to the current rows:
    /// the variables it reads are theirs (not a projection's items, nor a
    /// path the rows do not carry as a scan or value).
    pub(super) fn subqueries_readable(&self, e: &LogicalExpr) -> bool {
        subquery_vars(e).into_iter().all(|v| {
            self.subquery_of(v).is_ok_and(|sq| {
                sq.correlation
                    .iter()
                    .all(|c| self.scans.contains_key(c) || self.values.contains_key(c))
            })
        })
    }

    /// Apply every subquery `exprs` read that the current rows have not
    /// (their values become `self.values`).
    pub(super) fn apply_subqueries(&mut self, exprs: &[&LogicalExpr]) -> Result<(), LowerError> {
        for e in exprs {
            for v in subquery_vars(e) {
                if self.values.contains_key(&v) {
                    continue;
                }
                let sq = self.subquery_of(v)?;
                self.apply_subquery(sq)?;
            }
        }
        Ok(())
    }

    /// Apply one subquery: LEFT JOIN its grouped matches, its value a column
    /// of them (module doc).
    fn apply_subquery(&mut self, sq: &'s Subquery) -> Result<(), LowerError> {
        let BoundOp::Match {
            pattern, predicate, ..
        } = &sq.plan
        else {
            return unsupported("internal: a subquery that is not a MATCH");
        };
        if let Some(v) = sq
            .correlation
            .iter()
            .find(|c| !self.scans.contains_key(c) && !self.values.contains_key(c))
        {
            return unsupported(format!(
                "a subquery reading {v}, which its rows do not hold as a node, relationship or \
                 value (a projected item, a path)"
            ));
        }
        let projection = match &sq.kind {
            SubqueryKind::Collect(p) => Some(p),
            SubqueryKind::Exists | SubqueryKind::Count | SubqueryKind::Members { .. } => None,
        };
        // The shared variables: the pattern's, bound before.
        let mut shared: Vec<VarId> = Vec::new();
        for part in &pattern.parts {
            let nodes = part.nodes.iter().filter(|n| n.bound_before).map(|n| n.var);
            let rels = part.rels.iter().filter(|r| r.bound_before).map(|r| r.var);
            for v in nodes.chain(rels) {
                if !shared.contains(&v) {
                    shared.push(v);
                }
            }
        }
        // Read by its WHERE, property maps or projection only.
        let mut read: Vec<&LogicalExpr> = predicate.iter().chain(projection).collect();
        for part in &pattern.parts {
            read.extend(
                part.nodes
                    .iter()
                    .flat_map(|n| n.props.iter().map(|(_, e)| e)),
            );
            read.extend(
                part.rels
                    .iter()
                    .flat_map(|r| r.props.iter().map(|(_, e)| e)),
            );
        }
        let nested = self.nested_subquery_exprs(&read);
        read.extend(nested);
        let mut where_only: Vec<VarId> = Vec::new();
        let mut null_inputs: Vec<VarId> = Vec::new();
        for e in &read {
            for v in referenced_names(e, false)
                .iter()
                .filter_map(|n| parse_var(n))
            {
                if shared.contains(&v) || where_only.contains(&v) || null_inputs.contains(&v) {
                    continue;
                }
                if matches!(self.scans.get(&v), Some(Scan::Impossible)) {
                    null_inputs.push(v);
                    continue;
                }
                let input = self.scans.contains_key(&v)
                    || self.values.get(&v).is_some_and(|e| !is_constant(e));
                if input {
                    where_only.push(v);
                }
            }
        }
        // A shared element that matches nothing (NULL on every row) is in no
        // match.
        if shared
            .iter()
            .any(|v| matches!(self.scans.get(v), Some(Scan::Impossible)))
        {
            return self.subquery_constant(sq);
        }
        for v in shared.iter().chain(&where_only) {
            match self.scans.get(v) {
                None
                | Some(
                    Scan::Node { .. } | Scan::Labels { .. } | Scan::Rel { .. } | Scan::Rels { .. },
                ) => {}
                Some(_) => {
                    return unsupported(
                        "a subquery reading a variable-length relationship's list or a path",
                    )
                }
            }
        }
        // As an OPTIONAL MATCH (`Lowerer::optional_match`): anchored on the
        // shared nodes unless a relationship or a value is shared or read, or
        // the matches in the whole graph could be far more than the rows'.
        let hops: usize = pattern
            .parts
            .iter()
            .flat_map(|p| &p.rels)
            .map(|r| if r.length.is_some() { 2 } else { 1 })
            .sum();
        let has_path = pattern
            .parts
            .iter()
            .flat_map(|p| &p.rels)
            .any(|r| r.length.is_some());
        let anchor_aliases: Vec<String> = shared
            .iter()
            .filter(|v| {
                matches!(self.scans.get(*v).and_then(Scan::at), Some(At::Table(a)) if *a == v.name())
            })
            .map(|v| v.name())
            .collect();
        let restricted = self
            .filters
            .iter()
            .flat_map(conjuncts)
            .any(|c| reads_only(c, &anchor_aliases) && !table_aliases(c).is_empty());
        // A pattern predicate's nodes in a list comprehension's element's
        // place: looked for from its list's elements (S11o9).
        let members = match &sq.kind {
            SubqueryKind::Members { node, of: Some(of) } => match self.members_from(*node, of) {
                MembersFrom::Elements(a) => Some(a),
                MembersFrom::Nowhere => return self.subquery_constant(sq),
                MembersFrom::Graph => None,
            },
            _ => None,
        };
        let anchored = members.is_none()
            && where_only.is_empty()
            && shared.iter().all(|v| {
                matches!(
                    self.scans.get(v),
                    Some(Scan::Node { .. } | Scan::Labels { .. })
                )
            })
            && (shared.is_empty() || (!has_path && (hops <= 1 || restricted)));
        let mut q = Segment {
            values: self
                .values
                .iter()
                .filter(|(_, e)| is_constant(e))
                .map(|(v, e)| (*v, e.clone()))
                .collect(),
            ..Segment::default()
        };
        for v in &null_inputs {
            q.scans.insert(*v, Scan::Impossible);
        }
        let mut read_props: Vec<(VarId, String)> = read
            .iter()
            .flat_map(|e| property_refs(e))
            .filter_map(|(n, p)| Some((parse_var(&n)?, p)))
            .collect();
        for part in &pattern.parts {
            for n in &part.nodes {
                read_props.extend(n.props.iter().map(|(p, _)| (n.var, p.clone())));
            }
        }
        let ctes_before = self.ctes.len();
        let correlation = if anchored {
            self.anchor_correlation(pattern, &shared, &read_props, &mut q)?
        } else {
            self.subquery_drive(&shared, &where_only, &read_props, members.as_ref(), &mut q)?
        };
        let outer = self.swap_segment(q);
        let inner = self.subquery_inner(pattern, predicate.as_ref(), &correlation, sq);
        self.swap_segment(outer);
        let Some((plan, list)) = inner? else {
            // The pattern can match nothing: `Q`'s CTEs and `D` are unused.
            self.ctes.truncate(ctes_before);
            return self.subquery_constant(sq);
        };
        // Named after `Q`'s own CTEs (a nested subquery's among them).
        let n = self.ctes.len() + 1;
        let (alias, name) = (format!("s{n}"), format!("{SUBQUERY_CTE}{n}"));
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        if self.from.is_none() {
            // No rows read yet (a subquery before any MATCH): one empty row.
            let u = self.next_cte_alias();
            let mut unit = empty_plan();
            unit.select.items = vec![select(RenderExpr::Literal(Literal::Integer(1)), "__row")];
            self.ctes.push(Cte::new(
                format!("with_{u}"),
                CteContent::Structured(Box::new(unit)),
                false,
            ));
            self.from = Some(table_ref(format!("with_{u}"), &u));
            self.emitted.insert(0, u);
        }
        let mut on: Vec<OperatorApplication> = correlation
            .into_iter()
            .map(|c| {
                let inner = col_at(&alias, &c.column);
                if c.null_safe {
                    // One condition ClickHouse keys the join on (the `OR`
                    // form is not a join key past two relations: Code 403).
                    null_safe_equal(&c.outer, &inner)
                } else {
                    eq(c.outer, inner)
                }
            })
            .collect();
        if on.is_empty() {
            // Uncorrelated: one group, joined to every row.
            on.push(eq(
                RenderExpr::Literal(Literal::Integer(1)),
                RenderExpr::Literal(Literal::Integer(1)),
            ));
        }
        let mut j = join(name, &alias);
        j.join_type = JoinType::Left;
        j.joining_on = on;
        self.joins.push(j);
        self.emitted.push(alias.clone());
        let column = col_at(&alias, VALUE);
        let or_else = |default: Literal| {
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "coalesce".to_string(),
                args: vec![column.clone(), RenderExpr::Literal(default)],
            })
        };
        let value = match &sq.kind {
            // A boolean column of the groups: one operand as it is, so a
            // comparison of it needs no parentheses (`EXISTS {…} = …`).
            SubqueryKind::Exists => or_else(Literal::Boolean(false)),
            SubqueryKind::Count => or_else(Literal::Integer(0)),
            // A row that joins no group reads the empty list.
            SubqueryKind::Collect(_) | SubqueryKind::Members { .. } => {
                let Some(spelling) = current_function_mapper().lists() else {
                    return unsupported("a pattern comprehension in this SQL dialect");
                };
                match spelling.or_empty {
                    Some(or_empty) => RenderExpr::Raw(or_empty(&render_expr_to_sql_plain(&column))),
                    None => column.clone(),
                }
            }
        };
        self.values.insert(sq.var, value);
        if let Some((k, before)) = list {
            self.set_kind(
                sq.var,
                Kind::List(Box::new(k)),
                Kind::List(Box::new(before)),
            );
        }
        Ok(())
    }

    /// The value of a subquery whose pattern matches nothing.
    fn subquery_constant(&mut self, sq: &Subquery) -> Result<(), LowerError> {
        let value = match &sq.kind {
            SubqueryKind::Exists => RenderExpr::Literal(Literal::Boolean(false)),
            SubqueryKind::Count => RenderExpr::Literal(Literal::Integer(0)),
            SubqueryKind::Collect(_) | SubqueryKind::Members { .. } => RenderExpr::List(Vec::new()),
        };
        self.values.insert(sq.var, value);
        Ok(())
    }

    /// `D`: the distinct correlation columns over a copy of the rows so far
    /// (the segment stays open): the identity / endpoint columns of the
    /// `shared` and `where_only` elements, the properties `Q` reads of them
    /// (`read_props`), and the values. `Q` reads them from `D`. An element's
    /// key that is never NULL (a shared one is in no match when NULL, `Q`'s
    /// MATCH requires it) joins by equality; the rest NULL-safely.
    fn subquery_drive(
        &mut self,
        shared: &[VarId],
        where_only: &[VarId],
        read_props: &[(VarId, String)],
        members: Option<&MembersAnchor<'s>>,
        q: &mut Segment<'s>,
    ) -> Result<Vec<Correlated>, LowerError> {
        self.finish_relation()?;
        let n = self.ctes.len() + 1;
        let (d, name) = (format!("d{n}"), format!("subquery_d{n}"));
        let mut body = Body {
            distinct: true,
            ..Body::default()
        };
        let mut correlation = Vec::new();
        // Right after an aggregating WITH: joined on its keys, not on its
        // aggregated values (`Lowerer::grouped_drive`, #1395).
        let at_with = self.joins.is_empty() && self.emitted.len() == 1;
        let (determined, keys) = self.grouped_drive(shared, where_only, at_with);
        for v in shared.iter().chain(where_only).chain(&keys) {
            if self.graph_values.contains_key(v) {
                return unsupported("a path's or list's elements read by a subquery");
            }
            let joined = !determined.contains(v);
            if let Some(e) = self.values.get(v).cloned() {
                let column = v.name();
                body.select.push(select(e.clone(), &column));
                q.values.insert(*v, col_at(&d, &column));
                if joined {
                    correlation.push(Correlated {
                        inner: col_at(&d, &column),
                        outer: e,
                        column,
                        null_safe: true,
                    });
                }
                // When a list is NULL (`null_list.rs`): its column too, and
                // joined on (a NULL list and `[]` are held alike).
                match self.list_nulls.get(v).cloned() {
                    Some(Some(n)) => {
                        let mut import = |e: RenderExpr, suffix: &str| {
                            if is_constant(&e) {
                                return e;
                            }
                            let column = format!("{}__{suffix}", v.name());
                            body.select.push(select(e.clone(), &column));
                            if joined {
                                correlation.push(Correlated {
                                    inner: col_at(&d, &column),
                                    outer: e,
                                    column: column.clone(),
                                    null_safe: true,
                                });
                            }
                            col_at(&d, &column)
                        };
                        let flag = import(n.flag, "null");
                        let held = import(n.held, "held");
                        q.list_nulls
                            .insert(*v, Some(super::null_list::NullList { flag, held }));
                    }
                    Some(None) => {
                        q.list_nulls.insert(*v, None);
                    }
                    None => {}
                }
                continue;
            }
            // Its identity / endpoint columns and the properties `Q` reads.
            let names: BTreeSet<String> = read_props
                .iter()
                .filter(|(r, _)| r == v)
                .map(|(_, p)| p.clone())
                .collect();
            let mut exported = Body::default();
            let scan =
                self.export_element_with(*v, *v, &d, &mut exported, &mut Vec::new(), &names)?;
            let Some(At::Exported { physical, .. }) = scan.at() else {
                return unsupported(format!("internal: {v} is not exported"));
            };
            // A union's key past its definition's arity is NULL (label and
            // type unions); a node's or relationship's of its own table is not.
            let exact = (shared.contains(v) || !self.binding(*v).nullable)
                && !keys.contains(v)
                && matches!(scan, Scan::Node { .. } | Scan::Rel { .. });
            for item in exported.select {
                let Some(column) = item.col_alias.as_ref().map(|a| a.0.clone()) else {
                    continue;
                };
                let key = physical.values().any(|c| *c == column);
                correlation.push(Correlated {
                    inner: col_at(&d, &column),
                    outer: item.expression.clone(),
                    column,
                    null_safe: !(key && exact),
                });
                body.select.push(item);
            }
            q.scans.insert(*v, scan);
        }
        if let Some(a) = members {
            self.members_drive(a, &d, &mut body, &mut correlation, q)?;
        }
        if body.select.is_empty() {
            body.select
                .push(select(RenderExpr::Literal(Literal::Integer(1)), "__row"));
        }
        let plan = self.snapshot(body);
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        q.from = Some(table_ref(name, &d));
        q.emitted = vec![d];
        Ok(correlation)
    }

    /// The current rows under `body`'s SELECT, leaving the segment as it is.
    /// Without the subqueries applied to them so far: their joins neither
    /// multiply nor drop rows, and dropping the conditions reading them
    /// leaves more rows, of which `D` needs only the correlation columns (a
    /// drive is a superset of the rows' keys); each later drive would
    /// otherwise copy every earlier subquery.
    fn snapshot(&mut self, body: Body) -> RenderPlan {
        let saved = (
            self.from.clone(),
            self.joins.clone(),
            self.filters.clone(),
            self.emitted.clone(),
        );
        let applied: Vec<String> = self
            .joins
            .iter()
            .filter(|j| j.table_name.starts_with(SUBQUERY_CTE))
            .map(|j| j.table_alias.clone())
            .collect();
        self.joins.retain(|j| !applied.contains(&j.table_alias));
        self.emitted.retain(|a| !applied.contains(a));
        self.filters
            .retain(|f| !table_aliases(f).iter().any(|a| applied.contains(a)));
        let plan = self.render(body);
        (self.from, self.joins, self.filters, self.emitted) = saved;
        plan
    }

    /// `Q` in the current (swapped-in) segment, grouped by the correlation:
    /// its count, or its list of the projection, as [`VALUE`]; and a list's
    /// element kind. `None` when it matches nothing.
    fn subquery_inner(
        &mut self,
        pattern: &BoundPattern,
        predicate: Option<&LogicalExpr>,
        correlation: &[Correlated],
        sq: &Subquery,
    ) -> Result<Option<(RenderPlan, Option<(Kind, Kind)>)>, LowerError> {
        self.lower_match(pattern, predicate)?;
        if let SubqueryKind::Collect(p) = &sq.kind {
            // Nested subqueries of the projection, per match.
            self.apply_subqueries(&[p])?;
        }
        self.finish_relation()?;
        if self.empty {
            return Ok(None);
        }
        let mut body = Body::default();
        for c in correlation {
            body.select.push(select(c.inner.clone(), &c.column));
            body.group_by.push(c.inner.clone());
        }
        let count = RenderExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![RenderExpr::Star],
        });
        let (value, kind) = match &sq.kind {
            // A boolean column (`Nullable(Bool)`): it reads, compares and
            // shows as a boolean.
            SubqueryKind::Exists => (
                RenderExpr::Raw(
                    current_function_mapper()
                        .cast_bool(&format!("{} > 0", render_expr_to_sql_plain(&count))),
                ),
                None,
            ),
            SubqueryKind::Count => (
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "count".to_string(),
                    args: vec![RenderExpr::Star],
                }),
                None,
            ),
            SubqueryKind::Collect(p) => {
                if self.holds_elements(p) || self.holds_maps(p) {
                    return unsupported(
                        "a pattern comprehension of nodes, relationships, paths or maps",
                    );
                }
                let Some(spelling) = current_function_mapper().lists() else {
                    return unsupported("a pattern comprehension in this SQL dialect");
                };
                let kind = self.kind(p);
                // Every match's value, NULL included (unlike collect()).
                let e = self.element(p, &HashMap::new())?;
                (
                    self.in_fixed_order(RenderExpr::Raw((spelling.collect_all)(
                        &render_expr_to_sql_plain(&e),
                    )))?,
                    Some((kind, self.kind_before_column_types(p))),
                )
            }
            // Each matched node's label and identity, once, as
            // `in_members` reads them.
            SubqueryKind::Members { node: m, .. } => {
                let Some(spelling) = current_function_mapper().lists() else {
                    return unsupported(
                        "a pattern predicate in a list comprehension in this SQL dialect",
                    );
                };
                let fields: Vec<String> = match (self.scans.get(m), self.identity(*m)?) {
                    // A node of several labels: its label is its identity's
                    // first column.
                    (Some(Scan::Labels { .. }), Some(id)) => id,
                    (Some(Scan::Node { label, .. }), Some(id)) => {
                        std::iter::once(RenderExpr::Literal(Literal::String(label.clone())))
                            .chain(id)
                            .collect()
                    }
                    _ => return Ok(None),
                }
                .iter()
                .map(render_expr_to_sql_plain)
                .collect();
                (
                    RenderExpr::Raw((spelling.distinct)(&(spelling.collect_all)(&(spelling
                        .tuple)(
                        &fields
                    )))),
                    None,
                )
            }
        };
        body.select.push(select(value, VALUE));
        Ok(Some((self.render(body), kind)))
    }

    /// How the nodes `m` of a members subquery are looked for (S11o9), `of`
    /// the comprehension's list: from its elements when they are nodes of
    /// one label and nothing of `m` but its identity is read; of no match
    /// when `m` cannot have their label (the element is none of them).
    fn members_from(&self, m: VarId, of: &'s LogicalExpr) -> MembersFrom<'s> {
        let Kind::List(element) = self.kind(of) else {
            return MembersFrom::Graph;
        };
        let Kind::Element(Elem::Node(label)) = *element else {
            return MembersFrom::Graph;
        };
        let BindingKind::Node { labels } = &self.binding(m).kind else {
            return MembersFrom::Graph;
        };
        if !labels.contains(&label) {
            return MembersFrom::Nowhere;
        }
        let read = self
            .demand
            .get(&m)
            .is_some_and(|ps| ps.iter().any(|p| !p.starts_with('#')));
        match self.schema.node_schema_opt(&label) {
            Some(schema) if !read && schema.is_standard_own_table() => {
                MembersFrom::Elements(MembersAnchor {
                    node: m,
                    of,
                    label,
                    schema,
                })
            }
            _ => MembersFrom::Graph,
        }
    }

    /// The drive `D` of a members subquery reads its list's elements: a row
    /// per element (the list's distinct identities), whose identity is node
    /// `m`'s in `Q`; and `D` and the rows are joined by a hash of the list
    /// (as well as the correlation), so each row's group holds the matches
    /// among the elements of rows with the same list. Of its identities,
    /// distinct and sorted: the same however a re-evaluated `collect()`
    /// orders it (ClickHouse evaluates a CTE per read). Two lists of the
    /// same hash share a group: an element of the one that is a match of
    /// the other is a match (of the same correlation), so `x IN` it still
    /// answers right.
    fn members_drive(
        &mut self,
        a: &MembersAnchor<'s>,
        d: &str,
        body: &mut Body,
        correlation: &mut Vec<Correlated>,
        q: &mut Segment<'s>,
    ) -> Result<(), LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a pattern predicate in a list comprehension in this SQL dialect");
        };
        const E: &str = "__cg_e";
        const ELEMENT: &str = "__cg_member";
        const KEY: &str = "__cg_members";
        let elem = Elem::Node(a.label.clone());
        let id: Vec<String> = self
            .elem_identity(&RenderExpr::Raw(E.to_string()), &elem)?
            .iter()
            .map(render_expr_to_sql_plain)
            .collect();
        let list = render_expr_to_sql_plain(&self.expr(a.of, &HashMap::new())?);
        let ids = (spelling.distinct)(&(spelling.map)(E, &(spelling.tuple)(&id), &list));
        let key = RenderExpr::Raw((spelling.fingerprint)(&(spelling.sorted)(&ids)));
        body.select.push(select(key.clone(), KEY));
        correlation.push(Correlated {
            column: KEY.to_string(),
            outer: key,
            inner: col_at(d, KEY),
            null_safe: false,
        });
        body.array_join.push(ArrayJoin {
            expression: RenderExpr::Raw(ids),
            alias: ELEMENT.to_string(),
            position: None,
        });
        let mut physical = HashMap::new();
        for (i, c) in a.schema.id_physical_columns().into_iter().enumerate() {
            let name = format!("{}__{c}", a.node);
            body.select.push(select(
                slot(&RenderExpr::Raw(ELEMENT.to_string()), i)?,
                &name,
            ));
            physical.insert(c, name);
        }
        q.scans.insert(
            a.node,
            Scan::Node {
                schema: a.schema,
                label: a.label.clone(),
                at: At::Exported {
                    alias: d.to_string(),
                    physical,
                    props: HashMap::new(),
                },
            },
        );
        Ok(())
    }

    /// Split a MATCH's WHERE: the conjuncts that read no subquery, and those
    /// that do (applied after the clause's rows are built).
    pub(super) fn split_subquery_conjuncts(
        predicate: Option<&LogicalExpr>,
    ) -> (Option<LogicalExpr>, Vec<LogicalExpr>) {
        let Some(p) = predicate else {
            return (None, Vec::new());
        };
        let (deferred, plain): (Vec<&LogicalExpr>, Vec<&LogicalExpr>) = logical_conjuncts(p)
            .into_iter()
            .partition(|c| !subquery_vars(c).is_empty());
        if deferred.is_empty() {
            return (Some(p.clone()), Vec::new());
        }
        (
            logical_and(plain.into_iter().cloned().collect()),
            deferred.into_iter().cloned().collect(),
        )
    }
}

/// How a members subquery's nodes are looked for ([`Lowerer::members_from`]).
enum MembersFrom<'s> {
    Elements(MembersAnchor<'s>),
    /// It has none (`x IN` it is false).
    Nowhere,
    /// Its matches in the whole graph.
    Graph,
}

/// A members subquery's node looked for from its list's elements, nodes of
/// one label ([`Lowerer::members_drive`]).
struct MembersAnchor<'s> {
    node: VarId,
    of: &'s LogicalExpr,
    label: String,
    schema: &'s NodeSchema,
}

/// What a subquery's placeholder is, for `Lowerer::kind`: a list's element
/// kind is known once the subquery is applied.
pub(super) fn placeholder_kind(lowerer: &Lowerer<'_>, e: &LogicalExpr) -> Option<Kind> {
    let v = subquery_var(e)?;
    let sq = lowerer.subqueries.iter().find(|s| s.var == v)?;
    Some(match &sq.kind {
        SubqueryKind::Exists => Kind::Boolean,
        SubqueryKind::Count => Kind::Scalar,
        SubqueryKind::Collect(_) => lowerer
            .var_kind(v)
            .unwrap_or_else(|| Kind::List(Box::new(Kind::Unknown))),
        // Read by `x IN` only (`Lowerer::in_members`).
        SubqueryKind::Members { .. } => Kind::List(Box::new(Kind::Unknown)),
    })
}
