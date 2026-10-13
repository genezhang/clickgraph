//! A NULL list (user 2026-10-13; #1377, #1364, #1454): ClickHouse holds no
//! NULL array (`Nullable(Array)` is not a type), so a list value that is
//! NULL is held as `[]`, and what tells NULL from `[]` reads where the NULL
//! comes from instead ([`Lowerer::list_null`]): `IS [NOT] NULL`, `size()`,
//! `coalesce()`, `count()`, `collect()`, `+`, `IN` and `=` of lists, and a
//! grouped or DISTINCT list. A WITH carries the condition of a list it
//! projects as a column of its own (`{v}__null`); the final RETURN returns
//! NULL where it holds. Never `empty(l)`: a list that is `[]` is not NULL
//! (`collect()` of no rows, a comprehension that keeps nothing).

use std::collections::HashMap;

use crate::query_planner::logical_expr::{self as lx, LogicalExpr, OperatorApplication as LOp};
use crate::render_plan::render_expr::{Literal, OperatorApplication, RenderCase, RenderExpr};

use super::unwind::{comprehension, Kind};
use super::{parse_var, LowerError, Lowerer};
use crate::bound_plan::types::SubqueryKind;

type Items = HashMap<crate::bound_plan::types::VarId, RenderExpr>;

/// When a list value is NULL.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ListNull {
    /// Never (a literal, `collect()`, a pattern comprehension, `range()`).
    Never,
    /// Where this condition holds.
    When(RenderExpr),
    /// Not known here: read as it is.
    Unknown,
}

impl ListNull {
    /// The condition (`Never`: false); `None` when not known.
    pub(super) fn condition(&self) -> Option<RenderExpr> {
        match self {
            ListNull::Never => Some(RenderExpr::Literal(Literal::Boolean(false))),
            ListNull::When(c) => Some(c.clone()),
            ListNull::Unknown => None,
        }
    }

    /// NULL when any of `parts` is.
    pub(super) fn any(parts: Vec<ListNull>) -> ListNull {
        let mut conditions = Vec::new();
        for p in parts {
            match p {
                ListNull::Unknown => return ListNull::Unknown,
                ListNull::Never | ListNull::When(RenderExpr::Literal(Literal::Boolean(false))) => {}
                ListNull::When(RenderExpr::Literal(Literal::Boolean(true))) => {
                    return ListNull::When(RenderExpr::Literal(Literal::Boolean(true)))
                }
                ListNull::When(c) => conditions.push(c),
            }
        }
        match conditions.len() {
            0 => ListNull::Never,
            _ => ListNull::When(super::or_all(conditions)),
        }
    }

    /// NULL when every one of `parts` is.
    fn all(parts: Vec<ListNull>) -> ListNull {
        let mut conditions = Vec::new();
        for p in parts {
            match p {
                ListNull::Never | ListNull::When(RenderExpr::Literal(Literal::Boolean(false))) => {
                    return ListNull::Never
                }
                ListNull::Unknown => return ListNull::Unknown,
                ListNull::When(RenderExpr::Literal(Literal::Boolean(true))) => {}
                ListNull::When(c) => conditions.push(c),
            }
        }
        match super::and_all(conditions) {
            None => ListNull::When(RenderExpr::Literal(Literal::Boolean(true))),
            Some(c) => ListNull::When(c),
        }
    }
}

/// `e IS NULL`, as a condition.
fn is_null(e: RenderExpr) -> RenderExpr {
    match e {
        RenderExpr::Literal(Literal::Null) => RenderExpr::Literal(Literal::Boolean(true)),
        RenderExpr::Literal(_) | RenderExpr::List(_) => {
            RenderExpr::Literal(Literal::Boolean(false))
        }
        e => RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: lx::Operator::IsNull,
            operands: vec![e],
        }),
    }
}

impl Lowerer<'_> {
    /// When the list value `e` is NULL ([`ListNull`]). Of an expression
    /// that is not a list, `Unknown`.
    pub(super) fn list_null(&self, e: &LogicalExpr, items: &Items) -> Result<ListNull, LowerError> {
        use lx::Operator as O;
        let kind = self.kind(e);
        if kind == Kind::Null {
            return Ok(ListNull::When(RenderExpr::Literal(Literal::Boolean(true))));
        }
        if !matches!(kind, Kind::List(_)) {
            return Ok(ListNull::Unknown);
        }
        Ok(match e {
            LogicalExpr::List(_) => ListNull::Never,
            LogicalExpr::AggregateFnCall(f) if f.name.eq_ignore_ascii_case("collect") => {
                ListNull::Never
            }
            LogicalExpr::Case(c) => {
                let conditions = self.case_conditions(c, items)?;
                let mut when_then = Vec::new();
                let mut parts = Vec::new();
                for (condition, (_, t)) in conditions.into_iter().zip(&c.when_then) {
                    let null = self.result_null(t, items)?;
                    if let Some(condition) = condition {
                        parts.push(null.clone());
                        let Some(n) = null.condition() else {
                            return Ok(ListNull::Unknown);
                        };
                        when_then.push((condition, n));
                    }
                }
                let else_null = match &c.else_expr {
                    Some(e) => self.result_null(e, items)?,
                    None => ListNull::When(RenderExpr::Literal(Literal::Boolean(true))),
                };
                parts.push(else_null.clone());
                if parts.iter().all(|p| *p == ListNull::Never) {
                    return Ok(ListNull::Never);
                }
                let Some(else_condition) = else_null.condition() else {
                    return Ok(ListNull::Unknown);
                };
                if when_then.is_empty() {
                    return Ok(ListNull::When(else_condition));
                }
                ListNull::When(RenderExpr::Case(RenderCase {
                    expr: None,
                    when_then,
                    else_expr: Some(Box::new(else_condition)),
                }))
            }
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                match (op.operator, op.operands.as_slice()) {
                    // `+` of lists, or of a list and a value: NULL when
                    // either is (a value that may be NULL is refused,
                    // `Lowerer::list_addition`).
                    (O::Addition, [a, b]) => {
                        let mut parts = Vec::new();
                        for x in [a, b] {
                            parts.push(match self.kind(x) {
                                Kind::List(_) | Kind::Null => self.list_null(x, items)?,
                                _ => ListNull::Never,
                            });
                        }
                        ListNull::any(parts)
                    }
                    _ => ListNull::Unknown,
                }
            }
            LogicalExpr::ArraySlicing { array, .. } => self.list_null(array, items)?,
            LogicalExpr::TableAlias(lx::TableAlias(n)) => match parse_var(n) {
                // An item of this projection, in its ORDER BY / WHERE.
                Some(v) if items.contains_key(&v) => match self.projected.get(&v) {
                    Some(p) => self.list_null(p, &HashMap::new())?,
                    None => ListNull::Unknown,
                },
                Some(v) => match self.list_nulls.get(&v) {
                    Some(Some(c)) => ListNull::When(c.clone()),
                    Some(None) => ListNull::Never,
                    None => ListNull::Unknown,
                },
                None => ListNull::Unknown,
            },
            LogicalExpr::ScalarFnCall(f) => {
                if let Some(v) = crate::bound_plan::expr::subquery_var(e) {
                    // A pattern comprehension: the empty list of no match.
                    return Ok(match self.subqueries.iter().find(|s| s.var == v) {
                        Some(s)
                            if matches!(
                                s.kind,
                                SubqueryKind::Collect(_) | SubqueryKind::Members { .. }
                            ) =>
                        {
                            ListNull::Never
                        }
                        _ => ListNull::Unknown,
                    });
                }
                if let Some((_, _, _, list)) = comprehension(f) {
                    // `[x IN null | …]` is NULL.
                    return self.list_null(list, items);
                }
                match (f.name.to_ascii_lowercase().as_str(), f.args.as_slice()) {
                    // NULL of a NULL string or delimiter.
                    ("split", args) => {
                        let mut parts = Vec::new();
                        for a in args {
                            parts.push(ListNull::When(is_null(self.expr(a, items)?)));
                        }
                        ListNull::any(parts)
                    }
                    ("tail" | "reverse", [l]) => self.list_null(l, items)?,
                    ("coalesce", args) => {
                        let mut parts = Vec::new();
                        for a in args {
                            parts.push(match self.kind(a) {
                                Kind::List(_) | Kind::Null => self.list_null(a, items)?,
                                _ => ListNull::Unknown,
                            });
                        }
                        ListNull::all(parts)
                    }
                    // Of a node or a path an OPTIONAL MATCH left NULL.
                    ("labels" | "nodes" | "relationships", [LogicalExpr::TableAlias(a)])
                        if parse_var(&a.0).is_some_and(|v| !self.binding(v).nullable) =>
                    {
                        ListNull::Never
                    }
                    ("labels" | "nodes" | "relationships", [x]) => ListNull::When(self.expr(
                        &LogicalExpr::OperatorApplicationExp(LOp {
                            operator: O::IsNull,
                            operands: vec![x.clone()],
                        }),
                        items,
                    )?),
                    ("range", _) => ListNull::Never,
                    _ => ListNull::Unknown,
                }
            }
            _ => ListNull::Unknown,
        })
    }

    /// When a CASE's result `t` is NULL: the NULL literal always; a list
    /// as [`Lowerer::list_null`] says.
    fn result_null(&self, t: &LogicalExpr, items: &Items) -> Result<ListNull, LowerError> {
        match t {
            LogicalExpr::Literal(lx::Literal::Null) => {
                Ok(ListNull::When(RenderExpr::Literal(Literal::Boolean(true))))
            }
            t => self.list_null(t, items),
        }
    }
}

impl Lowerer<'_> {
    /// A function call `e` (`f`): `split()` of a NULL string is NULL, held
    /// as `[]` (ClickHouse splits NULL into `['']`); `coalesce()` of lists
    /// is the first that is not NULL by its source. Any other as
    /// [`Lowerer::scalar_fn`].
    pub(super) fn list_fn(
        &self,
        e: &LogicalExpr,
        f: &lx::ScalarFnCall,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        match f.name.to_ascii_lowercase().as_str() {
            "split" => Ok(match self.list_null(e, items)? {
                // Of a NULL literal: no split at all.
                ListNull::When(RenderExpr::Literal(Literal::Boolean(true))) => {
                    RenderExpr::List(Vec::new())
                }
                // Of the arguments, '' where NULL: an array is no `Nullable`
                // (ClickHouse 25 refuses to split a `Nullable(String)`).
                ListNull::When(c) => {
                    let args = f
                        .args
                        .iter()
                        .map(|a| match a {
                            LogicalExpr::Literal(_) => a.clone(),
                            a => LogicalExpr::ScalarFnCall(lx::ScalarFnCall {
                                name: "coalesce".to_string(),
                                args: vec![
                                    a.clone(),
                                    LogicalExpr::Literal(lx::Literal::String(String::new())),
                                ],
                            }),
                        })
                        .collect();
                    let split = lx::ScalarFnCall {
                        name: f.name.clone(),
                        args,
                    };
                    empty_where(c, self.scalar_fn(&split, items)?, false)
                }
                _ => self.scalar_fn(f, items)?,
            }),
            "coalesce" if matches!(self.kind(e), Kind::List(_)) => {
                self.coalesce_lists(&f.args, items)
            }
            _ => self.scalar_fn(f, items),
        }
    }

    /// `coalesce(args)` of lists: the first that is not NULL.
    fn coalesce_lists(
        &self,
        args: &[LogicalExpr],
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let [first, rest @ ..] = args else {
            return Ok(RenderExpr::Literal(Literal::Null));
        };
        if rest.is_empty() {
            return self.expr(first, items);
        }
        let null = match self.kind(first) {
            Kind::List(_) | Kind::Null => self.list_null(first, items)?,
            _ => ListNull::Unknown,
        };
        Ok(match null {
            ListNull::Never => self.expr(first, items)?,
            ListNull::When(RenderExpr::Literal(Literal::Boolean(true))) => {
                self.coalesce_lists(rest, items)?
            }
            ListNull::When(c) => RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(c, self.coalesce_lists(rest, items)?)],
                else_expr: Some(Box::new(self.expr(first, items)?)),
            }),
            ListNull::Unknown => {
                RenderExpr::ScalarFnCall(crate::render_plan::render_expr::ScalarFnCall {
                    name: "coalesce".to_string(),
                    args: vec![self.expr(first, items)?, self.coalesce_lists(rest, items)?],
                })
            }
        })
    }
}

/// The list `value`, empty where `c` holds (a NULL list, held as `[]`).
/// `typed`: the empty list of `value`'s type (its elements may be of
/// several, a `Variant`, which ClickHouse converts no `[]` to).
pub(super) fn empty_where(c: RenderExpr, value: RenderExpr, typed: bool) -> RenderExpr {
    let empty = match typed {
        true => empty_like(&value),
        false => RenderExpr::List(Vec::new()),
    };
    RenderExpr::Case(RenderCase {
        expr: None,
        when_then: vec![(c, empty)],
        else_expr: Some(Box::new(value)),
    })
}

/// An empty list of the type of the list `value` (its elements kept where
/// false), or `[]` in a dialect without lists.
pub(super) fn empty_like(value: &RenderExpr) -> RenderExpr {
    use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
    match crate::sql_generator::function_mapper::current_function_mapper().lists() {
        Some(spelling) => RenderExpr::Raw((spelling.filter)(
            "__cg_e",
            "false",
            &render_expr_to_sql_plain(value),
        )),
        None => RenderExpr::List(Vec::new()),
    }
}

impl Lowerer<'_> {
    /// `count(l)` / `collect(l)` (`name`, `distinct`) of a list `l` that is
    /// NULL where `c` holds: of the rows where it does not.
    pub(super) fn aggregate_present_list(
        &self,
        name: &str,
        l: &LogicalExpr,
        c: &RenderExpr,
        distinct: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        use crate::render_plan::render_expr::AggregateFnCall;
        let m = crate::sql_generator::function_mapper::current_function_mapper();
        let Some(spelling) = m.lists() else {
            return super::unsupported(format!(
                "{name}() of a list that may be NULL in this SQL dialect"
            ));
        };
        if name == "count" && !distinct {
            return Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
                name: "count".to_string(),
                args: vec![RenderExpr::Case(RenderCase {
                    expr: None,
                    when_then: vec![(c.clone(), RenderExpr::Literal(Literal::Null))],
                    else_expr: Some(Box::new(RenderExpr::Literal(Literal::Integer(1)))),
                })],
            }));
        }
        let plain =
            crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
        let value = plain(&self.element(l, items)?);
        let present = plain(&RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: lx::Operator::Not,
            operands: vec![c.clone()],
        }));
        let list = match (&self.collect_order, name) {
            (Some(key), "collect") => (spelling.ordered_collect)(&value, &plain(key), &present),
            _ => (spelling.collect_if)(&value, &present),
        };
        let list = match distinct {
            true => (spelling.distinct)(&list),
            false => list,
        };
        Ok(match name {
            "count" => RenderExpr::Raw(m.list_size(&list)),
            // `[]` of no row, as any `collect()`.
            _ => RenderExpr::ScalarFnCall(crate::render_plan::render_expr::ScalarFnCall {
                name: "coalesce".to_string(),
                args: vec![RenderExpr::Raw(list), RenderExpr::List(Vec::new())],
            }),
        })
    }
}
