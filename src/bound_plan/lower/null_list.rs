//! A NULL list (user 2026-10-13; #1377, #1364, #1454): ClickHouse holds no
//! NULL array (`Nullable(Array)` is not a type). Where ClickGraph knows
//! when a list is NULL ([`Lowerer::list_null`]: a CASE's null branch,
//! `split()` of NULL, a list a WITH carries, and list functions of them),
//! the operations that tell NULL from `[]` read that condition, and the
//! list as they take it ([`Lowerer::held_list`]) holds its NULL as `[]`:
//! `IS [NOT] NULL`, `size()`, `isEmpty()`, `head()` / `last()` / `l[i]`,
//! `coalesce()`, `count()` / `collect()` / `min()` / `max()`, `+`, `IN`,
//! comparisons, `reduce()`, `ORDER BY`, UNWIND, grouping and DISTINCT. A
//! WITH carries the condition (`{v}__null`) and the held list
//! (`{v}__held`) beside the list as anything else reads it; the final
//! RETURN returns NULL where the condition holds. Anywhere else a list is
//! lowered as it always was. Never `empty(l)`: a list that is `[]` is not
//! NULL (`collect()` of no rows, a comprehension that keeps nothing). A
//! dialect with NULL lists (Spark) needs none of this.

use std::collections::HashMap;

use crate::query_planner::logical_expr::{self as lx, LogicalExpr, OperatorApplication as LOp};
use crate::render_plan::render_expr::{Literal, OperatorApplication, RenderCase, RenderExpr};

use super::unwind::{comprehension, Kind};
use super::{parse_var, LowerError, Lowerer};
use crate::bound_plan::types::SubqueryKind;

type Items = HashMap<crate::bound_plan::types::VarId, RenderExpr>;

/// What a WITH carries of a list that may be NULL (`Lowerer::list_nulls`):
/// when it is NULL, and the list held so (`[]` where it is).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct NullList {
    pub(super) flag: RenderExpr,
    pub(super) held: RenderExpr,
}

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

/// The dialect holds no NULL list (ClickHouse): one is held as `[]`.
pub(super) fn null_as_empty() -> bool {
    crate::sql_generator::function_mapper::current_function_mapper()
        .lists()
        .is_some_and(|l| l.null_as_empty)
}

impl Lowerer<'_> {
    /// The list `e` as an operation that tells its NULL by its source
    /// ([`Lowerer::list_null`]) reads it: held as ClickHouse holds a NULL
    /// list, `[]` ([`Lowerer::held_raw`]), where its NULL is known; any
    /// other as [`Lowerer::expr`] lowers it.
    pub(super) fn held_list(
        &self,
        e: &LogicalExpr,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        if self.held_form(e, items) && matches!(self.list_null(e, items)?, ListNull::When(_)) {
            return self.held_raw(e, items);
        }
        self.expr(e, items)
    }

    /// Whether `e` is lowered differently held ([`Lowerer::held_raw`]):
    /// a list a WITH carries beside its condition, a CASE of lists, and
    /// the list functions whose list is NULL where theirs is.
    fn held_form(&self, e: &LogicalExpr, items: &Items) -> bool {
        null_as_empty()
            && match e {
                // Carried by a WITH, or an item of this projection (in its
                // WHERE / ORDER BY: held as the projection holds it).
                LogicalExpr::TableAlias(lx::TableAlias(n)) => parse_var(n).is_some_and(|v| {
                    if items.contains_key(&v) {
                        self.held_items.borrow().contains_key(&v)
                    } else {
                        matches!(self.list_nulls.get(&v), Some(Some(_)))
                    }
                }),
                LogicalExpr::Case(_) | LogicalExpr::ArraySlicing { .. } => true,
                LogicalExpr::ScalarFnCall(f) => {
                    comprehension(f).is_some()
                        || matches!(
                            f.name.to_ascii_lowercase().as_str(),
                            "split" | "coalesce" | "tail" | "reverse"
                        )
                }
                LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                    op.operator == lx::Operator::Addition
                }
                LogicalExpr::AggregateFnCall(f) => {
                    matches!(f.name.to_ascii_lowercase().as_str(), "min" | "max")
                }
                _ => false,
            }
    }

    /// `e` ([`Lowerer::held_form`]) held: its NULL as `[]`, each list it
    /// reads held too.
    fn held_raw(&self, e: &LogicalExpr, items: &Items) -> Result<RenderExpr, LowerError> {
        match e {
            LogicalExpr::TableAlias(lx::TableAlias(n)) => match parse_var(n) {
                // An item of this projection: as it holds it (of its group,
                // where it groups).
                Some(v) if items.contains_key(&v) => match self.held_items.borrow().get(&v) {
                    Some(n) => Ok(n.held.clone()),
                    None => self.expr(e, items),
                },
                Some(v) => match self.list_nulls.get(&v) {
                    Some(Some(n)) => Ok(n.held.clone()),
                    _ => self.expr(e, items),
                },
                None => self.expr(e, items),
            },
            LogicalExpr::Case(c) => self.case_held(c, true, items),
            LogicalExpr::AggregateFnCall(f) => match self.min_max_list(f, items)? {
                Some((_, held)) => Ok(held),
                None => self.expr(e, items),
            },
            LogicalExpr::ScalarFnCall(f) => {
                if let Some((how, x, body, list)) = comprehension(f) {
                    return self.comprehension(how, x, body, list, items, true);
                }
                match (f.name.to_ascii_lowercase().as_str(), f.args.as_slice()) {
                    ("coalesce", args) if matches!(self.kind(e), Kind::List(_)) => {
                        self.coalesce_lists(args, items, true)
                    }
                    ("tail" | "reverse", [l]) => Ok(RenderExpr::ScalarFnCall(
                        crate::render_plan::render_expr::ScalarFnCall {
                            name: f.name.clone(),
                            args: vec![self.held_list(l, items)?],
                        },
                    )),
                    _ => self.list_fn(e, f, items),
                }
            }
            LogicalExpr::ArraySlicing { array, from, to } => Ok(RenderExpr::ArraySlicing {
                array: Box::new(self.held_list(array, items)?),
                from: from
                    .as_ref()
                    .map(|f| self.expr(f, items).map(Box::new))
                    .transpose()?,
                to: to
                    .as_ref()
                    .map(|t| self.expr(t, items).map(Box::new))
                    .transpose()?,
            }),
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                match op.operands.as_slice() {
                    [a, b] if op.operator == lx::Operator::Addition => {
                        match self.list_addition(a, b, items, true)? {
                            Some(v) => Ok(v),
                            None => self.expr(e, items),
                        }
                    }
                    _ => self.expr(e, items),
                }
            }
            e => self.expr(e, items),
        }
    }

    /// When the list value `e` is NULL ([`ListNull`]). Of an expression
    /// that is not a list, `Unknown`.
    pub(super) fn list_null(&self, e: &LogicalExpr, items: &Items) -> Result<ListNull, LowerError> {
        use lx::Operator as O;
        // A dialect with NULL lists (Spark) tells them as they are.
        if !null_as_empty() {
            return Ok(ListNull::Unknown);
        }
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
            LogicalExpr::AggregateFnCall(f) => match self.min_max_list(f, items)? {
                Some((none, _)) => ListNull::When(none),
                None => ListNull::Unknown,
            },
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
                Some(v) if items.contains_key(&v) => {
                    let held = self.held_items.borrow().get(&v).map(|n| n.flag.clone());
                    match (held, self.projected.get(&v)) {
                        (Some(flag), _) => ListNull::When(flag),
                        (None, Some(p)) => match self.list_null(p, &HashMap::new())? {
                            // (Its condition unknown to the projection.)
                            ListNull::When(_) => ListNull::Unknown,
                            n => n,
                        },
                        (None, None) => ListNull::Unknown,
                    }
                }
                Some(v) => match self.list_nulls.get(&v) {
                    Some(Some(n)) => ListNull::When(n.flag.clone()),
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
                            parts.push(self.scalar_null(a, items)?);
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
            // A list property (an `Array` column, VALUE_TYPES.md T2, #1485)
            // holds no NULL: it is NULL where an OPTIONAL MATCH left its node
            // or relationship NULL.
            LogicalExpr::PropertyAccessExp(pa) => match parse_var(&pa.table_alias.0) {
                Some(v) if !self.binding(v).nullable => ListNull::Never,
                Some(_) => ListNull::When(self.expr(
                    &LogicalExpr::OperatorApplicationExp(LOp {
                        operator: O::IsNull,
                        operands: vec![LogicalExpr::TableAlias(pa.table_alias.clone())],
                    }),
                    items,
                )?),
                None => ListNull::Unknown,
            },
            _ => ListNull::Unknown,
        })
    }

    /// When a scalar `e` (`split()`'s string or delimiter) is NULL, where
    /// it is known: the NULL literal; a CASE's null or missing branch (its
    /// other results taken as values, as ClickHouse's `split()` of them
    /// is). A column's or a function's NULL is not known here.
    fn scalar_null(&self, e: &LogicalExpr, items: &Items) -> Result<ListNull, LowerError> {
        Ok(match e {
            LogicalExpr::Literal(lx::Literal::Null) => {
                ListNull::When(RenderExpr::Literal(Literal::Boolean(true)))
            }
            LogicalExpr::Literal(_) | LogicalExpr::Parameter(_) => ListNull::Never,
            LogicalExpr::Case(c) => {
                let null = |t: Option<&LogicalExpr>| {
                    t.is_none_or(|t| matches!(t, LogicalExpr::Literal(lx::Literal::Null)))
                };
                let mut when_then = Vec::new();
                for (condition, (_, t)) in self
                    .case_conditions(c, items)?
                    .into_iter()
                    .zip(&c.when_then)
                {
                    if let Some(condition) = condition {
                        let n = RenderExpr::Literal(Literal::Boolean(null(Some(t))));
                        when_then.push((condition, n));
                    }
                }
                let else_null = null(c.else_expr.as_deref());
                let never = RenderExpr::Literal(Literal::Boolean(false));
                if !else_null && when_then.iter().all(|(_, t)| *t == never) {
                    return Ok(ListNull::Never);
                }
                if when_then.is_empty() {
                    return Ok(ListNull::When(RenderExpr::Literal(Literal::Boolean(
                        else_null,
                    ))));
                }
                ListNull::When(RenderExpr::Case(RenderCase {
                    expr: None,
                    when_then,
                    else_expr: Some(Box::new(RenderExpr::Literal(Literal::Boolean(else_null)))),
                }))
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
            // The first list not NULL by its source.
            "coalesce" if matches!(self.kind(e), Kind::List(_)) => {
                self.coalesce_lists(&f.args, items, false)
            }
            // NULL of a NULL list (held as `[]`).
            "isempty" if matches!(f.args.as_slice(), [l] if matches!(self.kind(l), Kind::List(_))) =>
            {
                let value = self.scalar_fn(f, items)?;
                Ok(match self.list_null(&f.args[0], items)? {
                    ListNull::When(c) => RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(c, RenderExpr::Literal(Literal::Null))],
                        else_expr: Some(Box::new(value)),
                    }),
                    _ => value,
                })
            }
            "split" if null_as_empty() => Ok(match self.list_null(e, items)? {
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
            _ => self.scalar_fn(f, items),
        }
    }

    /// `coalesce(args)` of lists: the first that is not NULL by its
    /// source; `held`: each as an operation that knows its NULL takes it
    /// ([`Lowerer::held_list`]), else as lowered.
    fn coalesce_lists(
        &self,
        args: &[LogicalExpr],
        items: &Items,
        held: bool,
    ) -> Result<RenderExpr, LowerError> {
        let value = |a: &LogicalExpr| match held {
            true => self.held_list(a, items),
            false => self.expr(a, items),
        };
        let [first, rest @ ..] = args else {
            return Ok(RenderExpr::Literal(Literal::Null));
        };
        if rest.is_empty() {
            return value(first);
        }
        let null = match self.kind(first) {
            Kind::List(_) | Kind::Null => self.list_null(first, items)?,
            _ => ListNull::Unknown,
        };
        Ok(match null {
            ListNull::Never => value(first)?,
            ListNull::When(RenderExpr::Literal(Literal::Boolean(true))) => {
                self.coalesce_lists(rest, items, held)?
            }
            ListNull::When(c) => RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(c, self.coalesce_lists(rest, items, held)?)],
                else_expr: Some(Box::new(value(first)?)),
            }),
            ListNull::Unknown => {
                RenderExpr::ScalarFnCall(crate::render_plan::render_expr::ScalarFnCall {
                    name: "coalesce".to_string(),
                    args: vec![value(first)?, self.coalesce_lists(rest, items, held)?],
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

/// The list `value`, empty where `c` holds (a NULL list, held as `[]`):
/// its elements kept unless `c` (`value` read once, of its own type).
pub(super) fn empty_unless(c: RenderExpr, value: RenderExpr) -> RenderExpr {
    use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
    match crate::sql_generator::function_mapper::current_function_mapper().lists() {
        Some(spelling) => RenderExpr::Raw((spelling.filter)(
            "__cg_e",
            &format!("NOT ({})", render_expr_to_sql_plain(&c)),
            &render_expr_to_sql_plain(&value),
        )),
        None => empty_where(c, value, false),
    }
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
        let value = plain(&self.held_list(l, items)?);
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

impl Lowerer<'_> {
    /// `min(l)` / `max(l)` (DISTINCT or not) of a list `l` that may be NULL:
    /// when it is NULL — of a group with no list that is not NULL — and its
    /// value held (of the lists that are not NULL; `[]` of none).
    pub(super) fn min_max_list(
        &self,
        f: &lx::AggregateFnCall,
        items: &Items,
    ) -> Result<Option<(RenderExpr, RenderExpr)>, LowerError> {
        use crate::render_plan::render_expr::AggregateFnCall;
        let name = f.name.to_ascii_lowercase();
        let Some(arg) = min_max_arg(f) else {
            return Ok(None);
        };
        if !matches!(self.kind(arg), Kind::List(_)) || self.holds_elements(arg) {
            return Ok(None);
        }
        let ListNull::When(c) = self.list_null(arg, items)? else {
            return Ok(None);
        };
        let m = crate::sql_generator::function_mapper::current_function_mapper();
        let Some(spelling) = m.lists() else {
            return super::unsupported(format!("{name}() of lists in this SQL dialect"));
        };
        let plain =
            crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
        let present = plain(&RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: lx::Operator::Not,
            operands: vec![c.clone()],
        }));
        // (DISTINCT changes no least or greatest.)
        let value = plain(&self.held_list(arg, items)?);
        let held = RenderExpr::Raw((spelling.aggregate_if)(&name, &value, &present));
        let counted = RenderExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(c, RenderExpr::Literal(Literal::Null))],
                else_expr: Some(Box::new(RenderExpr::Literal(Literal::Integer(1)))),
            })],
        });
        let none = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: lx::Operator::Equal,
            operands: vec![counted, RenderExpr::Literal(Literal::Integer(0))],
        });
        Ok(Some((none, held)))
    }
}

/// The list `min(l)` / `max(l)` (DISTINCT or not) is of, as an argument.
pub(super) fn min_max_arg(f: &lx::AggregateFnCall) -> Option<&LogicalExpr> {
    if !matches!(f.name.to_ascii_lowercase().as_str(), "min" | "max") {
        return None;
    }
    match f.args.as_slice() {
        [LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)]
            if op.operator == lx::Operator::Distinct && op.operands.len() == 1 =>
        {
            Some(&op.operands[0])
        }
        [one] => Some(one),
        _ => None,
    }
}
