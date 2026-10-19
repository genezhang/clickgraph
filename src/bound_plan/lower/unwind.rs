//! UNWIND (P-4c S7c, `docs/design/EXPLICIT_SCOPE.md` §3, §4.4): one record
//! per element of a list; an empty list or NULL gives none, and a value that
//! is not a list gives one record of itself.
//!
//! The rows so far become a CTE whose SELECT repeats each of them once per
//! element (`ARRAY JOIN`, `FunctionMapper::unwind`) and exports the scope
//! plus the element as a value, and a new segment reads from it (as after a
//! SKIP / LIMIT).
//!
//! Whether the expression is a list is decided here, from the expression
//! ([`Kind`]): a list literal, `collect()`, `range()`, … is a list; a
//! literal, an arithmetic or comparison, a property whose type the schema
//! declares is not (UNWIND reads it as the list of itself, or of nothing
//! when NULL); a value of unknown type (a parameter, an undeclared property,
//! …) is read as a list and fails the query when it is not one, rather than
//! repeating a row per entry of a map.
//!
//! Rows in an order (after an ORDER BY) keep it, and each row's elements
//! follow it in list order: the rows are numbered in their order first (a
//! CTE), and the next rows are ordered by that number, then by the
//! element's position.
//!
//! A node, relationship or path in the list is `Unsupported` (S7e lists),
//! as is a property of the element (a map).

use std::collections::{BTreeSet, HashMap};

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::schema_types::{KeyPartType, SchemaType};
use crate::query_planner::logical_expr::{self as lx, LogicalExpr};
use crate::render_plan::render_expr::{Literal, RenderCase, RenderExpr, TableAlias};
use crate::render_plan::RenderPlan;
use crate::render_plan::{ArrayJoin, OrderByItem, OrderByOrder};
use crate::sql_generator::emitters::clickhouse::to_sql_query::{
    order_keys_to_sql_plain, render_expr_to_sql_plain, render_plan_parts_plain,
};
use crate::sql_generator::function_mapper::{current_function_mapper, Unwind};

use super::elements::Elem;
use super::{
    col_at, parse_var, select, table_ref, unsupported, Body, Exports, LowerError, Lowerer,
    RowOrder, Scan,
};
use crate::bound_plan::types::{BindingKind, VarId};

/// The element a SELECT's `ARRAY JOIN` repeats its row for, and its position
/// in the list.
const ELEMENT: &str = "__cg_element";
const POSITION: &str = "__cg_position";
/// A row's number in the order of the rows ([`Lowerer::number_rows`]).
const ROW_NUMBER: &str = "__cg_row";
/// The alias of the table of one row (`Unwind::one_row`).
const ONE_ROW: &str = "__cg_one";

impl Lowerer<'_> {
    /// What a projected, unwound or comprehension variable is.
    pub(super) fn var_kind(&self, v: VarId) -> Option<Kind> {
        let before = self
            .no_column_kinds
            .get()
            .then(|| self.kinds_before.get(&v).cloned())
            .flatten();
        before
            .or_else(|| self.kinds.get(&v).cloned())
            .or_else(|| self.local_kinds.borrow().get(&v).cloned())
    }

    /// Record `kind`, the kind of `expr`, as variable `v`'s
    /// ([`Self::set_kind`]).
    pub(super) fn record_kind(&mut self, v: VarId, expr: &LogicalExpr, kind: Kind) {
        let before = self.kind_before_column_types(expr);
        self.set_kind(v, kind, before);
    }

    /// Variable `v` is of `kind`, and of `before` with properties' column
    /// types left unread (`kind_before_column_types`). Every variable's kind
    /// is set here, so the two never disagree on which variables they know.
    pub(super) fn set_kind(&mut self, v: VarId, kind: Kind, before: Kind) {
        match before != kind {
            true => self.kinds_before.insert(v, before),
            false => self.kinds_before.remove(&v),
        };
        self.kinds.insert(v, kind);
    }

    /// `nodes(p)` / `relationships(p)` (`f`) whose elements are all of one
    /// label (one definition): what they are. A variable-length part must
    /// carry them as tuples (the demand pass) and walk one definition. A
    /// path an OPTIONAL MATCH may leave NULL is not: its list would be NULL.
    pub(super) fn path_elem(&self, f: &lx::ScalarFnCall) -> Option<Elem> {
        let (p, nodes) = self.path_part(f)?;
        if self.grouped_paths.contains(&p) {
            return None;
        }
        let elements = self.paths.get(&p)?;
        let mut elem: Option<Elem> = None;
        let mut agree = |e: Elem| -> Option<()> {
            match &elem {
                Some(x) if *x != e => None,
                _ => {
                    elem = Some(e);
                    Some(())
                }
            }
        };
        if nodes {
            for v in &elements.nodes {
                agree(self.elem_of(*v)?)?;
            }
        }
        for r in &elements.rels {
            match self.scans.get(r)? {
                Scan::Path {
                    walked: super::Walked::One { schema, rel_type },
                    node_tuples,
                    rel_tuples,
                    ..
                } => {
                    if nodes && *node_tuples && schema.from_node == schema.to_node {
                        agree(Elem::Node(schema.from_node.clone()))?;
                    } else if !nodes && *rel_tuples {
                        agree(Elem::Rel {
                            rel_type: rel_type.clone(),
                            from: schema.from_node.clone(),
                            to: schema.to_node.clone(),
                        })?;
                    } else {
                        return None;
                    }
                }
                Scan::Path { .. } => return None,
                _ if !nodes => agree(self.elem_of(*r)?)?,
                _ => {}
            }
        }
        elem
    }

    /// The path and whether `f` is its `nodes()` (else `relationships()`).
    pub(super) fn path_part(&self, f: &lx::ScalarFnCall) -> Option<(VarId, bool)> {
        let [LogicalExpr::TableAlias(lx::TableAlias(n))] = f.args.as_slice() else {
            return None;
        };
        let p = parse_var(n).filter(|p| matches!(self.binding(*p).kind, BindingKind::Path))?;
        if self.binding(p).nullable {
            return None;
        }
        match f.name.to_ascii_lowercase().as_str() {
            "nodes" => Some((p, true)),
            "relationships" => Some((p, false)),
            _ => None,
        }
    }

    /// A variable-length relationship's list as a list's elements: what
    /// they are, when its walk carries them (one definition).
    pub(super) fn walk_rel_elem(&self, r: VarId) -> Option<Elem> {
        match self.scans.get(&r)? {
            Scan::Path {
                walked: super::Walked::One { schema, rel_type },
                rel_tuples: true,
                ..
            } if !self.binding(r).nullable => Some(Elem::Rel {
                rel_type: rel_type.clone(),
                from: schema.from_node.clone(),
                to: schema.to_node.clone(),
            }),
            _ => None,
        }
    }
}

/// A list comprehension's part: `[x IN l WHERE p]` keeps elements, `[x IN l
/// | e]` maps them (`[x IN l WHERE p | e]` is a map of a filter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Comprehension {
    Filter,
    Map,
}

/// The comprehension `f` is (the AST converter spells one as `arrayFilter` /
/// `arrayMap` of a lambda of one parameter): which, its parameter, the
/// predicate or value, and the list.
pub(super) fn comprehension(
    f: &lx::ScalarFnCall,
) -> Option<(Comprehension, VarId, &LogicalExpr, &LogicalExpr)> {
    let how = if f.name.eq_ignore_ascii_case("arrayFilter") {
        Comprehension::Filter
    } else if f.name.eq_ignore_ascii_case("arrayMap") {
        Comprehension::Map
    } else {
        return None;
    };
    let [LogicalExpr::Lambda(l), list] = f.args.as_slice() else {
        return None;
    };
    let [x] = l.params.as_slice() else {
        return None;
    };
    Some((how, parse_var(x)?, &l.body, list))
}

/// What a value is, as far as UNWIND needs to know.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Kind {
    /// Always NULL.
    Null,
    /// Not a list (possibly NULL).
    Scalar,
    /// A boolean (possibly NULL): ClickHouse holds it as `UInt8`, so a
    /// column of it is cast for the result to show `true` / `false`.
    Boolean,
    /// A list, of elements of this kind.
    List(Box<Kind>),
    /// A node or relationship as a list's element (`elements.rs`): possibly
    /// NULL.
    Element(Elem),
    /// A map of these keys (in key order) and values (S7e3).
    Map(Vec<(String, Kind)>),
    /// Not known here.
    Unknown,
}

/// The kind of a value of a column of type `t` (T2): a list of its
/// element's kind, a boolean, a scalar of a known scalar type, else unknown.
fn column_kind(t: &KeyPartType) -> Kind {
    let scalar = |t: &KeyPartType| {
        let raw = t.raw.to_ascii_lowercase();
        [
            "int",
            "uint",
            "float",
            "bfloat",
            "decimal",
            "string",
            "fixedstring",
            "uuid",
            "date",
            "enum",
            "ipv4",
            "ipv6",
        ]
        .iter()
        .any(|p| raw.starts_with(p))
    };
    match t.array_element() {
        Some(e) => Kind::List(Box::new(column_kind(&e))),
        None if t.raw.eq_ignore_ascii_case("Bool") => Kind::Boolean,
        // A value of one scalar type; anything else (a map's or tuple's
        // entries, a `Dynamic` / `Variant` / `JSON`) is not known here
        // (review findings).
        None if scalar(t) => Kind::Scalar,
        None => Kind::Unknown,
    }
}

impl Kind {
    /// A value that is one or the other.
    fn either(self, other: Kind) -> Kind {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Null, k @ (Kind::Scalar | Kind::Boolean | Kind::Element(_)))
            | (k @ (Kind::Scalar | Kind::Boolean | Kind::Element(_)), Kind::Null) => k,
            (Kind::Scalar | Kind::Boolean, Kind::Scalar | Kind::Boolean) => Kind::Scalar,
            (Kind::List(a), Kind::List(b)) => Kind::List(Box::new(a.either(*b))),
            // Maps (of one type whatever their keys): the keys either may
            // have, each value either (NULL for a map without the key).
            (Kind::Map(a), Kind::Map(b)) => {
                let mut keys: Vec<String> = a.iter().chain(&b).map(|(k, _)| k.clone()).collect();
                keys.sort();
                keys.dedup();
                let value = |m: &[(String, Kind)], k: &str| {
                    m.iter()
                        .find(|(x, _)| x == k)
                        .map_or(Kind::Null, |(_, v)| v.clone())
                };
                Kind::Map(
                    keys.into_iter()
                        .map(|k| {
                            let v = value(&a, &k).either(value(&b, &k));
                            (k, v)
                        })
                        .collect(),
                )
            }
            // A list of values that may be NULL: on ClickHouse held as `[]`
            // where it is (`null_list.rs`), which what reads it tells by its
            // source. (A dialect with NULL lists reads it as it is.)
            (Kind::Null, Kind::List(k)) | (Kind::List(k), Kind::Null)
                if !matches!(*k, Kind::Element(_)) && super::null_list::null_as_empty() =>
            {
                Kind::List(k)
            }
            // A list of nodes or relationships that may be NULL.
            _ => Kind::Unknown,
        }
    }

    /// The kind of the records `UNWIND` of a value of this kind gives.
    pub(super) fn element(&self) -> Kind {
        match self {
            Kind::List(e) => (**e).clone(),
            k @ (Kind::Scalar
            | Kind::Boolean
            | Kind::Null
            | Kind::Unknown
            | Kind::Element(_)
            | Kind::Map(_)) => k.clone(),
        }
    }
}

impl<'s> Lowerer<'s> {
    /// What `e` is ([`Kind`]).
    pub(super) fn kind(&self, e: &LogicalExpr) -> Kind {
        use lx::Operator as O;
        match e {
            LogicalExpr::Literal(lx::Literal::Null) => Kind::Null,
            LogicalExpr::Literal(lx::Literal::Boolean(_)) => Kind::Boolean,
            LogicalExpr::Literal(_) => Kind::Scalar,
            LogicalExpr::MapLiteral(entries) => {
                let mut kinds: Vec<(String, Kind)> = entries
                    .iter()
                    .map(|(k, v)| (k.clone(), self.kind(v)))
                    .collect();
                kinds.sort_by(|a, b| a.0.cmp(&b.0));
                Kind::Map(kinds)
            }
            LogicalExpr::List(xs) => Kind::List(Box::new(
                xs.iter()
                    .map(|x| self.kind(x))
                    .reduce(Kind::either)
                    .unwrap_or(Kind::Null),
            )),
            LogicalExpr::TableAlias(lx::TableAlias(n)) => parse_var(n)
                .and_then(|v| match self.binding(v).kind {
                    BindingKind::Node { .. } | BindingKind::Rel { length: None, .. } => {
                        self.elem_of(v).map(Kind::Element)
                    }
                    BindingKind::Rel {
                        length: Some(_), ..
                    } => self
                        .walk_rel_elem(v)
                        .map(|e| Kind::List(Box::new(Kind::Element(e)))),
                    _ => self.var_kind(v),
                })
                .unwrap_or(Kind::Unknown),
            LogicalExpr::PropertyAccessExp(pa) => match &pa.column {
                PropertyValue::Column(p) => {
                    parse_var(&pa.table_alias.0).map_or(Kind::Unknown, |v| self.property_kind(v, p))
                }
                _ => Kind::Unknown,
            },
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                match (op.operator, op.operands.as_slice()) {
                    (O::Distinct, [x]) => self.kind(x),
                    // `+` of lists concatenates, and appends a value to a list
                    // (`Lowerer::list_addition`); with NULL it is NULL.
                    (O::Addition, [a, b]) => match (self.kind(a), self.kind(b)) {
                        (Kind::Scalar | Kind::Null, Kind::Scalar | Kind::Null) => Kind::Scalar,
                        (Kind::Null, Kind::List(_)) | (Kind::List(_), Kind::Null) => Kind::Null,
                        (Kind::List(x), Kind::List(y)) => Kind::List(Box::new(x.either(*y))),
                        (Kind::List(x), e @ (Kind::Scalar | Kind::Boolean))
                        | (e @ (Kind::Scalar | Kind::Boolean), Kind::List(x)) => {
                            Kind::List(Box::new(x.either(e)))
                        }
                        _ => Kind::Unknown,
                    },
                    (O::Addition | O::Distinct, _) => Kind::Unknown,
                    (
                        O::Subtraction
                        | O::Multiplication
                        | O::Division
                        | O::ModuloDivision
                        | O::Exponentiation,
                        _,
                    ) => Kind::Scalar,
                    _ => Kind::Boolean,
                }
            }
            LogicalExpr::ScalarFnCall(_)
                if super::subquery::placeholder_kind(self, e).is_some() =>
            {
                super::subquery::placeholder_kind(self, e).unwrap_or(Kind::Unknown)
            }
            LogicalExpr::ScalarFnCall(f) if comprehension(f).is_some() => {
                let Some((how, x, body, list)) = comprehension(f) else {
                    return Kind::Unknown;
                };
                let list = self.kind(list);
                self.local_kinds.borrow_mut().insert(x, list.element());
                // A list (or an error, when the list is not one).
                match (list, how) {
                    (Kind::Null, _) => Kind::Null,
                    (l @ Kind::List(_), Comprehension::Filter) => l,
                    (Kind::Unknown, Comprehension::Filter) => Kind::List(Box::new(Kind::Unknown)),
                    (Kind::List(_) | Kind::Unknown, Comprehension::Map) => {
                        Kind::List(Box::new(self.kind(body)))
                    }
                    _ => Kind::Unknown,
                }
            }
            LogicalExpr::ScalarFnCall(f) => {
                let arg = |i: usize| f.args.get(i).map_or(Kind::Unknown, |a| self.kind(a));
                match f.name.to_ascii_lowercase().as_str() {
                    "nodes" | "relationships" => self.path_elem(f).map_or(Kind::Unknown, |elem| {
                        Kind::List(Box::new(Kind::Element(elem)))
                    }),
                    "range" | "split" | "keys" => Kind::List(Box::new(Kind::Scalar)),
                    "toboolean" => Kind::Boolean,
                    "size" | "length" | "tostring" | "tointeger" | "tofloat" | "abs" | "ceil"
                    | "floor" | "round" | "sign" | "sqrt" | "exp" | "log" | "log10" | "rand"
                    | "tolower" | "toupper" | "trim" | "ltrim" | "rtrim" | "substring"
                    | "replace" | "left" | "right" | "type" => Kind::Scalar,
                    // (`reverse()` of a string is a string.)
                    "tail" | "reverse" => match arg(0) {
                        l @ Kind::List(_) => l,
                        _ => Kind::Unknown,
                    },
                    "head" | "last" => match arg(0) {
                        Kind::List(e) => e.either(Kind::Null),
                        _ => Kind::Unknown,
                    },
                    "coalesce" => {
                        let kinds: Vec<Kind> = f
                            .args
                            .iter()
                            .map(|a| self.kind(a))
                            .filter(|k| *k != Kind::Null)
                            .collect();
                        kinds.into_iter().reduce(Kind::either).unwrap_or(Kind::Null)
                    }
                    _ => Kind::Unknown,
                }
            }
            LogicalExpr::AggregateFnCall(f) => {
                let arg = f.args.first().map_or(Kind::Unknown, |a| self.kind(a));
                match f.name.to_ascii_lowercase().as_str() {
                    // NULLs are not collected: an element is never NULL.
                    "collect" => Kind::List(Box::new(arg)),
                    "count" | "sum" | "avg" => Kind::Scalar,
                    "min" | "max" => match arg {
                        Kind::Scalar | Kind::Boolean | Kind::Null => Kind::Scalar,
                        // Of lists of values that may be NULL (ClickHouse,
                        // `null_list.rs`): a list, NULL of a group of none.
                        Kind::List(k)
                            if !matches!(*k, Kind::Element(_))
                                && super::null_list::null_as_empty()
                                && super::null_list::min_max_arg(f).is_some_and(|a| {
                                    matches!(
                                        self.list_null(a, &HashMap::new()),
                                        Ok(super::null_list::ListNull::When(_))
                                    )
                                }) =>
                        {
                            Kind::List(k)
                        }
                        _ => Kind::Unknown,
                    },
                    _ => Kind::Unknown,
                }
            }
            LogicalExpr::Case(c) => {
                let mut kinds: Vec<Kind> = c.when_then.iter().map(|(_, t)| self.kind(t)).collect();
                kinds.push(c.else_expr.as_ref().map_or(Kind::Null, |e| self.kind(e)));
                kinds.into_iter().reduce(Kind::either).unwrap_or(Kind::Null)
            }
            LogicalExpr::ArraySubscript { array, .. } => match self.kind(array) {
                // NULL out of range.
                Kind::List(e) => e.either(Kind::Null),
                _ => Kind::Unknown,
            },
            LogicalExpr::ArraySlicing { array, .. } => match self.kind(array) {
                l @ Kind::List(_) => l,
                _ => Kind::Unknown,
            },
            _ => Kind::Unknown,
        }
    }

    /// What property `p` of node or relationship `v` is: a value of the
    /// type every label / type of `v` declares, if they do.
    fn property_kind(&self, v: VarId, p: &str) -> Kind {
        match self.declared_types(v, p).as_slice() {
            [] => Kind::Unknown,
            ts if ts.iter().all(|t| t == &Some(&SchemaType::Boolean)) => Kind::Boolean,
            ts if ts.iter().all(Option::is_some) => Kind::Scalar,
            _ => self.column_list_kind(v, p),
        }
    }

    /// The kind of `e` with properties' column types left unread: what a
    /// check written before they were read (T2a) decides on.
    pub(super) fn kind_before_column_types(&self, e: &LogicalExpr) -> Kind {
        let before = self.no_column_kinds.replace(true);
        let kind = self.kind(e);
        self.no_column_kinds.set(before);
        kind
    }

    /// VALUE_TYPES.md T2 (#1485): a property not declared typed whose
    /// column is an `Array(T)` in every label / type of `v` is a list of
    /// `T`'s kind; else `Unknown`, as before. Only a list: what a scalar
    /// column's kind would change is a later slice's.
    fn column_list_kind(&self, v: VarId, p: &str) -> Kind {
        if self.no_column_kinds.get() {
            return Kind::Unknown;
        }
        let columns = self.property_columns(v, p);
        let elements: Option<Vec<Kind>> = columns
            .iter()
            .map(|t| {
                t.and_then(KeyPartType::array_element)
                    .map(|e| column_kind(&e))
            })
            .collect();
        match elements {
            Some(kinds) if !kinds.is_empty() => {
                let first = kinds[0].clone();
                let element = match kinds.iter().all(|k| *k == first) {
                    true => first,
                    false => Kind::Unknown,
                };
                Kind::List(Box::new(element))
            }
            _ => Kind::Unknown,
        }
    }

    /// The column type of property `p` in each label / type of node or
    /// relationship `v` (`None`: not known there,
    /// `NodeSchema::property_column_type`).
    pub(super) fn property_columns(&self, v: VarId, p: &str) -> Vec<Option<&KeyPartType>> {
        match &self.binding(v).kind {
            BindingKind::Node { labels } => labels
                .iter()
                .map(|l| {
                    self.schema
                        .node_schema_opt(l)
                        .and_then(|s| s.property_column_type(p))
                })
                .collect(),
            BindingKind::Rel {
                types,
                length: None,
            } => types
                .iter()
                .flat_map(|t| {
                    let defs = self.schema.rel_schemas_for_type(t);
                    if defs.is_empty() {
                        vec![None]
                    } else {
                        defs.iter().map(|s| s.property_column_type(p)).collect()
                    }
                })
                .collect(),
            _ => vec![None],
        }
    }

    /// VALUE_TYPES.md T4a (#1396): the CAST target of the elements of list
    /// literal `xs` where the backend has no common type for them
    /// (the dialect's `common_type_where_none`: a `UInt64` and a signed column,
    /// whose list is otherwise a `Variant` in which equal integers are two
    /// values). `None` unless every element's type is known (a property of a
    /// known column type, an integer or NULL literal), and where they have
    /// one: the SQL as before. The target is Nullable.
    pub(super) fn list_meeting_type(&self, xs: &[LogicalExpr]) -> Option<String> {
        let mut known = Vec::new();
        for x in xs {
            match x {
                // A literal is of the narrowest type holding it: a negative
                // one is signed.
                LogicalExpr::Literal(lx::Literal::Integer(n)) if *n < 0 => {
                    known.push(KeyPartType::parse("Int64"))
                }
                LogicalExpr::Literal(lx::Literal::Integer(_)) => {}
                // `-1` is `0 - 1`: of integer literals, a signed integer.
                LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op)
                    if op.operator == lx::Operator::Subtraction
                        && op.operands.iter().all(|o| {
                            matches!(o, LogicalExpr::Literal(lx::Literal::Integer(_)))
                        }) =>
                {
                    known.push(KeyPartType::parse("Int64"))
                }
                LogicalExpr::Literal(lx::Literal::Null) => {}
                LogicalExpr::PropertyAccessExp(pa) => {
                    let (PropertyValue::Column(p), Some(v)) =
                        (&pa.column, parse_var(&pa.table_alias.0))
                    else {
                        return None;
                    };
                    let types = self.property_columns(v, p);
                    let first = types.first().copied().flatten()?;
                    if types.iter().any(|t| *t != Some(first)) {
                        return None;
                    }
                    known.push(first.clone());
                }
                _ => return None,
            }
        }
        let mut target = self.options.dialect.common_type_where_none(&known)?;
        // Nullable always: a column element (one there is, the `UInt64`) may
        // be NULL whatever its declared type says, and NULL literals join.
        target.nullable = true;
        Some(self.options.dialect.cast_target(&target))
    }

    /// Whether `e` is never a boolean: a number or a string (`Kind::Scalar`
    /// does not tell, as a `CASE` of a boolean or a number is one too). A
    /// boolean compared with it is not equal, and has no order (#1470).
    pub(super) fn never_boolean(&self, e: &LogicalExpr) -> bool {
        use lx::Operator as O;
        match e {
            LogicalExpr::Literal(
                lx::Literal::Integer(_) | lx::Literal::Float(_) | lx::Literal::String(_),
            ) => true,
            LogicalExpr::TableAlias(lx::TableAlias(n)) => {
                parse_var(n).is_some_and(|v| self.never_boolean.contains(&v))
            }
            LogicalExpr::PropertyAccessExp(pa) => match &pa.column {
                PropertyValue::Column(p) => parse_var(&pa.table_alias.0).is_some_and(|v| {
                    let ts = self.declared_types(v, p);
                    !ts.is_empty()
                        && ts
                            .iter()
                            .all(|t| t.is_some_and(|t| *t != SchemaType::Boolean))
                }),
                _ => false,
            },
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                match op.operator {
                    O::Subtraction
                    | O::Multiplication
                    | O::Division
                    | O::ModuloDivision
                    | O::Exponentiation => true,
                    // Of numbers or strings (a boolean's is an error).
                    O::Addition => self.kind(e) == Kind::Scalar,
                    _ => false,
                }
            }
            LogicalExpr::ScalarFnCall(f) => matches!(
                f.name.to_ascii_lowercase().as_str(),
                "size"
                    | "length"
                    | "tostring"
                    | "tointeger"
                    | "tofloat"
                    | "abs"
                    | "ceil"
                    | "floor"
                    | "round"
                    | "sign"
                    | "sqrt"
                    | "exp"
                    | "log"
                    | "log10"
                    | "rand"
                    | "tolower"
                    | "toupper"
                    | "trim"
                    | "ltrim"
                    | "rtrim"
                    | "substring"
                    | "replace"
                    | "left"
                    | "right"
                    | "type"
            ),
            LogicalExpr::AggregateFnCall(f) => matches!(
                f.name.to_ascii_lowercase().as_str(),
                "count" | "sum" | "avg"
            ),
            _ => false,
        }
    }

    /// The type each label / type of node or relationship `v` declares for
    /// property `p` (`None`: one that does not).
    fn declared_types(&self, v: VarId, p: &str) -> Vec<Option<&SchemaType>> {
        match &self.binding(v).kind {
            BindingKind::Node { labels } => labels
                .iter()
                .map(|l| {
                    self.schema
                        .node_schema_opt(l)
                        .and_then(|s| s.property_types.get(p))
                })
                .collect(),
            BindingKind::Rel {
                types,
                length: None,
            } => types
                .iter()
                .flat_map(|t| {
                    let defs = self.schema.rel_schemas_for_type(t);
                    if defs.is_empty() {
                        vec![None]
                    } else {
                        defs.iter().map(|s| s.property_types.get(p)).collect()
                    }
                })
                .collect(),
            _ => match self.var_kind(v) {
                Some(Kind::Element(elem)) => match self.layout(&elem) {
                    Ok(l) => vec![l
                        .node
                        .and_then(|n| n.property_types.get(p))
                        .or_else(|| l.rel.and_then(|r| r.property_types.get(p)))],
                    Err(_) => vec![None],
                },
                _ => vec![None],
            },
        }
    }

    /// `UNWIND expr AS var`.
    pub(super) fn unwind(&mut self, expr: &LogicalExpr, var: VarId) -> Result<(), LowerError> {
        let Some(spelling) = current_function_mapper().unwind() else {
            return unsupported("UNWIND in this SQL dialect");
        };
        let kind = self.kind(expr);
        let before = self.kind_before_column_types(expr).element();
        self.set_kind(var, kind.element(), before);
        // A list of no elements, or NULL: no rows.
        let empty = match (expr, &kind) {
            (LogicalExpr::List(xs), _) => xs.is_empty(),
            (_, Kind::Null) => true,
            _ => false,
        };
        // An expression can still be refused (a node in a list), and can be
        // NULL by the schema (a property of an element that matches nothing).
        // (A NULL list held as `[]`: no rows, as of NULL.)
        let probe = self.held_list(expr, &HashMap::new())?;
        // (`split(null, …)` lowers to `[]`, `null_list.rs`.)
        if empty
            || matches!(&probe, RenderExpr::Literal(Literal::Null))
            || matches!(&probe, RenderExpr::List(xs) if xs.is_empty())
        {
            self.empty = true;
            self.values.insert(var, RenderExpr::Literal(Literal::Null));
            return Ok(());
        }
        // Each row's elements follow it, in list order: the rows are
        // numbered in their order, or in any (a fixed one) when a later
        // clause takes the elements in order (`Lowerer::ordered_unwinds`).
        let ordered = match self.order {
            RowOrder::Keys(_) => true,
            RowOrder::Unordered => self.ordered_unwinds.contains(&var),
            RowOrder::Lost => false,
        };
        if ordered && !self.one_row {
            self.number_rows()?;
        }
        let e = self.held_list(expr, &HashMap::new())?;
        // (A NULL `split()` whose NULL `null_list.rs` does not know: no rows.)
        let e = match self.list_null(expr, &HashMap::new())? {
            super::null_list::ListNull::Unknown => self.split_of_null(expr, e, &spelling)?,
            _ => e,
        };
        let element = kind.element();
        let list = match kind {
            Kind::List(_) => e,
            // The list of the value, or of none when it is NULL.
            Kind::Scalar | Kind::Boolean | Kind::Map(_) => {
                RenderExpr::Raw((spelling.value_list)(&render_expr_to_sql_plain(&e)))
            }
            Kind::Unknown | Kind::Null => {
                RenderExpr::Raw((spelling.list_only)(&render_expr_to_sql_plain(&e)))
            }
            Kind::Element(_) => return unsupported("UNWIND of a node or relationship"),
        };
        let alias = self.next_cte_alias();
        let mut body = Body::default();
        let mut exports = Exports::default();
        self.export_scope(&alias, &mut body, &mut exports)?;
        match element {
            // Every element is NULL (ClickHouse cannot project the element
            // of a list typed `Array(Nothing)`).
            Kind::Null => exports
                .values
                .push((var, RenderExpr::Literal(Literal::Null))),
            _ => {
                let value = match element {
                    Kind::Boolean => RenderExpr::Raw(current_function_mapper().cast_bool(ELEMENT)),
                    _ => RenderExpr::TableAlias(TableAlias(ELEMENT.to_string())),
                };
                body.select.push(select(value, &var.name()));
                exports.values.push((var, col_at(&alias, &var.name())));
            }
        }
        // The rows' order: each row's elements follow it, in list order.
        let keys = match &self.order {
            RowOrder::Keys(keys) => Some(keys.clone()),
            _ if self.one_row => Some(Vec::new()),
            // Neo4j keeps each row's elements in list order; rows in no
            // order have no number to keep them together by.
            RowOrder::Unordered | RowOrder::Lost => None,
        };
        match keys {
            Some(mut keys) => {
                let position = RenderExpr::TableAlias(TableAlias(POSITION.to_string()));
                self.numbers.insert(render_expr_to_sql_plain(&position));
                keys.push(OrderByItem {
                    expression: position,
                    order: OrderByOrder::Asc,
                });
                body.order_by = keys;
                body.array_join.push(ArrayJoin {
                    expression: list,
                    alias: ELEMENT.to_string(),
                    position: Some(POSITION.to_string()),
                });
            }
            None => {
                body.order_lost = true;
                body.array_join.push(ArrayJoin {
                    expression: list,
                    alias: ELEMENT.to_string(),
                    position: None,
                });
            }
        }
        if self.from.is_none() {
            // `UNWIND` first: the rows so far are the one empty record.
            self.from = Some(table_ref(spelling.one_row.to_string(), ONE_ROW));
            self.emitted.push(ONE_ROW.to_string());
        }
        self.close_segment(body, exports, alias)?;
        self.one_row = false;
        Ok(())
    }

    /// `split(s, d)` is NULL when `s` or `d` is (Cypher), and ClickHouse's
    /// split of NULL is `['']`: UNWIND of it gives no rows.
    fn split_of_null(
        &self,
        expr: &LogicalExpr,
        list: RenderExpr,
        spelling: &Unwind,
    ) -> Result<RenderExpr, LowerError> {
        let LogicalExpr::ScalarFnCall(f) = expr else {
            return Ok(list);
        };
        if !f.name.eq_ignore_ascii_case("split") || f.args.is_empty() {
            return Ok(list);
        }
        let mut null = Vec::new();
        for a in &f.args {
            let a = render_expr_to_sql_plain(&self.expr(a, &HashMap::new())?);
            null.push((spelling.is_null)(&a));
        }
        Ok(RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(
                RenderExpr::Raw(null.join(" OR ")),
                RenderExpr::List(Vec::new()),
            )],
            else_expr: Some(Box::new(list)),
        }))
    }

    /// The rows so far, in their order (rows in no order: in any, fixed by
    /// the numbers), become a CTE exporting the scope and each row's number
    /// in that order; the rows are then in the order of that number.
    pub(super) fn number_rows(&mut self) -> Result<(), LowerError> {
        let Some(spelling) = current_function_mapper().unwind() else {
            return unsupported("UNWIND in this SQL dialect");
        };
        let alias = self.next_cte_alias();
        let mut body = Body::default();
        let mut exports = Exports::default();
        self.export_scope(&alias, &mut body, &mut exports)?;
        // Lowered stable, ties are broken by every column (#1395): rows
        // that still tie are alike, so their numbers are interchangeable.
        let ties = match self.stable {
            true => self.tie_breaks(&body.select, &exports, false)?,
            false => Vec::new(),
        };
        let (number, unordered) = match &self.order {
            RowOrder::Keys(keys) => {
                let keys: Vec<OrderByItem> = keys.iter().cloned().chain(ties).collect();
                (
                    (spelling.row_number)(&order_keys_to_sql_plain(&keys)),
                    keys.iter().any(|k| self.is_unordered_number(k)),
                )
            }
            RowOrder::Unordered if self.stable && !ties.is_empty() => (
                (spelling.row_number)(&order_keys_to_sql_plain(&ties)),
                false,
            ),
            RowOrder::Unordered => (spelling.any_row_number.to_string(), !self.stable),
            RowOrder::Lost => return unsupported("internal: numbering rows of a lost order"),
        };
        body.select
            .push(select(RenderExpr::Raw(number), ROW_NUMBER));
        self.close_segment(body, exports, alias.clone())?;
        let column = render_expr_to_sql_plain(&col_at(&alias, ROW_NUMBER));
        if unordered {
            self.unordered_numbers.insert(column.clone());
        }
        self.numbers.insert(column);
        self.order = RowOrder::Keys(vec![OrderByItem {
            expression: col_at(&alias, ROW_NUMBER),
            order: OrderByOrder::Asc,
        }]);
        Ok(())
    }
}

/// Refuse a plan that reads more than once rows that depend on how rows in
/// no order were numbered (`unstable`, by CTE name): numbers the stable
/// lowering could not fix ([`rereads`]).
pub(super) fn reject_rereads_of_numbered_rows(
    plan: &RenderPlan,
    unstable: &BTreeSet<String>,
) -> Result<(), LowerError> {
    if rereads(plan, unstable) {
        return unsupported(
            "rows numbered in no order (SKIP / LIMIT or collect() after an UNWIND) \
             read more than once",
        );
    }
    Ok(())
}

/// Whether the plan reads more than once a CTE of `marked` (by name), or
/// one reading such a CTE.
///
/// ClickHouse evaluates a CTE once per read, and rows in no order are
/// numbered (`Unwind::any_row_number`), grouped (`collect()`) or taken by
/// SKIP / LIMIT in whatever order an evaluation produces them: two reads can
/// differ, and a join of the reads (an OPTIONAL MATCH's or a subquery's
/// drive, a path's seed) matches the wrong rows. Rows that only carry such
/// a CTE's values are the same rows on every read. They are read once when
/// each CTE that reads them, directly or through others, is itself named
/// once in the rest of the SQL.
pub(super) fn rereads(plan: &RenderPlan, marked: &BTreeSet<String>) -> bool {
    if marked.is_empty() {
        return false;
    }
    let (ctes, select) = render_plan_parts_plain(plan.clone());
    // The names each CTE defines: its own, and a path relation's helpers.
    let names: Vec<Vec<&str>> = ctes
        .iter()
        .map(|c| {
            let mut names = vec![c.name.as_str()];
            names.extend(defined_names(&c.text).filter(|n| *n != c.name));
            names
        })
        .collect();
    // How often the rest of the SQL names CTE `i`.
    let reads = |i: usize| -> usize {
        let elsewhere = ctes
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, c)| c.text.as_str())
            .chain(std::iter::once(select.as_str()));
        elsewhere
            .map(|text| names[i].iter().map(|n| word_count(text, n)).sum::<usize>())
            .sum()
    };
    // The marked CTEs and those reading them; CTEs are in dependency
    // order, so one pass finds every reader.
    let mut tainted: Vec<bool> = ctes.iter().map(|c| marked.contains(&c.name)).collect();
    for i in 0..ctes.len() {
        if !tainted[i] {
            tainted[i] = (0..i)
                .any(|j| tainted[j] && names[j].iter().any(|n| word_count(&ctes[i].text, n) > 0));
        }
    }
    (0..ctes.len()).any(|i| tainted[i] && reads(i) > 1)
}

/// The names of the plan's CTEs.
pub(super) fn cte_names(plan: &RenderPlan) -> BTreeSet<String> {
    plan.ctes.0.iter().map(|c| c.cte_name.clone()).collect()
}

/// The CTEs of `plan` (by name) that `other`, a lowering of the same query,
/// spells otherwise: all of them when the two do not have the same CTEs.
pub(super) fn changed_ctes(plan: &RenderPlan, other: &RenderPlan) -> BTreeSet<String> {
    let (ctes, _) = render_plan_parts_plain(plan.clone());
    let (others, _) = render_plan_parts_plain(other.clone());
    let same_names =
        ctes.len() == others.len() && ctes.iter().zip(&others).all(|(a, b)| a.name == b.name);
    ctes.iter()
        .zip(&others)
        .filter(|(a, b)| !same_names || a.text != b.text)
        .map(|(a, _)| a.name.clone())
        .chain(
            (!same_names)
                .then(|| ctes.iter().map(|c| c.name.clone()))
                .into_iter()
                .flatten(),
        )
        .collect()
}

/// The CTE names `text` defines (`name AS (`).
fn defined_names(text: &str) -> impl Iterator<Item = &str> {
    text.match_indices(" AS (").filter_map(move |(at, _)| {
        let before = &text[..at];
        let start = before
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(0, |p| p + 1);
        let name = &before[start..];
        (!name.is_empty()).then_some(name)
    })
}

/// How often `word` occurs in `text` as a whole identifier.
fn word_count(text: &str, word: &str) -> usize {
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    text.match_indices(word)
        .filter(|(at, _)| {
            !text[..*at].ends_with(ident) && !text[at + word.len()..].starts_with(ident)
        })
        .count()
}
