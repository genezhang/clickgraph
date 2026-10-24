//! Bound expression → [`RenderExpr`]. Every variable is a binding's generated
//! name (`v{N}`, see `bound_plan::expr`), so a reference resolves through the
//! binding table and the element's scan, never by a user-visible name:
//! * a property of a node or relationship is `v{N}.<mapped column>`; a
//!   property the schema does not map is NULL (the graph has no such
//!   property), and a property of an element that matches nothing is NULL;
//! * a bare node or relationship stands for its identity only where that is
//!   its whole meaning: `count(a)`, `count(DISTINCT a)`, `a = b`, `a <> b`
//!   (elements of different labels / types are never equal), `a IS [NOT]
//!   NULL`. A RETURN item `a` is its columns (`Lowerer::return_element`).
//!   Anywhere else (`[a]`, `collect(a)`, `CASE … a …`) it is the entity's
//!   value: `Unsupported`;
//! * a projected item (ORDER BY after RETURN) is the item's expression;
//! * an element or value carried by a WITH is read from the WITH's CTE.
//!
//! The conversion is structural; it reads the dialect's spellings from its
//! `FunctionMapper`. Shapes that need more than that are `Unsupported`.
//!
//! A node of several possible labels (`Scan::Labels`) is its label and id:
//! `n:L` and `labels(n)` read its label column, and it equals a node of the
//! same label and id. A relationship of several possible types or label
//! pairs (`Scan::Rels`) is its definition and identity: `type(r)` and `r:T`
//! read its type column, and it equals a relationship of the same definition
//! and identity.

use std::collections::HashMap;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::{
    self as lx, AggregateFnCall as LAgg, LogicalCase, LogicalExpr, OperatorApplication as LOp,
    ScalarFnCall as LScalar,
};
use crate::render_plan::render_expr::{
    AggregateFnCall, Literal, OperatorApplication, PropertyAccess, ReduceExpr, RenderCase,
    RenderExpr, ScalarFnCall, TableAlias,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::{current_function_mapper, MapEntry};

use super::elements::Elem;
use super::null_list;
use super::unwind::{comprehension, Comprehension, Kind};
use super::{parse_var, unsupported, At, LowerError, Lowerer, Scan};
use crate::bound_plan::expr::subquery_var;
use crate::bound_plan::types::{BindingKind, BindingSource, SubqueryKind, VarId};

type Items = HashMap<VarId, RenderExpr>;

impl Lowerer<'_> {
    /// `v.prop` for a node or relationship binding.
    pub(super) fn property(&self, v: VarId, prop: &str) -> Result<RenderExpr, LowerError> {
        let (mapping, closed, at) = match self.scans.get(&v) {
            // Its CTE's column (the demand pass names what it carries).
            Some(Scan::Labels { arms, of, at, .. }) => {
                return match at {
                    At::Table(alias) if self.label_union_props(*of, arms).contains(prop) => {
                        let defs = self.label_value_definitions(prop, arms);
                        self.one_type(alias, *of, prop, &defs)
                    }
                    At::Table(_) => unsupported(format!("internal: {v}.{prop} is not carried")),
                    At::Exported { props, .. } => match props.get(prop) {
                        Some(e) => Ok(e.clone()),
                        None => unsupported(format!("internal: {v}.{prop} is not exported")),
                    },
                };
            }
            Some(Scan::Rels { arms, of, at, .. }) => {
                return match at {
                    At::Table(alias) if self.rel_union_props(*of, arms).contains(prop) => {
                        let defs = self.rel_value_definitions(prop, arms);
                        self.one_type(alias, *of, prop, &defs)
                    }
                    At::Table(_) => unsupported(format!("internal: {v}.{prop} is not carried")),
                    At::Exported { props, .. } => match props.get(prop) {
                        Some(e) => Ok(e.clone()),
                        None => unsupported(format!("internal: {v}.{prop} is not exported")),
                    },
                };
            }
            Some(Scan::Node { schema, at, .. }) => {
                // A node embedded in edge tables (S8b) has no table whose
                // same-named column an undeclared property could read.
                if !schema.property_mappings.contains_key(prop)
                    && !self.options.neo4j_compat
                    && self
                        .schema
                        .node_relation(&schema.full_table_name())
                        .is_some()
                {
                    return unsupported("an undeclared property of a node embedded in edge tables");
                }
                (
                    schema.property_mappings.get(prop),
                    schema.closed_properties,
                    at,
                )
            }
            Some(Scan::Rel { schema, at, .. }) => (
                schema.property_mappings.get(prop),
                schema.closed_properties,
                at,
            ),
            Some(Scan::Path { .. }) => {
                return unsupported("a property of a variable-length relationship's list")
            }
            Some(Scan::Impossible) => return Ok(RenderExpr::Literal(Literal::Null)),
            None => return unsupported("a property of a variable with no scan"),
        };
        let alias = match at {
            At::Table(alias) => alias,
            // Read from a CTE: exported by the demand pass.
            At::Exported { props, .. } => {
                return match props.get(prop) {
                    Some(e) => Ok(e.clone()),
                    None => unsupported(format!("internal: {v}.{prop} is not exported")),
                }
            }
        };
        let column = match mapping {
            Some(pv) => pv.clone(),
            // An undeclared property is absent (NULL, as in Cypher) when the
            // element's properties are complete (its columns were discovered)
            // or in Neo4j-compat mode. Otherwise it reads the same-named column,
            // as the legacy planner does: a wide table needs no mapping per
            // column, and a missing column is a ClickHouse error, not a value.
            None if closed || self.options.neo4j_compat => {
                return Ok(RenderExpr::Literal(Literal::Null))
            }
            None => PropertyValue::Column(prop.to_string()),
        };
        Ok(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.clone()),
            column,
        }))
    }

    /// Property `prop` of union element `of` read under `alias`: its column,
    /// or, with values of several definitions (`defs`), their columns as one
    /// value of one type, or an error (`FunctionMapper::one_type_guard`).
    pub(super) fn one_type(
        &self,
        alias: &str,
        of: VarId,
        prop: &str,
        defs: &[Option<usize>],
    ) -> Result<RenderExpr, LowerError> {
        let columns = super::union_property_columns(of, prop, super::definitions_with_value(defs));
        if let [one] = columns.as_slice() {
            return Ok(super::col_at(alias, one));
        }
        let sql: Vec<String> = columns
            .iter()
            .map(|c| render_expr_to_sql_plain(&super::col_at(alias, c)))
            .collect();
        match current_function_mapper().one_type_guard(&sql) {
            Some(sql) => Ok(RenderExpr::Raw(sql)),
            None => unsupported("a property several labels or types have, in this SQL dialect"),
        }
    }

    /// `value`, a constant for matched elements, as NULL when one of `vars`
    /// is NULL (an OPTIONAL MATCH found no match): Cypher's `b:User`,
    /// `type(r)`, `labels(b)`, `a = b` of a NULL element are NULL.
    pub(super) fn unless_null(
        &self,
        vars: &[VarId],
        value: RenderExpr,
    ) -> Result<RenderExpr, LowerError> {
        let mut nulls = Vec::new();
        for v in vars.iter().filter(|v| self.binding(**v).nullable) {
            match self.identity(*v)? {
                Some(id) => nulls.push(super::null_test(&self.presence(*v, &id), true)),
                None => return Ok(RenderExpr::Literal(Literal::Null)),
            }
        }
        if nulls.is_empty() {
            return Ok(value);
        }
        Ok(RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(super::or_all(nulls), RenderExpr::Literal(Literal::Null))],
            else_expr: Some(Box::new(value)),
        }))
    }

    /// A node's or relationship's identity as one expression.
    /// Key `k` of map value `v`, where the segment carries its keys
    /// (`Lowerer::map_keys`, S11m): the key's value, NULL for a key the map
    /// does not have. `None`: its keys are not known here.
    pub(super) fn map_key(&self, v: VarId, k: &str) -> Option<RenderExpr> {
        // `m.a.b`: a key of a key's value, whose own keys are not carried
        // (S11m review: not a key named `a.b`).
        if k.contains('.') {
            return None;
        }
        let keys = self.map_keys.get(&v)?;
        Some(
            keys.iter()
                .find(|(key, _)| key == k)
                .map_or(RenderExpr::Literal(Literal::Null), |(_, e)| e.clone()),
        )
    }

    /// `m.k.p` of a collected map `m` (S11o8): the property `p` of the
    /// node `k` holds, NULL where its row is missing; carried as its own
    /// key for a node of several labels.
    fn collected_map_property(
        &self,
        v: VarId,
        prop: &str,
        element: &dyn Fn(&str) -> Option<Elem>,
    ) -> Result<RenderExpr, LowerError> {
        let keys = self.map_keys.get(&v);
        let find = |name: &str| {
            keys.and_then(|ks| ks.iter().find(|(key, _)| key == name))
                .map(|(_, e)| e.clone())
        };
        let unknown = || unsupported("a property of a map (its value's type is not known)");
        let Some((k, p)) = prop.split_once('.').filter(|(_, p)| !p.contains('.')) else {
            return unknown();
        };
        if let Some(e) = find(&super::collected_key(k, Some(p))) {
            return Ok(e);
        }
        let (Some(elem), Some(t), Some(present)) =
            (element(k), find(k), find(&super::collected_key(k, None)))
        else {
            return unknown();
        };
        let value = self.elem_property(&t, &elem, p)?;
        Ok(RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(present, value)],
            else_expr: Some(Box::new(RenderExpr::Literal(Literal::Null))),
        }))
    }

    pub(super) fn identity_value(&self, v: VarId) -> Result<RenderExpr, LowerError> {
        match self.identity(v)? {
            None => Ok(RenderExpr::Literal(Literal::Null)),
            Some(mut cols) if cols.len() == 1 => Ok(cols.remove(0)),
            Some(_) => unsupported("a composite identity used as one value"),
        }
    }

    pub(super) fn expr(&self, e: &LogicalExpr, items: &Items) -> Result<RenderExpr, LowerError> {
        Ok(match e {
            LogicalExpr::Literal(l) => RenderExpr::Literal(literal(l)),
            LogicalExpr::Parameter(p) => RenderExpr::Parameter(p.clone()),
            LogicalExpr::Star => RenderExpr::Star,
            LogicalExpr::TableAlias(lx::TableAlias(n)) => self.variable(n, items)?,
            LogicalExpr::PropertyAccessExp(pa) => {
                let PropertyValue::Column(prop) = &pa.column else {
                    return unsupported("a computed property access");
                };
                if prop == super::ALL_PROPERTIES {
                    return unsupported("`v.*` other than as a RETURN item");
                }
                let Some(v) = parse_var(&pa.table_alias.0) else {
                    return unsupported("an unbound property access");
                };
                match (&self.binding(v).kind, self.var_kind(v)) {
                    (BindingKind::Node { .. } | BindingKind::Rel { .. }, _) => {
                        self.property(v, prop)?
                    }
                    // A list's node or relationship (`elements.rs`).
                    (_, Some(Kind::Element(elem))) => {
                        let t = self.variable(&pa.table_alias.0, items)?;
                        self.elem_property(&t, &elem, prop)?
                    }
                    // A map's value: the key's own column where a WITH
                    // projected the map's keys (S11m); otherwise its type is
                    // not known here (a value of any type would compare,
                    // sort and count unlike its own).
                    (_, Some(Kind::Map(entries))) => {
                        let element = |k: &str| {
                            entries.iter().find_map(|(key, kind)| match kind {
                                Kind::Element(elem) if key == k => Some(elem.clone()),
                                _ => None,
                            })
                        };
                        if element(prop).is_some() {
                            // Its tuple is no value of its own (S11o8 review).
                            return unsupported(
                                "a node or relationship a collected map holds, read whole",
                            );
                        }
                        match self.map_key(v, prop) {
                            Some(e) => e,
                            None => self.collected_map_property(v, prop, &element)?,
                        }
                    }
                    _ => {
                        return unsupported("a property of a value (map, list element, WITH item)")
                    }
                }
            }
            LogicalExpr::LabelExpression { variable, label } => {
                let Some(v) = parse_var(variable) else {
                    return unsupported("an unbound label test");
                };
                // A node of a union label's labels carries it too.
                let carries = |l: &str| {
                    l == label
                        || self
                            .schema
                            .union_label_members(label)
                            .is_some_and(|m| m.contains(l))
                };
                if let (BindingKind::Value, Some(Kind::Element(elem))) =
                    (&self.binding(v).kind, self.var_kind(v))
                {
                    let holds = match &elem {
                        Elem::Node(l) => carries(l),
                        Elem::Rel { rel_type, .. } => rel_type == label,
                    };
                    let t = self.variable(variable, items)?;
                    return self.elem_unless_null(&t, &elem, Literal::Boolean(holds));
                }
                let holds = match self.scans.get(&v) {
                    Some(Scan::Node { label: l, .. }) => carries(l),
                    // NULL where an OPTIONAL MATCH left it NULL, as Cypher.
                    Some(Scan::Labels { arms, .. }) if arms.iter().any(|(l, _)| carries(l)) => {
                        let column = self.physical(v, super::LABEL_COLUMN)?;
                        let mut tests: Vec<RenderExpr> = arms
                            .iter()
                            .filter(|(l, _)| carries(l))
                            .map(|(l, _)| {
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: lx::Operator::Equal,
                                    operands: vec![
                                        column.clone(),
                                        RenderExpr::Literal(Literal::String(l.clone())),
                                    ],
                                })
                            })
                            .collect();
                        return Ok(if tests.len() == 1 {
                            tests.remove(0)
                        } else {
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: lx::Operator::Or,
                                operands: tests,
                            })
                        });
                    }
                    Some(Scan::Labels { .. }) => false,
                    // NULL where an OPTIONAL MATCH left it NULL, as Cypher.
                    Some(Scan::Rels { arms, .. }) if arms.iter().any(|a| a.rel_type == *label) => {
                        return Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: lx::Operator::Equal,
                            operands: vec![
                                self.physical(v, super::REL_TYPE)?,
                                RenderExpr::Literal(Literal::String(label.clone())),
                            ],
                        }));
                    }
                    Some(Scan::Rels { .. }) => false,
                    Some(Scan::Rel { rel_type, .. }) => rel_type == label,
                    Some(Scan::Impossible) => return Ok(RenderExpr::Literal(Literal::Null)),
                    Some(Scan::Path { .. }) | None => {
                        return unsupported("a label test on a list or an unscanned variable")
                    }
                };
                self.unless_null(&[v], RenderExpr::Literal(Literal::Boolean(holds)))?
            }
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                self.operator(op, items, false)?
            }
            // A list's elements are tuples of one type: of one label, or one
            // definition, and nothing else (`elements.rs`).
            LogicalExpr::List(xs)
                if xs.iter().any(|x| self.holds_elements(x)) && !one_shape(&self.kind(e)) =>
            {
                return unsupported(
                    "a list of nodes or relationships of several labels or types, or with values",
                );
            }
            LogicalExpr::List(xs)
                if matches!(self.kind(e), Kind::List(k) if matches!(*k, Kind::Element(_)))
                    && xs
                        .iter()
                        .any(|x| matches!(x, LogicalExpr::Literal(lx::Literal::Null))) =>
            {
                return unsupported("a NULL in a list of nodes or relationships");
            }
            LogicalExpr::List(xs) => {
                // T4a (#1396): elements of no common type meet in one.
                let target = self.list_meeting_type(xs);
                RenderExpr::List(
                    xs.iter()
                        .map(|x| {
                            self.element(x, items).map(|e| match &target {
                                Some(t) => RenderExpr::Cast {
                                    expr: Box::new(e),
                                    target: t.clone(),
                                },
                                None => e,
                            })
                        })
                        .collect::<Result<_, _>>()?,
                )
            }
            LogicalExpr::MapLiteral(entries)
                if entries.iter().any(|(_, v)| self.holds_elements(v)) =>
            {
                return unsupported("a map of nodes or relationships of a list");
            }
            // A map whose values keep their types (S7e3); `{}` stays a map.
            LogicalExpr::MapLiteral(entries) => self.map_literal(entries, items)?,
            // A subquery's value, applied to the rows before
            // (`subquery.rs`).
            LogicalExpr::ScalarFnCall(_) if subquery_var(e).is_some() => {
                match subquery_var(e).and_then(|v| self.values.get(&v)) {
                    Some(value) => value.clone(),
                    None => return unsupported("internal: a subquery expression not applied"),
                }
            }
            LogicalExpr::ScalarFnCall(f) => self.list_fn(e, f, items)?,
            LogicalExpr::AggregateFnCall(f) => self.aggregate_fn(f, items)?,
            LogicalExpr::Case(c) => {
                if c.when_then.iter().any(|(_, t)| self.holds_elements(t))
                    || c.else_expr
                        .as_deref()
                        .is_some_and(|e| self.holds_elements(e))
                {
                    return unsupported("CASE of nodes or relationships of a list");
                }
                self.case(c, items)?
            }
            LogicalExpr::ReduceExpr(r) => {
                let reduce = RenderExpr::ReduceExpr(ReduceExpr {
                    accumulator: r.accumulator.clone(),
                    initial_value: Box::new(self.expr(&r.initial_value, items)?),
                    variable: r.variable.clone(),
                    list: Box::new(self.held_list(&r.list, items)?),
                    expression: Box::new(self.expr(&r.expression, items)?),
                });
                // Over a NULL list (held as `[]`, `null_list.rs`): NULL.
                match self.list_null(&r.list, items)? {
                    null_list::ListNull::When(c) => RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(c, RenderExpr::Literal(Literal::Null))],
                        else_expr: Some(Box::new(reduce)),
                    }),
                    _ => reduce,
                }
            }
            LogicalExpr::ArraySubscript { array, .. } | LogicalExpr::ArraySlicing { array, .. }
                if self.holds_elements(array) =>
            {
                // An element out of range would be a tuple of defaults.
                return unsupported("an element or slice of a list of nodes or relationships");
            }
            // `m['k']` of a map: as `m.k` (S11m), for a literal key.
            LogicalExpr::ArraySubscript { array, index }
                if matches!(self.kind(array), Kind::Map(_)) =>
            {
                let key = match (array.as_ref(), index.as_ref()) {
                    (
                        LogicalExpr::TableAlias(lx::TableAlias(n)),
                        LogicalExpr::Literal(lx::Literal::String(k)),
                    ) => parse_var(n).and_then(|v| self.map_key(v, k)),
                    _ => None,
                };
                match key {
                    Some(e) => e,
                    None => return unsupported("a key of a map (its value's type is not known)"),
                }
            }
            // (Of a NULL list held as `[]`: NULL, as `[][i]`.)
            LogicalExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
                array: Box::new(self.held_list(array, items)?),
                index: Box::new(self.expr(index, items)?),
            },
            LogicalExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
                array: Box::new(self.expr(array, items)?),
                from: from
                    .as_ref()
                    .map(|f| self.expr(f, items).map(Box::new))
                    .transpose()?,
                to: to
                    .as_ref()
                    .map(|t| self.expr(t, items).map(Box::new))
                    .transpose()?,
            },
            // A lambda other than a comprehension's (`scalar_fn`): a
            // higher-order function's argument written in the query
            // (`ch.arrayFilter(x -> x > 5, l)`, S11n). Its parameters are
            // the binder's local bindings; their values are the list's.
            LogicalExpr::Lambda(l) => {
                for p in &l.params {
                    if let Some(v) = parse_var(p) {
                        self.local_kinds.borrow_mut().insert(v, Kind::Unknown);
                    }
                }
                // The function's result is read as a value: a node,
                // relationship or map the body gives would be its tuple.
                if self.holds_elements(&l.body) || self.holds_maps(&l.body) {
                    return unsupported(
                        "a node, relationship or map as a lambda's value (a list comprehension \
                         returns them)",
                    );
                }
                let body = self.expr(&l.body, items)?;
                RenderExpr::Raw(
                    current_function_mapper().lambda(&l.params, &render_expr_to_sql_plain(&body)),
                )
            }
            LogicalExpr::Raw(_) | LogicalExpr::ColumnAlias(_) | LogicalExpr::Column(_) => {
                return unsupported("a planner-internal expression")
            }
            // Subquery expressions are bound as `Subquery`s (S9); these
            // are what conversion makes of a pattern the binder did not take
            // out (a node or shortestPath pattern as a value).
            LogicalExpr::PathPattern(_)
            | LogicalExpr::PatternCount(_)
            | LogicalExpr::PatternComprehension(_) => {
                return unsupported("a graph pattern inside an expression")
            }
        })
    }

    /// A list's element: a boolean one is cast to a boolean, as ClickHouse
    /// holds a comparison as `UInt8` (a list would show 1 / 0; a value in a
    /// column is cast by the emitter).
    pub(super) fn element(&self, e: &LogicalExpr, items: &Items) -> Result<RenderExpr, LowerError> {
        let value = self.expr(e, items)?;
        if self.kind(e) != Kind::Boolean
            || matches!(e, LogicalExpr::Literal(lx::Literal::Boolean(_)))
        {
            return Ok(value);
        }
        Ok(RenderExpr::Raw(
            current_function_mapper().cast_bool(&render_expr_to_sql_plain(&value)),
        ))
    }

    /// A list comprehension: `[x IN list WHERE p]` (`Filter`) or `[x IN list
    /// | e]` (`Map`). NULL when the list is.
    /// `held`: its list as an operation that knows its NULL takes it
    /// (`Lowerer::held_list`, `null_list.rs`).
    pub(super) fn comprehension(
        &self,
        how: Comprehension,
        x: VarId,
        body: &LogicalExpr,
        list: &LogicalExpr,
        items: &Items,
        held: bool,
    ) -> Result<RenderExpr, LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a list comprehension in this SQL dialect");
        };
        // Also gives the parameter its kind (an element of the list's).
        let kind = self.kind(list);
        self.local_kinds.borrow_mut().insert(x, kind.element());
        let list = match held {
            true => self.held_list(list, items)?,
            false => self.expr(list, items)?,
        };
        if kind == Kind::Null || matches!(list, RenderExpr::Literal(Literal::Null)) {
            return Ok(RenderExpr::Literal(Literal::Null));
        }
        self.local_lists
            .borrow_mut()
            .insert(x, render_expr_to_sql_plain(&list));
        let (spell, body) = match how {
            Comprehension::Filter => (spelling.filter, self.expr(body, items)?),
            Comprehension::Map => (spelling.map, self.element(body, items)?),
        };
        Ok(RenderExpr::Raw(spell(
            &x.name(),
            &render_expr_to_sql_plain(&body),
            &render_expr_to_sql_plain(&list),
        )))
    }

    /// A map literal (non-empty) as a map whose values keep their types.
    fn map_literal(
        &self,
        entries: &[(String, LogicalExpr)],
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a map literal in this SQL dialect");
        };
        let mut keys: Vec<&String> = entries.iter().map(|(k, _)| k).collect();
        keys.sort();
        keys.dedup();
        if keys.len() != entries.len() {
            return unsupported("a map literal with a repeated key");
        }
        let mut values = entries
            .iter()
            .map(|(k, v)| {
                let value = self.element(v, items)?;
                Ok(MapEntry {
                    key: k.clone(),
                    value: render_expr_to_sql_plain(&value),
                    holds_empty_list: holds_empty_list(v),
                })
            })
            .collect::<Result<Vec<_>, LowerError>>()?;
        // In key order: maps are equal by their entries (DISTINCT, grouping,
        // UNION), and ClickHouse compares them in order.
        values.sort_by(|a, b| a.key.cmp(&b.key));
        match (spelling.map_of)(&values) {
            Some(map) => Ok(RenderExpr::Raw(map)),
            None => unsupported("a map holding an empty list in this SQL dialect"),
        }
    }

    /// A map literal as the emitter reads it (a function's argument:
    /// `duration({days: 5})`), in key order.
    fn map_as_is(
        &self,
        entries: &[(String, LogicalExpr)],
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let mut entries = entries
            .iter()
            .map(|(k, v)| Ok((k.clone(), self.expr(v, items)?)))
            .collect::<Result<Vec<_>, LowerError>>()?;
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(RenderExpr::MapLiteral(entries))
    }

    fn all(&self, xs: &[LogicalExpr], items: &Items) -> Result<Vec<RenderExpr>, LowerError> {
        xs.iter().map(|x| self.expr(x, items)).collect()
    }

    /// A filter's condition, whose NULL drops a row as false does: as
    /// [`Self::expr`], but an `x IN l` reached through ANDs only is the
    /// dialect's two-valued `IN`, the same rows here, which a walk's start
    /// and ClickHouse's key analysis read (#1423 review).
    pub(super) fn condition(
        &self,
        p: &LogicalExpr,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        match p {
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                self.operator(op, items, true)
            }
            _ => self.expr(p, items),
        }
    }

    /// `filter`: of a condition ([`Self::condition`]).
    fn operator(&self, op: &LOp, items: &Items, filter: bool) -> Result<RenderExpr, LowerError> {
        use lx::Operator as O;
        let list = |e: &LogicalExpr| matches!(self.kind(e), Kind::List(_));
        match (op.operator, op.operands.as_slice()) {
            // A NULL list is held as `[]` (`null_list.rs`): NULL where its
            // source is.
            (O::IsNull | O::IsNotNull, [x]) if list(x) => {
                if let Some(c) = self.list_null(x, items)?.condition() {
                    let null = op.operator == O::IsNull;
                    return Ok(match c {
                        RenderExpr::Literal(Literal::Boolean(b)) => {
                            RenderExpr::Literal(Literal::Boolean(b == null))
                        }
                        c if null => c,
                        c => RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: O::Not,
                            operands: vec![c],
                        }),
                    });
                }
            }
            // `x IN [a, …]` of lists, one of which may be NULL: `x = a OR …`
            // through `=` of lists (NULL where either is), as Cypher's `IN`.
            (O::In | O::NotIn, [x, LogicalExpr::List(xs)])
                if !xs.is_empty() && (list(x) || xs.iter().any(list)) =>
            {
                let mut parts = Vec::new();
                for e in std::iter::once(x).chain(xs) {
                    if list(e) {
                        parts.push(self.list_null(e, items)?);
                    }
                }
                if matches!(
                    null_list::ListNull::any(parts),
                    null_list::ListNull::When(_)
                ) {
                    let op_of = |operator, operands| {
                        LogicalExpr::OperatorApplicationExp(LOp { operator, operands })
                    };
                    let any = xs
                        .iter()
                        .map(|item| op_of(O::Equal, vec![x.clone(), item.clone()]))
                        .reduce(|a, b| op_of(O::Or, vec![a, b]));
                    if let Some(any) = any {
                        let e = match op.operator {
                            O::NotIn => op_of(O::Not, vec![any]),
                            _ => any,
                        };
                        return self.expr(&e, items);
                    }
                }
            }
            // Lists compared: NULL where either is; else the held lists.
            (
                O::Equal
                | O::NotEqual
                | O::LessThan
                | O::GreaterThan
                | O::LessThanEqual
                | O::GreaterThanEqual,
                [a, b],
            ) if list(a) || list(b) => {
                let mut parts = Vec::new();
                for x in [a, b] {
                    if list(x) {
                        parts.push(self.list_null(x, items)?);
                    }
                }
                if let null_list::ListNull::When(c) = null_list::ListNull::any(parts) {
                    // (What it refuses stays refused.)
                    let value = match self.operator_body(op, items, filter)? {
                        constant @ RenderExpr::Literal(_) => constant,
                        _ => RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: op.operator,
                            operands: vec![self.held_list(a, items)?, self.held_list(b, items)?],
                        }),
                    };
                    // A boolean, as the comparison is (#1057).
                    return Ok(RenderExpr::Raw(current_function_mapper().cast_bool(
                        &render_expr_to_sql_plain(&RenderExpr::Case(RenderCase {
                            expr: None,
                            when_then: vec![(c, RenderExpr::Literal(Literal::Null))],
                            else_expr: Some(Box::new(value)),
                        })),
                    )));
                }
            }
            _ => {}
        }
        self.operator_body(op, items, filter)
    }

    fn operator_body(
        &self,
        op: &LOp,
        items: &Items,
        filter: bool,
    ) -> Result<RenderExpr, LowerError> {
        use lx::Operator as O;
        if let Some(e) = self.id_comparison(op)? {
            return Ok(e);
        }
        if let Some(e) = self.rel_id_comparison(op)? {
            return Ok(e);
        }
        // `id(a) = id(b)` / `<>` (S11l): ids are equal exactly when the
        // elements are (each endpoint's ids are unique per element).
        if let (O::Equal | O::NotEqual, [a, b]) = (op.operator, op.operands.as_slice()) {
            let id_of = |e: &LogicalExpr| match e {
                LogicalExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("id") => {
                    self.entity_arg(&f.args)
                }
                _ => None,
            };
            if let (Some(a), Some(b)) = (id_of(a), id_of(b)) {
                return self.identity_comparison(a, b, op.operator == O::Equal);
            }
        }
        // `p IS [NOT] NULL` of a path of the relation: NULL where an
        // OPTIONAL MATCH left it so, exactly where a node of it is (LDBC
        // IC 13: `CASE path IS NULL WHEN true THEN -1 …`).
        if let (O::IsNull | O::IsNotNull, [LogicalExpr::TableAlias(lx::TableAlias(n))]) =
            (op.operator, op.operands.as_slice())
        {
            let path = parse_var(n)
                .filter(|v| matches!(self.binding(*v).kind, BindingKind::Path))
                .and_then(|v| self.paths.get(&v));
            if let Some(path) = path {
                let null = op.operator == O::IsNull;
                let mut tests = Vec::new();
                for v in &path.nodes {
                    tests.push(match self.identity(*v)? {
                        Some(id) => super::null_test(&self.presence(*v, &id), null),
                        // A node that matches nothing: always NULL.
                        None => RenderExpr::Literal(Literal::Boolean(null)),
                    });
                }
                return Ok(match null {
                    true => super::or_all(tests),
                    false => {
                        super::and_all(tests).unwrap_or(RenderExpr::Literal(Literal::Boolean(true)))
                    }
                });
            }
        }
        // `x IN …` of nodes or relationships, by identity.
        if let (O::In | O::NotIn, [x, list]) = (op.operator, op.operands.as_slice()) {
            let negated = op.operator == O::NotIn;
            if let Some((sq, m)) = self.members_subquery(list) {
                if negated {
                    return unsupported("internal: NOT IN of a pattern predicate's nodes");
                }
                return self.in_members(x, list, (sq, m), items);
            }
            let element = |e: &LogicalExpr| {
                self.entity(e).is_some() || matches!(self.kind(e), Kind::Element(_))
            };
            match list {
                LogicalExpr::List(xs) if element(x) || xs.iter().any(element) => {
                    return self.in_listed(x, xs, negated, items);
                }
                _ if element(x)
                    && matches!(self.kind(list), Kind::List(k) if matches!(*k, Kind::Element(_))) =>
                {
                    return self.in_elements(x, list, negated, items);
                }
                _ => {}
            }
        }
        // A list's node or relationship (`elements.rs`) by its identity.
        let element_operands = op.operands.iter().any(|o| {
            matches!(o, LogicalExpr::TableAlias(_))
                && self.entity(o).is_none()
                && matches!(self.kind(o), Kind::Element(_))
        });
        if element_operands {
            return self.element_operator(op, items);
        }
        let entities: Vec<VarId> = op.operands.iter().filter_map(|o| self.entity(o)).collect();
        if !entities.is_empty() {
            return match (op.operator, op.operands.len(), entities.as_slice()) {
                (O::Equal | O::NotEqual, 2, [a, b]) => {
                    self.identity_comparison(*a, *b, op.operator == O::Equal)
                }
                // NULL exactly where its identity is (`null_test`).
                (O::IsNull | O::IsNotNull, 1, [a]) => {
                    let cols = match self.identity(*a)? {
                        Some(cols) => self.presence(*a, &cols),
                        None => vec![RenderExpr::Literal(Literal::Null)],
                    };
                    Ok(super::null_test(&cols, op.operator == O::IsNull))
                }
                _ => unsupported("a node or relationship as an operand (needs its value)"),
            };
        }
        if let (O::Addition, [a, b]) = (op.operator, op.operands.as_slice()) {
            if let Some(e) = self.list_addition(a, b, items, false)? {
                return Ok(e);
            }
        }
        if op.operands.iter().any(|o| self.holds_elements(o)) {
            return unsupported("an operator over nodes or relationships of a list");
        }
        // A map's values are held as values of any type (`Dynamic`): their
        // comparison tells `1` from `1.0`, and they have no order.
        if !matches!(op.operator, O::IsNull | O::IsNotNull)
            && op.operands.iter().any(|o| self.holds_maps(o))
        {
            return unsupported("a comparison or operator over maps");
        }
        if let Some(e) = self.boolean_operator(op, items)? {
            return Ok(e);
        }
        // `x IN …` of values.
        if let (O::In | O::NotIn, [x, list]) = (op.operator, op.operands.as_slice()) {
            let negated = op.operator == O::NotIn;
            let null = |e: &LogicalExpr| {
                matches!(e, LogicalExpr::Literal(lx::Literal::Null)) || self.kind(e) == Kind::Null
            };
            match list {
                // `null IN []` is false.
                LogicalExpr::List(xs) if xs.is_empty() => {
                    // Still lowered: what is refused in it stays refused.
                    self.expr(x, items)?;
                    return Ok(RenderExpr::Literal(Literal::Boolean(negated)));
                }
                _ if null(list) => {
                    self.expr(x, items)?;
                    return Ok(RenderExpr::Literal(Literal::Null));
                }
                // A list of items: `x = a OR x = b …` (the SQL printer).
                LogicalExpr::List(_) => {}
                // A filter's: as it is (NULL and false drop the row alike).
                _ if filter && !negated && !null(x) => {}
                _ => return self.in_values(x, list, negated, items),
            }
        }
        // A filter's AND: of conditions.
        if filter && op.operator == O::And {
            let operands: Result<Vec<RenderExpr>, LowerError> = op
                .operands
                .iter()
                .map(|o| self.condition(o, items))
                .collect();
            return Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: O::And,
                operands: operands?,
            }));
        }
        // A filter's `x IN l`: the list held (a NULL one as `[]`: no row
        // either way, `null_list.rs`).
        if let (O::In | O::NotIn, [x, list]) = (op.operator, op.operands.as_slice()) {
            return Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: vec![self.expr(x, items)?, self.held_list(list, items)?],
            }));
        }
        Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: op.operator,
            operands: self.all(&op.operands, items)?,
        }))
    }

    /// Whether `a` and `b` are a boolean and a number or a string (in either
    /// order): never equal, of no order (#1470).
    pub(super) fn apart(&self, a: &LogicalExpr, b: &LogicalExpr) -> bool {
        let boolean = |e: &LogicalExpr| self.kind(e) == Kind::Boolean;
        (boolean(a) && self.never_boolean(b)) || (self.never_boolean(a) && boolean(b))
    }

    /// Whether list `e` may hold a boolean, and whether it may hold a
    /// number or a string (`never_boolean`), as far as known here.
    fn list_elements(&self, e: &LogicalExpr) -> (bool, bool) {
        match e {
            LogicalExpr::List(xs) => (
                xs.iter().any(|x| self.kind(x) == Kind::Boolean),
                xs.iter().any(|x| self.never_boolean(x)),
            ),
            _ => (
                matches!(self.kind(e), Kind::List(k) if *k == Kind::Boolean),
                self.never_boolean_elements(e),
            ),
        }
    }

    /// Whether list `e` holds numbers or strings only (`never_boolean`).
    fn never_boolean_elements(&self, e: &LogicalExpr) -> bool {
        match e {
            LogicalExpr::ScalarFnCall(f) => matches!(
                f.name.to_ascii_lowercase().as_str(),
                "range" | "split" | "keys"
            ),
            LogicalExpr::AggregateFnCall(f) if f.name.eq_ignore_ascii_case("collect") => {
                f.args.first().is_some_and(|x| self.never_boolean(x))
            }
            _ => false,
        }
    }

    /// An operator over a boolean and a number or a string (#1470), which
    /// ClickHouse would read as a number (it holds a boolean as `UInt8`).
    /// Cypher: they are not equal (`=` false, `<>` true), have no order
    /// (`<` … NULL), and are NULL when either is; `x IN [ … ]` is an `=` per
    /// element; `+` is a type error. `None`: not such an operator.
    pub(super) fn boolean_operator(
        &self,
        op: &LOp,
        items: &Items,
    ) -> Result<Option<RenderExpr>, LowerError> {
        use lx::Operator as O;
        let boolean = |e: &LogicalExpr| self.kind(e) == Kind::Boolean;
        let apart = |a: &LogicalExpr, b: &LogicalExpr| self.apart(a, b);
        let [a, b] = op.operands.as_slice() else {
            return Ok(None);
        };
        // Lists of which one may hold a boolean where the other holds a
        // number or a string: ClickHouse compares them as numbers. Lists of
        // different lengths are not equal whatever they hold.
        let may_pair = || {
            let (ea, eb) = (self.list_elements(a), self.list_elements(b));
            (ea.0 && eb.1) || (ea.1 && eb.0)
        };
        let lists_apart = match (op.operator, a, b) {
            (O::Equal | O::NotEqual, LogicalExpr::List(xs), LogicalExpr::List(ys)) => {
                xs.len() == ys.len() && xs.iter().zip(ys).any(|(x, y)| apart(x, y))
            }
            (O::Equal | O::NotEqual, _, _) => may_pair(),
            (O::LessThan | O::GreaterThan | O::LessThanEqual | O::GreaterThanEqual, _, _) => {
                may_pair()
            }
            _ => false,
        };
        if lists_apart {
            return unsupported("a list of booleans compared with a list of other values");
        }
        match op.operator {
            O::Equal | O::NotEqual if apart(a, b) => {
                let operands = [self.expr(a, items)?, self.expr(b, items)?];
                let nulls: Vec<RenderExpr> = operands
                    .into_iter()
                    .filter(|e| !matches!(e, RenderExpr::Literal(l) if *l != Literal::Null))
                    .map(|e| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: O::IsNull,
                            operands: vec![e],
                        })
                    })
                    .collect();
                let value = RenderExpr::Literal(Literal::Boolean(op.operator == O::NotEqual));
                if nulls.is_empty() {
                    return Ok(Some(value));
                }
                Ok(Some(RenderExpr::Case(RenderCase {
                    expr: None,
                    when_then: vec![(super::or_all(nulls), RenderExpr::Literal(Literal::Null))],
                    else_expr: Some(Box::new(value)),
                })))
            }
            O::LessThan | O::GreaterThan | O::LessThanEqual | O::GreaterThanEqual
                if apart(a, b) =>
            {
                // Still lowered: what is refused in them stays refused.
                self.expr(a, items)?;
                self.expr(b, items)?;
                Ok(Some(RenderExpr::Literal(Literal::Null)))
            }
            O::In | O::NotIn => {
                let mixed = match b {
                    LogicalExpr::List(xs) => xs.iter().any(|x| apart(a, x)),
                    _ => false,
                };
                if mixed {
                    let LogicalExpr::List(xs) = b else {
                        return Ok(None);
                    };
                    // `x IN [e, …]` is `x = e OR …` (NULL where none is
                    // equal and one is NULL).
                    let any = xs
                        .iter()
                        .map(|x| {
                            LogicalExpr::OperatorApplicationExp(LOp {
                                operator: O::Equal,
                                operands: vec![a.clone(), x.clone()],
                            })
                        })
                        .reduce(|l, r| {
                            LogicalExpr::OperatorApplicationExp(LOp {
                                operator: O::Or,
                                operands: vec![l, r],
                            })
                        });
                    let Some(any) = any else {
                        return Ok(None);
                    };
                    let any = match op.operator {
                        O::NotIn => LogicalExpr::OperatorApplicationExp(LOp {
                            operator: O::Not,
                            operands: vec![any],
                        }),
                        _ => any,
                    };
                    return self.expr(&any, items).map(Some);
                }
                // Another list known to hold the other kind of values.
                let booleans = matches!(self.kind(b), Kind::List(k) if *k == Kind::Boolean);
                if (boolean(a) && self.never_boolean_elements(b))
                    || (self.never_boolean(a) && booleans)
                {
                    return unsupported("a boolean looked up in a list of other values (IN)");
                }
                Ok(None)
            }
            // A list's `+` was lowered before (`list_addition`); with NULL
            // it is NULL.
            O::Addition
            | O::Subtraction
            | O::Multiplication
            | O::Division
            | O::ModuloDivision
            | O::Exponentiation
                if (boolean(a) && (boolean(b) || self.never_boolean(b)))
                    || (boolean(b) && self.never_boolean(a)) =>
            {
                unsupported("arithmetic of a boolean (Neo4j: a type error)")
            }
            _ => Ok(None),
        }
    }

    /// `id(n) = N`, `id(n) <> N`, `id(n) [NOT] IN [N, …]` of a node `n` and
    /// integer literals (Neo4j Browser's expand and fetch queries; Bolt
    /// inlines their parameters). On the server an `N` is one of its ids
    /// (`LowerOptions::id_lookup`, as the `id()` rewrite reads them): the
    /// node of that label and key, or none. Without the server (library,
    /// `cg`) `id(n)` is the node's key, as an `id(n)` RETURN item is there.
    /// `None`: not such a comparison.
    fn id_comparison(&self, op: &LOp) -> Result<Option<RenderExpr>, LowerError> {
        use lx::Operator as O;
        let node_id = |e: &LogicalExpr| match e {
            LogicalExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("id") => self
                .entity_arg(&f.args)
                .filter(|v| matches!(self.binding(*v).kind, BindingKind::Node { .. })),
            _ => None,
        };
        let integer = |e: &LogicalExpr| match e {
            LogicalExpr::Literal(lx::Literal::Integer(i)) => Some(*i),
            _ => None,
        };
        let (v, ids, negated) = match (op.operator, op.operands.as_slice()) {
            (O::Equal | O::NotEqual, [a, b]) => {
                let (v, id) = match (node_id(a), integer(b), node_id(b), integer(a)) {
                    (Some(v), Some(id), _, _) | (_, _, Some(v), Some(id)) => (v, id),
                    _ => return Ok(None),
                };
                (v, vec![id], op.operator == O::NotEqual)
            }
            (O::In | O::NotIn, [a, LogicalExpr::List(xs)]) => {
                let (Some(v), Some(ids)) = (
                    node_id(a),
                    xs.iter().map(integer).collect::<Option<Vec<_>>>(),
                ) else {
                    return Ok(None);
                };
                (v, ids, op.operator == O::NotIn)
            }
            _ => return Ok(None),
        };
        let boolean = |b: bool| RenderExpr::Literal(Literal::Boolean(b));
        // `x IN []` is false, NULL `x` included.
        if ids.is_empty() {
            return Ok(Some(boolean(negated)));
        }
        let cols = match self.scans.get(&v) {
            // NULL on every row.
            Some(Scan::Impossible) => return Ok(Some(RenderExpr::Literal(Literal::Null))),
            Some(Scan::Node { .. } | Scan::Labels { .. }) => match self.identity(v)? {
                Some(cols) => cols,
                None => return Ok(Some(RenderExpr::Literal(Literal::Null))),
            },
            _ => return Ok(None),
        };
        // The node's labels, and where its key columns are in `cols`.
        let (labels, key): (Vec<&str>, &[RenderExpr]) = match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => (vec![label.as_str()], &cols[..]),
            Some(Scan::Labels { arms, .. }) => {
                (arms.iter().map(|(l, _)| l.as_str()).collect(), &cols[1..])
            }
            _ => return Ok(None),
        };
        // Its key's texts as its label's (I2).
        let own = self.own_texts(v, key.to_vec())?;
        let key: &[RenderExpr] = &own;
        let equal = |a: RenderExpr, b: RenderExpr| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: O::Equal,
                operands: vec![a, b],
            })
        };
        // A key compared with an id's text: a node of one label's column as
        // the `id()` rewrite compares it, the text read as the column's type
        // (ClickHouse; its key index still applies); a node of several
        // labels' as text, its labels' keys being of different types (a
        // `Variant`, which cannot read one text as all of them).
        let key_is = |k: &RenderExpr, text: &str| {
            // The id's text is a value: as a literal's raw body (a `\\` in it
            // is no escape).
            let text = RenderExpr::Literal(Literal::String(
                crate::utils::sql_literal::value_as_cypher_text(text),
            ));
            if labels.len() > 1 {
                let k = RenderExpr::Raw(format!(
                    "{}({})",
                    current_function_mapper().cast_string(),
                    render_expr_to_sql_plain(k)
                ));
                equal(k, text)
            } else {
                equal(k.clone(), text)
            }
        };
        let mut matches = Vec::new();
        for id in ids {
            let Some(lookup) = &self.options.id_lookup else {
                // The library's `id(n)`: its key, of whichever label.
                let [k] = key else {
                    return unsupported("id() of a node with a composite id compared with an id");
                };
                matches.push(key_is(k, &id.to_string()));
                continue;
            };
            let Some((label, value)) = (lookup.node)(id) else {
                continue;
            };
            // A composite key's parts escaped and joined by `|`, a NULL one
            // `\\N`; a key of one column is the whole text (which can hold
            // a `|`): `element_id::node_id_parts`.
            if !labels.contains(&label.as_str()) {
                continue;
            }
            let Some(parts) = crate::graph_catalog::element_id::node_id_parts(&value, key.len())
            else {
                continue;
            };
            let mut conds: Vec<RenderExpr> = key
                .iter()
                .zip(&parts)
                .map(|(k, p)| match p {
                    Some(p) => key_is(k, p),
                    // The node's own NULL part (ELEMENT_IDENTITY.md I6b).
                    None => RenderExpr::Raw(format!("{} IS NULL", render_expr_to_sql_plain(k))),
                })
                .collect();
            if labels.len() > 1 {
                conds.insert(
                    0,
                    equal(cols[0].clone(), RenderExpr::Literal(Literal::String(label))),
                );
            }
            matches.extend(super::and_all(conds));
        }
        let holds = if matches.is_empty() {
            // NULL where the node is.
            return Ok(Some(self.unless_null(&[v], boolean(negated))?));
        } else {
            super::or_all(matches)
        };
        Ok(Some(if negated {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: O::Not,
                operands: vec![holds],
            })
        } else {
            holds
        }))
    }

    /// `id(r) = N`, `id(r) <> N`, `id(r) [NOT] IN [N, …]` of a relationship
    /// `r` and integer literals, on the server (#1410, ELEMENT_IDENTITY.md
    /// I3b): an `N` is an id the server gave (`LowerOptions::id_lookup`'s
    /// `element`), the relationship of that element id, compared with `r`'s
    /// as its value spells it (`Lowerer::rel_element_id_sql`); an id it did
    /// not give, or of another type, names none. `None`: not such a
    /// comparison, or no server (the number is the server's).
    fn rel_id_comparison(&self, op: &LOp) -> Result<Option<RenderExpr>, LowerError> {
        use lx::Operator as O;
        let Some(lookup) = &self.options.id_lookup else {
            return Ok(None);
        };
        let rel_id = |e: &LogicalExpr| match e {
            LogicalExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("id") => {
                self.entity_arg(&f.args).filter(|v| {
                    matches!(
                        self.scans.get(v),
                        Some(Scan::Rel { .. } | Scan::Rels { .. } | Scan::Impossible)
                    ) && !matches!(self.binding(*v).kind, BindingKind::Node { .. })
                })
            }
            _ => None,
        };
        let integer = |e: &LogicalExpr| match e {
            LogicalExpr::Literal(lx::Literal::Integer(i)) => Some(*i),
            _ => None,
        };
        let (v, ids, negated) = match (op.operator, op.operands.as_slice()) {
            (O::Equal | O::NotEqual, [a, b]) => {
                let (v, id) = match (rel_id(a), integer(b), rel_id(b), integer(a)) {
                    (Some(v), Some(id), _, _) | (_, _, Some(v), Some(id)) => (v, id),
                    _ => return Ok(None),
                };
                (v, vec![id], op.operator == O::NotEqual)
            }
            (O::In | O::NotIn, [a, LogicalExpr::List(xs)]) => {
                let (Some(v), Some(ids)) = (
                    rel_id(a),
                    xs.iter().map(integer).collect::<Option<Vec<_>>>(),
                ) else {
                    return Ok(None);
                };
                (v, ids, op.operator == O::NotIn)
            }
            _ => return Ok(None),
        };
        let boolean = |b: bool| RenderExpr::Literal(Literal::Boolean(b));
        // `x IN []` is false, NULL `x` included.
        if ids.is_empty() {
            return Ok(Some(boolean(negated)));
        }
        let types: Vec<&str> = match self.scans.get(&v) {
            Some(Scan::Rel { rel_type, .. }) => vec![rel_type.as_str()],
            Some(Scan::Rels { arms, .. }) => arms.iter().map(|a| a.rel_type.as_str()).collect(),
            // NULL on every row.
            _ => return Ok(Some(RenderExpr::Literal(Literal::Null))),
        };
        let g = super::value::spelling()?;
        let Some(text) = self.rel_element_id_sql(v, &g)? else {
            return Ok(None);
        };
        let mut named: Vec<String> = Vec::new();
        for id in ids {
            let Some(element) = (lookup.element)(id) else {
                continue;
            };
            // Of one of its types: an element id starts `TYPE:`.
            let of_its_types = element
                .split_once(':')
                .is_some_and(|(t, _)| types.contains(&t));
            if of_its_types && !named.contains(&element) {
                named.push(element);
            }
        }
        if named.is_empty() {
            // NULL where the relationship is.
            return Ok(Some(self.unless_null(&[v], boolean(negated))?));
        }
        // Comparisons, so a RETURN item of one is a boolean (as a node's).
        // An element id's text as a literal's body, where `\` escapes.
        let holds = super::or_all(
            named
                .iter()
                .map(|e| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: O::Equal,
                        operands: vec![
                            RenderExpr::Raw(text.clone()),
                            RenderExpr::Literal(Literal::String(e.replace('\\', "\\\\"))),
                        ],
                    })
                })
                .collect(),
        );
        Ok(Some(if negated {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: O::Not,
                operands: vec![holds],
            })
        } else {
            holds
        }))
    }

    /// `x [NOT] IN [a, b, …]` of nodes or relationships: `x = a OR x = b …`
    /// (Cypher's `IN` is equality, three-valued as `OR` is: NULL where no
    /// item is `x` and one is NULL); of no item, false.
    fn in_listed(
        &self,
        x: &LogicalExpr,
        xs: &[LogicalExpr],
        negated: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let op = |operator, operands| {
            LogicalExpr::OperatorApplicationExp(lx::OperatorApplication { operator, operands })
        };
        let equalities: Vec<LogicalExpr> = xs
            .iter()
            .map(|item| match item {
                LogicalExpr::Literal(lx::Literal::Null) => item.clone(),
                _ => op(lx::Operator::Equal, vec![x.clone(), item.clone()]),
            })
            .collect();
        let found = match equalities.len() {
            0 => LogicalExpr::Literal(lx::Literal::Boolean(false)),
            1 => equalities
                .into_iter()
                .next()
                .unwrap_or(LogicalExpr::Literal(lx::Literal::Null)),
            _ => op(lx::Operator::Or, equalities),
        };
        let e = match negated {
            true => op(lx::Operator::Not, vec![found]),
            false => found,
        };
        self.expr(&e, items)
    }

    /// `x [NOT] IN l` of a node or relationship `x` and a list `l` of them
    /// (`collect(n)`: element tuples, `elements.rs`): whether an element of
    /// `l` has `x`'s identity; NULL where `x` or `l` is (a collected list
    /// holds no NULL).
    fn in_elements(
        &self,
        x: &LogicalExpr,
        list: &LogicalExpr,
        negated: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let (Kind::Element(x_elem), Kind::List(inner)) = (self.kind(x), self.kind(list)) else {
            return unsupported(
                "IN of a list of nodes or relationships, of a node of several labels",
            );
        };
        let Kind::Element(elem) = *inner else {
            return unsupported("IN of a list of nodes or relationships of several labels");
        };
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a list of nodes or relationships in this SQL dialect");
        };
        let x_id = match self.entity(x).map(|v| self.identity(v)).transpose()? {
            Some(Some(id)) => id,
            Some(None) => vec![RenderExpr::Literal(Literal::Null)],
            None => self.elem_identity(&self.expr(x, items)?, &x_elem)?,
        };
        let list_sql = render_expr_to_sql_plain(&self.expr(list, items)?);
        // Elements of other labels or types are never `x`.
        let found = if x_elem != elem {
            "false".to_string()
        } else {
            const T: &str = "__cg_in";
            let t_id = self.elem_identity(&RenderExpr::Raw(T.to_string()), &elem)?;
            // A composite id's part that may be NULL compared NULL-safely
            // (I6c).
            let same = super::same_identity(t_id, x_id.clone(), &self.elem_nullable(&elem), true)
                .unwrap_or(RenderExpr::Literal(Literal::Boolean(false)));
            let matching = (spelling.filter)(T, &render_expr_to_sql_plain(&same), &list_sql);
            format!("{} > 0", current_function_mapper().list_size(&matching))
        };
        let found = match negated {
            true => format!("NOT ({found})"),
            false => found,
        };
        // Of an empty list, false (`null IN []` too); else NULL where `x` is:
        // where every column of its presence is (a composite id's part can
        // be NULL in a node, I6c).
        let x_absent: Vec<String> = elem_presence(&x_id, &x_elem)
            .iter()
            .map(|c| format!("{} IS NULL", render_expr_to_sql_plain(c)))
            .collect();
        let m = current_function_mapper();
        Ok(RenderExpr::Raw(m.cast_bool(&format!(
            "CASE WHEN {list_sql} IS NULL THEN NULL WHEN {} = 0 THEN {} WHEN {} THEN NULL \
             ELSE {found} END",
            m.list_size(&list_sql),
            if negated { "true" } else { "false" },
            x_absent.join(" AND ")
        ))))
    }

    /// `x [NOT] IN l` of a value `x` and a list expression `l` (not a list of
    /// items): Cypher's three-valued `IN`. NULL where `l` is; of an empty
    /// `l`, false (`null IN []` too); else NULL where `x` is; true where an
    /// element is `x`; NULL where an element is NULL; else false.
    /// (ClickHouse's `x IN l` is false where an element is NULL, #1423.)
    fn in_values(
        &self,
        x: &LogicalExpr,
        list: &LogicalExpr,
        negated: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let m = current_function_mapper();
        let Some(spelling) = m.lists() else {
            return unsupported("IN of a list expression in this SQL dialect");
        };
        let x_sql = render_expr_to_sql_plain(&self.expr(x, items)?);
        let list_sql = render_expr_to_sql_plain(&self.held_list(list, items)?);
        // A NULL list is held as `[]` (`null_list.rs`): NULL where its
        // source is.
        let list_null = match self.list_null(list, items)?.condition() {
            Some(RenderExpr::Literal(Literal::Boolean(false))) => String::new(),
            Some(c) => format!("WHEN {} THEN NULL ", render_expr_to_sql_plain(&c)),
            None => format!("WHEN ({list_sql}) IS NULL THEN NULL "),
        };
        let (found, not_found) = match negated {
            true => ("false", "true"),
            false => ("true", "false"),
        };
        // A NULL `x`: not looked up (Spark refuses a NULL-typed one).
        if matches!(x, LogicalExpr::Literal(lx::Literal::Null)) || self.kind(x) == Kind::Null {
            return Ok(RenderExpr::Raw(m.cast_bool(&format!(
                "CASE {list_null}WHEN {} = 0 THEN {not_found} ELSE NULL END",
                m.list_size(&list_sql),
            ))));
        }
        const T: &str = "__cg_in";
        let nulls = (spelling.filter)(T, &format!("{T} IS NULL"), &list_sql);
        Ok(RenderExpr::Raw(m.cast_bool(&format!(
            "CASE {list_null}WHEN {} = 0 THEN {not_found} \
             WHEN ({x_sql}) IS NULL THEN NULL WHEN {}({list_sql}, {x_sql}) THEN {found} \
             WHEN {} > 0 THEN NULL ELSE {not_found} END",
            m.list_size(&list_sql),
            m.array_contains(),
            m.list_size(&nulls),
        ))))
    }

    /// The subquery `list` reads the value of when it is one of the nodes of
    /// a pattern predicate's matches ([`SubqueryKind::Members`]), and its
    /// node.
    fn members_subquery(&self, list: &LogicalExpr) -> Option<(VarId, VarId)> {
        let v = subquery_var(list)?;
        match self.subqueries.iter().find(|s| s.var == v)?.kind {
            SubqueryKind::Members { node, .. } => Some((v, node)),
            _ => None,
        }
    }

    /// `x IN` the nodes `m` of subquery `sq`'s matches (S11o9: a pattern
    /// predicate of a list comprehension's element `x`): whether `x`, a node
    /// of one label, is one of them, by its label and key. False for a NULL
    /// `x` (Neo4j: the predicate of a NULL node is false) and of no match.
    fn in_members(
        &self,
        x: &LogicalExpr,
        list: &LogicalExpr,
        (sq, m): (VarId, VarId),
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let never = || Ok(RenderExpr::Literal(Literal::Boolean(false)));
        // Its label, identity and the columns NULL when it is.
        let (label, x_id, absent) =
            match self.entity(x) {
                Some(v) => match self.scans.get(&v) {
                    Some(Scan::Node { label, .. }) => {
                        let Some(id) = self.identity(v)? else {
                            return never();
                        };
                        let absent = self.presence(v, &id);
                        (label.clone(), id, absent)
                    }
                    Some(Scan::Impossible) => return never(),
                    _ => return unsupported(
                        "a pattern predicate in a list comprehension of a node of several labels",
                    ),
                },
                None => match self.kind(x) {
                    Kind::Element(elem @ Elem::Node(_)) => {
                        let Elem::Node(label) = &elem else {
                            return unsupported("internal: not a node");
                        };
                        let id = self.elem_identity(&self.expr(x, items)?, &elem)?;
                        let absent = elem_presence(&id, &elem);
                        (label.clone(), id, absent)
                    }
                    _ => return unsupported(
                        "a pattern predicate in a list comprehension of an element that is not \
                         a node of one label",
                    ),
                },
            };
        if matches!(self.values.get(&sq), Some(RenderExpr::List(v)) if v.is_empty()) {
            return never();
        }
        let BindingKind::Node { labels } = &self.binding(m).kind else {
            return unsupported("internal: a pattern predicate's element that is not a node");
        };
        if !labels.contains(&label) {
            return never();
        }
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a pattern predicate in a list comprehension in this SQL dialect");
        };
        // A match's tuple: its label, then its identity.
        let probe = |id: &[RenderExpr]| {
            let mut fields = vec![render_expr_to_sql_plain(&RenderExpr::Literal(
                Literal::String(label.clone()),
            ))];
            fields.extend(id.iter().map(render_expr_to_sql_plain));
            (spelling.tuple)(&fields)
        };
        let mut members = render_expr_to_sql_plain(&self.expr(list, items)?);
        // Of a comprehension's element: those of its list's elements, once
        // per row (rather than each element searching every match).
        let local = match x {
            LogicalExpr::TableAlias(a) => parse_var(&a.0),
            _ => None,
        };
        if let (Some(elements), Kind::Element(elem)) = (
            local.and_then(|v| self.local_lists.borrow().get(&v).cloned()),
            self.kind(x),
        ) {
            const E: &str = "__cg_e";
            let id = self.elem_identity(&RenderExpr::Raw(E.to_string()), &elem)?;
            let probes = (spelling.map)(E, &probe(&id), &elements);
            members = (spelling.intersect)(&members, &probes);
        }
        let found = (spelling.contains)(&members, &probe(&x_id));
        let absent: Vec<String> = absent
            .iter()
            .map(|c| format!("{} IS NULL", render_expr_to_sql_plain(c)))
            .collect();
        let m = current_function_mapper();
        Ok(RenderExpr::Raw(m.cast_bool(&format!(
            "CASE WHEN {} THEN false WHEN {found} THEN true ELSE false END",
            absent.join(" OR ")
        ))))
    }

    /// `a IS [NOT] NULL`, `a = b`, `a <> b` where an operand is a list's node
    /// or relationship: by its identity (elements of different labels or
    /// types are never equal).
    fn element_operator(&self, op: &LOp, items: &Items) -> Result<RenderExpr, LowerError> {
        use lx::Operator as O;
        let mut els = Vec::new();
        for o in &op.operands {
            let Kind::Element(elem) = self.kind(o) else {
                return unsupported("a node or relationship of a list compared with a value");
            };
            // A node or relationship of the relation: its own columns.
            let id = match self.entity(o).map(|v| self.identity(v)).transpose()? {
                Some(Some(id)) => id,
                Some(None) => vec![RenderExpr::Literal(Literal::Null)],
                None => self.elem_identity(&self.expr(o, items)?, &elem)?,
            };
            els.push((id, elem));
        }
        let is_null = |id: &RenderExpr| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: O::IsNull,
                operands: vec![id.clone()],
            })
        };
        match (op.operator, els.as_slice()) {
            (O::IsNull | O::IsNotNull, [(id, elem)]) => Ok(super::null_test(
                &elem_presence(id, elem),
                op.operator == O::IsNull,
            )),
            (O::Equal | O::NotEqual, [(a, ea), (b, eb)]) => {
                let equal = op.operator == O::Equal;
                if ea != eb {
                    // NULL when either is NULL.
                    return Ok(RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(
                            super::or_all(vec![is_null(&a[0]), is_null(&b[0])]),
                            RenderExpr::Literal(Literal::Null),
                        )],
                        else_expr: Some(Box::new(RenderExpr::Literal(Literal::Boolean(!equal)))),
                    }));
                }
                // A composite id's part that may be NULL compared NULL-safely
                // (I6c): then NULL only where an element is.
                let nullable = self.elem_nullable(ea);
                let same = super::same_identity(a.clone(), b.clone(), &nullable, equal)
                    .expect("an identity has columns");
                if !nullable.contains(&true) {
                    return Ok(same);
                }
                let absent = super::or_all(vec![
                    super::null_test(&elem_presence(a, ea), true),
                    super::null_test(&elem_presence(b, eb), true),
                ]);
                Ok(RenderExpr::Case(RenderCase {
                    expr: None,
                    when_then: vec![(absent, RenderExpr::Literal(Literal::Null))],
                    else_expr: Some(Box::new(same)),
                }))
            }
            _ => unsupported("an operator over nodes or relationships of a list"),
        }
    }

    /// Whether `e` is a map, or a list holding maps (S7e3).
    pub(super) fn holds_maps(&self, e: &LogicalExpr) -> bool {
        fn holds(k: &Kind) -> bool {
            match k {
                Kind::Map(_) => true,
                Kind::List(k) => holds(k),
                _ => false,
            }
        }
        holds(&self.kind(e))
    }

    /// Whether `e` is a list's node or relationship, or a list holding them.
    pub(super) fn holds_elements(&self, e: &LogicalExpr) -> bool {
        fn holds(k: &Kind) -> bool {
            match k {
                Kind::Element(_) => true,
                Kind::List(k) => holds(k),
                _ => false,
            }
        }
        holds(&self.kind(e))
    }

    /// `value` for an element, NULL where the element tuple `t` is NULL.
    fn elem_unless_null(
        &self,
        t: &RenderExpr,
        elem: &Elem,
        value: Literal,
    ) -> Result<RenderExpr, LowerError> {
        let id = self.elem_identity(t, elem)?;
        Ok(RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(
                super::null_test(&elem_presence(&id, elem), true),
                RenderExpr::Literal(Literal::Null),
            )],
            else_expr: Some(Box::new(RenderExpr::Literal(value))),
        }))
    }

    /// `a + b` where one side is a list: the lists concatenated, a value
    /// appended or prepended as a list of itself; NULL with NULL. `None`
    /// when neither side is known to be a list (the emitter decides).
    /// ClickHouse's `plus` of arrays adds them element by element.
    /// `held`: as an operation that knows its NULL takes it, its lists
    /// held, NULL (`[]`) where either is (`null_list.rs`).
    pub(super) fn list_addition(
        &self,
        a: &LogicalExpr,
        b: &LogicalExpr,
        items: &Items,
        held: bool,
    ) -> Result<Option<RenderExpr>, LowerError> {
        let (ka, kb) = (self.kind(a), self.kind(b));
        if !matches!(ka, Kind::List(_)) && !matches!(kb, Kind::List(_)) {
            return Ok(None);
        }
        if ka == Kind::Null || kb == Kind::Null {
            return Ok(Some(RenderExpr::Literal(Literal::Null)));
        }
        // Lists of elements of one label or definition only (their tuples
        // are of one type); a node appended is NULL when it is NULL.
        if (self.holds_elements(a) || self.holds_elements(b)) && !(one_shape(&ka) && ka == kb) {
            return unsupported(
                "`+` of lists of nodes or relationships of different labels or types, \
                 or with a value",
            );
        }
        let mut args = Vec::new();
        for (e, k) in [(a, ka), (b, kb)] {
            args.push(match k {
                // `[1] + NULL` is NULL, and a ClickHouse array cannot be:
                // only a value that is never NULL is appended.
                Kind::Scalar | Kind::Boolean
                    if !matches!(e, LogicalExpr::Literal(l) if *l != lx::Literal::Null) =>
                {
                    return unsupported("`+` of a list and a value that may be NULL");
                }
                Kind::Scalar | Kind::Boolean => RenderExpr::List(vec![self.element(e, items)?]),
                // A list, or a value that must be one (ClickHouse refuses
                // to concatenate anything else).
                _ if held => self.held_list(e, items)?,
                _ => self.expr(e, items)?,
            });
        }
        let concat = RenderExpr::ScalarFnCall(ScalarFnCall {
            name: current_function_mapper().array_concat().to_string(),
            args,
        });
        if !held {
            return Ok(Some(concat));
        }
        // NULL (held as `[]`, `null_list.rs`) where either list is.
        let mut parts = Vec::new();
        for x in [a, b] {
            if matches!(self.kind(x), Kind::List(_)) {
                parts.push(self.list_null(x, items)?);
            }
        }
        if parts
            .iter()
            .any(|p| matches!(p, null_list::ListNull::When(_)))
            && parts.contains(&null_list::ListNull::Unknown)
        {
            return unsupported(
                "`+` of a list that may be NULL and a list not known to be NULL or not",
            );
        }
        Ok(Some(match null_list::ListNull::any(parts) {
            null_list::ListNull::When(c) => null_list::empty_unless(c, concat),
            _ => concat,
        }))
    }

    /// `a = b` / `a <> b` between two nodes or two relationships: equal only
    /// when they have the same label (type) and identity.
    fn identity_comparison(
        &self,
        a: VarId,
        b: VarId,
        equal: bool,
    ) -> Result<RenderExpr, LowerError> {
        let labels = |v: VarId| match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Some(vec![label.clone()]),
            Some(Scan::Labels { arms, .. }) => Some(arms.iter().map(|(l, _)| l.clone()).collect()),
            _ => None,
        };
        if let (Some(la), Some(lb)) = (labels(a), labels(b)) {
            if la.len() > 1 || lb.len() > 1 {
                return self.labeled_identity_comparison(a, b, &la, &lb, equal);
            }
        }
        let same_kind = match (self.scans.get(&a), self.scans.get(&b)) {
            // An element that matches nothing: the relation has no rows.
            (Some(Scan::Impossible), _) | (_, Some(Scan::Impossible)) => {
                return Ok(RenderExpr::Literal(Literal::Null))
            }
            (Some(Scan::Rels { .. }), Some(Scan::Rel { .. } | Scan::Rels { .. }))
            | (Some(Scan::Rel { .. }), Some(Scan::Rels { .. })) => {
                return self.defined_identity_comparison(a, b, equal)
            }
            (Some(Scan::Node { label: x, .. }), Some(Scan::Node { label: y, .. })) => x == y,
            // One type can have several edge definitions (one per endpoint
            // label pair, each its own table): only rows of the same
            // definition can be the same relationship.
            (Some(Scan::Rel { schema: x, .. }), Some(Scan::Rel { schema: y, .. })) => {
                std::ptr::eq(*x, *y)
            }
            _ => false,
        };
        if !same_kind {
            return self.unless_null(&[a, b], RenderExpr::Literal(Literal::Boolean(!equal)));
        }
        let (Some(ca), Some(cb)) = (self.identity(a)?, self.identity(b)?) else {
            return Ok(RenderExpr::Literal(Literal::Null));
        };
        // A composite id's part that may be NULL compared NULL-safely (I6c):
        // then NULL only where an element is.
        let nullable = self.identity_nullable(a);
        let same = super::same_identity(ca, cb, &nullable, equal).expect("an identity has columns");
        match nullable.contains(&true) {
            true => self.unless_null(&[a, b], same),
            false => Ok(same),
        }
    }

    /// `a = b` / `a <> b` between relationships, one of several possible
    /// types or label pairs: equal when they have the same definition (type,
    /// labels of the stored ends) and identity. Past a definition's own arity
    /// a union's identity is NULL in both, so it compares NULL-safely.
    fn defined_identity_comparison(
        &self,
        a: VarId,
        b: VarId,
        equal: bool,
    ) -> Result<RenderExpr, LowerError> {
        let texts = self.definition_texts(a, b);
        let (ia, ib) = (
            self.definition_identity(a, &texts)?,
            self.definition_identity(b, &texts)?,
        );
        let same: Vec<RenderExpr> = ia
            .into_iter()
            .zip(ib)
            .enumerate()
            .map(|(i, (x, y))| {
                RenderExpr::OperatorApplicationExp(if i < 3 {
                    super::eq(x, y)
                } else {
                    super::not_distinct(x, y)
                })
            })
            .collect();
        let same = super::and_all(same).expect("a definition has columns");
        let value = if equal {
            same
        } else {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: lx::Operator::Not,
                operands: vec![same],
            })
        };
        self.unless_null(&[a, b], value)
    }

    /// `a = b` / `a <> b` between nodes, one of several possible labels:
    /// equal when they have the same label and id. A label column and a
    /// table's constant label compare as values, so an OPTIONAL MATCH's
    /// NULL node compares as NULL.
    fn labeled_identity_comparison(
        &self,
        a: VarId,
        b: VarId,
        la: &[String],
        lb: &[String],
        equal: bool,
    ) -> Result<RenderExpr, LowerError> {
        if !la.iter().any(|l| lb.contains(l)) {
            return self.unless_null(&[a, b], RenderExpr::Literal(Literal::Boolean(!equal)));
        }
        let label = |v: VarId, labels: &[String]| -> Result<RenderExpr, LowerError> {
            match labels {
                [one] => self.unless_null(&[v], RenderExpr::Literal(Literal::String(one.clone()))),
                _ => self.physical(v, super::LABEL_COLUMN),
            }
        };
        let (Some(ia), Some(ib)) = (self.id_columns(a)?, self.id_columns(b)?) else {
            return Ok(RenderExpr::Literal(Literal::Null));
        };
        if ia.len() != ib.len() {
            return unsupported("comparing nodes whose ids have different arities (S8)");
        }
        // A node of one label compared with one of several: its id as theirs
        // holds it (I2).
        let mixed = |v: VarId| matches!(self.scans.get(&v), Some(Scan::Labels { .. }));
        let held = |labels: &[String], ids: Vec<RenderExpr>, other: VarId| match labels {
            [one] if mixed(other) => ids
                .into_iter()
                .enumerate()
                .map(|(i, id)| self.as_held(one, i, id))
                .collect(),
            _ => ids,
        };
        let (ia, ib) = (held(la, ia, b), held(lb, ib, a));
        // The id parts that may be NULL on either side compared NULL-safely
        // (I6c), its label not.
        let parts = |v: VarId| -> Vec<bool> {
            let n = self.identity_nullable(v);
            match self.scans.get(&v) {
                Some(Scan::Labels { .. }) => n.into_iter().skip(1).collect(),
                _ => n,
            }
        };
        let (na, nb) = (parts(a), parts(b));
        let nullable: Vec<bool> = std::iter::once(false)
            .chain((0..ia.len()).map(|i| na.get(i) == Some(&true) || nb.get(i) == Some(&true)))
            .collect();
        let mut left = vec![label(a, la)?];
        left.extend(ia);
        let mut right = vec![label(b, lb)?];
        right.extend(ib);
        let same =
            super::same_identity(left, right, &nullable, equal).expect("an identity has columns");
        match nullable.contains(&true) {
            true => self.unless_null(&[a, b], same),
            false => Ok(same),
        }
    }

    /// The condition of each `WHEN` of `c` (`None`: it never matches). Of a
    /// simple CASE, `CASE x WHEN w …` is `CASE WHEN x = w …`: three-valued,
    /// a NULL never matches (ClickHouse's `caseWithExpression` takes NULL for
    /// equal to NULL, #1412).
    pub(super) fn case_conditions(
        &self,
        c: &LogicalCase,
        items: &Items,
    ) -> Result<Vec<Option<RenderExpr>>, LowerError> {
        let null = |e: &LogicalExpr| matches!(e, LogicalExpr::Literal(lx::Literal::Null));
        c.when_then
            .iter()
            .map(|(w, _)| match c.expr.as_deref() {
                None => self.expr(w, items).map(Some),
                Some(x) if null(x) || null(w) => Ok(None),
                // `CASE b WHEN 1 …` of a boolean and a number or a string
                // (or the other way): never equal (ClickHouse would compare
                // them as numbers, #1470).
                Some(x) if self.apart(x, w) => {
                    // Still lowered: what is refused in it stays refused.
                    self.expr(w, items)?;
                    Ok(None)
                }
                Some(x) => self
                    .expr(
                        &LogicalExpr::OperatorApplicationExp(LOp {
                            operator: lx::Operator::Equal,
                            operands: vec![x.clone(), w.clone()],
                        }),
                        items,
                    )
                    .map(Some),
            })
            .collect()
    }

    fn case(&self, c: &LogicalCase, items: &Items) -> Result<RenderExpr, LowerError> {
        self.case_held(c, false, items)
    }

    /// A CASE; `held`: of lists whose NULL travels beside it (a WITH's
    /// `{v}__null`, a reader of its source: `null_list.rs`), its NULL results
    /// the empty list, as ClickHouse holds a NULL list (an array is never
    /// NULL; a `Variant` of one no array function takes). Elsewhere a NULL
    /// stays NULL (a list of them, a map's value, …).
    pub(super) fn case_held(
        &self,
        c: &LogicalCase,
        held: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let lists = held
            && c.when_then
                .iter()
                .map(|(_, t)| t)
                .chain(c.else_expr.as_deref())
                .any(|t| matches!(self.kind(t), Kind::List(_)));
        // A list literal of several items may be of several types (a
        // `Variant`), which ClickHouse converts no `[]` to: then the empty
        // list of its type.
        let empty = match c
            .when_then
            .iter()
            .map(|(_, t)| t)
            .chain(c.else_expr.as_deref())
            .find(|t| matches!(t, LogicalExpr::List(xs) if xs.len() > 1))
        {
            Some(t) if lists => null_list::empty_like(&self.expr(t, items)?),
            _ => RenderExpr::List(Vec::new()),
        };
        let result = |t: &LogicalExpr| -> Result<RenderExpr, LowerError> {
            match t {
                LogicalExpr::Literal(lx::Literal::Null) if lists => Ok(empty.clone()),
                // A list result held too (a NULL one as `[]`).
                t if lists && matches!(self.kind(t), Kind::List(_)) => self.held_list(t, items),
                t => self.expr(t, items),
            }
        };
        let mut when_then = Vec::new();
        for (condition, (_, t)) in self
            .case_conditions(c, items)?
            .into_iter()
            .zip(&c.when_then)
        {
            let t_sql = result(t)?;
            if let Some(w_sql) = condition {
                when_then.push((w_sql, t_sql));
            }
        }
        let else_expr = match &c.else_expr {
            Some(e) => Some(Box::new(result(e)?)),
            None if lists => Some(Box::new(empty.clone())),
            None => None,
        };
        if when_then.is_empty() {
            return Ok(else_expr.map_or(RenderExpr::Literal(Literal::Null), |e| *e));
        }
        Ok(RenderExpr::Case(RenderCase {
            expr: None,
            when_then,
            else_expr,
        }))
    }

    /// A bare variable: a projected item, a node / relationship identity, or
    /// a comprehension / reduce variable (printed by its generated name).
    pub(super) fn variable(&self, name: &str, items: &Items) -> Result<RenderExpr, LowerError> {
        let Some(v) = parse_var(name) else {
            return unsupported("an unbound variable");
        };
        if let Some(item) = items.get(&v) {
            return Ok(item.clone());
        }
        let b = self.binding(v);
        match (&b.kind, &b.source) {
            // A walk's relationships as a list's elements (`elements.rs`).
            (
                BindingKind::Rel {
                    length: Some(_), ..
                },
                _,
            ) if self.walk_rel_elem(v).is_some() => {
                let Some(Scan::Path { reversed, .. }) = self.scans.get(&v) else {
                    return unsupported(format!("internal: {v} is not a path relation"));
                };
                Ok(RenderExpr::Raw(self.walk_tuples_of(v, false, *reversed)?))
            }
            (
                BindingKind::Rel {
                    length: Some(_), ..
                },
                _,
            ) => unsupported(
                "a variable-length relationship's list other than as a RETURN / WITH item or in \
                 size() (S7 lists)",
            ),
            // As a list's element (`elements.rs`): where an element is
            // read is decided by its kind (`holds_elements`).
            (BindingKind::Node { .. } | BindingKind::Rel { .. }, _)
                if self.elem_of(v).is_some() =>
            {
                self.elem_tuple(v)
            }
            (BindingKind::Node { .. } | BindingKind::Rel { .. }, _) => {
                unsupported("a node or relationship as a value (in a list, collect(), CASE …)")
            }
            // A comprehension's parameter: its name as written, which the
            // emitter does not read as a node (a bare `TableAlias` compared
            // with another is taken as a node identity, `x.id = y.id`).
            (BindingKind::Value, BindingSource::Local)
                if self.local_kinds.borrow().contains_key(&v) =>
            {
                Ok(RenderExpr::Raw(name.to_string()))
            }
            (BindingKind::Value, BindingSource::Local) => {
                Ok(RenderExpr::TableAlias(TableAlias(name.to_string())))
            }
            (BindingKind::Path, _) => {
                unsupported("a path other than as a RETURN / WITH item or in length() (S7 lists)")
            }
            (BindingKind::Value, _) if self.graph_values.contains_key(&v) => unsupported(
                "a carried list of nodes or relationships other than as an item or in size() \
                 (S7 lists)",
            ),
            (BindingKind::Value, _) => match self.values.get(&v) {
                Some(e) => Ok(e.clone()),
                // `head(collect({…}))`: its keys only (S11o8).
                None if self.map_keys.contains_key(&v) => {
                    unsupported("a collected map other than by its keys")
                }
                None => unsupported("an UNWIND value (S7)"),
            },
        }
    }

    /// The node or relationship a bare variable expression names (not a
    /// variable-length relationship: that is a list).
    pub(super) fn entity(&self, e: &LogicalExpr) -> Option<VarId> {
        match e {
            LogicalExpr::TableAlias(lx::TableAlias(n)) => parse_var(n).filter(|v| {
                matches!(
                    self.binding(*v).kind,
                    BindingKind::Node { .. } | BindingKind::Rel { length: None, .. }
                )
            }),
            _ => None,
        }
    }

    /// The path a bare variable expression names.
    fn path_var(&self, args: &[LogicalExpr]) -> Option<VarId> {
        match args {
            [LogicalExpr::TableAlias(lx::TableAlias(n))] => {
                parse_var(n).filter(|v| matches!(self.binding(*v).kind, BindingKind::Path))
            }
            _ => None,
        }
    }

    /// `length(p)`: the path's fixed relationships, plus the hops of each
    /// variable-length one. NULL for a path an OPTIONAL MATCH did not match.
    pub(super) fn path_length(&self, p: VarId) -> Result<RenderExpr, LowerError> {
        let length = self.ungrouped_path_length(p)?;
        // A path a grouping RETURN returns: any of its group's (the path,
        // the group's key, determines it), all of it one aggregate.
        Ok(match self.grouped_paths.contains(&p) {
            true => RenderExpr::Raw(
                current_function_mapper().any_non_null(&render_expr_to_sql_plain(&length)),
            ),
            false => length,
        })
    }

    /// Refuses reading the nodes or relationships of a path a grouping
    /// RETURN returns (its ORDER BY): their columns are not the group's.
    pub(super) fn ungrouped_elements(&self, p: VarId) -> Result<(), LowerError> {
        if self.grouped_paths.contains(&p) {
            return unsupported(
                "the nodes or relationships of a path a DISTINCT or aggregating RETURN \
                 returns, in its ORDER BY",
            );
        }
        Ok(())
    }

    fn ungrouped_path_length(&self, p: VarId) -> Result<RenderExpr, LowerError> {
        let Some(elements) = self.paths.get(&p) else {
            return unsupported(format!("internal: path {p} has no elements"));
        };
        let mut fixed = 0;
        let mut terms = Vec::new();
        for r in &elements.rels {
            match self.scans.get(r) {
                Some(Scan::Rel { .. } | Scan::Rels { .. }) => fixed += 1,
                Some(Scan::Path { .. }) => terms.push(self.physical(*r, "hop_count")?),
                // The relation has no rows (or the OPTIONAL MATCH no match).
                Some(Scan::Impossible) => return Ok(RenderExpr::Literal(Literal::Null)),
                _ => return unsupported(format!("internal: {r} is not a relationship scan")),
            }
        }
        if fixed > 0 || terms.is_empty() {
            terms.push(RenderExpr::Literal(Literal::Integer(fixed)));
        }
        let length = terms
            .into_iter()
            .reduce(|a, b| {
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: lx::Operator::Addition,
                    operands: vec![a, b],
                })
            })
            .expect("a term");
        if !self.binding(p).nullable {
            return Ok(length);
        }
        let nullable: Vec<VarId> = elements
            .nodes
            .iter()
            .chain(&elements.rels)
            .copied()
            .filter(|v| self.binding(*v).nullable)
            .collect();
        if nullable.is_empty() {
            // Every element is bound before; whether the path matched is not
            // in any column.
            return unsupported("the length of an OPTIONAL path of bound elements");
        }
        self.unless_null(&nullable, length)
    }

    /// A list of nodes or relationships built so that it is never NULL: a
    /// path's nodes / relationships (of a path no OPTIONAL MATCH leaves
    /// NULL), a variable-length relationship's list, `collect()`, a list
    /// literal, their concatenation, or a WITH item of one. (A
    /// comprehension over a list that may be NULL, `split(NULL)`, is NULL in
    /// Cypher but an array here: S11b review.)
    fn never_null_element_list(&self, e: &LogicalExpr) -> bool {
        match e {
            LogicalExpr::TableAlias(lx::TableAlias(n)) => {
                let Some(v) = parse_var(n) else {
                    return false;
                };
                match &self.binding(v).kind {
                    BindingKind::Rel {
                        length: Some(_), ..
                    } => !self.binding(v).nullable,
                    _ => self
                        .projected
                        .get(&v)
                        .is_some_and(|x| self.never_null_element_list(x)),
                }
            }
            LogicalExpr::ScalarFnCall(f) => self.path_part(f).is_some(),
            LogicalExpr::AggregateFnCall(f) => f.name.eq_ignore_ascii_case("collect"),
            LogicalExpr::List(_) => true,
            LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)
                if op.operator == lx::Operator::Addition =>
            {
                op.operands.iter().all(|o| {
                    matches!(self.kind(o), Kind::Element(_)) || self.never_null_element_list(o)
                })
            }
            _ => false,
        }
    }

    pub(super) fn entity_arg(&self, args: &[LogicalExpr]) -> Option<VarId> {
        match args {
            [only] => self.entity(only),
            _ => None,
        }
    }

    pub(super) fn scalar_fn(&self, f: &LScalar, items: &Items) -> Result<RenderExpr, LowerError> {
        // A value used as a node: of a node, its identity; of anything else,
        // Neo4j's type error (a relationship is no node).
        if f.name == crate::bound_plan::expr::NODE_TIE {
            let [_, value] = f.args.as_slice() else {
                return unsupported("internal: a node tie of other than two operands");
            };
            if !matches!(self.kind(value), Kind::Element(Elem::Node(_))) {
                return unsupported("a value not known to be a node used as a node");
            }
            return self.operator(
                &LOp {
                    operator: lx::Operator::Equal,
                    operands: f.args.clone(),
                },
                items,
                false,
            );
        }
        if let Some((how, x, body, list)) = comprehension(f) {
            return self.comprehension(how, x, body, list, items, false);
        }
        // `size()` of a path's list: from the path's structure (`value.rs`).
        // `length()` of a path's nodes / relationships is their size: Neo4j
        // 4 read it so and the legacy pipeline answers it (Neo4j 5 refuses
        // it). Of a variable-length relationship's list both refuse it.
        if let [arg] = f.args.as_slice() {
            let size = f.name.eq_ignore_ascii_case("size");
            if size || f.name.eq_ignore_ascii_case("length") {
                if let Some(g) = self.graph_ref(arg) {
                    use super::value::GraphRef;
                    if size || matches!(g, GraphRef::Nodes(_) | GraphRef::Rels(_)) {
                        return self.graph_size(g);
                    }
                }
            }
        }
        if self.path_elem(f).is_some() {
            return self.path_tuples(f);
        }
        if let [arg] = f.args.as_slice() {
            if let (Kind::Element(elem), false) = (self.kind(arg), self.entity(arg).is_some()) {
                let t = self.expr(arg, items)?;
                return match (f.name.to_ascii_lowercase().as_str(), &elem) {
                    // `labels()` of a NULL element is NULL, and a ClickHouse
                    // array cannot be.
                    ("type", Elem::Rel { rel_type, .. }) => {
                        self.elem_unless_null(&t, &elem, Literal::String(rel_type.clone()))
                    }
                    ("label", Elem::Node(label)) => {
                        self.elem_unless_null(&t, &elem, Literal::String(label.clone()))
                    }
                    _ => unsupported(format!("{}() of a node or relationship of a list", f.name)),
                };
            }
        }
        if let Some(p) = self.path_var(&f.args) {
            return match f.name.to_ascii_lowercase().as_str() {
                "length" => self.path_length(p),
                _ => unsupported(format!(
                    "{}() of a path other than as a RETURN / WITH item or in size() (S7 lists)",
                    f.name
                )),
            };
        }
        if let Some(v) = self.entity_arg(&f.args) {
            let lower = f.name.to_ascii_lowercase();
            return match (lower.as_str(), self.scans.get(&v)) {
                // An element that matches nothing is NULL (a MATCH of it has
                // no rows; an OPTIONAL MATCH leaves it NULL): so is any
                // function of it, but its value type, which is "NULL".
                ("valuetype", Some(Scan::Impossible)) => {
                    Ok(RenderExpr::Literal(Literal::String("NULL".to_string())))
                }
                (_, Some(Scan::Impossible)) => Ok(RenderExpr::Literal(Literal::Null)),
                // `id(n)` of a node as a value is its key (S11l, user
                // 2026-10-11), as an `id(n)` RETURN item is over HTTP and in
                // the library (Bolt encodes a RETURN item's: the result
                // shape). Compared with another `id()`, the elements are
                // (`Self::operator`).
                ("id", Some(Scan::Node { .. })) => self.identity_value(v),
                // A relationship's id is Bolt's number for it (user
                // 2026-10-11), which only the server computes: an `id(r)`
                // RETURN item (`ResultKind::RelId`), or compared with another
                // `id()`.
                ("id", Some(Scan::Rels { .. })) => {
                    unsupported("id() of a relationship of several types or definitions")
                }
                ("id", Some(Scan::Rel { .. })) => unsupported(
                    "id() of a relationship other than as a RETURN item or compared with another \
                     id()",
                ),
                ("id", _) => unsupported("id() of a node of several labels as a value"),
                ("elementid", _) => unsupported("elementId()"),
                ("type", Some(Scan::Rel { rel_type, .. })) => {
                    self.unless_null(&[v], RenderExpr::Literal(Literal::String(rel_type.clone())))
                }
                // NULL where an OPTIONAL MATCH left it NULL.
                ("type", Some(Scan::Rels { .. })) => self.physical(v, super::REL_TYPE),
                // NULL for an unmatched node, and a ClickHouse array cannot
                // be NULL.
                ("labels", Some(Scan::Node { .. } | Scan::Labels { .. }))
                    if self.binding(v).nullable =>
                {
                    unsupported("labels() of a node an OPTIONAL MATCH may leave NULL")
                }
                // A node's label, then the union labels it carries.
                ("labels", Some(Scan::Labels { arms, .. })) => {
                    let column = self.physical(v, super::LABEL_COLUMN)?;
                    let when_then: Vec<(RenderExpr, RenderExpr)> = arms
                        .iter()
                        .filter(|(l, _)| !self.schema.union_labels_of(l).is_empty())
                        .map(|(l, _)| {
                            (
                                RenderExpr::Literal(Literal::String(l.clone())),
                                self.node_labels(l),
                            )
                        })
                        .collect();
                    let own = RenderExpr::List(vec![column.clone()]);
                    Ok(match when_then.is_empty() {
                        true => own,
                        false => RenderExpr::Case(RenderCase {
                            expr: Some(Box::new(column)),
                            when_then,
                            else_expr: Some(Box::new(own)),
                        }),
                    })
                }
                ("labels", Some(Scan::Node { label, .. })) => Ok(self.node_labels(label)),
                // `label(n)` (a ClickGraph function, as the legacy pipeline
                // reads it): a node's one label as a string, NULL where an
                // OPTIONAL MATCH left it NULL (P-4c S11d).
                ("label", Some(Scan::Labels { .. })) => self.physical(v, super::LABEL_COLUMN),
                ("label", Some(Scan::Node { label, .. })) => {
                    self.unless_null(&[v], RenderExpr::Literal(Literal::String(label.clone())))
                }
                _ => unsupported(format!("{}() of a node or relationship", f.name)),
            };
        }
        // (`length()` takes a path only: Neo4j refuses a list.)
        let size = f.name.eq_ignore_ascii_case("size");
        if !size && f.args.iter().any(|a| self.holds_elements(a)) {
            return unsupported(format!("{}() of nodes or relationships of a list", f.name));
        }
        // `size()` of a map: Neo4j refuses it (a string or a list only).
        if let (true, [arg]) = (f.name.eq_ignore_ascii_case("size"), f.args.as_slice()) {
            match self.kind(arg) {
                Kind::Map(_) => return unsupported("size() of a map"),
                // Of a list, which the dialect may count apart from a
                // string's characters (S12).
                Kind::List(_) => {
                    let list = render_expr_to_sql_plain(&self.held_list(arg, items)?);
                    let size = RenderExpr::Raw(current_function_mapper().list_size(&list));
                    // NULL of a NULL list (held as `[]`, `null_list.rs`).
                    return Ok(match self.list_null(arg, items)? {
                        null_list::ListNull::When(c) => RenderExpr::Case(RenderCase {
                            expr: None,
                            when_then: vec![(c, RenderExpr::Literal(Literal::Null))],
                            else_expr: Some(Box::new(size)),
                        }),
                        _ => size,
                    });
                }
                _ => {}
            }
        }
        // A map argument of a temporal or spatial constructor as the
        // emitter reads it (`duration({days: 5})`); any other is a map.
        const CONSTRUCTORS: [&str; 7] = [
            "duration",
            "date",
            "datetime",
            "localdatetime",
            "localtime",
            "time",
            "point",
        ];
        let constructor = CONSTRUCTORS.iter().any(|c| f.name.eq_ignore_ascii_case(c));
        // `keys()` of a map: its keys, in key order (as `main`'s lowering of
        // a map literal gave them).
        if let (true, [arg]) = (f.name.eq_ignore_ascii_case("keys"), f.args.as_slice()) {
            if matches!(self.kind(arg), Kind::Map(_)) {
                let Some(spelling) = current_function_mapper().lists() else {
                    return unsupported("keys() of a map in this SQL dialect");
                };
                let m = render_expr_to_sql_plain(&self.expr(arg, items)?);
                return Ok(RenderExpr::Raw((spelling.map_keys)(&m)));
            }
        }
        // `toString()` of a boolean is `'true'` / `'false'`: ClickHouse holds
        // it as `UInt8` (`'1'`), so it is cast first, as a returned one is
        // (#1470).
        if let (true, [arg]) = (f.name.eq_ignore_ascii_case("tostring"), f.args.as_slice()) {
            if self.kind(arg) == Kind::Boolean
                && !matches!(arg, LogicalExpr::Literal(lx::Literal::Boolean(_)))
            {
                let value = render_expr_to_sql_plain(&self.expr(arg, items)?);
                return Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: f.name.clone(),
                    args: vec![RenderExpr::Raw(current_function_mapper().cast_bool(&value))],
                }));
            }
        }
        // A map given to another function: its values are held as values of
        // any type.
        let map_arg = |a: &LogicalExpr| !matches!(a, LogicalExpr::MapLiteral(_)) || !constructor;
        if f.args.iter().any(|a| self.holds_maps(a) && map_arg(a)) {
            return unsupported(format!("{}() of a map", f.name));
        }
        // A list these read is held (a NULL one as `[]`, `null_list.rs`):
        // `head` / `last` of it is NULL; `isEmpty` reads its NULL
        // (`Lowerer::list_fn`).
        let reads_list = matches!(
            f.name.to_ascii_lowercase().as_str(),
            "head" | "last" | "isempty"
        );
        let args = f
            .args
            .iter()
            .map(|a| match a {
                LogicalExpr::MapLiteral(entries) if constructor => self.map_as_is(entries, items),
                a if reads_list && matches!(self.kind(a), Kind::List(_)) => {
                    self.held_list(a, items)
                }
                a => self.expr(a, items),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: f.name.clone(),
            args,
        }))
    }

    fn aggregate_fn(&self, f: &LAgg, items: &Items) -> Result<RenderExpr, LowerError> {
        let value = self.aggregate_value(f, items)?;
        // A `collect()` over rows in no order, lowered stable: its values
        // in an order of their own (#1395).
        if f.name.eq_ignore_ascii_case("collect") && self.collect_order.is_none() {
            return self.in_fixed_order(value);
        }
        Ok(value)
    }

    /// The list `l`, collected over rows in no order: in an order fixed by
    /// its values when lowering stable ([`Lowerer::stable`]) a CTE's, as
    /// listed otherwise (the final SELECT is read once).
    pub(super) fn in_fixed_order(&self, l: RenderExpr) -> Result<RenderExpr, LowerError> {
        if !self.stable || self.final_items {
            return Ok(l);
        }
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("collect() read more than once in this SQL dialect");
        };
        Ok(RenderExpr::Raw((spelling.fixed_order)(
            &render_expr_to_sql_plain(&l),
        )))
    }

    fn aggregate_value(&self, f: &LAgg, items: &Items) -> Result<RenderExpr, LowerError> {
        let name = f.name.to_ascii_lowercase();
        // The argument, and whether it is `DISTINCT x`.
        let (arg, distinct) = match f.args.as_slice() {
            [LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)]
                if op.operator == lx::Operator::Distinct && op.operands.len() == 1 =>
            {
                (Some(&op.operands[0]), true)
            }
            [one] => (Some(one), false),
            _ => (None, false),
        };
        // count(p) / count(DISTINCT p) of a path or a path's list
        // (`value.rs`).
        if let (Some(g), "count") = (arg.and_then(|a| self.graph_ref(a)), name.as_str()) {
            return self.count_graph_value(g, distinct);
        }
        // count(a) / count(DISTINCT a) count identities.
        if let (Some(v), "count") = (arg.and_then(|a| self.entity(a)), name.as_str()) {
            let id = match self.identity(v)? {
                // An element that matches nothing: no values to count.
                None => return Ok(aggregate_constant(RenderExpr::Literal(Literal::Integer(0)))),
                Some(mut cols) if cols.len() == 1 => cols.remove(0),
                // Not DISTINCT: counted where it is not NULL.
                Some(cols) if !distinct && self.presence(v, &cols).len() == 1 => {
                    self.presence(v, &cols).remove(0)
                }
                // Its columns as one value, NULL where the element is (an
                // OPTIONAL MATCH left it NULL: every column, `null_test`):
                // not counted.
                Some(cols) => {
                    let absent = super::null_test(&self.presence(v, &cols), true);
                    let tuple = RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: current_function_mapper().tuple_constructor().to_string(),
                        args: cols,
                    });
                    RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(absent, RenderExpr::Literal(Literal::Null))],
                        else_expr: Some(Box::new(tuple)),
                    })
                }
            };
            let arg = if distinct {
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: lx::Operator::Distinct,
                    operands: vec![id],
                })
            } else {
                id
            };
            return Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
                name: f.name.clone(),
                args: vec![arg],
            }));
        }
        // A list's node or relationship (`elements.rs`).
        // A map's values are of any type (`Dynamic`): DISTINCT would tell
        // `1` from `1.0`, and they have no order.
        if arg.is_some_and(|a| self.holds_maps(a))
            && (distinct || !matches!(name.as_str(), "collect" | "count"))
        {
            return unsupported(format!("{}() of maps", f.name));
        }
        // (`count(a)` of a node or relationship of the relation counts its
        // identity, below.)
        if let Some(arg) = arg.filter(|a| name == "collect" || self.entity(a).is_none()) {
            if let Kind::Element(elem) = self.kind(arg) {
                return self.element_aggregate(&name, arg, &elem, distinct, items);
            }
            // count() of a list of nodes or relationships (never NULL: a
            // path an OPTIONAL MATCH may leave NULL carries a value, not
            // tuples): its rows, or its distinct lists by their elements'
            // whole tuples in order, as DISTINCT compares them (P-4c S11b).
            // (Identities alone would merge parallel relationships of a
            // type without `edge_id`, which Neo4j tells apart: review.)
            if let (Kind::List(inner), "count") = (self.kind(arg), name.as_str()) {
                if matches!(*inner, Kind::Element(_)) && self.never_null_element_list(arg) {
                    let list = self.expr(arg, items)?;
                    let value = if distinct {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: lx::Operator::Distinct,
                            operands: vec![list],
                        })
                    } else {
                        list
                    };
                    return Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: f.name.clone(),
                        args: vec![value],
                    }));
                }
            }
            // collect() of a list of nodes or relationships that is never
            // NULL: a list of those lists (P-4c S11c), lowered as any
            // collect() below.
            let collected_list = name == "collect"
                && matches!(self.kind(arg), Kind::List(inner) if matches!(*inner, Kind::Element(_)))
                && self.never_null_element_list(arg);
            if self.holds_elements(arg) && !collected_list {
                return unsupported(format!("{}() of lists of nodes or relationships", f.name));
            }
        }
        // A list that may be NULL, held as `[]` (`null_list.rs`): its least /
        // greatest of the rows where it is not NULL.
        // (DISTINCT changes no least or greatest.)
        // Of lists that may be NULL (`null_list.rs`): as a value, NULL of a
        // group with no list that is not NULL (a ClickHouse `Variant`); what
        // knows its NULL reads it held (`Lowerer::min_max_list`).
        if let Some((none, held)) = self.min_max_list(f, items)? {
            return Ok(RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(none, RenderExpr::Literal(Literal::Null))],
                else_expr: Some(Box::new(held)),
            }));
        }
        // A list that may be NULL, held as `[]` (`null_list.rs`): counted
        // and collected where its source is not NULL.
        if let (Some(arg), "count" | "collect") = (arg, name.as_str()) {
            // (`collect()` of a CASE of lists or NULL: `collect_list_case`,
            // whose NULL results are of the lists' type.)
            let case = name == "collect" && list_or_null_case(self, arg).is_some();
            if matches!(self.kind(arg), Kind::List(_)) && !self.holds_elements(arg) && !case {
                if let null_list::ListNull::When(c) = self.list_null(arg, items)? {
                    return self.aggregate_present_list(&name, arg, &c, distinct, items);
                }
            }
        }
        let args = match (name.as_str(), arg) {
            // A list of booleans shows them as booleans.
            ("collect", Some(arg)) => {
                let value = self.element(arg, items)?;
                vec![if distinct {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: lx::Operator::Distinct,
                        operands: vec![value],
                    })
                } else {
                    value
                }]
            }
            _ => self.all(&f.args, items)?,
        };
        // An aggregate of the NULL literal (an unmapped property, an element
        // that matches nothing) aggregates no values. ClickHouse types it
        // `Nullable(Nothing)` and returns NULL where Cypher returns [] / 0.
        let null_arg = match args.as_slice() {
            [RenderExpr::Literal(Literal::Null)] => true,
            [RenderExpr::OperatorApplicationExp(op)]
                if op.operator == lx::Operator::Distinct
                    && matches!(op.operands.as_slice(), [RenderExpr::Literal(Literal::Null)]) =>
            {
                true
            }
            _ => false,
        };
        if null_arg {
            let value = match name.as_str() {
                "collect" => RenderExpr::List(Vec::new()),
                "count" | "sum" => RenderExpr::Literal(Literal::Integer(0)),
                "min" | "max" | "avg" => RenderExpr::Literal(Literal::Null),
                _ => return unsupported(format!("{}() of no values", f.name)),
            };
            return Ok(aggregate_constant(value));
        }
        if let (Some(c), "collect") = (arg.and_then(|a| list_or_null_case(self, a)), name.as_str())
        {
            return self.collect_list_case(c, distinct, items);
        }
        // Over rows in an order: the values in the order of the rows'
        // number (`Lowerer::collect_order`); DISTINCT keeps each value where
        // it first occurs.
        if let (Some(key), "collect") = (&self.collect_order, name.as_str()) {
            let Some(spelling) = current_function_mapper().lists() else {
                return unsupported("collect() over ordered rows in this SQL dialect");
            };
            let value = match args.as_slice() {
                [RenderExpr::OperatorApplicationExp(op)]
                    if op.operator == lx::Operator::Distinct && op.operands.len() == 1 =>
                {
                    &op.operands[0]
                }
                [one] => one,
                _ => return unsupported("internal: collect() of other than one value"),
            };
            let value = render_expr_to_sql_plain(value);
            let list = (spelling.ordered_collect)(
                &value,
                &render_expr_to_sql_plain(key),
                &format!("NOT {}", (spelling.is_null)(&value)),
            );
            return Ok(RenderExpr::Raw(if distinct {
                (spelling.distinct)(&list)
            } else {
                list
            }));
        }
        let call = RenderExpr::AggregateFnCall(AggregateFnCall {
            name: f.name.clone(),
            args,
        });
        // Cypher: `sum` of no values is 0, `collect` of none is []. ClickHouse
        // returns NULL when the argument is always NULL (`Nullable(Nothing)`:
        // `sum(a.unmapped + 1)`); with any value the result is unchanged.
        let empty = match name.as_str() {
            "sum" => Some(RenderExpr::Literal(Literal::Integer(0))),
            "collect" => Some(RenderExpr::List(Vec::new())),
            _ => None,
        };
        Ok(match empty {
            Some(e) => RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "coalesce".to_string(),
                args: vec![call, e],
            }),
            None => call,
        })
    }
}

/// A CASE whose results are lists or the NULL literal (a branch of each):
/// ClickHouse holds no NULL array, so a NULL result reads `[]` there
/// (`to_sql_query`), which `collect()` would keep.
fn list_or_null_case<'e>(l: &Lowerer<'_>, e: &'e LogicalExpr) -> Option<&'e LogicalCase> {
    let LogicalExpr::Case(c) = e else {
        return None;
    };
    let results: Vec<Option<&LogicalExpr>> = c
        .when_then
        .iter()
        .map(|(_, t)| Some(t))
        .chain([c.else_expr.as_deref()])
        .collect();
    let null = |r: &Option<&LogicalExpr>| {
        r.is_none_or(|e| matches!(e, LogicalExpr::Literal(lx::Literal::Null)))
    };
    let list = |r: &Option<&LogicalExpr>| r.is_some_and(|e| matches!(l.kind(e), Kind::List(_)));
    (results.iter().any(null)
        && results.iter().any(list)
        && results.iter().all(|r| null(r) || list(r)))
    .then_some(c)
}

impl Lowerer<'_> {
    /// `collect(CASE … THEN NULL … ELSE [..] END)` ([`list_or_null_case`]):
    /// of the rows whose result is a list, as `collect()` skips NULLs. The
    /// NULL results read the first list result (never collected), so every
    /// result is of one type (an empty array would be of none: ClickHouse
    /// converts it to no `Variant`).
    fn collect_list_case(
        &self,
        c: &LogicalCase,
        distinct: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("collect() of a CASE of lists in this SQL dialect");
        };
        let is_null = |e: Option<&LogicalExpr>| {
            e.is_none_or(|e| matches!(e, LogicalExpr::Literal(lx::Literal::Null)))
        };
        let Some(first) = c
            .when_then
            .iter()
            .map(|(_, t)| Some(t))
            .chain([c.else_expr.as_deref()])
            .find(|r| !is_null(*r))
            .flatten()
        else {
            return unsupported("internal: a CASE of lists without one");
        };
        let first = self.expr(first, items)?;
        let result = |e: Option<&LogicalExpr>| -> Result<RenderExpr, LowerError> {
            match e {
                Some(e) if !is_null(Some(e)) => self.expr(e, items),
                _ => Ok(first.clone()),
            }
        };
        let flag = |e: Option<&LogicalExpr>| RenderExpr::Literal(Literal::Boolean(!is_null(e)));
        let mut value = RenderCase {
            expr: None,
            when_then: Vec::new(),
            else_expr: Some(Box::new(result(c.else_expr.as_deref())?)),
        };
        let mut present = RenderCase {
            expr: None,
            when_then: Vec::new(),
            else_expr: Some(Box::new(flag(c.else_expr.as_deref()))),
        };
        for (condition, (_, t)) in self
            .case_conditions(c, items)?
            .into_iter()
            .zip(&c.when_then)
        {
            let t_sql = result(Some(t))?;
            if let Some(w) = condition {
                value.when_then.push((w.clone(), t_sql));
                present.when_then.push((w, flag(Some(t))));
            }
        }
        // Of no branch that may match: the ELSE's.
        let case = |c: RenderCase| match (c.when_then.is_empty(), c.else_expr) {
            (true, Some(e)) => render_expr_to_sql_plain(&e),
            (_, else_expr) => {
                render_expr_to_sql_plain(&RenderExpr::Case(RenderCase { else_expr, ..c }))
            }
        };
        let value = case(value);
        let present = case(present);
        // Over rows in an order (`Lowerer::collect_order`): in it.
        let list = match &self.collect_order {
            Some(key) => {
                (spelling.ordered_collect)(&value, &render_expr_to_sql_plain(key), &present)
            }
            None => (spelling.collect_if)(&value, &present),
        };
        let list = match distinct {
            true => (spelling.distinct)(&list),
            false => list,
        };
        Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: "coalesce".to_string(),
            args: vec![RenderExpr::Raw(list), RenderExpr::List(Vec::new())],
        }))
    }

    /// `labels()` of a node of `label`: it, then the union labels it carries
    /// (`GraphSchema::union_labels_of`).
    pub(super) fn node_labels(&self, label: &str) -> RenderExpr {
        RenderExpr::List(
            std::iter::once(label)
                .chain(self.schema.union_labels_of(label))
                .map(|l| RenderExpr::Literal(Literal::String(l.to_string())))
                .collect(),
        )
    }

    /// `collect(x)` / `count(x)` of a list's node or relationship `x`: the
    /// tuples (`elements.rs`) whose identity is not NULL; their number.
    fn element_aggregate(
        &self,
        name: &str,
        arg: &LogicalExpr,
        elem: &Elem,
        distinct: bool,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a list of nodes or relationships in this SQL dialect");
        };
        let t = self.expr(arg, items)?;
        // A node or relationship of the relation: its own identity columns.
        let id = match self.entity(arg) {
            Some(v) => match self.identity(v)? {
                Some(id) => id,
                None => return unsupported("internal: a listed element with no identity"),
            },
            None => self.elem_identity(&t, elem)?,
        };
        let present = match elem_presence(&id, elem).as_slice() {
            [one] => format!("NOT {}", (spelling.is_null)(&render_expr_to_sql_plain(one))),
            all => render_expr_to_sql_plain(&super::null_test(all, false)),
        };
        let t_sql = render_expr_to_sql_plain(&t);
        match name {
            "collect" => {
                let list = match &self.collect_order {
                    Some(key) => {
                        (spelling.ordered_collect)(&t_sql, &render_expr_to_sql_plain(key), &present)
                    }
                    None => (spelling.collect_if)(&t_sql, &present),
                };
                Ok(RenderExpr::Raw(if distinct {
                    (spelling.distinct)(&list)
                } else {
                    list
                }))
            }
            "count" => {
                let presence = elem_presence(&id, elem);
                let counted = match (id.as_slice(), presence.as_slice()) {
                    ([one], _) => one.clone(),
                    (_, [first]) if !distinct => first.clone(),
                    // Its identity's columns as one value, NULL where the
                    // element is (`null_test`): not counted (a composite id,
                    // a relationship identified by its ends, S8d2c).
                    _ => RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(
                            super::null_test(&presence, true),
                            RenderExpr::Literal(Literal::Null),
                        )],
                        else_expr: Some(Box::new(RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: current_function_mapper().tuple_constructor().to_string(),
                            args: id.clone(),
                        }))),
                    }),
                };
                let arg = if distinct {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: lx::Operator::Distinct,
                        operands: vec![counted],
                    })
                } else {
                    counted
                };
                Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: "count".to_string(),
                    args: vec![arg],
                }))
            }
            _ => unsupported(format!("{name}() of a node or relationship of a list")),
        }
    }
}

/// The identity slots of a list's element `id` that are all NULL exactly
/// when it is ([`Lowerer::presence`]): a composite node id's, every one;
/// otherwise the first.
fn elem_presence(id: &[RenderExpr], elem: &Elem) -> Vec<RenderExpr> {
    match elem {
        Elem::Node(_) if id.len() > 1 => id.to_vec(),
        _ => id.iter().take(1).cloned().collect(),
    }
}

/// A constant that is still an aggregate: `CASE WHEN count(*) >= 0 THEN c
/// ELSE c END`. The projection stays an aggregation (one row, or one per
/// group, also on an empty input) even when every aggregate in it folded.
pub(super) fn aggregate_constant(value: RenderExpr) -> RenderExpr {
    let count_all = RenderExpr::AggregateFnCall(AggregateFnCall {
        name: "count".to_string(),
        args: vec![RenderExpr::Star],
    });
    RenderExpr::Case(RenderCase {
        expr: None,
        when_then: vec![(
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: lx::Operator::GreaterThanEqual,
                operands: vec![count_all, RenderExpr::Literal(Literal::Integer(0))],
            }),
            value.clone(),
        )],
        else_expr: Some(Box::new(value)),
    })
}

/// Whether `k` is a node or relationship of one label or definition, or a
/// list (of lists) of them: their tuples are of one type.
fn one_shape(k: &Kind) -> bool {
    match k {
        Kind::Element(_) => true,
        Kind::List(k) => one_shape(k),
        _ => false,
    }
}

fn literal(l: &lx::Literal) -> Literal {
    match l {
        lx::Literal::Integer(i) => Literal::Integer(*i),
        lx::Literal::Float(f) => Literal::Float(*f),
        lx::Literal::Boolean(b) => Literal::Boolean(*b),
        lx::Literal::String(s) => Literal::String(s.clone()),
        lx::Literal::Null => Literal::Null,
    }
}

/// Whether `e` is an empty list literal, or a list or map literal holding
/// one (a list of elements of no type).
fn holds_empty_list(e: &LogicalExpr) -> bool {
    match e {
        LogicalExpr::List(items) => items.is_empty() || items.iter().any(holds_empty_list),
        LogicalExpr::MapLiteral(entries) => entries.iter().any(|(_, v)| holds_empty_list(v)),
        _ => false,
    }
}
