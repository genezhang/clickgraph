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
use crate::sql_generator::function_mapper::current_function_mapper;

use super::unwind::{comprehension, Comprehension, Kind};
use super::{parse_var, unsupported, At, LowerError, Lowerer, Scan};
use crate::bound_plan::types::{BindingKind, BindingSource, VarId};

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
            Some(Scan::Node { schema, at, .. }) => (
                schema.property_mappings.get(prop),
                schema.closed_properties,
                at,
            ),
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
                Some(id) => nulls.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: lx::Operator::IsNull,
                    operands: vec![id[0].clone()],
                })),
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
                match &self.binding(v).kind {
                    BindingKind::Node { .. } | BindingKind::Rel { .. } => self.property(v, prop)?,
                    _ => {
                        return unsupported("a property of a value (map, list element, WITH item)")
                    }
                }
            }
            LogicalExpr::LabelExpression { variable, label } => {
                let Some(v) = parse_var(variable) else {
                    return unsupported("an unbound label test");
                };
                let holds = match self.scans.get(&v) {
                    Some(Scan::Node { label: l, .. }) => l == label,
                    // NULL where an OPTIONAL MATCH left it NULL, as Cypher.
                    Some(Scan::Labels { arms, .. }) if arms.iter().any(|(l, _)| l == label) => {
                        return Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: lx::Operator::Equal,
                            operands: vec![
                                self.physical(v, super::LABEL_COLUMN)?,
                                RenderExpr::Literal(Literal::String(label.clone())),
                            ],
                        }));
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
                self.operator(op, items)?
            }
            LogicalExpr::List(xs) => RenderExpr::List(
                xs.iter()
                    .map(|x| self.element(x, items))
                    .collect::<Result<_, _>>()?,
            ),
            LogicalExpr::MapLiteral(entries) => {
                // In key order: maps are equal by their entries (DISTINCT,
                // grouping, UNION), and ClickHouse compares them in order.
                let mut entries = entries
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), self.expr(v, items)?)))
                    .collect::<Result<Vec<_>, LowerError>>()?;
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                RenderExpr::MapLiteral(entries)
            }
            LogicalExpr::ScalarFnCall(f) => self.scalar_fn(f, items)?,
            LogicalExpr::AggregateFnCall(f) => self.aggregate_fn(f, items)?,
            LogicalExpr::Case(c) => RenderExpr::Case(self.case(c, items)?),
            LogicalExpr::ReduceExpr(r) => RenderExpr::ReduceExpr(ReduceExpr {
                accumulator: r.accumulator.clone(),
                initial_value: Box::new(self.expr(&r.initial_value, items)?),
                variable: r.variable.clone(),
                list: Box::new(self.expr(&r.list, items)?),
                expression: Box::new(self.expr(&r.expression, items)?),
            }),
            LogicalExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
                array: Box::new(self.expr(array, items)?),
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
            // ClickHouse function's argument written in the query.
            LogicalExpr::Lambda(_) => return unsupported("a lambda other than a comprehension's"),
            LogicalExpr::Raw(_)
            | LogicalExpr::ColumnAlias(_)
            | LogicalExpr::Column(_)
            | LogicalExpr::CteEntityRef(_) => return unsupported("a planner-internal expression"),
            LogicalExpr::PathPattern(_)
            | LogicalExpr::InSubquery(_)
            | LogicalExpr::ExistsSubquery(_)
            | LogicalExpr::PatternCount(_)
            | LogicalExpr::PatternComprehension(_) => {
                return unsupported("a graph pattern inside an expression (S9)")
            }
        })
    }

    /// A list's element: a boolean one is cast to a boolean, as ClickHouse
    /// holds a comparison as `UInt8` (a list would show 1 / 0; a value in a
    /// column is cast by the emitter).
    fn element(&self, e: &LogicalExpr, items: &Items) -> Result<RenderExpr, LowerError> {
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
    fn comprehension(
        &self,
        how: Comprehension,
        x: VarId,
        body: &LogicalExpr,
        list: &LogicalExpr,
        items: &Items,
    ) -> Result<RenderExpr, LowerError> {
        let Some(spelling) = current_function_mapper().lists() else {
            return unsupported("a list comprehension in this SQL dialect");
        };
        // Also gives the parameter its kind (an element of the list's).
        let kind = self.kind(list);
        self.local_kinds.borrow_mut().insert(x, kind.element());
        let list = self.expr(list, items)?;
        if kind == Kind::Null || matches!(list, RenderExpr::Literal(Literal::Null)) {
            return Ok(RenderExpr::Literal(Literal::Null));
        }
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

    fn all(&self, xs: &[LogicalExpr], items: &Items) -> Result<Vec<RenderExpr>, LowerError> {
        xs.iter().map(|x| self.expr(x, items)).collect()
    }

    fn operator(&self, op: &LOp, items: &Items) -> Result<RenderExpr, LowerError> {
        use lx::Operator as O;
        let entities: Vec<VarId> = op.operands.iter().filter_map(|o| self.entity(o)).collect();
        if !entities.is_empty() {
            return match (op.operator, op.operands.len(), entities.as_slice()) {
                (O::Equal | O::NotEqual, 2, [a, b]) => {
                    self.identity_comparison(*a, *b, op.operator == O::Equal)
                }
                // NULL exactly where its first identity column is.
                (O::IsNull | O::IsNotNull, 1, [a]) => {
                    let first = match self.identity(*a)? {
                        Some(mut cols) => cols.remove(0),
                        None => RenderExpr::Literal(Literal::Null),
                    };
                    Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator,
                        operands: vec![first],
                    }))
                }
                _ => unsupported("a node or relationship as an operand (needs its value)"),
            };
        }
        if let (O::Addition, [a, b]) = (op.operator, op.operands.as_slice()) {
            if let Some(e) = self.list_addition(a, b, items)? {
                return Ok(e);
            }
        }
        Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: op.operator,
            operands: self.all(&op.operands, items)?,
        }))
    }

    /// `a + b` where one side is a list: the lists concatenated, a value
    /// appended or prepended as a list of itself; NULL with NULL. `None`
    /// when neither side is known to be a list (the emitter decides).
    /// ClickHouse's `plus` of arrays adds them element by element.
    fn list_addition(
        &self,
        a: &LogicalExpr,
        b: &LogicalExpr,
        items: &Items,
    ) -> Result<Option<RenderExpr>, LowerError> {
        let (ka, kb) = (self.kind(a), self.kind(b));
        if !matches!(ka, Kind::List(_)) && !matches!(kb, Kind::List(_)) {
            return Ok(None);
        }
        if ka == Kind::Null || kb == Kind::Null {
            return Ok(Some(RenderExpr::Literal(Literal::Null)));
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
                _ => self.expr(e, items)?,
            });
        }
        Ok(Some(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: current_function_mapper().array_concat().to_string(),
            args,
        })))
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
        let per_column: Vec<RenderExpr> = ca
            .into_iter()
            .zip(cb)
            .map(|(x, y)| {
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: if equal {
                        lx::Operator::Equal
                    } else {
                        lx::Operator::NotEqual
                    },
                    operands: vec![x, y],
                })
            })
            .collect();
        Ok(if equal {
            super::and_all(per_column).expect("an identity has columns")
        } else {
            super::or_all(per_column)
        })
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
        let (ia, ib) = (self.definition_identity(a)?, self.definition_identity(b)?);
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
        let op = if equal {
            lx::Operator::Equal
        } else {
            lx::Operator::NotEqual
        };
        let cmp = |x: RenderExpr, y: RenderExpr| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op,
                operands: vec![x, y],
            })
        };
        let mut per_column = vec![cmp(label(a, la)?, label(b, lb)?)];
        per_column.extend(ia.into_iter().zip(ib).map(|(x, y)| cmp(x, y)));
        Ok(if equal {
            super::and_all(per_column).expect("an identity has columns")
        } else {
            super::or_all(per_column)
        })
    }

    fn case(&self, c: &LogicalCase, items: &Items) -> Result<RenderCase, LowerError> {
        Ok(RenderCase {
            expr: c
                .expr
                .as_ref()
                .map(|e| self.expr(e, items).map(Box::new))
                .transpose()?,
            when_then: c
                .when_then
                .iter()
                .map(|(w, t)| Ok((self.expr(w, items)?, self.expr(t, items)?)))
                .collect::<Result<_, LowerError>>()?,
            else_expr: c
                .else_expr
                .as_ref()
                .map(|e| self.expr(e, items).map(Box::new))
                .transpose()?,
        })
    }

    /// A bare variable: a projected item, a node / relationship identity, or
    /// a comprehension / reduce variable (printed by its generated name).
    fn variable(&self, name: &str, items: &Items) -> Result<RenderExpr, LowerError> {
        let Some(v) = parse_var(name) else {
            return unsupported("an unbound variable");
        };
        if let Some(item) = items.get(&v) {
            return Ok(item.clone());
        }
        let b = self.binding(v);
        match (&b.kind, &b.source) {
            (
                BindingKind::Rel {
                    length: Some(_), ..
                },
                _,
            ) => unsupported(
                "a variable-length relationship's list other than as a RETURN / WITH item or in \
                 size() (S7 lists)",
            ),
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
                None => unsupported("an UNWIND value (S7)"),
            },
        }
    }

    /// The node or relationship a bare variable expression names (not a
    /// variable-length relationship: that is a list).
    fn entity(&self, e: &LogicalExpr) -> Option<VarId> {
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

    fn entity_arg(&self, args: &[LogicalExpr]) -> Option<VarId> {
        match args {
            [only] => self.entity(only),
            _ => None,
        }
    }

    fn scalar_fn(&self, f: &LScalar, items: &Items) -> Result<RenderExpr, LowerError> {
        if let Some((how, x, body, list)) = comprehension(f) {
            return self.comprehension(how, x, body, list, items);
        }
        // `size()` of a path's list: from the path's structure (`value.rs`).
        if let (true, [arg]) = (f.name.eq_ignore_ascii_case("size"), f.args.as_slice()) {
            if let Some(g) = self.graph_ref(arg) {
                return self.graph_size(g);
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
                // `id()` is the server's encoded id (`IdMapper`): Bolt encodes
                // the key column of an `id(n)` RETURN item (the result shape),
                // and the HTTP / Bolt `id()` rewrite turns comparisons with
                // ids into key predicates before planning. Anywhere else the
                // key is not the id.
                ("id" | "elementid", _) => {
                    unsupported("id() / elementId() other than a node's `id(n)` RETURN item")
                }
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
                ("labels", Some(Scan::Labels { .. })) => Ok(RenderExpr::List(vec![
                    self.physical(v, super::LABEL_COLUMN)?
                ])),
                ("labels", Some(Scan::Node { label, .. })) => {
                    Ok(RenderExpr::List(vec![RenderExpr::Literal(
                        Literal::String(label.clone()),
                    )]))
                }
                (_, Some(Scan::Impossible)) => Ok(RenderExpr::Literal(Literal::Null)),
                _ => unsupported(format!("{}() of a node or relationship", f.name)),
            };
        }
        Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: f.name.clone(),
            args: self.all(&f.args, items)?,
        }))
    }

    fn aggregate_fn(&self, f: &LAgg, items: &Items) -> Result<RenderExpr, LowerError> {
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
        // count(a) / count(DISTINCT a) count identities.
        if let (Some(v), "count") = (arg.and_then(|a| self.entity(a)), name.as_str()) {
            let id = match self.identity(v)? {
                // An element that matches nothing: no values to count.
                None => return Ok(aggregate_constant(RenderExpr::Literal(Literal::Integer(0)))),
                Some(mut cols) if cols.len() == 1 || !distinct => cols.remove(0),
                // Its columns as one value, NULL where the first is (an
                // OPTIONAL MATCH left the element NULL): not counted.
                Some(cols) => {
                    let first = cols[0].clone();
                    let tuple = RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: current_function_mapper().tuple_constructor().to_string(),
                        args: cols,
                    });
                    RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: lx::Operator::IsNull,
                                operands: vec![first],
                            }),
                            RenderExpr::Literal(Literal::Null),
                        )],
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
            let list = (spelling.ordered_collect)(
                &render_expr_to_sql_plain(value),
                &render_expr_to_sql_plain(key),
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

/// A constant that is still an aggregate: `CASE WHEN count(*) >= 0 THEN c
/// ELSE c END`. The projection stays an aggregation (one row, or one per
/// group, also on an empty input) even when every aggregate in it folded.
fn aggregate_constant(value: RenderExpr) -> RenderExpr {
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

fn literal(l: &lx::Literal) -> Literal {
    match l {
        lx::Literal::Integer(i) => Literal::Integer(*i),
        lx::Literal::Float(f) => Literal::Float(*f),
        lx::Literal::Boolean(b) => Literal::Boolean(*b),
        lx::Literal::String(s) => Literal::String(s.clone()),
        lx::Literal::Null => Literal::Null,
    }
}
