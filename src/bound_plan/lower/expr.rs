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
//! The conversion is structural and reads no task-local state. Shapes that
//! need more than that are `Unsupported`.

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

use super::{parse_var, unsupported, At, LowerError, Lowerer, Scan};
use crate::bound_plan::types::{BindingKind, BindingSource, VarId};

type Items = HashMap<VarId, RenderExpr>;

impl Lowerer<'_> {
    /// `v.prop` for a node or relationship binding.
    pub(super) fn property(&self, v: VarId, prop: &str) -> Result<RenderExpr, LowerError> {
        let (mapping, closed, at) = match self.scans.get(&v) {
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
            LogicalExpr::List(xs) => RenderExpr::List(self.all(xs, items)?),
            LogicalExpr::MapLiteral(entries) => RenderExpr::MapLiteral(
                entries
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), self.expr(v, items)?)))
                    .collect::<Result<_, LowerError>>()?,
            ),
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
            // The legacy converter prints a lambda body to text while
            // converting; comprehensions are lowered with S7 (lists).
            LogicalExpr::Lambda(_) => return unsupported("a list comprehension or lambda (S7)"),
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
                (O::IsNull | O::IsNotNull, 1, [a]) => {
                    Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator,
                        operands: vec![self.identity_value(*a)?],
                    }))
                }
                _ => unsupported("a node or relationship as an operand (needs its value)"),
            };
        }
        Ok(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: op.operator,
            operands: self.all(&op.operands, items)?,
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
        let same_kind = match (self.scans.get(&a), self.scans.get(&b)) {
            (Some(Scan::Node { label: x, .. }), Some(Scan::Node { label: y, .. })) => x == y,
            // One type can have several edge definitions (one per endpoint
            // label pair, each its own table): only rows of the same
            // definition can be the same relationship.
            (Some(Scan::Rel { schema: x, .. }), Some(Scan::Rel { schema: y, .. })) => {
                std::ptr::eq(*x, *y)
            }
            // An element that matches nothing: the relation has no rows.
            (Some(Scan::Impossible), _) | (_, Some(Scan::Impossible)) => {
                return Ok(RenderExpr::Literal(Literal::Null))
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
                Some(Scan::Rel { .. }) => fixed += 1,
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
                // NULL for an unmatched node, and a ClickHouse array cannot
                // be NULL.
                ("labels", Some(Scan::Node { .. })) if self.binding(v).nullable => {
                    unsupported("labels() of a node an OPTIONAL MATCH may leave NULL")
                }
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
                Some(_) => return unsupported("count(DISTINCT) of a composite identity"),
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
        let args = self.all(&f.args, items)?;
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
