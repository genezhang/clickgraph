//! Bound expression → [`RenderExpr`]. Every variable is a binding's generated
//! name (`v{N}`, see `bound_plan::expr`), so a reference resolves through the
//! binding table and the element's scan, never by a user-visible name:
//! * a property of a node or relationship is `v{N}.<mapped column>`; a
//!   property the schema does not map is NULL (the graph has no such
//!   property), and a property of an element that matches nothing is NULL;
//! * a bare node or relationship is its identity (`count(a)`, `a = b`,
//!   `a IS NULL`);
//! * a projected item (ORDER BY after RETURN) is the item's expression.
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

use super::{col, parse_var, unsupported, LowerError, Lowerer, Scan};
use crate::bound_plan::types::{BindingKind, BindingSource, VarId};

type Items = HashMap<VarId, RenderExpr>;

impl Lowerer<'_> {
    /// `v.prop` for a node or relationship binding.
    pub(super) fn property(&self, v: VarId, prop: &str) -> Result<RenderExpr, LowerError> {
        let mapping = match self.scans.get(&v) {
            Some(Scan::Node { schema, .. }) => schema.property_mappings.get(prop),
            Some(Scan::Rel { schema, .. }) => schema.property_mappings.get(prop),
            Some(Scan::Impossible) => None,
            None => return unsupported("a property of a variable with no scan"),
        };
        Ok(match mapping {
            Some(pv) => RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(v.name()),
                column: pv.clone(),
            }),
            None => RenderExpr::Literal(Literal::Null),
        })
    }

    /// A node's or relationship's identity as one expression.
    fn identity(&self, v: VarId) -> Result<RenderExpr, LowerError> {
        match self.identity_columns(v) {
            None => Ok(RenderExpr::Literal(Literal::Null)),
            Some(cols) if cols.len() == 1 => Ok(col(v, &cols[0])),
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
                    Some(Scan::Impossible) => false,
                    None => return unsupported("a label test on a variable with no scan"),
                };
                RenderExpr::Literal(Literal::Boolean(holds))
            }
            LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
                RenderExpr::OperatorApplicationExp(self.operator(op, items)?)
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

    fn operator(&self, op: &LOp, items: &Items) -> Result<OperatorApplication, LowerError> {
        Ok(OperatorApplication {
            operator: op.operator,
            operands: self.all(&op.operands, items)?,
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
            (BindingKind::Node { .. } | BindingKind::Rel { .. }, _) => self.identity(v),
            (BindingKind::Value, BindingSource::Local) => {
                Ok(RenderExpr::TableAlias(TableAlias(name.to_string())))
            }
            (BindingKind::Path, _) => unsupported("a path variable (S6)"),
            (BindingKind::Value, _) => unsupported("a WITH / UNWIND value (S4b, S7)"),
        }
    }

    fn entity_arg(&self, args: &[LogicalExpr]) -> Option<VarId> {
        match args {
            [LogicalExpr::TableAlias(lx::TableAlias(n))] => parse_var(n).filter(|v| {
                matches!(
                    self.binding(*v).kind,
                    BindingKind::Node { .. } | BindingKind::Rel { .. }
                )
            }),
            _ => None,
        }
    }

    fn scalar_fn(&self, f: &LScalar, items: &Items) -> Result<RenderExpr, LowerError> {
        if let Some(v) = self.entity_arg(&f.args) {
            let lower = f.name.to_ascii_lowercase();
            return match (lower.as_str(), self.scans.get(&v)) {
                // `id()` is the server's encoded id (the HTTP / Bolt `id()`
                // rewrite, `IdMapper`), not a column; lowered with the result
                // shape.
                ("id" | "elementid", _) => unsupported("id() / elementId()"),
                ("type", Some(Scan::Rel { rel_type, .. })) => {
                    Ok(RenderExpr::Literal(Literal::String(rel_type.clone())))
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
        if f.args.iter().any(|a| self.is_entity(a)) {
            return unsupported(format!("{}() with a node or relationship argument", f.name));
        }
        Ok(RenderExpr::ScalarFnCall(ScalarFnCall {
            name: f.name.clone(),
            args: self.all(&f.args, items)?,
        }))
    }

    fn aggregate_fn(&self, f: &LAgg, items: &Items) -> Result<RenderExpr, LowerError> {
        let counts = f.name.eq_ignore_ascii_case("count");
        // count(a) / count(DISTINCT a) count identities; any other aggregate
        // of a whole entity (collect(a)) needs the entity's value.
        let entity_in = |e: &LogicalExpr| match e {
            LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)
                if op.operator == lx::Operator::Distinct =>
            {
                op.operands.iter().any(|o| self.is_entity(o))
            }
            other => self.is_entity(other),
        };
        if !counts && f.args.iter().any(entity_in) {
            return unsupported(format!("{}() of a node or relationship", f.name));
        }
        Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
            name: f.name.clone(),
            args: self.all(&f.args, items)?,
        }))
    }

    fn is_entity(&self, e: &LogicalExpr) -> bool {
        self.entity_arg(std::slice::from_ref(e)).is_some()
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
