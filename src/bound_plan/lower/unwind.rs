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

use std::collections::HashMap;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::{self as lx, LogicalExpr};
use crate::render_plan::render_expr::{Literal, RenderExpr, TableAlias};
use crate::render_plan::{ArrayJoin, OrderByItem, OrderByOrder};
use crate::sql_generator::emitters::clickhouse::to_sql_query::{
    order_keys_to_sql_plain, render_expr_to_sql_plain,
};
use crate::sql_generator::function_mapper::current_function_mapper;

use super::{
    col_at, parse_var, select, table_ref, unsupported, Body, Exports, LowerError, Lowerer, RowOrder,
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

/// What a value is, as far as UNWIND needs to know.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Kind {
    /// Always NULL.
    Null,
    /// Not a list (possibly NULL).
    Scalar,
    /// A list, of elements of this kind.
    List(Box<Kind>),
    /// Not known here.
    Unknown,
}

impl Kind {
    /// A value that is one or the other.
    fn either(self, other: Kind) -> Kind {
        match (self, other) {
            (a, b) if a == b => a,
            (Kind::Null, Kind::Scalar) | (Kind::Scalar, Kind::Null) => Kind::Scalar,
            (Kind::List(a), Kind::List(b)) => Kind::List(Box::new(a.either(*b))),
            // A list that may be NULL: a ClickHouse array is never NULL.
            _ => Kind::Unknown,
        }
    }

    /// The kind of the records `UNWIND` of a value of this kind gives.
    fn element(&self) -> Kind {
        match self {
            Kind::List(e) => (**e).clone(),
            Kind::Scalar => Kind::Scalar,
            Kind::Null => Kind::Null,
            Kind::Unknown => Kind::Unknown,
        }
    }
}

impl<'s> Lowerer<'s> {
    /// What `e` is ([`Kind`]).
    pub(super) fn kind(&self, e: &LogicalExpr) -> Kind {
        use lx::Operator as O;
        match e {
            LogicalExpr::Literal(lx::Literal::Null) => Kind::Null,
            LogicalExpr::Literal(_) | LogicalExpr::MapLiteral(_) => Kind::Scalar,
            LogicalExpr::List(xs) => Kind::List(Box::new(
                xs.iter()
                    .map(|x| self.kind(x))
                    .reduce(Kind::either)
                    .unwrap_or(Kind::Null),
            )),
            LogicalExpr::TableAlias(lx::TableAlias(n)) => parse_var(n)
                .and_then(|v| self.kinds.get(&v).cloned())
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
                    // `+` of lists concatenates, and appends a value to a list.
                    (O::Addition, [a, b]) => match (self.kind(a), self.kind(b)) {
                        (Kind::Scalar | Kind::Null, Kind::Scalar | Kind::Null) => Kind::Scalar,
                        _ => Kind::Unknown,
                    },
                    (O::Addition | O::Distinct, _) => Kind::Unknown,
                    _ => Kind::Scalar,
                }
            }
            LogicalExpr::ScalarFnCall(f) => {
                let arg = |i: usize| f.args.get(i).map_or(Kind::Unknown, |a| self.kind(a));
                match f.name.to_ascii_lowercase().as_str() {
                    "range" | "split" | "keys" => Kind::List(Box::new(Kind::Scalar)),
                    "size" | "length" | "tostring" | "tointeger" | "tofloat" | "toboolean"
                    | "abs" | "ceil" | "floor" | "round" | "sign" | "sqrt" | "exp" | "log"
                    | "log10" | "rand" | "tolower" | "toupper" | "trim" | "ltrim" | "rtrim"
                    | "substring" | "replace" | "left" | "right" | "type" => Kind::Scalar,
                    "tail" => match arg(0) {
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
                    "collect" => Kind::List(Box::new(match arg {
                        Kind::Null => Kind::Null,
                        Kind::Scalar => Kind::Scalar,
                        Kind::List(e) => Kind::List(e),
                        Kind::Unknown => Kind::Unknown,
                    })),
                    "count" | "sum" | "avg" => Kind::Scalar,
                    "min" | "max" => match arg {
                        Kind::Scalar | Kind::Null => Kind::Scalar,
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
        let declared = match &self.binding(v).kind {
            BindingKind::Node { labels } => labels.iter().all(|l| {
                self.schema
                    .node_schema_opt(l)
                    .is_some_and(|s| s.property_types.contains_key(p))
            }),
            BindingKind::Rel {
                types,
                length: None,
            } => types.iter().all(|t| {
                let defs = self.schema.rel_schemas_for_type(t);
                !defs.is_empty() && defs.iter().all(|s| s.property_types.contains_key(p))
            }),
            _ => false,
        };
        if declared {
            Kind::Scalar
        } else {
            Kind::Unknown
        }
    }

    /// `UNWIND expr AS var`.
    pub(super) fn unwind(&mut self, expr: &LogicalExpr, var: VarId) -> Result<(), LowerError> {
        let Some(spelling) = current_function_mapper().unwind() else {
            return unsupported("UNWIND in this SQL dialect");
        };
        let kind = self.kind(expr);
        self.kinds.insert(var, kind.element());
        // A list of no elements, or NULL: no rows.
        let empty = match (expr, &kind) {
            (LogicalExpr::List(xs), _) => xs.is_empty(),
            (_, Kind::Null) => true,
            _ => false,
        };
        if empty {
            // An expression can still be refused (a node in a list).
            self.expr(expr, &HashMap::new())?;
            self.empty = true;
            self.values.insert(var, RenderExpr::Literal(Literal::Null));
            return Ok(());
        }
        if matches!(self.order, RowOrder::Keys(_)) && !self.one_row {
            self.number_rows()?;
        }
        let e = self.expr(expr, &HashMap::new())?;
        let list = match kind {
            Kind::List(_) => e,
            // The list of the value, or of none when it is NULL.
            Kind::Scalar => RenderExpr::Case(crate::render_plan::render_expr::RenderCase {
                expr: None,
                when_then: vec![(
                    RenderExpr::OperatorApplicationExp(
                        crate::render_plan::render_expr::OperatorApplication {
                            operator: lx::Operator::IsNull,
                            operands: vec![e.clone()],
                        },
                    ),
                    RenderExpr::List(Vec::new()),
                )],
                else_expr: Some(Box::new(RenderExpr::List(vec![e]))),
            }),
            Kind::Unknown | Kind::Null => {
                RenderExpr::Raw((spelling.list_only)(&render_expr_to_sql_plain(&e)))
            }
        };
        let alias = self.next_cte_alias();
        let mut body = Body::default();
        let mut exports = Exports::default();
        self.export_scope(&alias, &mut body, &mut exports)?;
        body.select.push(select(
            RenderExpr::TableAlias(TableAlias(ELEMENT.to_string())),
            &var.name(),
        ));
        exports.values.push((var, col_at(&alias, &var.name())));
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
                keys.push(OrderByItem {
                    expression: RenderExpr::TableAlias(TableAlias(POSITION.to_string())),
                    order: OrderByOrder::Asc,
                });
                body.order_by = keys;
                let positions =
                    RenderExpr::Raw((spelling.positions)(&render_expr_to_sql_plain(&list)));
                body.array_join.push(ArrayJoin {
                    expression: list,
                    alias: ELEMENT.to_string(),
                });
                body.array_join.push(ArrayJoin {
                    expression: positions,
                    alias: POSITION.to_string(),
                });
            }
            None => {
                body.order_lost = true;
                body.array_join.push(ArrayJoin {
                    expression: list,
                    alias: ELEMENT.to_string(),
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

    /// The rows so far, in their order, become a CTE exporting the scope and
    /// each row's number in that order; the rows are then in the order of
    /// that number.
    fn number_rows(&mut self) -> Result<(), LowerError> {
        let Some(spelling) = current_function_mapper().unwind() else {
            return unsupported("UNWIND in this SQL dialect");
        };
        let RowOrder::Keys(keys) = &self.order else {
            return unsupported("internal: numbering rows in no order");
        };
        let keys = order_keys_to_sql_plain(keys);
        let alias = self.next_cte_alias();
        let mut body = Body::default();
        let mut exports = Exports::default();
        self.export_scope(&alias, &mut body, &mut exports)?;
        body.select.push(select(
            RenderExpr::Raw((spelling.row_number)(&keys)),
            ROW_NUMBER,
        ));
        self.close_segment(body, exports, alias.clone())?;
        self.order = RowOrder::Keys(vec![OrderByItem {
            expression: col_at(&alias, ROW_NUMBER),
            order: OrderByOrder::Asc,
        }]);
        Ok(())
    }
}
