//! Cypher UNION (S7d, `docs/design/EXPLICIT_SCOPE.md` §4.4):
//! `q1 UNION [ALL] q2 …`.
//!
//! Each arm is a complete query with its own scopes, lowered on its own
//! ([`Query::lower`]) to a CTE `with_w{n}` of its RETURN's columns, named
//! `__cg_w{n}_c{i}` in the UNION's column order (the binder matches the
//! arms' columns by name). Its ORDER BY, SKIP and LIMIT apply to it alone,
//! as in Neo4j, where an ORDER BY / LIMIT after the last arm is that arm's.
//! A CTE is the `UNION ALL` of a SELECT of each arm's CTE, and the final
//! SELECT reads it:
//! * A column of values may hold values of different types in different
//!   arms, as in Cypher, so each arm's value is read as a value of any type
//!   (`CypherUnion::any_type`): ClickHouse's common type of `Bool` and
//!   `UInt8` is `Bool`, which turns `5` into `true`. A node or relationship
//!   (its columns), a graph value and an `id()` must be of one label / type
//!   in every arm (otherwise the UNION is not lowered), and keep their
//!   columns' types.
//! * UNION ALL: when an arm's rows are ordered (an ORDER BY), the result is
//!   ordered by arm, then by row: each arm's SELECT numbers its rows in the
//!   order of the sort keys its CTE exports (after the arm's own DISTINCT and
//!   LIMIT). Neo4j returns each arm's rows in turn, in their order. The keys
//!   stay in their arm, so arms need not have keys of one type.
//! * UNION: one row per group of rows equal in every column by
//!   `CypherUnion::distinct_key` (Cypher's DISTINCT: `1` equals `1.0`, not
//!   `true` or `'1'`), and of the same nodes and relationships: each arm's
//!   CTE also exports the identity of each one it returns, alone or in a
//!   graph value, as columns (rows equal in every returned column can be
//!   different relationships, whose `edge_id` is not a property, and a
//!   relationship's graph value has its ends' `elementId`). The identities
//!   must have the same columns in every arm (the same labels' or types'
//!   ids), or the UNION is not lowered. The result has no order.

use std::collections::HashMap;

use crate::render_plan::render_expr::{Literal, RenderExpr};
use crate::render_plan::{
    Cte, CteContent, CteItems, FromTableItem, OrderByItem, OrderByOrder, RenderPlan, SelectItem,
    SelectItems, Union, UnionItems, UnionType,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::{
    order_keys_to_sql_plain, render_expr_to_sql_plain,
};
use crate::sql_generator::function_mapper::current_function_mapper;

use super::{
    col_at, empty_plan, select, table_ref, unsupported, LowerError, Lowered, LoweredQuery, Query,
    ResultColumn, ResultKind,
};
use crate::bound_plan::types::{BoundOp, ProjItem, VarId};

/// The UNION's columns of its rows' order: the arm, and the row's number in
/// its arm.
const ARM: &str = "__cg_arm";
const ROW: &str = "__cg_row";

/// One arm's CTE: its alias, the sort keys it exports (none unless the rows
/// are ordered and the UNION is ALL), and the number of identity columns it
/// exports (none unless the UNION is DISTINCT).
struct Arm {
    alias: String,
    keys: Vec<OrderByItem>,
    identity_columns: usize,
}

/// Lower the UNION of `arms`, arm `i` returning the bindings
/// `arm_columns[i]` in the UNION's column order; `all` is UNION ALL.
pub(super) fn lower(
    q: &Query<'_>,
    arms: &[BoundOp],
    arm_columns: &[Vec<VarId>],
    all: bool,
) -> Result<Lowered, LowerError> {
    let Some(spelling) = current_function_mapper().cypher_union() else {
        return unsupported("UNION in this SQL dialect");
    };
    let mut ctes = Vec::new();
    let mut lowered: Vec<Arm> = Vec::new();
    let mut first: Option<Vec<ResultColumn>> = None;
    for (arm, columns) in arms.iter().zip(arm_columns) {
        let BoundOp::Project { projection, .. } = arm else {
            return unsupported("internal: a UNION arm that does not end in RETURN");
        };
        let LoweredQuery {
            mut plan,
            shape,
            identities,
        } = q.lower(arm, &mut ctes)?;
        let shape = in_union_order(shape, &projection.items, columns)?;
        if let Some(first) = &first {
            same_kinds(first, &shape)?;
        }
        let alias = format!("w{}", ctes.len() + 1);
        let mut items: HashMap<String, SelectItem> = HashMap::new();
        for i in std::mem::take(&mut plan.select.items) {
            let Some(a) = i.col_alias.clone() else {
                return unsupported("internal: a RETURN column without a name");
            };
            items.insert(a.0, i);
        }
        for (n, (_, column)) in shape.iter().flat_map(|c| &c.columns).enumerate() {
            let Some(item) = items.remove(column) else {
                return unsupported(format!("internal: no SELECT column {column}"));
            };
            plan.select
                .items
                .push(select(item.expression, &format!("__cg_{alias}_c{n}")));
        }
        if !items.is_empty() {
            return unsupported("internal: a RETURN column outside the result shape");
        }
        // Each returned element or graph value's identity, in the UNION's
        // column order.
        let mut identity_columns = 0;
        if !all {
            for c in &shape {
                let Some((_, identity)) = identities.iter().find(|(n, _)| *n == c.name) else {
                    if matches!(c.kind, ResultKind::Node { .. } | ResultKind::Rel { .. }) {
                        return unsupported(format!("internal: no identity of {}", c.name));
                    }
                    continue;
                };
                for e in identity {
                    plan.select.items.push(select(
                        e.clone(),
                        &format!("__cg_{alias}_k{identity_columns}"),
                    ));
                    identity_columns += 1;
                }
            }
        }
        // The CTE exports the sort keys (`close_segment`); it needs an ORDER
        // BY only for its SKIP / LIMIT.
        let mut keys = Vec::new();
        if all {
            for (k, key) in plan.order_by.0.iter().enumerate() {
                let name = format!("__cg_{alias}_o{k}");
                plan.select
                    .items
                    .push(select(key.expression.clone(), &name));
                keys.push(OrderByItem {
                    expression: col_at(&alias, &name),
                    order: key.order.clone(),
                });
            }
        }
        if plan.skip.0.is_none() && plan.limit.0.is_none() {
            plan.order_by.0.clear();
        }
        ctes.push(Cte::new(
            format!("with_{alias}"),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        if lowered
            .first()
            .is_some_and(|a| a.identity_columns != identity_columns)
        {
            return unsupported(
                "UNION of nodes or relationships identified by different columns in different arms",
            );
        }
        lowered.push(Arm {
            alias,
            keys,
            identity_columns,
        });
        first.get_or_insert(shape);
    }
    let shape = first.ok_or_else(|| LowerError::Unsupported("internal: no UNION arm".into()))?;
    let kinds: Vec<&ResultKind> = shape
        .iter()
        .flat_map(|c| c.columns.iter().map(|_| &c.kind))
        .collect();
    let ordered = lowered.iter().any(|a| !a.keys.is_empty());
    let mut selects = Vec::new();
    for (i, arm) in lowered.iter().enumerate() {
        let a = &arm.alias;
        let mut items: Vec<SelectItem> = kinds
            .iter()
            .enumerate()
            .map(|(n, kind)| {
                let column = col_at(a, &format!("__cg_{a}_c{n}"));
                let e = if **kind == ResultKind::Value {
                    RenderExpr::Raw((spelling.any_type)(&render_expr_to_sql_plain(&column)))
                } else {
                    column
                };
                select(e, &format!("__cg_c{n}"))
            })
            .collect();
        items.extend(
            (0..arm.identity_columns)
                .map(|m| select(col_at(a, &format!("__cg_{a}_k{m}")), &format!("__cg_k{m}"))),
        );
        if ordered {
            items.push(select(RenderExpr::Literal(Literal::Integer(i as i64)), ARM));
            let row = if arm.keys.is_empty() {
                RenderExpr::Literal(Literal::Integer(0))
            } else {
                RenderExpr::Raw((spelling.row_number)(&order_keys_to_sql_plain(&arm.keys)))
            };
            items.push(select(row, ROW));
        }
        selects.push(RenderPlan {
            select: SelectItems {
                items,
                distinct: false,
            },
            from: FromTableItem(Some(table_ref(format!("with_{a}"), a))),
            ..empty_plan()
        });
    }
    let u = format!("w{}", ctes.len() + 1);
    ctes.push(Cte::new(
        format!("with_{u}"),
        CteContent::Structured(Box::new(RenderPlan {
            union: UnionItems(Some(Union {
                input: selects,
                union_type: UnionType::All,
                is_cypher_union: true,
            })),
            ..empty_plan()
        })),
        false,
    ));
    // The final SELECT: the UNION's columns, named as the first arm's.
    // UNION: grouped by each value's key, an `id()` as it is (of one type in
    // every arm), and each element's identity.
    let names = shape.iter().flat_map(|c| c.columns.iter().map(|(_, n)| n));
    let mut plan = RenderPlan {
        from: FromTableItem(Some(table_ref(format!("with_{u}"), &u))),
        ..empty_plan()
    };
    for ((n, name), kind) in names.enumerate().zip(&kinds) {
        let column = col_at(&u, &format!("__cg_c{n}"));
        if all {
            plan.select.items.push(select(column, name));
            continue;
        }
        let sql = render_expr_to_sql_plain(&column);
        plan.select
            .items
            .push(select(RenderExpr::Raw((spelling.any)(&sql)), name));
        match kind {
            ResultKind::Value | ResultKind::Graph(_) => plan
                .group_by
                .0
                .push(RenderExpr::Raw((spelling.distinct_key)(&sql))),
            ResultKind::NodeId { .. } => plan.group_by.0.push(column),
            // A node or relationship is grouped by its identity alone
            // (exported below); its columns follow from it, and may be of a
            // type ClickHouse cannot group by (`Dynamic`).
            ResultKind::Node { .. } | ResultKind::Rel { .. } => {}
        }
    }
    // Identity columns are of one type in every arm.
    let identity_columns = lowered.first().map_or(0, |a| a.identity_columns);
    plan.group_by
        .0
        .extend((0..identity_columns).map(|m| col_at(&u, &format!("__cg_k{m}"))));
    if ordered {
        plan.order_by.0 = [ARM, ROW]
            .iter()
            .map(|c| OrderByItem {
                expression: col_at(&u, c),
                order: OrderByOrder::Asc,
            })
            .collect();
    }
    plan.ctes = CteItems(ctes);
    Ok(Lowered { plan, shape })
}

/// An arm's result shape (one column per RETURN item, in `items`' order) in
/// the UNION's column order: the arm's bindings `columns`.
fn in_union_order(
    shape: Vec<ResultColumn>,
    items: &[ProjItem],
    columns: &[VarId],
) -> Result<Vec<ResultColumn>, LowerError> {
    if shape.len() != items.len() || columns.len() != items.len() {
        return unsupported("internal: a RETURN item without one result column");
    }
    let mut by_var: HashMap<VarId, ResultColumn> = items.iter().map(|i| i.var).zip(shape).collect();
    columns
        .iter()
        .map(|v| {
            by_var
                .remove(v)
                .ok_or_else(|| LowerError::Unsupported(format!("internal: {v} not returned")))
        })
        .collect()
}

/// The arms' columns at each position must be read alike: values, or nodes
/// of one label, relationships of one type between one label pair, graph
/// values of one kind, `id()`s of one label.
fn same_kinds(first: &[ResultColumn], other: &[ResultColumn]) -> Result<(), LowerError> {
    for (a, b) in first.iter().zip(other) {
        let keys = |c: &ResultColumn| c.columns.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        if a.kind != b.kind || keys(a) != keys(b) {
            return unsupported(format!(
                "UNION column `{}` of a different kind or label in another arm",
                a.name
            ));
        }
    }
    Ok(())
}
