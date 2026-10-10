//! Paths, and lists of nodes or relationships, as values (P-4c S6c,
//! `docs/design/EXPLICIT_SCOPE.md` §4.11).
//!
//! A value is in Neo4j's JSON form (its Query API's): a node is
//! `{elementId, labels, properties}`, a relationship `{elementId,
//! startNodeElementId, endNodeElementId, type, properties}`, a path the list
//! of its nodes and relationships in turn. Element ids are ClickGraph's
//! (`graph_catalog::element_id`, as Bolt and the graph output make them);
//! the properties are the declared ones that are not NULL (Neo4j stores no
//! NULL property). The dialect spells it ([`GraphValues`]): on ClickHouse an
//! element is a `Map(String, Dynamic)`, so one list holds nodes and
//! relationships of any label.
//!
//! A fixed element's value is built from its columns; a variable-length
//! relationship's nodes and relationships are carried through its search
//! (`path_node_values`, `path_rel_values`, generated when the demand pass
//! asks for them), in the order of the walk, reversed when the walk starts
//! at the pattern's right end.
//!
//! A value is never grouped by itself (ClickHouse refuses `Dynamic` in GROUP
//! BY): DISTINCT and grouping use the identities of its elements
//! ([`GraphValue::keys`]) and pick the value with `any()`.

use crate::graph_catalog::element_id::{
    node_element_id_affixes, node_element_id_separators, relationship_element_id_affixes,
    relationship_element_id_separators,
};
use crate::graph_catalog::graph_schema::{NodeSchema, RelationshipSchema};
use crate::query_planner::logical_expr::{self as lx, LogicalExpr};
use crate::render_plan::render_expr::{
    Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::{current_function_mapper, GraphValues};

use super::{
    col_at, or_all, parse_var, select, unsupported, Body, Exports, LowerError, Lowerer,
    ResultColumn, ResultKind, Scan, Walked,
};
use crate::bound_plan::types::{Binding, BindingKind, ProjItem, VarId};

/// The type of a graph value: how Bolt and the graph output read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphType {
    Node,
    Relationship,
    Path,
    List(Box<GraphType>),
}

/// Demand-pass names (`Lowerer::demand`) of a variable-length relationship
/// whose nodes / relationships are read as values. A property name cannot
/// start with `#`.
pub(super) const NODE_VALUES: &str = "#nodes";
pub(super) const REL_VALUES: &str = "#rels";
/// Demand-pass names of a variable-length relationship whose nodes /
/// relationships are read as a list's elements (`elements.rs`).
pub(super) const NODE_TUPLES: &str = "#node_tuples";
pub(super) const REL_TUPLES: &str = "#rel_tuples";
/// The demand-pass name of a path or list whose identity is read (DISTINCT
/// or grouping by it): a shortest path's is in its recovered paths.
pub(super) const PATH_KEY: &str = "#key";

/// What an expression names as a graph value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GraphRef {
    /// A path variable.
    Path(VarId),
    /// `nodes(p)`.
    Nodes(VarId),
    /// `relationships(p)`.
    Rels(VarId),
    /// A variable-length relationship's list (`-[r*]->` `r`).
    List(VarId),
    /// A value of one of these carried by a WITH (`WITH nodes(p) AS ns`).
    Carried(VarId),
}

/// The graph value an expression names, by the bindings' kinds (a carried
/// one is the segment's: [`Lowerer::graph_ref`]).
pub(super) fn graph_ref(e: &LogicalExpr, bindings: &[Binding]) -> Option<GraphRef> {
    let var = |e: &LogicalExpr| match e {
        LogicalExpr::TableAlias(lx::TableAlias(n)) => parse_var(n),
        _ => None,
    };
    let kind = |v: VarId| &bindings[v.0 as usize].kind;
    match e {
        LogicalExpr::TableAlias(_) => {
            let v = var(e)?;
            match kind(v) {
                BindingKind::Path => Some(GraphRef::Path(v)),
                BindingKind::Rel {
                    length: Some(_), ..
                } => Some(GraphRef::List(v)),
                _ => None,
            }
        }
        LogicalExpr::ScalarFnCall(f) => {
            let [arg] = f.args.as_slice() else {
                return None;
            };
            let p = var(arg).filter(|p| matches!(kind(*p), BindingKind::Path))?;
            match f.name.to_ascii_lowercase().as_str() {
                "nodes" => Some(GraphRef::Nodes(p)),
                "relationships" => Some(GraphRef::Rels(p)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// A graph value of the current relation.
#[derive(Debug, Clone)]
pub(super) struct GraphValue {
    pub value: RenderExpr,
    /// The identities of its elements, in order: equal exactly when the
    /// values are equal.
    pub keys: Vec<RenderExpr>,
    pub ty: GraphType,
    /// It can be NULL (an OPTIONAL MATCH's path that did not match).
    pub nullable: bool,
}

/// A graph value a WITH carried: its value is the segment's `values` entry.
#[derive(Debug, Clone)]
pub(super) struct Carried {
    pub keys: Vec<RenderExpr>,
    pub ty: GraphType,
    pub nullable: bool,
}

/// The dialect's spelling, or `Unsupported`.
pub(super) fn spelling() -> Result<GraphValues, LowerError> {
    match current_function_mapper().graph_values() {
        Some(g) => Ok(g),
        None => {
            unsupported("a path or a list of nodes or relationships as a value on this dialect")
        }
    }
}

fn sql(e: &RenderExpr) -> String {
    render_expr_to_sql_plain(e)
}

pub(super) fn string(s: &str) -> String {
    sql(&RenderExpr::Literal(Literal::String(s.to_string())))
}

/// An identity of one or more columns (SQL) as one value: the column, or
/// their tuple, as a path's `path_edges` spells a relationship's.
fn spelled_identity(cols: &[String]) -> String {
    match cols {
        [one] => one.clone(),
        _ => format!(
            "{}({})",
            current_function_mapper().tuple_constructor(),
            cols.join(", ")
        ),
    }
}

/// The text a relationship's definition puts before its identity in
/// [`rel_key`].
pub(super) fn rel_key_prefix(rel_type: &str, from_label: &str, to_label: &str) -> String {
    string(&format!("{rel_type}:{from_label}:{to_label}:"))
}

/// A relationship's identity as a text, as a path's identity spells it
/// (`Lowerer::path_key`) and a walk over several definitions keeps it
/// (`path_edges`, S7b3a): its definition (type, labels of its stored ends)
/// and its identity columns (SQL `ids`: the `edge_id`, else the stored ends,
/// #887). Every scan of a relationship spells it alike, so a path that
/// splits between its parts in several ways has one identity.
pub(super) fn rel_key(
    g: &GraphValues,
    rel_type: &str,
    from_label: &str,
    to_label: &str,
    ids: &[String],
) -> String {
    (g.text)(&[
        rel_key_prefix(rel_type, from_label, to_label),
        (g.to_text)(&spelled_identity(ids)),
    ])
}

/// A node's identity as a text, with its label (SQL `label`) and id (SQL
/// `id`): as a path's identity spells it (`Lowerer::node_key`).
pub(super) fn node_key_text(g: &GraphValues, label: &str, id: &str) -> String {
    (g.text)(&[label.to_string(), string(":"), (g.to_text)(id)])
}

/// A node: its label, the SQL of its (single-column) id, its properties
/// (name, SQL).
pub(super) fn node_object(
    g: &GraphValues,
    label: &str,
    id: &str,
    props: &[(String, String)],
) -> String {
    let (prefix, suffix) = node_element_id_affixes(label);
    let labels = current_function_mapper().array_literal(&string(label));
    (g.object)(&[
        (
            string("elementId"),
            (g.text)(&[string(&prefix), (g.to_text)(id), string(suffix)]),
        ),
        (string("labels"), labels),
        (string("properties"), properties(g, props)),
    ])
}

/// A node whose label is the SQL `label` (a column: one of several
/// possible labels), else as [`node_object`]. Its properties are those of
/// every label, NULL where its own has none, so the value has its own.
fn labeled_node_object(
    g: &GraphValues,
    label: &str,
    id: &str,
    props: &[(String, String)],
) -> String {
    let (after_label, suffix) = node_element_id_separators();
    let labels = current_function_mapper().array_literal(label);
    (g.object)(&[
        (
            string("elementId"),
            (g.text)(&[
                label.to_string(),
                string(after_label),
                (g.to_text)(id),
                string(suffix),
            ]),
        ),
        (string("labels"), labels),
        (string("properties"), properties(g, props)),
    ])
}

/// A relationship: its type, the labels of its stored `from` and `to`
/// nodes, the SQL of their ids, its properties.
#[allow(clippy::too_many_arguments)]
pub(super) fn rel_object(
    g: &GraphValues,
    rel_type: &str,
    from_label: &str,
    to_label: &str,
    from: &str,
    to: &str,
    props: &[(String, String)],
) -> String {
    let (prefix, between, suffix) = relationship_element_id_affixes(rel_type);
    let node_id = |label: &str, id: &str| {
        let (p, s) = node_element_id_affixes(label);
        (g.text)(&[string(&p), (g.to_text)(id), string(s)])
    };
    (g.object)(&[
        (
            string("elementId"),
            (g.text)(&[
                string(&prefix),
                (g.to_text)(from),
                string(between),
                (g.to_text)(to),
                string(suffix),
            ]),
        ),
        (string("startNodeElementId"), node_id(from_label, from)),
        (string("endNodeElementId"), node_id(to_label, to)),
        (string("type"), string(rel_type)),
        (string("properties"), properties(g, props)),
    ])
}

/// A relationship whose type and stored end labels are the SQL `rel_type`,
/// `from_label`, `to_label` (columns: one of several possible definitions),
/// else as [`rel_object`]. Its properties are those of every definition,
/// NULL where its own has none, so the value has its own.
fn labeled_rel_object(
    g: &GraphValues,
    rel_type: &str,
    (from_label, to_label): (&str, &str),
    (from, to): (&str, &str),
    props: &[(String, String)],
) -> String {
    let (after_type, between, suffix) = relationship_element_id_separators();
    let (after_label, node_suffix) = node_element_id_separators();
    let node_id = |label: &str, id: &str| {
        (g.text)(&[
            label.to_string(),
            string(after_label),
            (g.to_text)(id),
            string(node_suffix),
        ])
    };
    (g.object)(&[
        (
            string("elementId"),
            (g.text)(&[
                rel_type.to_string(),
                string(after_type),
                (g.to_text)(from),
                string(between),
                (g.to_text)(to),
                string(suffix),
            ]),
        ),
        (string("startNodeElementId"), node_id(from_label, from)),
        (string("endNodeElementId"), node_id(to_label, to)),
        (string("type"), rel_type.to_string()),
        (string("properties"), properties(g, props)),
    ])
}

fn properties(g: &GraphValues, props: &[(String, String)]) -> String {
    let entries: Vec<(String, String)> =
        props.iter().map(|(k, v)| (string(k), v.clone())).collect();
    (g.object_without_nulls)(&entries)
}

/// The declared properties of a table read under `alias` (name, SQL), by
/// name.
fn table_props(
    mappings: &std::collections::HashMap<
        String,
        crate::graph_catalog::expression_parser::PropertyValue,
    >,
    alias: &str,
) -> Vec<(String, String)> {
    let mut props: Vec<(String, String)> = mappings
        .iter()
        .map(|(name, pv)| {
            let e = RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: pv.clone(),
            });
            (name.clone(), sql(&e))
        })
        .collect();
    props.sort();
    props
}

/// A node of `schema`'s table read under `alias` (inside a path's search).
pub(super) fn table_node_object(
    g: &GraphValues,
    schema: &NodeSchema,
    label: &str,
    alias: &str,
) -> Result<String, LowerError> {
    let [id] = schema
        .id_physical_columns()
        .try_into()
        .map_err(|_| LowerError::Unsupported("a composite node id in a value (S8)".to_string()))?;
    let id = sql(&super::col_at(alias, &id));
    Ok(node_object(
        g,
        label,
        &id,
        &table_props(&schema.property_mappings, alias),
    ))
}

/// A relationship of `schema`'s table read under `alias`.
pub(super) fn table_rel_object(
    g: &GraphValues,
    schema: &RelationshipSchema,
    rel_type: &str,
    alias: &str,
) -> Result<String, LowerError> {
    let (Ok([from]), Ok([to])) = (
        <[&str; 1]>::try_from(schema.from_id.columns()),
        <[&str; 1]>::try_from(schema.to_id.columns()),
    ) else {
        return unsupported("a composite relationship endpoint in a value (S8)");
    };
    Ok(rel_object(
        g,
        rel_type,
        &schema.from_node,
        &schema.to_node,
        &sql(&super::col_at(alias, from)),
        &sql(&super::col_at(alias, to)),
        &table_props(&schema.property_mappings, alias),
    ))
}

impl<'s> Lowerer<'s> {
    /// A WITH / RETURN item that is a graph value (`cte`: the WITH's CTE
    /// alias). Returns whether it is handled here: a WITH carries a
    /// `-[r*]->` list as an element (`export_element`).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn graph_item(
        &mut self,
        g: GraphRef,
        it: &ProjItem,
        cte: Option<&str>,
        aggregating: bool,
        body: &mut Body,
        exports: &mut Exports<'s>,
        shape: &mut Vec<ResultColumn>,
        exported: &mut Vec<VarId>,
    ) -> Result<bool, LowerError> {
        match (g, cte) {
            // A WITH carries a path's elements; the next segment builds its
            // value (and length) from them.
            (GraphRef::Path(src), Some(alias)) => {
                let Some(elements) = self.paths.get(&src).cloned() else {
                    return unsupported(format!("internal: path {src} has no elements"));
                };
                // Grouped (DISTINCT, aggregation) by the path's identity, not
                // its elements': two splits of one path are one path.
                let grouping = aggregating || body.distinct;
                let first_column = body.select.len();
                for e in elements.nodes.iter().chain(&elements.rels) {
                    if exported.contains(e) {
                        continue;
                    }
                    exported.push(*e);
                    let scan = self.export_element(*e, *e, alias, body, &mut Vec::new())?;
                    exports.scans.push((*e, scan));
                }
                if grouping {
                    let added: Vec<String> = body.select[first_column..]
                        .iter()
                        .filter_map(|i| i.col_alias.as_ref().map(|a| a.0.clone()))
                        .collect();
                    body.determined.extend(added);
                    let s = spelling()?;
                    let key = self.path_key(&elements, Part::Whole, &s)?;
                    let all: Vec<VarId> = elements
                        .nodes
                        .iter()
                        .chain(&elements.rels)
                        .copied()
                        .collect();
                    let (keys, _) = self.null_safe(&s, vec![RenderExpr::Raw(key)], &all)?;
                    for (i, k) in keys.into_iter().enumerate() {
                        body.select
                            .push(select(k.clone(), &format!("{}__k{i}", it.var)));
                        if aggregating {
                            body.group_by.push(k);
                        }
                    }
                }
                self.paths.insert(it.var, elements);
                Ok(true)
            }
            (GraphRef::List(_), Some(_)) => Ok(false),
            // A fixed path's nodes or relationships of one label or type: a
            // list of tuples (`elements.rs`), whose elements a later clause
            // reads.
            (GraphRef::Nodes(_) | GraphRef::Rels(_), Some(_)) if matches!(&it.expr, LogicalExpr::ScalarFnCall(f) if self.path_elem(f).is_some()) => {
                Ok(false)
            }
            (g, None) => {
                let v = self.graph_value(g)?;
                let column = body.column(v.value, &it.name);
                body.determined.push(column.clone());
                self.identities.push((it.name.clone(), v.keys.clone()));
                if aggregating {
                    body.group_by.extend(v.keys);
                } else if body.distinct {
                    body.distinct_keys.extend(v.keys);
                }
                shape.push(ResultColumn {
                    name: it.name.clone(),
                    kind: ResultKind::Graph(v.ty),
                    columns: vec![(it.name.clone(), column)],
                });
                Ok(true)
            }
            (g, Some(alias)) => {
                let v = self.graph_value(g)?;
                let carried = export_graph(it.var, v, alias, body, aggregating);
                exports.values.push((it.var, col_at(alias, &it.var.name())));
                exports.graph.push((it.var, carried));
                Ok(true)
            }
        }
    }

    /// [`graph_ref`], and a graph value the segment's CTE carries.
    pub(super) fn graph_ref(&self, e: &LogicalExpr) -> Option<GraphRef> {
        if let LogicalExpr::TableAlias(lx::TableAlias(n)) = e {
            if let Some(v) = parse_var(n).filter(|v| self.graph_values.contains_key(v)) {
                return Some(GraphRef::Carried(v));
            }
        }
        graph_ref(e, self.bindings)
    }

    /// `size()` of a list a [`GraphRef`] names.
    pub(super) fn graph_size(&self, g: GraphRef) -> Result<RenderExpr, LowerError> {
        let plus_one = |e: RenderExpr| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Addition,
                operands: vec![e, RenderExpr::Literal(Literal::Integer(1))],
            })
        };
        match g {
            GraphRef::Nodes(p) => Ok(plus_one(self.path_length(p)?)),
            GraphRef::Rels(p) => self.path_length(p),
            GraphRef::List(r) => match self.scans.get(&r) {
                Some(Scan::Path { .. }) => self.unless_null(&[r], self.physical(r, "hop_count")?),
                Some(Scan::Impossible) => Ok(RenderExpr::Literal(Literal::Null)),
                _ => unsupported(format!("internal: {r} is not a path relation")),
            },
            GraphRef::Carried(v) => {
                let carried = &self.graph_values[&v];
                if carried.nullable || !matches!(carried.ty, GraphType::List(_)) {
                    return unsupported("size() of a carried path or an OPTIONAL path's list");
                }
                let value = sql(&self.values[&v]);
                Ok(RenderExpr::Raw((spelling()?.length)(&value)))
            }
            GraphRef::Path(_) => unsupported("size() of a path"),
        }
    }

    /// The value a [`GraphRef`] names.
    pub(super) fn graph_value(&self, g: GraphRef) -> Result<GraphValue, LowerError> {
        match g {
            GraphRef::Path(p) => self.path_value(p, Part::Whole),
            GraphRef::Nodes(p) => self.path_value(p, Part::Nodes),
            GraphRef::Rels(p) => self.path_value(p, Part::Rels),
            GraphRef::List(r) => {
                let s = spelling()?;
                let (value, keys) = match self.scans.get(&r) {
                    Some(Scan::Path { .. }) => (self.vlp_rels(r, &s)?, self.vlp_keys(r)?),
                    Some(Scan::Impossible) => {
                        return Ok(GraphValue {
                            value: RenderExpr::Literal(Literal::Null),
                            keys: Vec::new(),
                            ty: GraphType::List(Box::new(GraphType::Relationship)),
                            nullable: true,
                        })
                    }
                    _ => return unsupported(format!("internal: {r} is not a path relation")),
                };
                self.finish(
                    &s,
                    value,
                    keys,
                    GraphType::List(Box::new(GraphType::Relationship)),
                    &[r],
                )
            }
            GraphRef::Carried(v) => {
                let c = &self.graph_values[&v];
                Ok(GraphValue {
                    value: self.values[&v].clone(),
                    keys: c.keys.clone(),
                    ty: c.ty.clone(),
                    nullable: c.nullable,
                })
            }
        }
    }

    /// Wrap a non-NULL value as NULL where the OPTIONAL MATCH of `vars` (any
    /// nullable one) did not match.
    fn finish(
        &self,
        s: &GraphValues,
        value: String,
        keys: Vec<RenderExpr>,
        ty: GraphType,
        vars: &[VarId],
    ) -> Result<GraphValue, LowerError> {
        let (keys, cond) = self.null_safe(s, keys, vars)?;
        Ok(match cond {
            None => GraphValue {
                value: RenderExpr::Raw(value),
                keys,
                ty,
                nullable: false,
            },
            Some(cond) => GraphValue {
                value: RenderExpr::Raw((s.null_if)(&cond, &value)),
                keys,
                ty,
                nullable: true,
            },
        })
    }

    /// The keys of a value that is NULL where the OPTIONAL MATCH of `vars`
    /// (any nullable one) did not match, and that condition. Every such row
    /// has the one NULL value: its keys are the condition and no identity.
    fn null_safe(
        &self,
        s: &GraphValues,
        keys: Vec<RenderExpr>,
        vars: &[VarId],
    ) -> Result<(Vec<RenderExpr>, Option<String>), LowerError> {
        let mut nulls = Vec::new();
        for v in vars.iter().filter(|v| self.binding(**v).nullable) {
            let Some(id) = self.identity(*v)? else {
                continue;
            };
            nulls.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::IsNull,
                operands: vec![id[0].clone()],
            }));
        }
        if nulls.is_empty() {
            return Ok((keys, None));
        }
        let cond = or_all(nulls);
        let cond_sql = sql(&cond);
        let mut null_keys = vec![cond];
        null_keys.extend(
            keys.iter()
                .map(|k| RenderExpr::Raw((s.empty_if)(&cond_sql, &sql(k)))),
        );
        Ok((null_keys, Some(cond_sql)))
    }

    /// A path, its nodes or its relationships, in path order.
    fn path_value(&self, p: VarId, part: Part) -> Result<GraphValue, LowerError> {
        let s = spelling()?;
        let Some(elements) = self.paths.get(&p).cloned() else {
            return unsupported(format!("internal: path {p} has no elements"));
        };
        let ty = match part {
            Part::Whole => GraphType::Path,
            Part::Nodes => GraphType::List(Box::new(GraphType::Node)),
            Part::Rels => GraphType::List(Box::new(GraphType::Relationship)),
        };
        let all: Vec<VarId> = elements
            .nodes
            .iter()
            .chain(&elements.rels)
            .copied()
            .collect();
        if all
            .iter()
            .any(|v| matches!(self.scans.get(v), Some(Scan::Impossible)))
        {
            // The relation has no rows.
            return Ok(GraphValue {
                value: RenderExpr::Literal(Literal::Null),
                keys: Vec::new(),
                ty,
                nullable: true,
            });
        }
        if self.binding(p).nullable && !all.iter().any(|v| self.binding(*v).nullable) {
            // Every element is bound before; whether the path matched is not
            // in any column.
            return unsupported("an OPTIONAL path of bound elements as a value");
        }
        // Lists of the value, one after another; `items` collects single
        // elements until a variable-length relationship's list.
        let mut lists: Vec<String> = Vec::new();
        let mut items: Vec<String> = Vec::new();
        let first = elements.nodes[0];
        if part != Part::Rels {
            items.push(self.node_value(first, &s)?);
        }
        for (i, r) in elements.rels.iter().enumerate() {
            let next = elements.nodes[i + 1];
            match self.scans.get(r) {
                Some(Scan::Rel { .. } | Scan::Rels { .. }) => {
                    if part != Part::Nodes {
                        items.push(self.rel_value(*r, &s)?);
                    }
                    if part != Part::Rels {
                        items.push(self.node_value(next, &s)?);
                    }
                }
                Some(Scan::Path { .. }) => {
                    if !items.is_empty() {
                        lists.push((s.list)(&std::mem::take(&mut items)));
                    }
                    let list = match part {
                        Part::Whole => (s.interleave)(
                            &self.vlp_rels(*r, &s)?,
                            &(s.tail)(&self.vlp_nodes(*r, &s)?),
                        ),
                        Part::Nodes => (s.tail)(&self.vlp_nodes(*r, &s)?),
                        Part::Rels => self.vlp_rels(*r, &s)?,
                    };
                    lists.push(list);
                }
                _ => return unsupported(format!("internal: {r} is not a relationship scan")),
            }
        }
        if !items.is_empty() || lists.is_empty() {
            lists.push((s.list)(&items));
        }
        let value = (s.concat)(&lists);
        let key = self.path_key(&elements, part, &s)?;
        self.finish(&s, value, vec![RenderExpr::Raw(key)], ty, &all)
    }

    /// The identity of a path, or of its nodes or relationships, as one list
    /// of texts in path order: the first node and every relationship (which
    /// determine the other nodes), every node, or every relationship, each
    /// with its label or type. One list,
    /// because the same path can split between two variable-length parts in
    /// several ways (`*0..1` then `*0..1`); its elements' sequence cannot.
    fn path_key(
        &self,
        elements: &super::PathElements,
        part: Part,
        s: &GraphValues,
    ) -> Result<String, LowerError> {
        let mut lists: Vec<String> = Vec::new();
        let mut items: Vec<String> = Vec::new();
        if part != Part::Rels {
            items.push(self.node_key(elements.nodes[0], s)?);
        }
        for (i, r) in elements.rels.iter().enumerate() {
            match self.scans.get(r) {
                Some(Scan::Rel {
                    schema, rel_type, ..
                }) => {
                    if part != Part::Nodes {
                        let ids: Vec<String> = self
                            .identity(*r)?
                            .unwrap_or_default()
                            .iter()
                            .map(sql)
                            .collect();
                        items.push(rel_key(
                            s,
                            rel_type,
                            &schema.from_node,
                            &schema.to_node,
                            &ids,
                        ));
                    }
                    if part == Part::Nodes {
                        items.push(self.node_key(elements.nodes[i + 1], s)?);
                    }
                }
                // Each row's own, as its definition spells it.
                Some(Scan::Rels { .. }) => {
                    if part != Part::Nodes {
                        items.push(sql(&self.physical(*r, super::KEY)?));
                    }
                    if part == Part::Nodes {
                        items.push(self.node_key(elements.nodes[i + 1], s)?);
                    }
                }
                Some(Scan::Path {
                    walked,
                    edges,
                    nodes,
                    reversed,
                    shortest,
                    ..
                }) => {
                    // A shortest path's identity is in its recovered paths,
                    // which the demand pass asks for (`PATH_KEY`).
                    if shortest.is_some() && !(*edges && *nodes) {
                        return unsupported(format!(
                            "internal: the paths of {r} are not recovered"
                        ));
                    }
                    if !items.is_empty() {
                        lists.push((s.texts)(&std::mem::take(&mut items)));
                    }
                    let in_order = |column: &str| -> Result<String, LowerError> {
                        let list = sql(&self.physical(*r, column)?);
                        Ok(if *reversed { (s.reverse)(&list) } else { list })
                    };
                    match walked {
                        Walked::One { schema, rel_type } => {
                            if part != Part::Nodes && *edges {
                                lists.push((s.prefixed_texts)(
                                    &rel_key_prefix(rel_type, &schema.from_node, &schema.to_node),
                                    &in_order("path_edges")?,
                                ));
                            }
                            if part == Part::Nodes {
                                lists.push((s.prefixed_texts)(
                                    &string(&format!("{}:", schema.to_node)),
                                    &(s.tail)(&in_order("path_nodes")?),
                                ));
                            }
                        }
                        // Its relationships and nodes are texts already.
                        Walked::Union { .. } => {
                            if part != Part::Nodes {
                                lists.push(in_order("path_edges")?);
                            }
                            if part == Part::Nodes {
                                lists.push((s.tail)(&in_order("path_nodes")?));
                            }
                        }
                    }
                }
                _ => return unsupported(format!("internal: {r} is not a relationship scan")),
            }
        }
        if !items.is_empty() || lists.is_empty() {
            lists.push((s.texts)(&items));
        }
        Ok((s.concat)(&lists))
    }

    /// A node's identity as a text, with its label.
    fn node_key(&self, v: VarId, s: &GraphValues) -> Result<String, LowerError> {
        let id = sql(&self.node_id_value(v)?);
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Ok(node_key_text(s, &string(label), &id)),
            Some(Scan::Labels { .. }) => {
                let label = sql(&self.physical(v, super::LABEL_COLUMN)?);
                Ok(node_key_text(s, &label, &id))
            }
            _ => unsupported(format!("internal: {v} is not a node scan")),
        }
    }

    /// A node's id (without its label) as one expression.
    fn node_id_value(&self, v: VarId) -> Result<RenderExpr, LowerError> {
        match self.id_columns(v)? {
            Some(mut ids) if ids.len() == 1 => Ok(ids.remove(0)),
            Some(_) => unsupported("a composite node id in a value (S8)"),
            None => Ok(RenderExpr::Literal(Literal::Null)),
        }
    }

    /// A variable-length relationship's list: its relationships (two empty
    /// lists are equal, whatever node they are at).
    fn vlp_keys(&self, r: VarId) -> Result<Vec<RenderExpr>, LowerError> {
        match self.scans.get(&r) {
            Some(Scan::Path { edges: true, .. }) => Ok(vec![self.physical(r, "path_edges")?]),
            Some(Scan::Path {
                shortest: Some(_), ..
            }) => unsupported(format!("internal: the paths of {r} are not recovered")),
            _ => Ok(Vec::new()),
        }
    }

    /// A fixed node of the current relation.
    fn node_value(&self, v: VarId, s: &GraphValues) -> Result<String, LowerError> {
        let id = sql(&self.node_id_value(v)?);
        let mut props = Vec::new();
        for name in self.all_property_names(v) {
            let e = self.property(v, &name)?;
            props.push((name, sql(&e)));
        }
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Ok(node_object(s, label, &id, &props)),
            Some(Scan::Labels { .. }) => {
                let label = sql(&self.physical(v, super::LABEL_COLUMN)?);
                Ok(labeled_node_object(s, &label, &id, &props))
            }
            _ => unsupported(format!("internal: {v} is not a node scan")),
        }
    }

    /// A node of several possible labels returned whole: its value, as a
    /// node of a path is (Bolt and the graph output read its label from it),
    /// grouped by its identity.
    pub(super) fn labeled_node(&self, v: VarId) -> Result<GraphValue, LowerError> {
        let s = spelling()?;
        let value = self.node_value(v, &s)?;
        let keys = self.identity(v)?.unwrap_or_default();
        self.finish(&s, value, keys, GraphType::Node, &[v])
    }

    /// A relationship of several possible types or label pairs returned
    /// whole: its value, as a relationship of a path is (Bolt and the graph
    /// output read its type from it), grouped by its identity.
    pub(super) fn labeled_rel(&self, v: VarId) -> Result<GraphValue, LowerError> {
        let s = spelling()?;
        let value = self.rel_value(v, &s)?;
        let keys = self.identity(v)?.unwrap_or_default();
        self.finish(&s, value, keys, GraphType::Relationship, &[v])
    }

    /// A fixed relationship of the current relation.
    fn rel_value(&self, v: VarId, s: &GraphValues) -> Result<String, LowerError> {
        if let Some(Scan::Rels { arms, .. }) = self.scans.get(&v) {
            let shape = super::rel_union_shape(arms)?;
            if shape.from != 1 || shape.to != 1 {
                return unsupported("a composite relationship endpoint in a value (S8)");
            }
            let column = |c: &str| self.physical(v, c).map(|e| sql(&e));
            let mut props = Vec::new();
            for name in self.all_property_names(v) {
                let e = self.property(v, &name)?;
                props.push((name, sql(&e)));
            }
            return Ok(labeled_rel_object(
                s,
                &column(super::REL_TYPE)?,
                (
                    &column(super::REL_FROM_LABEL)?,
                    &column(super::REL_TO_LABEL)?,
                ),
                (
                    &column(&super::indexed_column(super::REL_FROM, 0))?,
                    &column(&super::indexed_column(super::REL_TO, 0))?,
                ),
                &props,
            ));
        }
        let Some(Scan::Rel {
            schema, rel_type, ..
        }) = self.scans.get(&v)
        else {
            return unsupported(format!("internal: {v} is not a relationship scan"));
        };
        let (Ok([from]), Ok([to])) = (
            <[&str; 1]>::try_from(schema.from_id.columns()),
            <[&str; 1]>::try_from(schema.to_id.columns()),
        ) else {
            return unsupported("a composite relationship endpoint in a value (S8)");
        };
        let mut props = Vec::new();
        for name in self.all_property_names(v) {
            let e = self.property(v, &name)?;
            props.push((name, sql(&e)));
        }
        Ok(rel_object(
            s,
            rel_type,
            &schema.from_node,
            &schema.to_node,
            &sql(&self.physical(v, from)?),
            &sql(&self.physical(v, to)?),
            &props,
        ))
    }

    /// A variable-length relationship's nodes, in path order.
    fn vlp_nodes(&self, r: VarId, s: &GraphValues) -> Result<String, LowerError> {
        self.vlp_list(r, super::path::VALUE_COLUMNS[0], s)
    }

    /// A variable-length relationship's relationships, in path order.
    fn vlp_rels(&self, r: VarId, s: &GraphValues) -> Result<String, LowerError> {
        self.vlp_list(r, super::path::VALUE_COLUMNS[1], s)
    }

    fn vlp_list(&self, r: VarId, column: &str, s: &GraphValues) -> Result<String, LowerError> {
        let Some(Scan::Path {
            reversed,
            node_values,
            rel_values,
            ..
        }) = self.scans.get(&r)
        else {
            return unsupported(format!("internal: {r} is not a path relation"));
        };
        let carried = if column == super::path::VALUE_COLUMNS[0] {
            *node_values
        } else {
            *rel_values
        };
        if !carried {
            return unsupported(format!("internal: {r} does not carry {column}"));
        }
        let list = sql(&self.physical(r, column)?);
        Ok(if *reversed { (s.reverse)(&list) } else { list })
    }
}

/// Export a graph value as column `out` of the CTE aliased `alias`, with its
/// keys (`out__k{i}`): how the next segment reads it.
pub(super) fn export_graph(
    out: VarId,
    v: GraphValue,
    alias: &str,
    body: &mut Body,
    aggregating: bool,
) -> Carried {
    let name = out.name();
    body.select.push(select(v.value, &name));
    body.determined.push(name.clone());
    let mut keys = Vec::new();
    for (i, k) in v.keys.into_iter().enumerate() {
        let column = format!("{name}__k{i}");
        body.select.push(select(k.clone(), &column));
        if aggregating {
            body.group_by.push(k);
        }
        keys.push(col_at(alias, &column));
    }
    Carried {
        keys,
        ty: v.ty,
        nullable: v.nullable,
    }
}

/// Which part of a path a value is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Whole,
    Nodes,
    Rels,
}
