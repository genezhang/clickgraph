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
    edge_id_escapes, edge_id_separators, node_element_id_affixes, node_element_id_separators,
    relationship_element_id_affixes, relationship_element_id_separators, NULL_EDGE_ID_PART,
};
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_expr::{self as lx, LogicalExpr};
use crate::render_plan::render_expr::{
    AggregateFnCall, Literal, Operator, OperatorApplication, PropertyAccess, RenderCase,
    RenderExpr, ScalarFnCall, TableAlias,
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

/// A value's identities and the condition under which it is NULL
/// ([`Lowerer::graph_keys`]).
type KeysAndAbsence = (Vec<RenderExpr>, Option<RenderExpr>);

/// `value`, or NULL where `absent` holds.
fn null_where(absent: Option<RenderExpr>, value: RenderExpr) -> RenderExpr {
    match absent {
        Some(cond) => RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(cond, RenderExpr::Literal(Literal::Null))],
            else_expr: Some(Box::new(value)),
        }),
        None => value,
    }
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

/// An identity of one or more columns (SQL) as a text: the column's, or
/// their tuple's ([`GraphValues::tuple_text`]), as every key of a node or
/// relationship spells it.
pub(super) fn identity_text(g: &GraphValues, cols: &[String]) -> String {
    match cols {
        [one] => (g.to_text)(one),
        _ => (g.tuple_text)(cols),
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
        identity_text(g, ids),
    ])
}

/// A node's identity as a text, with its label (SQL `label`) and id (SQL
/// `id`): as a path's identity spells it (`Lowerer::node_key`).
pub(super) fn node_key_text(g: &GraphValues, label: &str, id: &str) -> String {
    (g.text)(&[label.to_string(), string(":"), (g.to_text)(id)])
}

/// #1360: cast a walk-key part so both sides spell the same type before they
/// are stringified.
///
/// Walk keys are compared as *text* (`identity_text`), which is why a cast is
/// needed at all: `toString(Date)` and `toString(DateTime)` differ, so a Date
/// node and a DateTime edge can never join without one. `target` is the
/// least-common type for this part's (node, edge) pair: `KeyPartType::widen_to`
/// folds a pair, and `graph_catalog::schema_types::fold_common` folds a whole
/// walk's set of sides. `None` means the two sides already agree and the
/// column is emitted untouched.
///
/// Both sides are cast, to the same target. Casting only one side is what
/// this function's first version did, and it was wrong: casting a DateTime
/// edge down to a Date node *widens* the Date to midnight and manufactures
/// matches that `=` correctly refuses.
pub(super) fn cast_key_part(col_sql: &str, target: Option<&str>) -> String {
    match target {
        Some(DAY_OR_TIME) => day_or_time(col_sql),
        Some(t) => format!("CAST({col_sql} AS {t})"),
        None => col_sql.to_string(),
    }
}

/// The key "target" of a `Date` id part met by times of several zones
/// (`schema_types::TextSpelling::DayOrTime`, ELEMENT_IDENTITY.md I2): no type
/// spells them alike, so [`cast_key_part`] spells [`day_or_time`] instead.
pub(super) const DAY_OR_TIME: &str = "__cg_day_or_time";

/// A date or a time (SQL `col_sql`) as a text equal to another's exactly
/// where `=` holds of them (`FunctionMapper::day_or_time_text`).
pub(super) fn day_or_time(col_sql: &str) -> String {
    current_function_mapper().day_or_time_text(col_sql)
}

/// A node's id parts (SQL) as its element id spells them
/// (`graph_catalog::element_id`): the one id, or the parts' texts joined by
/// `|` (the element id's contract; a value's identity is its key, which no
/// two ids share, [`super::walk_key_sql`]).
pub(super) fn element_id_sql(g: &GraphValues, ids: &[String]) -> String {
    match ids {
        [one] => one.clone(),
        _ => {
            let mut parts = Vec::new();
            for (i, p) in ids.iter().enumerate() {
                if i > 0 {
                    parts.push(string("|"));
                }
                // A NULL part is spelled, not NULL: the element id of a node
                // with one is not NULL (review).
                parts.push(format!("coalesce({}, {})", (g.to_text)(p), string("null")));
            }
            (g.text)(&parts)
        }
    }
}

/// A node: its label, the SQL of its id (a composite one's
/// [`element_id_sql`]), its properties (name, SQL).
pub(super) fn node_object(
    g: &GraphValues,
    schema: &GraphSchema,
    label: &str,
    id: &str,
    props: &[(String, String)],
) -> String {
    let (prefix, suffix) = node_element_id_affixes(label);
    let labels = node_labels_sql(schema, label);
    (g.object)(&[
        (
            string("elementId"),
            (g.text)(&[string(&prefix), (g.to_text)(id), string(suffix)]),
        ),
        (string("labels"), labels),
        (string("properties"), properties(g, props)),
    ])
}

/// The labels of a node of `label` (SQL): it, then the union labels it
/// carries (`GraphSchema::union_labels_of`).
fn node_labels_sql(schema: &GraphSchema, label: &str) -> String {
    let labels: Vec<String> = std::iter::once(label)
        .chain(schema.union_labels_of(label))
        .map(string)
        .collect();
    current_function_mapper().array_literal(&labels.join(", "))
}

/// The labels of a node whose label is the SQL `label` (a column), as
/// [`node_labels_sql`]: by the label, where a union label has it.
fn labeled_node_labels_sql(schema: &GraphSchema, label: &str) -> String {
    let own = current_function_mapper().array_literal(label);
    let carried: std::collections::BTreeSet<&String> =
        schema.union_labels().values().flatten().collect();
    if carried.is_empty() {
        return own;
    }
    let whens: String = carried
        .iter()
        .map(|l| format!(" WHEN {} THEN {}", string(l), node_labels_sql(schema, l)))
        .collect();
    format!("CASE {label}{whens} ELSE {own} END")
}

/// A node whose label is the SQL `label` (a column: one of several
/// possible labels), else as [`node_object`]. Its properties are those of
/// every label, NULL where its own has none, so the value has its own.
fn labeled_node_object(
    g: &GraphValues,
    schema: &GraphSchema,
    label: &str,
    id: &str,
    props: &[(String, String)],
) -> String {
    let (after_label, suffix) = node_element_id_separators();
    let labels = labeled_node_labels_sql(schema, label);
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

/// The SQL string literal of the text `text` (`string` reads a Cypher
/// literal's body, in which a backslash escapes).
pub(super) fn text_literal(text: &str) -> String {
    string(&text.replace('\\', "\\\\"))
}

/// An `edge_id`'s parts (SQL) as its text in a relationship's element id
/// (`element_id::edge_id_text`): each part's text escaped, NULL as
/// `NULL_EDGE_ID_PART`, joined by `|`.
pub(super) fn edge_id_sql(g: &GraphValues, parts: &[String]) -> String {
    let m = current_function_mapper();
    let mut texts = Vec::new();
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            texts.push(string(edge_id_separators().0));
        }
        let mut text = (g.to_text)(p);
        for (from, to) in edge_id_escapes() {
            text = m.replace_text(&text, &text_literal(from), &text_literal(to));
        }
        // By the part itself: a union's `Variant` of NULL is the text `''`.
        texts.push(format!(
            "CASE WHEN {p} IS NULL THEN {} ELSE {text} END",
            text_literal(NULL_EDGE_ID_PART)
        ));
    }
    (g.text)(&texts)
}

/// A relationship's element id (as Bolt makes it,
/// `element_id::relationship_element_id`): its type, its `edge_id`'s text
/// ([`edge_id_sql`]) where its schema declares one, and the element-id texts
/// of its stored ends (`from`, `to`).
pub(super) fn rel_element_id(
    g: &GraphValues,
    rel_type: &str,
    edge: Option<&str>,
    from: &str,
    to: &str,
) -> String {
    let (prefix, between, suffix) = relationship_element_id_affixes(rel_type);
    let mut parts = vec![string(&prefix)];
    if let Some(edge) = edge {
        parts.push(edge.to_string());
        parts.push(string(edge_id_separators().1));
    }
    parts.extend([
        (g.to_text)(from),
        string(between),
        (g.to_text)(to),
        string(suffix),
    ]);
    (g.text)(&parts)
}

/// A relationship: its type, the labels of its stored `from` and `to`
/// nodes, its `edge_id`'s text ([`edge_id_sql`]) where its schema declares
/// one, the SQL of its ends' ids (as their nodes spell theirs,
/// `Lowerer::end_spellings`), its properties.
#[allow(clippy::too_many_arguments)]
pub(super) fn rel_object(
    g: &GraphValues,
    rel_type: &str,
    from_label: &str,
    to_label: &str,
    edge: Option<&str>,
    from: &str,
    to: &str,
    props: &[(String, String)],
) -> String {
    let node_id = |label: &str, id: &str| {
        let (p, s) = node_element_id_affixes(label);
        (g.text)(&[string(&p), (g.to_text)(id), string(s)])
    };
    (g.object)(&[
        (
            string("elementId"),
            rel_element_id(g, rel_type, edge, from, to),
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
/// NULL where its own has none, so the value has its own. `edge`: the text
/// (SQL) of its `edge_id` and the separator after it where its definition
/// declares one, else an empty text.
fn labeled_rel_object(
    g: &GraphValues,
    rel_type: &str,
    (from_label, to_label): (&str, &str),
    edge: Option<&str>,
    (from, to): (&str, &str),
    props: &[(String, String)],
) -> String {
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
            labeled_rel_element_id(g, rel_type, edge, from, to),
        ),
        (string("startNodeElementId"), node_id(from_label, from)),
        (string("endNodeElementId"), node_id(to_label, to)),
        (string("type"), rel_type.to_string()),
        (string("properties"), properties(g, props)),
    ])
}

/// The element id of a relationship whose type is the SQL `rel_type` (a
/// column), else as [`rel_element_id`].
fn labeled_rel_element_id(
    g: &GraphValues,
    rel_type: &str,
    edge: Option<&str>,
    from: &str,
    to: &str,
) -> String {
    let (after_type, between, suffix) = relationship_element_id_separators();
    (g.text)(
        &[rel_type.to_string(), string(after_type)]
            .into_iter()
            .chain(edge.map(str::to_string))
            .chain([
                (g.to_text)(from),
                string(between),
                (g.to_text)(to),
                string(suffix),
            ])
            .collect::<Vec<_>>(),
    )
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
    graph: &GraphSchema,
    schema: &NodeSchema,
    label: &str,
    alias: &str,
) -> Result<String, LowerError> {
    let ids: Vec<String> = schema
        .id_physical_columns()
        .iter()
        .map(|c| sql(&super::col_at(alias, c)))
        .collect();
    Ok(node_object(
        g,
        graph,
        label,
        &element_id_sql(g, &ids),
        &table_props(&schema.property_mappings, alias),
    ))
}

/// How a relationship end's id part is spelled as its node spells its id
/// (`schema_types::EndSpelling`, its types spelled as CAST targets).
#[derive(Debug, Clone, PartialEq)]
pub(super) enum EndSpell {
    Own,
    In(String),
    DayOrTime,
    Exact(String),
}

/// How a relationship's `from` and `to` ends' id parts are spelled as their
/// nodes spell theirs (`Lowerer::end_spellings`); a missing entry: as its own.
pub(super) type EndSpellings = (Vec<EndSpell>, Vec<EndSpell>);

/// An end's id part (SQL) as its node spells it ([`EndSpell`]).
pub(super) fn spell_end(part: &str, spell: &EndSpell) -> String {
    match spell {
        EndSpell::Own => part.to_string(),
        EndSpell::In(t) => format!("CAST({part} AS {t})"),
        EndSpell::DayOrTime => day_or_time(part),
        EndSpell::Exact(t) => {
            let text = current_function_mapper().cast_string();
            format!(
                "CASE WHEN CAST({part} AS {t}) = {part} THEN {text}(CAST({part} AS {t})) \
                 ELSE {text}({part}) END"
            )
        }
    }
}

/// An end's id parts (SQL) as its node spells them ([`EndSpellings`]).
pub(super) fn spelled_end(parts: Vec<String>, spells: &[EndSpell]) -> Vec<String> {
    parts
        .into_iter()
        .enumerate()
        .map(|(i, p)| match spells.get(i) {
            Some(s) => spell_end(&p, s),
            None => p,
        })
        .collect()
}

/// A relationship of `schema`'s table read under `alias`, its ends' ids
/// spelled as `ends`.
pub(super) fn table_rel_object(
    g: &GraphValues,
    schema: &RelationshipSchema,
    rel_type: &str,
    alias: &str,
    ends: &EndSpellings,
) -> Result<String, LowerError> {
    let cols = |cols: Vec<&str>| -> Vec<String> {
        cols.iter().map(|c| sql(&super::col_at(alias, c))).collect()
    };
    let edge = schema
        .edge_id
        .as_ref()
        .map(|id| edge_id_sql(g, &cols(id.columns())));
    Ok(rel_object(
        g,
        rel_type,
        &schema.from_node,
        &schema.to_node,
        edge.as_deref(),
        &element_id_sql(g, &spelled_end(cols(schema.from_id.columns()), &ends.0)),
        &element_id_sql(g, &spelled_end(cols(schema.to_id.columns()), &ends.1)),
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
                // The RETURN's ORDER BY reads the item (`ORDER BY
                // length(p)`): its elements are the path's (#1427).
                if let GraphRef::Path(src) = g {
                    if let Some(elements) = self.paths.get(&src).cloned() {
                        self.paths.insert(it.var, elements);
                        if aggregating || body.distinct {
                            self.grouped_paths.insert(it.var);
                        }
                    }
                }
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
                if !matches!(carried.ty, GraphType::List(_)) {
                    return unsupported("size() of a carried path");
                }
                // Signed, as every size: a NULL-able unsigned one would not
                // mix with a negative number (Code 44 in a grouping key).
                let length = RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: current_function_mapper().cast_int64().to_string(),
                    args: vec![RenderExpr::Raw((spelling()?.length)(
                        &sql(&self.values[&v]),
                    ))],
                });
                if !carried.nullable {
                    return Ok(length);
                }
                // NULL where the list is: its first key is that condition
                // (`null_safe`); without keys it is NULL in every row.
                match carried.keys.first() {
                    Some(absent) => Ok(RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then: vec![(absent.clone(), RenderExpr::Literal(Literal::Null))],
                        else_expr: Some(Box::new(length)),
                    })),
                    None => Ok(RenderExpr::Literal(Literal::Null)),
                }
            }
            GraphRef::Path(_) => unsupported("size() of a path"),
        }
    }

    /// `count(g)` / `count(DISTINCT g)`: the rows where the value is not
    /// NULL, or its distinct values (by its elements' identities, which are
    /// equal exactly when the values are). Neither builds the value: a
    /// variable-length relationship's walk carries no element values for it.
    pub(super) fn count_graph_value(
        &self,
        g: GraphRef,
        distinct: bool,
    ) -> Result<RenderExpr, LowerError> {
        let Some((keys, absent)) = self.graph_keys(g, distinct)? else {
            // It matches nothing: no values to count (still an aggregate:
            // one row of a global aggregation).
            return Ok(super::expr::aggregate_constant(RenderExpr::Literal(
                Literal::Integer(0),
            )));
        };
        let value = if distinct {
            if keys.is_empty() {
                return unsupported("count(DISTINCT) of a value without identities");
            }
            let tuple = RenderExpr::ScalarFnCall(ScalarFnCall {
                name: current_function_mapper().tuple_constructor().to_string(),
                args: keys,
            });
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Distinct,
                operands: vec![null_where(absent, tuple)],
            })
        } else {
            null_where(absent, RenderExpr::Literal(Literal::Integer(1)))
        };
        Ok(RenderExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![value],
        }))
    }

    /// The identities of the value `g` names (with `keys`) and the
    /// condition under which it is NULL, without building the value. `None`:
    /// it matches nothing.
    fn graph_keys(&self, g: GraphRef, keys: bool) -> Result<Option<KeysAndAbsence>, LowerError> {
        let s = spelling()?;
        let (identities, vars) = match g {
            GraphRef::Path(p) | GraphRef::Nodes(p) | GraphRef::Rels(p) => {
                self.ungrouped_elements(p)?;
                let Some(elements) = self.paths.get(&p).cloned() else {
                    return unsupported(format!("internal: path {p} has no elements"));
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
                    return Ok(None);
                }
                if self.binding(p).nullable && !all.iter().any(|v| self.binding(*v).nullable) {
                    return unsupported("an OPTIONAL path of bound elements as a value");
                }
                let part = match g {
                    GraphRef::Nodes(_) => Part::Nodes,
                    GraphRef::Rels(_) => Part::Rels,
                    _ => Part::Whole,
                };
                let identities = if keys {
                    vec![RenderExpr::Raw(self.path_key(&elements, part, &s)?)]
                } else {
                    Vec::new()
                };
                (identities, all)
            }
            GraphRef::List(r) => match self.scans.get(&r) {
                Some(Scan::Path { .. }) => {
                    let identities = if keys { self.vlp_keys(r)? } else { Vec::new() };
                    (identities, vec![r])
                }
                Some(Scan::Impossible) => return Ok(None),
                _ => return unsupported(format!("internal: {r} is not a path relation")),
            },
            GraphRef::Carried(v) => {
                if matches!(
                    self.values.get(&v),
                    Some(RenderExpr::Literal(Literal::Null))
                ) {
                    return Ok(None);
                }
                let c = &self.graph_values[&v];
                // A nullable one's first key is its NULL condition
                // (`null_safe`).
                return Ok(Some(match (c.nullable, c.keys.split_first()) {
                    (true, Some((cond, rest))) => (rest.to_vec(), Some(cond.clone())),
                    (true, None) => return Ok(None),
                    (false, _) => (c.keys.clone(), None),
                }));
            }
        };
        let (identities, cond) = self.null_safe(&s, identities, &vars)?;
        Ok(Some(match (cond, identities.split_first()) {
            (Some(_), Some((cond, rest))) => (rest.to_vec(), Some(cond.clone())),
            _ => (identities, None),
        }))
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
            nulls.push(super::null_test(&self.presence(*v, &id), true));
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
        self.ungrouped_elements(p)?;
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
                                    super::path::edge_arity(schema),
                                ));
                            }
                            if part == Part::Nodes {
                                // A composite id is its text already.
                                lists.push((s.prefixed_texts)(
                                    &string(&format!("{}:", schema.to_node)),
                                    &(s.tail)(&in_order("path_nodes")?),
                                    1,
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

    /// A node's identity as a text, with its label (a composite id's the
    /// text of its tuple, `super::walk_key_sql`, as walks key it).
    fn node_key(&self, v: VarId, s: &GraphValues) -> Result<String, LowerError> {
        let ids = self.node_id_parts(v)?;
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Ok(super::walk_key_sql(s, &string(label), &ids)),
            Some(Scan::Labels { .. }) => {
                let label = sql(&self.physical(v, super::LABEL_COLUMN)?);
                Ok(super::walk_key_sql(s, &label, &ids))
            }
            _ => unsupported(format!("internal: {v} is not a node scan")),
        }
    }

    /// A node's id columns (SQL, without its label); NULL for a node that
    /// matches nothing.
    fn node_id_parts(&self, v: VarId) -> Result<Vec<String>, LowerError> {
        Ok(match self.id_columns(v)? {
            Some(ids) => self.own_texts(v, ids)?.iter().map(sql).collect(),
            None => vec![sql(&RenderExpr::Literal(Literal::Null))],
        })
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
        let id = element_id_sql(s, &self.node_id_parts(v)?);
        let mut props = Vec::new();
        for name in self.all_property_names(v) {
            let e = self.property(v, &name)?;
            props.push((name, sql(&e)));
        }
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Ok(node_object(s, self.schema, label, &id, &props)),
            Some(Scan::Labels { .. }) => {
                let label = sql(&self.physical(v, super::LABEL_COLUMN)?);
                Ok(labeled_node_object(s, self.schema, &label, &id, &props))
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
            let column = |c: &str| self.physical(v, c).map(|e| sql(&e));
            let (edge, from, to) = self.union_identity_texts(v, arms, s)?;
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
                edge.as_deref(),
                (&from, &to),
                &props,
            ));
        }
        let Some(Scan::Rel {
            schema, rel_type, ..
        }) = self.scans.get(&v)
        else {
            return unsupported(format!("internal: {v} is not a relationship scan"));
        };
        let (edge, from, to) = self.rel_identity_texts(v, schema, s)?;
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
            edge.as_deref(),
            &from,
            &to,
            &props,
        ))
    }

    /// The texts (SQL) a relationship `v` of a union of definitions `arms`
    /// spells its element id from ([`labeled_rel_element_id`]): its `edge_id`'s
    /// where its definition declares one (`Lowerer::union_edge_text`), and
    /// its ends' ids', as their nodes spell theirs (`Lowerer::own_end_text`).
    fn union_identity_texts(
        &self,
        v: VarId,
        arms: &[super::RelArm],
        s: &GraphValues,
    ) -> Result<(Option<String>, String, String), LowerError> {
        let shape = super::rel_union_shape(arms)?;
        let end = |side: &str, arity: usize| -> Result<String, LowerError> {
            let ids = (0..arity)
                .map(|i| {
                    let id = self.physical(v, &super::indexed_column(side, i))?;
                    self.own_end_text(v, arms, side == super::REL_TO, i, id)
                        .map(|e| sql(&e))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(element_id_sql(s, &ids))
        };
        Ok((
            self.union_edge_text(v, arms, s)?,
            end(super::REL_FROM, shape.from)?,
            end(super::REL_TO, shape.to)?,
        ))
    }

    /// The element id (SQL) of relationship `v` of one definition or of a
    /// union of them, as its value spells it (Bolt's, from which its `id()`
    /// number is computed); `None`: not a relationship of such a scan.
    pub(super) fn rel_element_id_sql(
        &self,
        v: VarId,
        s: &GraphValues,
    ) -> Result<Option<String>, LowerError> {
        match self.scans.get(&v) {
            Some(Scan::Rel {
                schema, rel_type, ..
            }) => {
                let (edge, from, to) = self.rel_identity_texts(v, schema, s)?;
                Ok(Some(rel_element_id(
                    s,
                    rel_type,
                    edge.as_deref(),
                    &from,
                    &to,
                )))
            }
            Some(Scan::Rels { arms, .. }) => {
                let (edge, from, to) = self.union_identity_texts(v, arms, s)?;
                let rel_type = sql(&self.physical(v, super::REL_TYPE)?);
                Ok(Some(labeled_rel_element_id(
                    s,
                    &rel_type,
                    edge.as_deref(),
                    &from,
                    &to,
                )))
            }
            _ => Ok(None),
        }
    }

    /// The texts (SQL) relationship `v` of one definition, `schema`, spells
    /// its element id from ([`rel_element_id`]): its `edge_id`'s, where
    /// `schema` declares one, and its ends' ids', as their nodes spell theirs
    /// (`Lowerer::end_spellings`).
    pub(super) fn rel_identity_texts(
        &self,
        v: VarId,
        schema: &RelationshipSchema,
        s: &GraphValues,
    ) -> Result<(Option<String>, String, String), LowerError> {
        let cols = |cols: Vec<&str>| -> Result<Vec<String>, LowerError> {
            cols.iter()
                .map(|c| self.physical(v, c).map(|e| sql(&e)))
                .collect()
        };
        let ends = self.end_spellings(schema);
        let edge = match &schema.edge_id {
            Some(id) => Some(edge_id_sql(s, &cols(id.columns())?)),
            None => None,
        };
        Ok((
            edge,
            element_id_sql(s, &spelled_end(cols(schema.from_id.columns())?, &ends.0)),
            element_id_sql(s, &spelled_end(cols(schema.to_id.columns())?, &ends.1)),
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
