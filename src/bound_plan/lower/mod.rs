//! Lowering (P-4c S4, `docs/design/EXPLICIT_SCOPE.md` §4.6, §4.10, §4.13,
//! §4.14): a bound plan → a [`RenderPlan`] printed by
//! `render_plan_to_sql_plain`.
//!
//! Every element of a pattern is its own scan, aliased by its binding's
//! generated name (`v{N}`), so no two elements can share an alias and a
//! column reference is always `v{N}.<physical column>`:
//! * a node is a scan of its label's table;
//! * a relationship is a scan of its type's edge table, tied to its endpoint
//!   nodes by `edge.from = from-node.id`, `edge.to = to-node.id` in the
//!   stored orientation;
//! * a variable appearing again (in the same clause or an earlier one) is the
//!   same scan: its appearances are ties, never a second scan.
//!
//! Scans are joined in pattern order; each tie is placed in the ON of the
//! later of its two scans, so the joins are already in dependency order and
//! the printer runs no repair pass. Within one MATCH, relationships of the
//! same table must differ (relationship uniqueness).
//!
//! A WITH ends a **segment** (§4.10): the rows so far become a CTE whose
//! columns are exactly the WITH's output scope, and the next segment reads
//! from it. A carried node or relationship exports its identity columns (a
//! relationship also its endpoint columns) and the properties later clauses
//! read (the demand pass, [`demand`]); a value exports its value. A
//! free-standing SKIP / LIMIT ends a segment the same way, exporting the
//! scope unchanged. Rows keep an order until a MATCH; while they have one
//! it travels as exported sort-key columns, so a later SKIP / LIMIT / RETURN
//! reads the rows in that order. After DISTINCT or aggregation the order
//! Neo4j keeps is lost in SQL (`RowOrder::Lost`), and a clause relying on
//! it is not lowered.
//!
//! An OPTIONAL MATCH is one unit (§4.9): the rows so far LEFT JOIN a CTE of
//! the clause's matches, its WHERE inside (`Lowerer::optional_match`).
//!
//! Scope today — everything else is [`LowerError::Unsupported`] and the
//! query is translated by the legacy pipeline:
//! * MATCH and OPTIONAL MATCH over standard-layout labels and types
//!   (`NodeSchema::is_standard_own_table`, `RelationshipSchema::
//!   is_standard_edge_table`), each node with one label and each
//!   relationship with one type after label inference, fixed length,
//!   directed;
//! * WITH and RETURN with aggregation, DISTINCT, ORDER BY, SKIP, LIMIT and
//!   (WITH) WHERE, evaluated in that order; free-standing ORDER BY, SKIP and
//!   LIMIT;
//! * a final RETURN of values, whole nodes and relationships (`n`, `n.*`)
//!   and `id(n)`, with the result shape that Bolt, the HTTP graph output and
//!   embedded `query_graph` read (§4.13, [`ResultColumn`]).
//!
//! A node or relationship whose label / type set is empty matches nothing
//! (Cypher returns no rows; it is not an error): the query lowers to a
//! relation with no rows, and its properties read as NULL.

mod expr;
mod path;
#[cfg(test)]
mod tests;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{
    ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::{
    ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem, GroupByExpressions, Join,
    JoinItems, JoinType, LimitItem, OrderByItem, OrderByItems, OrderByOrder, RenderPlan,
    SelectItem, SelectItems, SkipItem, UnionItems, ViewTableRef,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::utils::cte_column_naming::cte_column_name;

use super::expr::{calls_aggregate, property_refs, referenced_names};
use super::types::*;

/// Why a bound plan was not lowered.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LowerError {
    /// Not lowered yet; the legacy pipeline translates the query.
    #[error("not lowered yet: {0}")]
    Unsupported(String),
}

fn unsupported<T>(what: impl Into<String>) -> Result<T, LowerError> {
    Err(LowerError::Unsupported(what.into()))
}

/// Request inputs the lowering needs.
#[derive(Debug, Clone, Default)]
pub struct LowerOptions {
    /// Values for parameterized views (`ReadOptions::view_parameter_values`).
    pub view_parameter_values: Option<HashMap<String, String>>,
    /// Neo4j-compat mode: an undeclared property is NULL on every element,
    /// not only on those whose columns were discovered.
    pub neo4j_compat: bool,
}

/// A lowered statement: the plan, and what its result columns are.
#[derive(Debug)]
pub struct Lowered {
    pub plan: RenderPlan,
    pub shape: Vec<ResultColumn>,
}

/// One RETURN item of the result (§4.13), in RETURN order.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultColumn {
    /// The item's name: its column, or the prefix of its columns.
    pub name: String,
    pub kind: ResultKind,
    /// The SQL columns holding the item: (key, column). A value's key is its
    /// name; an element's keys are `from_id` / `to_id` and its property
    /// names. A column is the key's legacy name (`name`, `name.<key>`)
    /// unless another item took that name first (then `…_2`, …), so a
    /// reader must not collect columns by prefix.
    pub columns: Vec<(String, String)>,
}

/// What a RETURN item is, and so which columns hold it. Element columns use
/// the legacy pipeline's names, so the HTTP rows and the Bolt / graph
/// transformation (`bolt_protocol::result_transformer`) are unchanged.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultKind {
    /// One column, `name`.
    Value,
    /// A node with this label: a column `name.<prop>` per property.
    Node { label: String },
    /// A relationship: `name.from_id` and `name.to_id` (its stored endpoint
    /// columns, `from_id_1`, `from_id_2`, … when composite), then a column
    /// `name.<prop>` per property.
    Rel {
        rel_type: String,
        from_label: String,
        to_label: String,
    },
    /// `id(n)` of a node with this label: column `name` holds the node's key,
    /// which Bolt returns encoded (`IdMapper`), as on the legacy pipeline.
    NodeId { label: String },
}

/// Lower a bound statement to a render plan.
pub fn lower_statement(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
) -> Result<Lowered, LowerError> {
    let BoundOp::Project { input, projection } = &stmt.plan else {
        return unsupported("a statement that does not end in RETURN (UNION: S7)");
    };
    let mut l = Lowerer {
        schema,
        bindings: &stmt.bindings,
        options,
        demand: demand(stmt),
        ctes: Vec::new(),
        scans: HashMap::new(),
        values: HashMap::new(),
        emitted: Vec::new(),
        from: None,
        joins: Vec::new(),
        pending: Vec::new(),
        filters: Vec::new(),
        empty: false,
        order: RowOrder::Unordered,
        elided: HashMap::new(),
        paths: HashMap::new(),
    };
    l.relation(input)?;
    l.finish_relation();
    let (mut plan, shape) = l.project(projection)?;
    plan.ctes = CteItems(std::mem::take(&mut l.ctes));
    Ok(Lowered { plan, shape })
}

/// How a pattern element is read.
#[derive(Debug, Clone)]
enum Scan<'s> {
    Node {
        schema: &'s NodeSchema,
        label: String,
        at: At,
    },
    Rel {
        schema: &'s RelationshipSchema,
        rel_type: String,
        at: At,
    },
    /// A variable-length relationship: a relation of paths (`path.rs`),
    /// read from the CTE `cte`.
    Path {
        schema: &'s RelationshipSchema,
        rel_type: String,
        cte: String,
        at: At,
        /// It has a `path_edges` column.
        edges: bool,
        /// Walked against the relationships' direction: `start_id` is their
        /// `to` end, and `path_edges` spells them backward.
        backward: bool,
    },
    /// An element whose label / type set is empty: it matches nothing.
    Impossible,
}

/// Where an element's columns are.
#[derive(Debug, Clone)]
enum At {
    /// Its own table, under this alias (`v{N}`).
    Table(String),
    /// Columns of a CTE (a WITH or a SKIP / LIMIT) under `alias`: the
    /// element's physical identity / endpoint columns by exported name, and
    /// its demanded properties as expressions over the CTE (a property that
    /// is a constant, such as an unmapped one's NULL, stays the constant).
    Exported {
        alias: String,
        physical: HashMap<String, String>,
        props: HashMap<String, RenderExpr>,
    },
}

impl At {
    fn alias(&self) -> &str {
        match self {
            At::Table(a) => a,
            At::Exported { alias, .. } => alias,
        }
    }
}

impl<'s> Scan<'s> {
    fn at(&self) -> Option<&At> {
        match self {
            Scan::Node { at, .. } | Scan::Rel { at, .. } | Scan::Path { at, .. } => Some(at),
            Scan::Impossible => None,
        }
    }

    /// The same element, read at `at`.
    fn with_at(self, at: At) -> Scan<'s> {
        match self {
            Scan::Node { schema, label, .. } => Scan::Node { schema, label, at },
            Scan::Rel {
                schema, rel_type, ..
            } => Scan::Rel {
                schema,
                rel_type,
                at,
            },
            Scan::Path {
                schema,
                rel_type,
                cte,
                edges,
                backward,
                ..
            } => Scan::Path {
                schema,
                rel_type,
                cte,
                at,
                edges,
                backward,
            },
            Scan::Impossible => Scan::Impossible,
        }
    }
}

/// The rows of one segment as they are being built: what [`Lowerer`] holds
/// for the current segment, swapped out while an OPTIONAL MATCH builds its
/// matches (`Lowerer::swap_segment`).
#[derive(Default)]
struct Segment<'s> {
    scans: HashMap<VarId, Scan<'s>>,
    values: HashMap<VarId, RenderExpr>,
    emitted: Vec<String>,
    from: Option<ViewTableRef>,
    joins: Vec<Join>,
    pending: Vec<Tie>,
    filters: Vec<RenderExpr>,
    empty: bool,
    order: RowOrder,
    elided: HashMap<VarId, VarId>,
}

/// One column an OPTIONAL MATCH's matches are joined to the input rows on:
/// `outer` (in the input) and `inner` (in the matches), exported by the
/// matches as `column`.
struct Correlated {
    column: String,
    outer: RenderExpr,
    inner: RenderExpr,
    /// NULL equals NULL.
    null_safe: bool,
}

/// An equality between columns of two scans, placed in the ON of whichever
/// is emitted later (or in WHERE if both already are).
struct Tie {
    a: VarId,
    b: VarId,
    eqs: Vec<(RenderExpr, RenderExpr)>,
}

struct Lowerer<'s> {
    schema: &'s GraphSchema,
    bindings: &'s [Binding],
    options: &'s LowerOptions,
    /// Properties read of each binding, anywhere downstream (§4.10).
    demand: HashMap<VarId, BTreeSet<String>>,
    /// Finished segments, in order.
    ctes: Vec<Cte>,
    // --- the current segment
    scans: HashMap<VarId, Scan<'s>>,
    /// Values exported by the segment's CTE: a column, or a constant.
    values: HashMap<VarId, RenderExpr>,
    /// Relation aliases in join order.
    emitted: Vec<String>,
    from: Option<ViewTableRef>,
    joins: Vec<Join>,
    pending: Vec<Tie>,
    filters: Vec<RenderExpr>,
    /// Some element matches nothing: the relation has no rows.
    empty: bool,
    /// The rows' order (§3: after an ORDER BY, until a MATCH).
    order: RowOrder,
    /// Nodes read from a relationship's endpoint columns instead of their
    /// own table (an OPTIONAL MATCH's shared node, `anchor_correlation`).
    elided: HashMap<VarId, VarId>,
    /// Path variables (`p = …`) and their elements, of every clause.
    paths: HashMap<VarId, PathElements>,
}

/// The elements of a path variable, in path order.
#[derive(Debug, Clone)]
struct PathElements {
    nodes: Vec<VarId>,
    rels: Vec<VarId>,
}

/// The order of the current rows.
#[derive(Debug, Clone, Default)]
enum RowOrder {
    /// None that a later clause may rely on.
    #[default]
    Unordered,
    /// Sorted by these keys over the current relation.
    Keys(Vec<OrderByItem>),
    /// Neo4j would keep an order here (DISTINCT and aggregation keep the
    /// first-seen order of their input) that the SQL does not: a later
    /// clause that relies on it is not lowered.
    Lost,
}

/// `alias.column`.
pub(crate) fn col_at(alias: &str, column: &str) -> RenderExpr {
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(alias.to_string()),
        column: PropertyValue::Column(column.to_string()),
    })
}

fn eq(a: RenderExpr, b: RenderExpr) -> OperatorApplication {
    OperatorApplication {
        operator: Operator::Equal,
        operands: vec![a, b],
    }
}

fn select(expression: RenderExpr, alias: &str) -> SelectItem {
    SelectItem {
        expression,
        col_alias: Some(ColumnAlias(alias.to_string())),
    }
}

/// What a projection or a SKIP / LIMIT puts in its SELECT.
#[derive(Default)]
struct Body {
    select: Vec<SelectItem>,
    distinct: bool,
    /// Aggregating with grouping items (even if none is left as a GROUP BY
    /// key: a constant, or an element that matches nothing): one row per
    /// group, so no row on an empty input.
    grouped: bool,
    group_by: Vec<RenderExpr>,
    having: Vec<RenderExpr>,
    order_by: Vec<OrderByItem>,
    /// The output rows have an order the SQL does not keep ([`RowOrder::Lost`]).
    order_lost: bool,
    /// DISTINCT also by these unreturned expressions (an element's identity).
    distinct_keys: Vec<RenderExpr>,
    skip: Option<i64>,
    limit: Option<i64>,
}

impl Body {
    /// Add a result column named `name`, or `name_2`, `name_3`, … if an
    /// earlier column has that name (the printer's rule, so it renames
    /// nothing after us). Returns the column's name.
    fn column(&mut self, e: RenderExpr, name: &str) -> String {
        let taken = |n: &str, select: &[SelectItem]| {
            select
                .iter()
                .any(|i| i.col_alias.as_ref().is_some_and(|a| a.0 == n))
        };
        let mut column = name.to_string();
        let mut n = 1;
        while taken(&column, &self.select) {
            n += 1;
            column = format!("{name}_{n}");
        }
        self.select.push(select(e, &column));
        column
    }
}

impl<'s> Lowerer<'s> {
    fn binding(&self, v: VarId) -> &'s Binding {
        &self.bindings[v.0 as usize]
    }

    // ------------------------------------------------------------- relation

    fn relation(&mut self, op: &BoundOp) -> Result<(), LowerError> {
        match op {
            BoundOp::Unit => Ok(()),
            BoundOp::Match {
                input,
                optional,
                pattern,
                predicate,
                introduces,
            } => {
                self.relation(input)?;
                if *optional {
                    self.optional_match(pattern, predicate.as_ref(), introduces)?;
                } else {
                    self.lower_match(pattern, predicate.as_ref())?;
                }
                // Joining other rows to them leaves the rows in no order.
                self.order = RowOrder::Unordered;
                Ok(())
            }
            BoundOp::Project { input, projection } => {
                if projection.kind != ProjectionKind::With {
                    return unsupported("a RETURN that is not the last clause");
                }
                self.relation(input)?;
                self.finish_relation();
                self.with(projection)
            }
            BoundOp::Sort { input, keys } => {
                self.relation(input)?;
                let keys = self.sort_keys(keys, &HashMap::new())?;
                // Sorting by constants keeps the order the rows had.
                if !keys.is_empty() {
                    self.order = RowOrder::Keys(keys);
                }
                Ok(())
            }
            BoundOp::Skip { input, count } => {
                self.relation(input)?;
                self.finish_relation();
                self.page(Some(*count), None)
            }
            BoundOp::Limit { input, count } => {
                self.relation(input)?;
                self.finish_relation();
                self.page(None, Some(*count))
            }
            BoundOp::Unwind { .. } => unsupported("UNWIND (S7)"),
            BoundOp::Union { .. } => unsupported("UNION (S7)"),
        }
    }

    /// A MATCH clause: its elements joined in path order, its property maps
    /// and WHERE (`predicate`) as filters over the join (§4.6).
    fn lower_match(
        &mut self,
        pattern: &BoundPattern,
        predicate: Option<&LogicalExpr>,
    ) -> Result<(), LowerError> {
        // Decide every element's scan first (a relationship's table depends
        // on its endpoints' labels), then join them in path order: node,
        // relationship, node, … so each scan joins on the one before it.
        let mut clause_rels: Vec<VarId> = Vec::new();
        for part in &pattern.parts {
            if part.shortest.is_some() {
                return unsupported("shortestPath (S6b)");
            }
            for n in &part.nodes {
                self.node_scan(n.var)?;
            }
            for (i, r) in part.rels.iter().enumerate() {
                self.rel_scan(r, part.nodes[i].var, part.nodes[i + 1].var)?;
                clause_rels.push(r.var);
            }
            if let Some(p) = part.path_var {
                let elements = PathElements {
                    nodes: part.nodes.iter().map(|n| n.var).collect(),
                    rels: part.rels.iter().map(|r| r.var).collect(),
                };
                self.paths.insert(p, elements);
            }
        }
        // The clause's conditions: its property maps (a variable-length
        // relationship's holds of each of its relationships, inside the
        // search), then its WHERE.
        let mut conditions = Vec::new();
        for part in &pattern.parts {
            for n in &part.nodes {
                self.written(n.var, &n.labels);
                conditions.extend(self.props(n.var, &n.props)?);
            }
            for r in &part.rels {
                self.written(r.var, &r.types);
                if r.length.is_none() {
                    conditions.extend(self.props(r.var, &r.props)?);
                }
            }
        }
        let filter = predicate
            .map(|p| self.expr(p, &HashMap::new()))
            .transpose()?;
        // Two paths of one table whose relationships are identified by their
        // endpoints must spell them alike (their uniqueness compares them):
        // those are walked forward.
        let mut by_table: HashMap<String, usize> = HashMap::new();
        for v in &clause_rels {
            if let Some(Scan::Path { schema, .. }) = self.scans.get(v) {
                if schema.edge_id.is_none() {
                    *by_table.entry(schema.full_table_name()).or_default() += 1;
                }
            }
        }
        let own: Vec<RenderExpr> = conditions.iter().chain(&filter).cloned().collect();
        for part in &pattern.parts {
            self.emit(part.nodes[0].var)?;
            for (i, r) in part.rels.iter().enumerate() {
                if let Some(Scan::Path { schema, .. }) = self.scans.get(&r.var) {
                    let forward_only = by_table
                        .get(&schema.full_table_name())
                        .is_some_and(|n| *n > 1);
                    let (left, right) = (part.nodes[i].var, part.nodes[i + 1].var);
                    self.build_path(r, left, right, forward_only, &own)?;
                }
                self.emit(r.var)?;
                self.emit(part.nodes[i + 1].var)?;
            }
        }
        self.filters.extend(conditions);
        // A variable an OPTIONAL MATCH left NULL matches nothing here. A tie
        // to it already fails; a node with no relationship in the pattern
        // (`MATCH (b)`) has none.
        let bound: Vec<VarId> = pattern
            .parts
            .iter()
            .flat_map(|p| {
                let nodes = p.nodes.iter().filter(|n| n.bound_before).map(|n| n.var);
                nodes.chain(p.rels.iter().filter(|r| r.bound_before).map(|r| r.var))
            })
            .collect();
        let mut checked = Vec::new();
        for v in bound {
            if !self.binding(v).nullable || checked.contains(&v) {
                continue;
            }
            checked.push(v);
            if let Some(id) = self.identity(v)? {
                self.filters
                    .push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::IsNotNull,
                        operands: vec![id[0].clone()],
                    }));
            }
        }
        self.uniqueness(&clause_rels)?;
        self.filters.extend(filter);
        Ok(())
    }

    /// OPTIONAL MATCH as one unit (§4.9): the rows so far, `I`, LEFT JOIN
    /// the clause's matches `Q` on the correlation `C` (the input variables
    /// the pattern shares, then those only its WHERE / property maps read).
    /// `Q` is a MATCH with its WHERE inside, so the WHERE decides which
    /// matches there are and never removes an input row; an input row with
    /// no match keeps NULL for every variable the clause introduces.
    ///
    /// * When `C` is only nodes of the pattern, `Q` reads them from their own
    ///   tables (each node once), so `Q` is the pattern's matches in the
    ///   whole graph and the join picks each input row's own. A shared node
    ///   is in `I`, so it exists; `Q` needs no restriction to `I`.
    /// * Otherwise (a shared relationship, or a variable only the WHERE
    ///   reads) `Q` reads `C` from a **drive** `D = SELECT DISTINCT C FROM
    ///   I`, so `I` becomes a CTE. A variable only the WHERE reads can be
    ///   NULL and still decide the WHERE (`x IS NULL`), so it joins
    ///   NULL-safely.
    ///
    /// ClickHouse fills an unmatched row's `Array` / `Map` / `Tuple` columns
    /// with defaults even under `join_use_nulls = 1` (§4.15). `Q` exports
    /// element identities, endpoints and properties only, so the one gap is
    /// a list-typed property of an unmatched element, which reads `[]`.
    fn optional_match(
        &mut self,
        pattern: &BoundPattern,
        predicate: Option<&LogicalExpr>,
        introduces: &[VarId],
    ) -> Result<(), LowerError> {
        for part in &pattern.parts {
            if part.shortest.is_some() {
                return unsupported("shortestPath (S6b)");
            }
        }
        // Rows joined to other rows are in no order.
        self.order = RowOrder::Unordered;
        // Input variables the clause reads: shared by the pattern, or read by
        // its WHERE / property maps only.
        let mut shared: Vec<VarId> = Vec::new();
        for part in &pattern.parts {
            let nodes = part.nodes.iter().filter(|n| n.bound_before).map(|n| n.var);
            let rels = part.rels.iter().filter(|r| r.bound_before).map(|r| r.var);
            for v in nodes.chain(rels) {
                if !shared.contains(&v) {
                    shared.push(v);
                }
            }
        }
        let mut read: Vec<&LogicalExpr> = predicate.into_iter().collect();
        for part in &pattern.parts {
            read.extend(
                part.nodes
                    .iter()
                    .flat_map(|n| n.props.iter().map(|(_, e)| e)),
            );
            read.extend(
                part.rels
                    .iter()
                    .flat_map(|r| r.props.iter().map(|(_, e)| e)),
            );
        }
        let mut where_only: Vec<VarId> = Vec::new();
        // Read by the clause, NULL on every input row (an element that
        // matches nothing): a constant NULL in `Q` too, no column.
        let mut null_inputs: Vec<VarId> = Vec::new();
        for e in &read {
            for v in referenced_names(e, false)
                .iter()
                .filter_map(|n| parse_var(n))
            {
                if shared.contains(&v) || where_only.contains(&v) {
                    continue;
                }
                if matches!(self.scans.get(&v), Some(Scan::Impossible)) {
                    null_inputs.push(v);
                    continue;
                }
                // A constant value is read as it is, not through a column.
                let input = self.scans.contains_key(&v)
                    || self.values.get(&v).is_some_and(|e| !is_constant(e));
                if input {
                    where_only.push(v);
                }
            }
        }
        // The introduced variables later clauses can read: the named ones,
        // and the elements of a named path (its length reads them).
        let mut named: Vec<VarId> = introduces
            .iter()
            .copied()
            .filter(|v| {
                let b = self.binding(*v);
                b.name.is_some() && !matches!(b.kind, BindingKind::Path)
            })
            .collect();
        for part in pattern.parts.iter().filter(|p| p.path_var.is_some()) {
            let elements = part.nodes.iter().map(|n| n.var);
            for v in elements.chain(part.rels.iter().map(|r| r.var)) {
                if introduces.contains(&v) && !named.contains(&v) {
                    named.push(v);
                }
            }
        }
        // A shared element that matches nothing (NULL on every row) is in no
        // match. (One only the WHERE reads is NULL there, and `IS NULL` can
        // still hold.)
        if shared
            .iter()
            .any(|v| matches!(self.scans.get(v), Some(Scan::Impossible)))
        {
            for v in named {
                self.scans.insert(v, Scan::Impossible);
            }
            return Ok(());
        }
        // Without the drive, `Q` holds the pattern's matches in the whole
        // graph. That is bounded by one edge table for a single
        // relationship; with more it can be far larger than the result
        // (every two-hop path), unless the input's WHERE restricts a shared
        // node (copied into `Q`, `anchor_correlation`).
        // A variable-length relationship counts as several.
        let hops: usize = pattern
            .parts
            .iter()
            .flat_map(|p| &p.rels)
            .map(|r| if r.length.is_some() { 2 } else { 1 })
            .sum();
        let anchor_aliases: Vec<String> = shared
            .iter()
            .filter(|v| matches!(self.scans.get(*v).and_then(Scan::at), Some(At::Table(a)) if *a == v.name()))
            .map(|v| v.name())
            .collect();
        let restricted = self
            .filters
            .iter()
            .flat_map(conjuncts)
            .any(|c| reads_only(c, &anchor_aliases) && !table_aliases(c).is_empty());
        let anchored = where_only.is_empty()
            && shared
                .iter()
                .all(|v| matches!(self.scans.get(v), Some(Scan::Node { .. })))
            && (hops <= 1 || shared.is_empty() || restricted);
        let mut q = Segment {
            // Constants are read as they are, in `Q` too.
            values: self
                .values
                .iter()
                .filter(|(_, e)| is_constant(e))
                .map(|(v, e)| (*v, e.clone()))
                .collect(),
            ..Segment::default()
        };
        // Properties the clause reads of its variables (its pattern's own
        // property maps included).
        let mut read_props: Vec<(VarId, String)> = read
            .iter()
            .flat_map(|e| property_refs(e))
            .filter_map(|(n, p)| Some((parse_var(&n)?, p)))
            .collect();
        for part in &pattern.parts {
            for n in &part.nodes {
                read_props.extend(n.props.iter().map(|(p, _)| (n.var, p.clone())));
            }
        }
        for v in &null_inputs {
            q.scans.insert(*v, Scan::Impossible);
        }
        let correlation = if anchored {
            self.anchor_correlation(pattern, &shared, &read_props, &mut q)?
        } else {
            self.drive_correlation(&shared, &where_only, &read_props, &mut q)?
        };
        let n = self.ctes.len() + 1;
        let (q_alias, q_name) = (format!("o{n}"), format!("optional_o{n}"));
        let ctes_before_q = self.ctes.len();
        let outer = self.swap_segment(q);
        let inner = self.optional_inner(pattern, predicate, &correlation, &named, &q_alias);
        self.swap_segment(outer);
        let Some((q_plan, exports)) = inner? else {
            // The pattern can match nothing: every input row keeps NULLs.
            // `Q`'s paths and `D` are unused.
            self.ctes.truncate(ctes_before_q);
            if !anchored {
                self.ctes.pop(); // `D`
            }
            for v in named {
                self.scans.insert(v, Scan::Impossible);
            }
            return Ok(());
        };
        self.ctes.push(Cte::new(
            q_name.clone(),
            CteContent::Structured(Box::new(q_plan)),
            false,
        ));
        if self.from.is_none() {
            // No input relation (`OPTIONAL MATCH` first): one empty row.
            let u = self.next_cte_alias();
            let mut unit = empty_plan();
            unit.select.items = vec![select(RenderExpr::Literal(Literal::Integer(1)), "__row")];
            self.ctes.push(Cte::new(
                format!("with_{u}"),
                CteContent::Structured(Box::new(unit)),
                false,
            ));
            self.from = Some(table_ref(format!("with_{u}"), &u));
            self.emitted.insert(0, u);
        }
        let mut on: Vec<OperatorApplication> = correlation
            .into_iter()
            .map(|c| {
                let inner = col_at(&q_alias, &c.column);
                if c.null_safe {
                    not_distinct(c.outer, inner)
                } else {
                    eq(c.outer, inner)
                }
            })
            .collect();
        if on.is_empty() {
            // Nothing to correlate: every input row with every match.
            on.push(eq(
                RenderExpr::Literal(Literal::Integer(1)),
                RenderExpr::Literal(Literal::Integer(1)),
            ));
        }
        let mut j = join(q_name, &q_alias);
        j.join_type = JoinType::Left;
        j.joining_on = on;
        self.joins.push(j);
        self.emitted.push(q_alias);
        for (v, scan) in exports {
            self.scans.insert(v, scan);
        }
        Ok(())
    }

    /// `C` of only shared nodes: `Q` reads each from its own table, or from
    /// the endpoint column of a relationship of the pattern when that is all
    /// `Q` reads of it. A shared node is in the input, so it exists; a match
    /// whose endpoint is no input node joins no row.
    fn anchor_correlation(
        &self,
        pattern: &BoundPattern,
        shared: &[VarId],
        read_props: &[(VarId, String)],
        q: &mut Segment<'s>,
    ) -> Result<Vec<Correlated>, LowerError> {
        // A conjunct of the input's WHERE over the shared nodes' own columns
        // restricts `Q` too: a match it removes has a shared node no input
        // row has, so it joins no row. A shared node read from its table
        // here has the same alias in `Q` (a CTE-carried one does not).
        let aliases: Vec<String> = shared
            .iter()
            .filter(|v| matches!(self.scans[*v].at(), Some(At::Table(a)) if *a == v.name()))
            .map(|v| v.name())
            .collect();
        let mut restricted: Vec<String> = Vec::new();
        for f in &self.filters {
            for c in conjuncts(f) {
                if reads_only(c, &aliases) {
                    q.filters.push(c.clone());
                    restricted.extend(table_aliases(c));
                }
            }
        }
        let mut correlation = Vec::new();
        for v in shared {
            let Some(Scan::Node { schema, label, .. }) = self.scans.get(v).cloned() else {
                return unsupported(format!("internal: {v} is not a node"));
            };
            let columns = self.identity_physical(*v).unwrap_or_default();
            let outer = self.identity(*v)?.unwrap_or_default();
            let endpoint =
                if restricted.contains(&v.name()) || read_props.iter().any(|(r, _)| r == v) {
                    None
                } else {
                    self.endpoint_of(pattern, *v)
                };
            let (at, inner): (At, Vec<RenderExpr>) = match endpoint {
                Some((r, cols)) if cols.len() == columns.len() => {
                    q.elided.insert(*v, r);
                    let alias = r.name();
                    let inner = cols.iter().map(|c| col_at(&alias, c)).collect();
                    let physical = columns.iter().cloned().zip(cols).collect();
                    let props = HashMap::new();
                    (
                        At::Exported {
                            alias,
                            physical,
                            props,
                        },
                        inner,
                    )
                }
                _ => {
                    let inner = columns.iter().map(|c| col_at(&v.name(), c)).collect();
                    (At::Table(v.name()), inner)
                }
            };
            for (c, (outer, inner)) in columns.iter().zip(outer.into_iter().zip(inner)) {
                correlation.push(Correlated {
                    column: format!("{v}__{c}"),
                    outer,
                    inner,
                    null_safe: false,
                });
            }
            q.scans.insert(*v, Scan::Node { schema, label, at });
        }
        Ok(correlation)
    }

    /// The first relationship of `pattern` at node `v`, and its endpoint
    /// columns at `v` (stored orientation), when the relationship is a
    /// fixed-length scan of one table.
    fn endpoint_of(&self, pattern: &BoundPattern, v: VarId) -> Option<(VarId, Vec<String>)> {
        let single = |n: VarId| match &self.binding(n).kind {
            BindingKind::Node { labels } if labels.len() == 1 => labels.iter().next().cloned(),
            _ => None,
        };
        for part in &pattern.parts {
            for (i, r) in part.rels.iter().enumerate() {
                let (left, right) = (part.nodes[i].var, part.nodes[i + 1].var);
                if v != left && v != right {
                    continue;
                }
                let (from, to) = match r.direction {
                    RelDirection::Right => (left, right),
                    RelDirection::Left => (right, left),
                    RelDirection::Either => return None,
                };
                let types = match &self.binding(r.var).kind {
                    BindingKind::Rel { types, .. } if types.len() == 1 => types,
                    _ => return None,
                };
                if r.length.is_some() || r.bound_before || from == to {
                    return None;
                }
                let (Some(fl), Some(tl)) = (single(from), single(to)) else {
                    return None;
                };
                let rel_type = types.iter().next().expect("one type");
                // No such edge: the pattern matches nothing (decided later).
                let Ok(rs) = self.edge_schema(rel_type, &fl, &tl) else {
                    return None;
                };
                let id = if v == from { &rs.from_id } else { &rs.to_id };
                let cols = id.columns().iter().map(|c| c.to_string()).collect();
                return Some((r.var, cols));
            }
        }
        None
    }

    /// `C` with a relationship or a variable only the WHERE reads: the rows
    /// so far become a CTE, `I`, and `Q` reads `C` from the CTE `D` of its
    /// distinct columns: identities, endpoints, and the properties the clause
    /// reads (`read_props`). Each column keeps its name from `I`.
    fn drive_correlation(
        &mut self,
        shared: &[VarId],
        where_only: &[VarId],
        read_props: &[(VarId, String)],
        q: &mut Segment<'s>,
    ) -> Result<Vec<Correlated>, LowerError> {
        self.finish_relation();
        // A segment that is only its CTE (right after a WITH) is that CTE.
        let only_a_cte = self.joins.is_empty()
            && self.filters.is_empty()
            && !self.empty
            && self.emitted.len() == 1
            && self
                .from
                .as_ref()
                .is_some_and(|f| f.name == format!("with_{}", self.emitted[0]));
        if !only_a_cte {
            self.page(None, None)?;
        }
        let w = self.emitted[0].clone();
        let d = format!("d{}", self.ctes.len() + 1);
        let mut columns: Vec<String> = Vec::new();
        let mut correlation = Vec::new();
        for v in shared.iter().chain(where_only) {
            // Shared: NULL never matches (a tie to it fails). Read by the
            // WHERE only: NULL is a value the WHERE decides on.
            let null_safe = where_only.contains(v) && self.binding(*v).nullable;
            if let Some(e) = self.values.get(v) {
                let column = column_of(e, &w)?;
                q.values.insert(*v, col_at(&d, &column));
                correlation.push(Correlated {
                    column: column.clone(),
                    outer: e.clone(),
                    inner: col_at(&d, &column),
                    // A value's nullability is not tracked.
                    null_safe: where_only.contains(v),
                });
                columns.push(column);
                continue;
            }
            let Some(scan) = self.scans.get(v).cloned() else {
                return unsupported(format!("internal: {v} is not in scope"));
            };
            let Some(At::Exported {
                physical, props, ..
            }) = scan.at().cloned()
            else {
                return unsupported(format!("internal: {v} is not exported"));
            };
            columns.extend(physical.values().cloned());
            let mut d_props = HashMap::new();
            for (prop, e) in props {
                if !read_props.iter().any(|(r, p)| r == v && *p == prop) {
                    continue;
                }
                let e = if is_constant(&e) {
                    e
                } else {
                    let column = column_of(&e, &w)?;
                    columns.push(column.clone());
                    col_at(&d, &column)
                };
                d_props.insert(prop, e);
            }
            // Joined on the identity. A node's or an `edge_id`
            // relationship's other columns depend on it; a relationship
            // identified by its endpoints can have parallel edges with other
            // properties, so it also joins on those `D` holds (NULL-safely).
            if matches!(&scan, Scan::Rel { schema, .. } if schema.edge_id.is_none()) {
                let mut props: Vec<&RenderExpr> =
                    d_props.values().filter(|e| !is_constant(e)).collect();
                props.sort_by_key(|e| column_of(e, &d).unwrap_or_default());
                for e in props {
                    let column = column_of(e, &d)?;
                    correlation.push(Correlated {
                        column: column.clone(),
                        outer: col_at(&w, &column),
                        inner: e.clone(),
                        null_safe: true,
                    });
                }
            }
            for c in self.identity_physical(*v).unwrap_or_default() {
                let column = physical[&c].clone();
                correlation.push(Correlated {
                    column: column.clone(),
                    outer: col_at(&w, &column),
                    inner: col_at(&d, &column),
                    null_safe,
                });
            }
            let at = At::Exported {
                alias: d.clone(),
                physical,
                props: d_props,
            };
            q.scans.insert(*v, scan.with_at(at));
        }
        columns.sort();
        columns.dedup();
        let mut plan = empty_plan();
        plan.select = SelectItems {
            items: columns.iter().map(|c| select(col_at(&w, c), c)).collect(),
            distinct: true,
        };
        if plan.select.items.is_empty() {
            plan.select.items = vec![select(RenderExpr::Literal(Literal::Integer(1)), "__row")];
        }
        plan.from = FromTableItem(Some(table_ref(format!("with_{w}"), &w)));
        let name = format!("optional_{d}");
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        q.from = Some(table_ref(name, &d));
        q.emitted = vec![d];
        Ok(correlation)
    }

    /// `Q` in the current (swapped-in) segment: the pattern, its WHERE, and
    /// a SELECT of the correlation columns and the named introduced elements
    /// (read from `Q` aliased `q_alias`). `None` when it matches nothing.
    #[allow(clippy::type_complexity)]
    fn optional_inner(
        &mut self,
        pattern: &BoundPattern,
        predicate: Option<&LogicalExpr>,
        correlation: &[Correlated],
        named: &[VarId],
        q_alias: &str,
    ) -> Result<Option<(RenderPlan, Vec<(VarId, Scan<'s>)>)>, LowerError> {
        self.lower_match(pattern, predicate)?;
        self.finish_relation();
        if self.empty {
            return Ok(None);
        }
        let mut body = Body::default();
        for c in correlation {
            body.select.push(select(c.inner.clone(), &c.column));
        }
        let mut exports = Vec::new();
        for v in named {
            let scan = self.export_element(*v, *v, q_alias, &mut body.select, &mut Vec::new())?;
            exports.push((*v, scan));
        }
        if body.select.is_empty() {
            body.select
                .push(select(RenderExpr::Literal(Literal::Integer(1)), "__row"));
        }
        Ok(Some((self.render(body), exports)))
    }

    /// Replace the current segment, returning it.
    fn swap_segment(&mut self, mut s: Segment<'s>) -> Segment<'s> {
        std::mem::swap(&mut self.scans, &mut s.scans);
        std::mem::swap(&mut self.values, &mut s.values);
        std::mem::swap(&mut self.emitted, &mut s.emitted);
        std::mem::swap(&mut self.from, &mut s.from);
        std::mem::swap(&mut self.joins, &mut s.joins);
        std::mem::swap(&mut self.pending, &mut s.pending);
        std::mem::swap(&mut self.filters, &mut s.filters);
        std::mem::swap(&mut self.empty, &mut s.empty);
        std::mem::swap(&mut self.order, &mut s.order);
        std::mem::swap(&mut self.elided, &mut s.elided);
        s
    }

    /// Decide how a node is read (once per variable).
    fn node_scan(&mut self, v: VarId) -> Result<(), LowerError> {
        if self.scans.contains_key(&v) {
            return Ok(());
        }
        let BindingKind::Node { labels } = &self.binding(v).kind else {
            return unsupported("a pattern node that is not a node binding");
        };
        let scan = match labels.len() {
            0 => Scan::Impossible,
            1 => {
                let label = labels.iter().next().expect("one label").clone();
                let Some(ns) = self.schema.node_schema_opt(&label) else {
                    return unsupported(format!("label {label} has no node schema"));
                };
                if !ns.is_standard_own_table() {
                    return unsupported(format!("label {label} is not the standard layout (S8)"));
                }
                Scan::Node {
                    schema: ns,
                    label,
                    at: At::Table(v.name()),
                }
            }
            _ => return unsupported("a node with several possible labels (S7)"),
        };
        self.scans.insert(v, scan);
        Ok(())
    }

    /// Decide how a relationship is read (once per variable) and tie it to
    /// its endpoints in the stored orientation.
    fn rel_scan(&mut self, r: &PatRel, left: VarId, right: VarId) -> Result<(), LowerError> {
        let (from, to) = match r.direction {
            RelDirection::Right => (left, right),
            RelDirection::Left => (right, left),
            RelDirection::Either => return unsupported("an undirected relationship (S7)"),
        };
        if let Some((min, max)) = r.length {
            return self.path_scan(r, from, to, min, max);
        }
        if !self.scans.contains_key(&r.var) {
            let BindingKind::Rel { types, .. } = &self.binding(r.var).kind else {
                return unsupported("a pattern relationship that is not a relationship binding");
            };
            let scan = match (types.len(), self.single_label(from), self.single_label(to)) {
                (1, Some(fl), Some(tl)) => {
                    let rel_type = types.iter().next().expect("one type").clone();
                    Scan::Rel {
                        schema: self.edge_schema(&rel_type, &fl, &tl)?,
                        rel_type,
                        at: At::Table(r.var.name()),
                    }
                }
                // No feasible type, or an endpoint that matches nothing.
                (0, ..) | (1, ..) => Scan::Impossible,
                _ => return unsupported("a relationship with several possible types (S7)"),
            };
            self.scans.insert(r.var, scan);
        }
        if let Scan::Rel { schema: rs, .. } = self.scans[&r.var].clone() {
            for (end, cols) in [(from, rs.from_id.columns()), (to, rs.to_id.columns())] {
                let Some(ids) = self.identity(end)? else {
                    continue; // an endpoint that matches nothing
                };
                if ids.len() != cols.len() {
                    return unsupported("endpoint id arity differs from the edge's");
                }
                let mut eqs = Vec::new();
                for (c, id) in cols.iter().zip(ids) {
                    eqs.push((self.physical(r.var, c)?, id));
                }
                self.tie(r.var, end, eqs);
            }
        }
        Ok(())
    }

    /// Decide how a variable-length relationship is read: a relation of
    /// paths (`path.rs`), generated by [`Self::build_path`] once the clause's
    /// conditions are known, and tied to its endpoints in the stored
    /// orientation (`start_id` = the `from` node, `end_id` = the `to` node).
    fn path_scan(
        &mut self,
        r: &PatRel,
        from: VarId,
        to: VarId,
        min: u32,
        max: Option<u32>,
    ) -> Result<(), LowerError> {
        if r.bound_before {
            return unsupported("re-matching a variable-length relationship list");
        }
        let BindingKind::Rel { types, .. } = &self.binding(r.var).kind else {
            return unsupported("a pattern relationship that is not a relationship binding");
        };
        let scan = match (types.len(), self.single_label(from), self.single_label(to)) {
            _ if max.is_some_and(|m| m < min) => Scan::Impossible,
            (1, Some(fl), Some(tl)) => {
                let rel_type = types.iter().next().expect("one type").clone();
                let rs = self.edge_schema(&rel_type, &fl, &tl)?;
                // Every node of the path has the edge's one label.
                if fl != tl || rs.from_node != rs.to_node {
                    return unsupported(
                        "a variable-length relationship between nodes of different labels",
                    );
                }
                let ns = self.schema.node_schema_opt(&fl).expect("a scanned label");
                let plain = |filter: bool, params: bool, fin: bool| !filter && !params && !fin;
                if !plain(
                    rs.filter.is_some(),
                    rs.view_parameters.is_some(),
                    rs.should_use_final(),
                ) || !plain(
                    ns.filter.is_some(),
                    ns.view_parameters.is_some(),
                    ns.should_use_final(),
                ) {
                    return unsupported(
                        "a variable-length relationship over a table with a filter, view \
                         parameters or FINAL (S8)",
                    );
                }
                Scan::Path {
                    schema: rs,
                    rel_type,
                    cte: String::new(),
                    at: At::Table(r.var.name()),
                    edges: false,
                    backward: false,
                }
            }
            // A path of no relationship needs none: `*0..` with no feasible
            // type still matches each node to itself.
            (0, ..) if min == 0 => {
                return unsupported("a zero-length path with no feasible relationship type")
            }
            // No feasible type, or an endpoint that matches nothing.
            (0, ..) | (1, ..) => Scan::Impossible,
            _ => return unsupported("a relationship with several possible types (S7)"),
        };
        if let (Scan::Path { .. }, Some(ids)) = (&scan, self.identity(from)?) {
            if ids.len() != 1 {
                return unsupported("a variable-length relationship between composite ids (S8)");
            }
        }
        self.scans.insert(r.var, scan);
        Ok(())
    }

    /// Generate the relation of paths of a variable-length relationship
    /// (decided by [`Self::path_scan`]) when it joins the rows so far (its
    /// `left` node is joined), and tie it to its endpoints: `start_id` is
    /// the node the walk starts at, `end_id` the other. The walk starts at a
    /// restricted end (§4.8 d), the left one when both or neither are, and
    /// follows the relationships forward or backward from there
    /// (`forward_only`: from their `from` end). Inside the search go:
    /// * the first node's values in the rows so far (a semi-join), when they
    ///   are more than its table's rows;
    /// * conjuncts that read only the first node's own columns, from the
    ///   segment's filters and the clause's own conditions (`own`);
    /// * the relationship's property map, which every relationship of the
    ///   path satisfies.
    ///
    /// Each holds of the result's rows, and stays where it is: pushing is
    /// never needed for correctness.
    fn build_path(
        &mut self,
        r: &PatRel,
        left: VarId,
        right: VarId,
        forward_only: bool,
        own: &[RenderExpr],
    ) -> Result<(), LowerError> {
        let Some(Scan::Path {
            schema: edge,
            rel_type,
            at,
            ..
        }) = self.scans.get(&r.var).cloned()
        else {
            return Ok(()); // it matches nothing
        };
        let (min, max) = r.length.expect("a variable-length relationship");
        // The `from` end of the relationships (stored orientation).
        let from_end = if r.direction == RelDirection::Left {
            right
        } else {
            left
        };
        let first = if forward_only {
            from_end
        } else if !self.restricted(left, own) && self.restricted(right, own) {
            right
        } else {
            left
        };
        let last = if first == left { right } else { left };
        let backward = first != from_end;
        let Some(Scan::Node {
            schema: node,
            label,
            at: first_at,
        }) = self.scans.get(&first).cloned()
        else {
            return unsupported("internal: a path endpoint is not a node");
        };
        let mut start = Vec::new();
        if let At::Table(alias) = &first_at {
            for c in self.own_conjuncts(first, own) {
                start.push(realias(c, alias, path::START));
            }
        }
        if self.joined_rows_restrict(first) {
            let Some(id) = self.identity(first)? else {
                return Ok(());
            };
            let mut rows = empty_plan();
            rows.select = SelectItems {
                items: vec![select(id[0].clone(), "id")],
                distinct: true,
            };
            rows.from = FromTableItem(self.from.clone());
            rows.joins = JoinItems(self.joins.clone());
            let held: Vec<RenderExpr> = self
                .filters
                .iter()
                .chain(own)
                .flat_map(conjuncts)
                .filter(|c| reads_only(c, &self.emitted))
                .cloned()
                .collect();
            rows.filters = FilterItems(and_all(held));
            let id_column = node.id_physical_columns();
            start.push(RenderExpr::Raw(format!(
                "{} IN ({})",
                render_expr_to_sql_plain(&col_at(path::START, &id_column[0])),
                crate::sql_generator::emitters::clickhouse::to_sql_query::render_plan_to_sql_plain(
                    rows
                )
                .trim_end()
            )));
        }
        let mut rel = Vec::new();
        for (prop, value) in &r.props {
            let column = match edge.property_mappings.get(prop) {
                Some(pv) => RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(path::REL.to_string()),
                    column: pv.clone(),
                }),
                None if edge.closed_properties || self.options.neo4j_compat => {
                    RenderExpr::Literal(Literal::Null)
                }
                None => col_at(path::REL, prop),
            };
            let value = self.expr(value, &HashMap::new())?;
            if !is_constant(&value) {
                return unsupported(
                    "a variable-length relationship's property map reading a variable",
                );
            }
            rel.push(RenderExpr::OperatorApplicationExp(eq(column, value)));
        }
        let built = path::path_cte(
            self.schema,
            path::PathCall {
                var: &r.var.name(),
                rel_type: &rel_type,
                edge,
                label: &label,
                node,
                min,
                max,
                backward,
                start,
                rel,
            },
        )?;
        let cte = built.cte.cte_name.clone();
        self.ctes.push(built.cte);
        self.scans.insert(
            r.var,
            Scan::Path {
                schema: edge,
                rel_type,
                cte,
                at,
                edges: built.edges,
                backward,
            },
        );
        for (end, column) in [(first, "start_id"), (last, "end_id")] {
            let Some(ids) = self.identity(end)? else {
                continue; // an endpoint that matches nothing
            };
            self.tie(
                r.var,
                end,
                vec![(col_at(&r.var.name(), column), ids[0].clone())],
            );
        }
        Ok(())
    }

    /// Conjuncts of the segment's filters and `own` that read only node
    /// `v`'s own table (it is read from its table): they restrict `v`.
    fn own_conjuncts<'e>(&'e self, v: VarId, own: &'e [RenderExpr]) -> Vec<&'e RenderExpr> {
        let Some(Scan::Node {
            at: At::Table(alias),
            ..
        }) = self.scans.get(&v)
        else {
            return Vec::new();
        };
        let aliases = [alias.clone()];
        self.filters
            .iter()
            .chain(own)
            .flat_map(conjuncts)
            .filter(|c| reads_only(c, &aliases) && !table_aliases(c).is_empty())
            .collect()
    }

    /// The rows joined so far hold node `v` and are more than its table's
    /// rows (a join, or a CTE): they restrict `v`.
    fn joined_rows_restrict(&self, v: VarId) -> bool {
        let own_table_only = matches!(
            self.scans.get(&v).and_then(Scan::at),
            Some(At::Table(a)) if self.emitted.len() == 1 && self.emitted[0] == *a
        );
        self.is_emitted(v) && !own_table_only && !self.empty
    }

    /// Something restricts node `v` (a path walked from it starts at fewer
    /// nodes than its table holds).
    fn restricted(&self, v: VarId, own: &[RenderExpr]) -> bool {
        !self.own_conjuncts(v, own).is_empty() || self.joined_rows_restrict(v)
    }

    /// The edge definition of `(from_label)-[:rel_type]->(to_label)`, when
    /// it is lowered (the standard layout).
    fn edge_schema(
        &self,
        rel_type: &str,
        from_label: &str,
        to_label: &str,
    ) -> Result<&'s RelationshipSchema, LowerError> {
        let Ok(rs) =
            self.schema
                .get_rel_schema_with_nodes(rel_type, Some(from_label), Some(to_label))
        else {
            return unsupported(format!(
                "no schema for ({from_label})-[:{rel_type}]->({to_label})"
            ));
        };
        if !rs.is_standard_edge_table() {
            return unsupported(format!("type {rel_type} is not the standard layout (S8)"));
        }
        if rs.constraints.is_some() {
            return unsupported("an edge `constraints:` expression (S8)");
        }
        Ok(rs)
    }

    /// Written labels / types are alternatives the element must have. Labels
    /// are static (one per scan), so this is decided here: a variable from an
    /// earlier clause written with a label it does not have (`MATCH (a:User)
    /// MATCH (a:Post)`) makes the clause match nothing.
    fn written(&mut self, v: VarId, written: &[String]) {
        if written.is_empty() {
            return;
        }
        let holds = match &self.scans[&v] {
            Scan::Node { label, .. } => written.contains(label),
            Scan::Rel { rel_type, .. } | Scan::Path { rel_type, .. } => written.contains(rel_type),
            Scan::Impossible => false,
        };
        if !holds {
            self.empty = true;
        }
    }

    /// Inline property maps: `v.prop = value`.
    fn props(
        &self,
        v: VarId,
        props: &[(String, LogicalExpr)],
    ) -> Result<Vec<RenderExpr>, LowerError> {
        props
            .iter()
            .map(|(prop, value)| {
                let lhs = self.property(v, prop)?;
                let rhs = self.expr(value, &HashMap::new())?;
                Ok(RenderExpr::OperatorApplicationExp(eq(lhs, rhs)))
            })
            .collect()
    }

    /// The node's label when it has exactly one (its scan is a table).
    fn single_label(&self, v: VarId) -> Option<String> {
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Some(label.clone()),
            _ => None,
        }
    }

    /// The physical identity columns of a node, or of a relationship: its
    /// `edge_id`, else its stored (from, to) endpoint columns (#887 policy).
    fn identity_physical(&self, v: VarId) -> Option<Vec<String>> {
        match self.scans.get(&v)? {
            Scan::Node { schema, .. } => Some(schema.id_physical_columns()),
            Scan::Rel { schema, .. } => Some(match &schema.edge_id {
                Some(id) => id.columns().iter().map(|c| c.to_string()).collect(),
                None => schema
                    .from_id
                    .columns()
                    .iter()
                    .chain(schema.to_id.columns().iter())
                    .map(|c| c.to_string())
                    .collect(),
            }),
            Scan::Path { .. } => Some(path::PATH_COLUMNS.iter().map(|c| c.to_string()).collect()),
            Scan::Impossible => None,
        }
    }

    /// An element's identity, one expression per identity column; `None` for
    /// an element that matches nothing.
    fn identity(&self, v: VarId) -> Result<Option<Vec<RenderExpr>>, LowerError> {
        let Some(cols) = self.identity_physical(v) else {
            return Ok(None);
        };
        cols.iter()
            .map(|c| self.physical(v, c))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    /// A physical column of an element's table, wherever the element is read.
    fn physical(&self, v: VarId, column: &str) -> Result<RenderExpr, LowerError> {
        match self.scans.get(&v).and_then(Scan::at) {
            Some(At::Table(alias)) => Ok(col_at(alias, column)),
            Some(At::Exported {
                alias, physical, ..
            }) => match physical.get(column) {
                Some(exported) => Ok(col_at(alias, exported)),
                None => unsupported(format!("internal: column {column} of {v} is not exported")),
            },
            None => unsupported(format!("internal: {v} has no columns")),
        }
    }

    /// Relationships of one MATCH are pairwise distinct (§4.6.3). Only scans
    /// of the same edge table can share a relationship: two relationships
    /// differ in identity, a relationship is not on a path, two paths share
    /// none. (Within a path the recursive CTE keeps them distinct.)
    fn uniqueness(&mut self, rels: &[VarId]) -> Result<(), LowerError> {
        for (i, a) in rels.iter().enumerate() {
            for b in &rels[i + 1..] {
                let table = |v: &VarId| match self.scans.get(v) {
                    Some(Scan::Rel { schema, .. }) => Some(schema.full_table_name()),
                    Some(Scan::Path {
                        schema,
                        edges: true,
                        ..
                    }) => Some(schema.full_table_name()),
                    _ => None,
                };
                let (Some(ta), Some(tb)) = (table(a), table(b)) else {
                    continue;
                };
                if ta != tb {
                    continue;
                }
                let differs = match (&self.scans[a], &self.scans[b]) {
                    (Scan::Rel { .. }, Scan::Rel { .. }) => {
                        let ia = self.identity(*a)?.expect("a relationship scan");
                        let ib = self.identity(*b)?.expect("a relationship scan");
                        or_all(
                            ia.into_iter()
                                .zip(ib)
                                .map(|(x, y)| {
                                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                                        operator: Operator::NotEqual,
                                        operands: vec![x, y],
                                    })
                                })
                                .collect(),
                        )
                    }
                    (
                        Scan::Rel { schema, at, .. },
                        Scan::Path {
                            at: p, backward, ..
                        },
                    )
                    | (
                        Scan::Path {
                            at: p, backward, ..
                        },
                        Scan::Rel { schema, at, .. },
                    ) => {
                        let At::Table(edge) = at else {
                            return unsupported(
                                "a relationship from an earlier clause and a variable-length \
                                 relationship of the same table in one MATCH",
                            );
                        };
                        let Some(identity) = path::edge_identity_sql(schema, edge, *backward)
                        else {
                            return unsupported("a composite relationship endpoint (S8)");
                        };
                        path::path_avoids_edge(p.alias(), &identity)
                    }
                    (Scan::Path { at: x, .. }, Scan::Path { at: y, .. }) => {
                        path::paths_disjoint(x.alias(), y.alias())
                    }
                    _ => continue,
                };
                self.filters.push(differs);
            }
        }
        Ok(())
    }

    /// Add a decided scan to the join tree (once per variable). An element
    /// read from the segment's CTE is already there.
    fn emit(&mut self, v: VarId) -> Result<(), LowerError> {
        if self.is_emitted(v) {
            return Ok(());
        }
        if let Some(r) = self.elided.get(&v).copied() {
            return self.emit(r); // read from the relationship's columns
        }
        let table = match &self.scans[&v] {
            Scan::Node {
                schema,
                at: At::Table(alias),
                ..
            } => (
                alias.clone(),
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Rel {
                schema,
                at: At::Table(alias),
                ..
            } => (
                alias.clone(),
                schema.full_table_name(),
                schema.view_parameters.as_deref(),
                schema.should_use_final(),
                schema.filter.as_ref(),
            ),
            Scan::Path {
                cte,
                at: At::Table(alias),
                ..
            } => (alias.clone(), cte.clone(), None, false, None),
            Scan::Node {
                at: At::Exported { .. },
                ..
            }
            | Scan::Rel {
                at: At::Exported { .. },
                ..
            }
            | Scan::Path {
                at: At::Exported { .. },
                ..
            } => return unsupported("internal: a CTE element outside its segment"),
            Scan::Impossible => {
                self.empty = true;
                return Ok(());
            }
        };
        let (alias, base, params, use_final, filter) = table;
        let name = ViewTableRef::parameterized_name(
            &base,
            params,
            self.options.view_parameter_values.as_ref(),
        );
        if let Some(f) = filter {
            match f.to_sql(&alias) {
                Ok(sql) => self.filters.push(RenderExpr::Raw(format!("({sql})"))),
                Err(e) => return unsupported(format!("schema filter: {e}")),
            }
        }
        if self.from.is_none() {
            self.from = Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name,
                alias: Some(alias.clone()),
                use_final,
            });
        } else {
            if use_final {
                // `Join` prints no FINAL (S8 with the other table options).
                return unsupported("FINAL on a joined table");
            }
            self.joins.push(join(name, &alias));
        }
        self.emitted.push(alias);
        // Ties that this scan completes go in its ON.
        let mut still = Vec::new();
        for t in std::mem::take(&mut self.pending) {
            if (t.a == v || t.b == v) && self.is_emitted(t.a) && self.is_emitted(t.b) {
                self.place(t);
            } else {
                still.push(t);
            }
        }
        self.pending = still;
        Ok(())
    }

    /// The position of an element's relation in the join tree.
    fn position(&self, v: VarId) -> Option<usize> {
        let alias = self.scans.get(&v)?.at()?.alias();
        self.emitted.iter().position(|e| e == alias)
    }

    fn is_emitted(&self, v: VarId) -> bool {
        self.position(v).is_some()
    }

    fn tie(&mut self, a: VarId, b: VarId, mut eqs: Vec<(RenderExpr, RenderExpr)>) {
        // A node read from its relationship's endpoint column is tied to it
        // already.
        eqs.retain(|(x, y)| x != y);
        if eqs.is_empty() {
            return;
        }
        let t = Tie { a, b, eqs };
        if self.is_emitted(a) && self.is_emitted(b) {
            self.place(t);
        } else {
            self.pending.push(t);
        }
    }

    /// Put a tie whose scans are both emitted in the ON of the later one. Its
    /// ON can see the earlier scan, and for inner joins ON and WHERE mean the
    /// same; a self-tie (`(a)-[r]->(a)` ties `r` to `a` twice) is no special
    /// case. A tie within the FROM relation (two elements of the segment's
    /// CTE, `WITH a, r MATCH (a)-[r]->()`) is a WHERE.
    fn place(&mut self, t: Tie) {
        let later = self
            .position(t.a)
            .max(self.position(t.b))
            .expect("both emitted");
        let eqs = t.eqs.into_iter().map(|(x, y)| eq(x, y));
        if later == 0 {
            self.filters
                .extend(eqs.map(RenderExpr::OperatorApplicationExp));
            return;
        }
        let alias = self.emitted[later].clone();
        let join = self
            .joins
            .iter_mut()
            .find(|j| j.table_alias == alias)
            .expect("every emitted relation after the first is a join");
        if join.join_type == JoinType::Left {
            // An OPTIONAL MATCH's matches: in its ON the tie would keep the
            // row with NULLs instead of dropping it.
            self.filters
                .extend(eqs.map(RenderExpr::OperatorApplicationExp));
            return;
        }
        join.joining_on.extend(eqs);
    }

    fn finish_relation(&mut self) {
        // A tie to an impossible scan never completes; the relation is empty.
        self.pending.clear();
    }

    // ------------------------------------------------------------- segments

    /// `ORDER BY` keys over the current relation (and a projection's items).
    fn sort_keys(
        &self,
        keys: &[SortKey],
        items: &HashMap<VarId, RenderExpr>,
    ) -> Result<Vec<OrderByItem>, LowerError> {
        let keys = keys
            .iter()
            .map(|k| {
                Ok(OrderByItem {
                    expression: self.expr(&k.expr, items)?,
                    order: if k.descending {
                        OrderByOrder::Desc
                    } else {
                        OrderByOrder::Asc
                    },
                })
            })
            .collect::<Result<Vec<_>, LowerError>>()?;
        // A constant key orders nothing, and ClickHouse would read an
        // integer one as a column position (`ORDER BY 1`).
        Ok(keys
            .into_iter()
            .filter(|o| !is_constant(&o.expression))
            .collect())
    }

    /// The SELECT of a WITH or RETURN over the current relation. For a WITH
    /// (`cte` = the alias its CTE will have), node and relationship items are
    /// exported as columns and the returned scope maps each item to where
    /// the next segment reads it. For the final RETURN (`cte` = `None`), the
    /// returned shape says what each item is.
    fn projection_body(
        &mut self,
        p: &Projection,
        cte: Option<&str>,
    ) -> Result<(Body, Exports<'s>, Vec<ResultColumn>), LowerError> {
        let aggregating = p.aggregates();
        let mut body = Body {
            distinct: p.distinct,
            grouped: aggregating && p.items.iter().any(|i| !i.aggregate),
            skip: p.skip,
            limit: p.limit,
            ..Body::default()
        };
        let mut exports = Exports::default();
        let mut shape = Vec::new();
        let mut items_env: HashMap<VarId, RenderExpr> = HashMap::new();
        // `id(n)` items: their column is the node's key, not the id returned.
        let mut id_items: Vec<VarId> = Vec::new();
        for it in &p.items {
            let element = match &it.expr {
                LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) => {
                    parse_var(n).filter(|v| {
                        !matches!(self.binding(*v).kind, BindingKind::Value) && !it.aggregate
                    })
                }
                _ => None,
            };
            if let Some(src) = element {
                if matches!(self.binding(src).kind, BindingKind::Path) {
                    return unsupported("a path variable as a value (S6c)");
                }
                // In this projection's ORDER BY / WHERE the item is the
                // element itself.
                let scan = self.scans.get(&src).cloned();
                let Some(alias) = cte else {
                    shape.push(self.return_element(src, &it.name, aggregating, &mut body)?);
                    if let Some(s) = scan {
                        self.scans.insert(it.var, s);
                    }
                    continue;
                };
                let mut keys = Vec::new();
                let out = self.export_element(src, it.var, alias, &mut body.select, &mut keys)?;
                if aggregating {
                    body.group_by.extend(keys);
                }
                if let Some(s) = scan {
                    self.scans.insert(it.var, s);
                }
                exports.scans.push((it.var, out));
                continue;
            }
            if cte.is_none() {
                // `RETURN n.*`: the node's properties, named as for `RETURN n`.
                if let Some(src) = self.all_properties_of(&it.expr) {
                    let name = it.name.strip_suffix(".*").unwrap_or(&it.name);
                    shape.push(self.return_element(src, name, aggregating, &mut body)?);
                    continue;
                }
                if let Some((v, label)) = self.node_id_item(&it.expr) {
                    let e = self.identity_value(v)?;
                    if aggregating {
                        body.group_by.push(e.clone());
                    }
                    let column = body.column(e, &it.name);
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::NodeId { label },
                        columns: vec![(it.name.clone(), column)],
                    });
                    id_items.push(it.var);
                    continue;
                }
            }
            let e = self.expr(&it.expr, &HashMap::new())?;
            if aggregating && !it.aggregate {
                body.group_by.push(e.clone());
            }
            items_env.insert(it.var, e.clone());
            match cte {
                // A constant needs no column: the next segment uses it as is.
                Some(_) if is_constant(&e) => {
                    exports.values.push((it.var, e));
                }
                Some(alias) => {
                    let name = it.var.name();
                    body.select.push(select(e, &name));
                    exports.values.push((it.var, col_at(alias, &name)));
                }
                None => {
                    let column = body.column(e, &it.name);
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::Value,
                        columns: vec![(it.name.clone(), column)],
                    });
                }
            }
        }
        // Bolt returns `id(n)` encoded, and the encoding does not keep the
        // key's order (a string key is hashed).
        if p.order_by.iter().any(|k| {
            referenced_names(&k.expr, false)
                .iter()
                .any(|n| parse_var(n).is_some_and(|v| id_items.contains(&v)))
        }) {
            return unsupported("ORDER BY an id() item (the encoded id has another order)");
        }
        body.order_by = self.sort_keys(&p.order_by, &items_env)?;
        let ordered_input = !matches!(self.order, RowOrder::Unordered);
        // `collect` lists its input rows in their order, whatever the
        // projection's own ORDER BY does to its output.
        if ordered_input
            && aggregating
            && p.items.iter().any(|i| calls_aggregate(&i.expr, "collect"))
        {
            return unsupported("collect() over ordered rows (keeping the order)");
        }
        // Without its own ORDER BY, a projection keeps the order of its input
        // rows. The SQL keeps it only for a plain projection; after DISTINCT or
        // aggregation it is lost, so a SKIP / LIMIT relying on it is refused.
        if body.order_by.is_empty() {
            let paged = p.skip.is_some() || p.limit.is_some();
            match &self.order {
                RowOrder::Keys(keys) if !aggregating && !p.distinct => {
                    body.order_by = keys.clone();
                }
                RowOrder::Unordered => {}
                RowOrder::Keys(_) | RowOrder::Lost => {
                    if paged {
                        return unsupported(
                            "SKIP / LIMIT over ordered rows after DISTINCT or aggregation",
                        );
                    }
                    body.order_lost = true;
                }
            }
        }
        if let Some(f) = &p.filter {
            let e = self.expr(f, &items_env)?;
            if p.skip.is_some() || p.limit.is_some() {
                // The WHERE filters the rows the SKIP / LIMIT kept (#1311):
                // computed per row here, applied by the next segment.
                let alias = cte.expect("only a WITH has a WHERE");
                body.select.push(select(e, KEEP));
                exports.keep = Some(col_at(alias, KEEP));
            } else if aggregating {
                body.having.push(e);
            } else {
                self.filters.push(e);
            }
        }
        Ok((body, exports, shape))
    }

    /// A node or relationship item of the final RETURN: its columns, named
    /// `name.<…>` as on the legacy pipeline. Returns what the item is.
    fn return_element(
        &self,
        src: VarId,
        name: &str,
        aggregating: bool,
        body: &mut Body,
    ) -> Result<ResultColumn, LowerError> {
        let Some((columns, kind)) = self.element_columns(src)? else {
            // An element that matches nothing: the relation has no rows.
            let column = body.column(RenderExpr::Literal(Literal::Null), name);
            return Ok(ResultColumn {
                name: name.to_string(),
                kind: ResultKind::Value,
                columns: vec![(name.to_string(), column)],
            });
        };
        let Some(identity) = self.identity(src)? else {
            return unsupported(format!("internal: {src} has columns but no identity"));
        };
        let unreturned: Vec<RenderExpr> = identity
            .iter()
            .filter(|i| !columns.iter().any(|(_, e)| e == *i))
            .cloned()
            .collect();
        if body.distinct && !unreturned.is_empty() {
            // Rows equal in every returned column can be different elements
            // (a relationship whose `edge_id` is not a property): DISTINCT
            // also by the identity.
            if aggregating {
                return unsupported(
                    "DISTINCT aggregation over a relationship whose edge_id is not returned",
                );
            }
            body.distinct_keys.extend(unreturned);
        }
        let mut named = Vec::new();
        for (key, e) in columns {
            if aggregating {
                body.group_by.push(e.clone());
            }
            let column = body.column(e, &format!("{name}.{key}"));
            named.push((key, column));
        }
        if aggregating {
            // One group per element, whatever its columns.
            for i in identity {
                if !body.group_by.contains(&i) {
                    body.group_by.push(i);
                }
            }
        }
        Ok(ResultColumn {
            name: name.to_string(),
            kind,
            columns: named,
        })
    }

    /// The columns of a whole node or relationship, in the legacy pipeline's
    /// order: a relationship's stored endpoint columns, then every property
    /// by name. `None` for an element that matches nothing.
    fn element_columns(&self, v: VarId) -> Result<Option<ElementColumns>, LowerError> {
        let mut columns = Vec::new();
        let kind = match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => ResultKind::Node {
                label: label.clone(),
            },
            Some(Scan::Rel {
                schema, rel_type, ..
            }) => {
                for (role, id) in [("from_id", &schema.from_id), ("to_id", &schema.to_id)] {
                    let cols = id.columns();
                    for (i, c) in cols.iter().enumerate() {
                        let suffix = if cols.len() > 1 {
                            format!("{role}_{}", i + 1)
                        } else {
                            role.to_string()
                        };
                        columns.push((suffix, self.physical(v, c)?));
                    }
                }
                ResultKind::Rel {
                    rel_type: rel_type.clone(),
                    from_label: schema.from_node.clone(),
                    to_label: schema.to_node.clone(),
                }
            }
            Some(Scan::Path { .. }) => {
                return unsupported("a variable-length relationship's list of relationships (S6c)")
            }
            Some(Scan::Impossible) => return Ok(None),
            None => return unsupported(format!("internal: {v} has no scan")),
        };
        for prop in self.all_property_names(v) {
            let e = self.property(v, &prop)?;
            columns.push((prop, e));
        }
        Ok(Some((columns, kind)))
    }

    /// Every property of an element's label / type, by name.
    fn all_property_names(&self, v: VarId) -> Vec<String> {
        let mapped = match self.scans.get(&v) {
            Some(Scan::Node { schema, .. }) => &schema.property_mappings,
            Some(Scan::Rel { schema, .. }) => &schema.property_mappings,
            _ => return Vec::new(),
        };
        let mut names: Vec<String> = mapped.keys().cloned().collect();
        names.sort();
        names
    }

    /// The node or relationship of a `v.*` item.
    fn all_properties_of(&self, e: &LogicalExpr) -> Option<VarId> {
        let LogicalExpr::PropertyAccessExp(pa) = e else {
            return None;
        };
        if !matches!(&pa.column, PropertyValue::Column(c) if c == ALL_PROPERTIES) {
            return None;
        }
        parse_var(&pa.table_alias.0).filter(|v| {
            matches!(
                self.binding(*v).kind,
                BindingKind::Node { .. } | BindingKind::Rel { .. }
            )
        })
    }

    /// `id(n)` of a node read from a table or a CTE: the node and its label.
    fn node_id_item(&self, e: &LogicalExpr) -> Option<(VarId, String)> {
        let LogicalExpr::ScalarFnCall(f) = e else {
            return None;
        };
        if !f.name.eq_ignore_ascii_case("id") {
            return None;
        }
        let [LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n))] =
            f.args.as_slice()
        else {
            return None;
        };
        let v = parse_var(n)?;
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Some((v, label.clone())),
            _ => None,
        }
    }

    /// Export a node or relationship `src` as `out` from a CTE aliased
    /// `alias`: its physical identity (and endpoint) columns, and the
    /// properties read of `out` downstream. Returns how the next segment
    /// reads it, and (in `keys`) the exported expressions.
    fn export_element(
        &self,
        src: VarId,
        out: VarId,
        alias: &str,
        select_list: &mut Vec<SelectItem>,
        keys: &mut Vec<RenderExpr>,
    ) -> Result<Scan<'s>, LowerError> {
        let Some(scan) = self.scans.get(&src).cloned() else {
            return unsupported(format!("internal: {src} has no scan"));
        };
        let mut physical_cols = self.identity_physical(src).unwrap_or_default();
        if let Scan::Rel { schema, .. } = &scan {
            for c in schema
                .from_id
                .columns()
                .into_iter()
                .chain(schema.to_id.columns())
            {
                if !physical_cols.iter().any(|p| p == c) {
                    physical_cols.push(c.to_string());
                }
            }
        }
        if matches!(scan, Scan::Impossible) {
            return Ok(Scan::Impossible);
        }
        let mut physical = HashMap::new();
        for c in &physical_cols {
            let name = format!("{out}__{c}");
            let e = self.physical(src, c)?;
            select_list.push(select(e.clone(), &name));
            keys.push(e);
            physical.insert(c.clone(), name);
        }
        let mut props = HashMap::new();
        let demanded = self.demand.get(&out);
        let mut names: BTreeSet<String> = demanded.into_iter().flatten().cloned().collect();
        if names.remove(ALL_PROPERTIES) {
            names.extend(self.all_property_names(src));
        }
        for prop in &names {
            let e = self.property(src, prop)?;
            if is_constant(&e) {
                props.insert(prop.clone(), e);
                continue;
            }
            let name = cte_column_name(&out.name(), prop);
            select_list.push(select(e.clone(), &name));
            keys.push(e);
            props.insert(prop.clone(), col_at(alias, &name));
        }
        Ok(scan.with_at(At::Exported {
            alias: alias.to_string(),
            physical,
            props,
        }))
    }

    /// A WITH: the rows so far become a CTE whose columns are the WITH's
    /// output scope, and a new segment reads from it.
    fn with(&mut self, p: &Projection) -> Result<(), LowerError> {
        let alias = self.next_cte_alias();
        let (body, exports, _) = self.projection_body(p, Some(&alias))?;
        self.close_segment(body, exports, alias)
    }

    /// A free-standing SKIP / LIMIT: the rows so far, in their order, become
    /// a CTE exporting the scope unchanged.
    fn page(&mut self, skip: Option<i64>, limit: Option<i64>) -> Result<(), LowerError> {
        let alias = self.next_cte_alias();
        let order_by = match &self.order {
            RowOrder::Keys(keys) => keys.clone(),
            RowOrder::Unordered => Vec::new(),
            RowOrder::Lost => {
                return unsupported("SKIP / LIMIT over ordered rows after DISTINCT or aggregation")
            }
        };
        let mut body = Body {
            order_by,
            skip,
            limit,
            ..Body::default()
        };
        let mut exports = Exports::default();
        // The scope: every named element and value of this segment, and the
        // elements of a named path (its length reads them).
        let in_named_path = |v: &VarId| {
            self.paths.iter().any(|(p, e)| {
                self.binding(*p).name.is_some() && (e.nodes.contains(v) || e.rels.contains(v))
            })
        };
        let mut vars: Vec<VarId> = self
            .scans
            .keys()
            .chain(self.values.keys())
            .copied()
            .filter(|v| self.binding(*v).name.is_some() || in_named_path(v))
            .collect();
        vars.sort();
        vars.dedup();
        for v in vars {
            if let Some(e) = self.values.get(&v).cloned() {
                if is_constant(&e) {
                    exports.values.push((v, e));
                } else {
                    body.select.push(select(e, &v.name()));
                    exports.values.push((v, col_at(&alias, &v.name())));
                }
                continue;
            }
            let scan = self.export_element(v, v, &alias, &mut body.select, &mut Vec::new())?;
            exports.scans.push((v, scan));
        }
        self.close_segment(body, exports, alias)
    }

    fn next_cte_alias(&self) -> String {
        format!("w{}", self.ctes.len() + 1)
    }

    /// Turn the current segment and `body` into a CTE named after `alias`,
    /// and start a segment that reads it.
    fn close_segment(
        &mut self,
        mut body: Body,
        exports: Exports<'s>,
        alias: String,
    ) -> Result<(), LowerError> {
        // The rows' order travels as exported sort-key columns; the CTE body
        // itself needs an ORDER BY only for its SKIP / LIMIT.
        let mut keys = Vec::new();
        for (i, k) in body.order_by.iter().enumerate() {
            let name = format!("__o{i}");
            body.select.push(select(k.expression.clone(), &name));
            keys.push(OrderByItem {
                expression: col_at(&alias, &name),
                order: k.order.clone(),
            });
        }
        if body.skip.is_none() && body.limit.is_none() {
            body.order_by.clear();
        }
        let order = if !keys.is_empty() {
            RowOrder::Keys(keys)
        } else if body.order_lost {
            RowOrder::Lost
        } else {
            RowOrder::Unordered
        };
        if body.select.is_empty() {
            // Nothing in scope needs a column; the rows still count.
            body.select
                .push(select(RenderExpr::Literal(Literal::Integer(1)), "__row"));
        }
        let plan = self.render(body);
        let name = format!("with_{alias}");
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(plan)),
            false,
        ));
        // The new segment: the CTE is its FROM; only the exported scope is
        // visible.
        self.scans = exports.scans.into_iter().collect();
        self.values = exports.values.into_iter().collect();
        self.emitted = vec![alias.clone()];
        self.from = Some(table_ref(name, &alias));
        self.joins = Vec::new();
        self.pending = Vec::new();
        self.filters = exports.keep.into_iter().collect();
        self.empty = false;
        self.order = order;
        Ok(())
    }

    /// The final RETURN.
    fn project(&mut self, p: &Projection) -> Result<(RenderPlan, Vec<ResultColumn>), LowerError> {
        if p.filter.is_some() {
            return unsupported("internal: a RETURN with a WHERE");
        }
        let (body, _, shape) = self.projection_body(p, None)?;
        Ok((self.render(body), shape))
    }

    /// Join order (§4.6.4) for ClickHouse, which builds a hash table of each
    /// joined relation and streams the FROM rows through them: a path
    /// relation, the largest of a clause in every shape measured (a path
    /// multiplies its start rows by the walks from each), goes first. The
    /// others follow in their order, each after a relation it is tied to,
    /// and every ON conjunct moves to the ON of the later of its relations
    /// (the tie rule of [`Self::place`]). Only a join of inner joins is
    /// reordered (an equality of inner joins holds in any order); a FROM
    /// read with FINAL stays first (a join prints no FINAL).
    fn path_first(&mut self) {
        if self.joins.iter().any(|j| j.join_type != JoinType::Join)
            || self.from.as_ref().is_none_or(|f| f.use_final)
        {
            return;
        }
        let Some(root) = self.emitted.iter().skip(1).find(|a| {
            self.scans
                .values()
                .any(|s| matches!(s, Scan::Path { at: At::Table(p), .. } if p == *a))
        }) else {
            return;
        };
        let root = root.clone();
        // (alias, table) of each relation, in join order, and every ON
        // conjunct with the relation it was placed at.
        let from = self.from.take().expect("checked");
        let mut relations = vec![(from.alias.clone().unwrap_or_default(), from.name.clone())];
        let mut conjuncts: Vec<(String, OperatorApplication)> = Vec::new();
        for j in std::mem::take(&mut self.joins) {
            relations.push((j.table_alias.clone(), j.table_name.clone()));
            for c in j.joining_on {
                conjuncts.push((j.table_alias.clone(), c));
            }
        }
        let reads =
            |c: &OperatorApplication| table_aliases(&RenderExpr::OperatorApplicationExp(c.clone()));
        let mut order: Vec<String> = vec![root];
        while order.len() < relations.len() {
            let next = relations
                .iter()
                .map(|(a, _)| a)
                .filter(|a| !order.contains(a))
                .find(|a| {
                    conjuncts.iter().any(|(_, c)| {
                        let r = reads(c);
                        r.contains(a) && r.iter().any(|x| order.contains(x))
                    })
                })
                .or_else(|| {
                    relations
                        .iter()
                        .map(|(a, _)| a)
                        .find(|a| !order.contains(a))
                })
                .expect("a relation left")
                .clone();
            order.push(next);
        }
        let table = |a: &str| {
            relations
                .iter()
                .find(|(x, _)| x == a)
                .map(|(_, t)| t.clone())
                .expect("a relation")
        };
        let mut joins: Vec<Join> = order[1..].iter().map(|a| join(table(a), a)).collect();
        for (placed, c) in conjuncts {
            let at = reads(&c)
                .iter()
                .filter_map(|a| order.iter().position(|o| o == a))
                .max()
                .unwrap_or_else(|| order.iter().position(|o| *o == placed).expect("placed"));
            if at == 0 {
                self.filters.push(RenderExpr::OperatorApplicationExp(c));
            } else {
                joins[at - 1].joining_on.push(c);
            }
        }
        self.from = Some(table_ref(table(&order[0]), &order[0]));
        self.joins = joins;
        self.emitted = order;
    }

    /// The current relation under `body`'s SELECT.
    fn render(&mut self, body: Body) -> RenderPlan {
        self.path_first();
        // A relation that matches nothing keeps its scans (expressions may
        // read them) and filters every row out. With no scan at all (every
        // element impossible, or no MATCH: `RETURN 1 + 1`) there is no FROM.
        let mut filters = std::mem::take(&mut self.filters);
        if self.empty {
            filters = vec![RenderExpr::Literal(Literal::Boolean(false))];
        }
        let (from, joins) = (self.from.take(), std::mem::take(&mut self.joins));
        // A constant grouping key forms one group: drop it. When no key is
        // left (constants, or an element that matches nothing and exports no
        // column), keep the per-group semantics on an empty input: no row,
        // unlike a global aggregate.
        let mut group_by = body.group_by;
        let mut having = body.having;
        let mut distinct = body.distinct;
        if distinct && !body.distinct_keys.is_empty() {
            // DISTINCT by the returned columns and keys that are not returned:
            // a GROUP BY of both (a plain projection, so no aggregate).
            group_by = body
                .select
                .iter()
                .map(|i| i.expression.clone())
                .chain(body.distinct_keys)
                .collect();
            distinct = false;
        }
        group_by.retain(|g| !is_constant(g));
        if body.grouped && group_by.is_empty() {
            having.insert(
                0,
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::GreaterThan,
                    operands: vec![
                        RenderExpr::AggregateFnCall(
                            crate::render_plan::render_expr::AggregateFnCall {
                                name: "count".to_string(),
                                args: vec![RenderExpr::Star],
                            },
                        ),
                        RenderExpr::Literal(Literal::Integer(0)),
                    ],
                }),
            );
        }
        RenderPlan {
            select: SelectItems {
                items: body.select,
                distinct,
            },
            from: FromTableItem(from),
            joins: JoinItems(joins),
            filters: FilterItems(and_all(filters)),
            group_by: GroupByExpressions(group_by),
            having_clause: and_all(having),
            order_by: OrderByItems(body.order_by),
            skip: SkipItem(body.skip),
            limit: LimitItem(body.limit),
            ..empty_plan()
        }
    }
}

/// The per-row WITH WHERE value a CTE exports when the WHERE follows a SKIP
/// or LIMIT.
const KEEP: &str = "__keep";

/// `v.*`, and in the demand pass: every property of `v` (a whole-entity
/// RETURN).
const ALL_PROPERTIES: &str = "*";

/// A returned element's columns (`<item>.<suffix>`, expression) and kind.
type ElementColumns = (Vec<(String, RenderExpr)>, ResultKind);

/// What a segment's CTE exports: the next segment's scope.
#[derive(Default)]
struct Exports<'s> {
    scans: Vec<(VarId, Scan<'s>)>,
    values: Vec<(VarId, RenderExpr)>,
    /// A filter the next segment applies first (a WITH's WHERE after its
    /// SKIP / LIMIT).
    keep: Option<RenderExpr>,
}

fn join(table_name: String, alias: &str) -> Join {
    Join {
        table_name,
        table_alias: alias.to_string(),
        joining_on: Vec::new(),
        join_type: JoinType::Join,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
        graph_rel: None,
        is_cartesian: false,
    }
}

/// `name AS alias` in a FROM.
fn table_ref(name: String, alias: &str) -> ViewTableRef {
    ViewTableRef {
        source: Arc::new(LogicalPlan::Empty),
        name,
        alias: Some(alias.to_string()),
        use_final: false,
    }
}

/// A plan with nothing in it.
fn empty_plan() -> RenderPlan {
    RenderPlan {
        ctes: CteItems(Vec::new()),
        select: SelectItems {
            items: Vec::new(),
            distinct: false,
        },
        from: FromTableItem(None),
        joins: JoinItems(Vec::new()),
        array_join: ArrayJoinItem(Vec::new()),
        filters: FilterItems(None),
        group_by: GroupByExpressions(Vec::new()),
        having_clause: None,
        order_by: OrderByItems(Vec::new()),
        skip: SkipItem(None),
        limit: LimitItem(None),
        union: UnionItems(None),
        fixed_path_info: None,
        is_multi_label_scan: false,
        variable_registry: None,
    }
}

/// The column of `alias` an exported expression reads.
fn column_of(e: &RenderExpr, alias: &str) -> Result<String, LowerError> {
    match e {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias,
            column: PropertyValue::Column(c),
        }) if table_alias.0 == alias => Ok(c.clone()),
        _ => unsupported(format!("internal: not a column of {alias}")),
    }
}

/// The table aliases an operator tree over columns reads.
fn table_aliases(e: &RenderExpr) -> Vec<String> {
    match e {
        RenderExpr::PropertyAccessExp(pa) => vec![pa.table_alias.0.clone()],
        RenderExpr::List(xs) => xs.iter().flat_map(table_aliases).collect(),
        RenderExpr::OperatorApplicationExp(op) => {
            op.operands.iter().flat_map(table_aliases).collect()
        }
        _ => Vec::new(),
    }
}

/// The top-level AND operands of `e`.
fn conjuncts(e: &RenderExpr) -> Vec<&RenderExpr> {
    match e {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::And => {
            op.operands.iter().flat_map(conjuncts).collect()
        }
        _ => vec![e],
    }
}

/// `e` is an operator tree over columns of `aliases`, literals and
/// parameters: a deterministic function of those rows (no function call,
/// so no `rand()`), true of the same rows wherever it is evaluated.
fn reads_only(e: &RenderExpr, aliases: &[String]) -> bool {
    match e {
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) => true,
        RenderExpr::PropertyAccessExp(pa) => aliases.contains(&pa.table_alias.0),
        RenderExpr::List(xs) => xs.iter().all(|x| reads_only(x, aliases)),
        RenderExpr::OperatorApplicationExp(op) => {
            op.operands.iter().all(|x| reads_only(x, aliases))
        }
        _ => false,
    }
}

/// `e` (an operator tree, [`reads_only`]) with the columns of `from` read
/// from `to`.
fn realias(e: &RenderExpr, from: &str, to: &str) -> RenderExpr {
    match e {
        RenderExpr::PropertyAccessExp(pa) if pa.table_alias.0 == from => {
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(to.to_string()),
                column: pa.column.clone(),
            })
        }
        RenderExpr::List(xs) => RenderExpr::List(xs.iter().map(|x| realias(x, from, to)).collect()),
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op.operands.iter().map(|x| realias(x, from, to)).collect(),
            })
        }
        other => other.clone(),
    }
}

/// `a = b`, with NULL equal to NULL, in a form every dialect joins on.
fn not_distinct(a: RenderExpr, b: RenderExpr) -> OperatorApplication {
    let is_null = |e: RenderExpr| {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNull,
            operands: vec![e],
        })
    };
    OperatorApplication {
        operator: Operator::Or,
        operands: vec![
            RenderExpr::OperatorApplicationExp(eq(a.clone(), b.clone())),
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: vec![is_null(a), is_null(b)],
            }),
        ],
    }
}

/// The properties read of each binding anywhere in the statement, carried
/// back through pass-through projection items (`WITH a AS b … b.name` reads
/// `a.name`), so a CTE exports what later clauses use (§4.10).
fn demand(stmt: &BoundStatement) -> HashMap<VarId, BTreeSet<String>> {
    fn exprs<'a>(op: &'a BoundOp, out: &mut Vec<&'a LogicalExpr>) {
        match op {
            BoundOp::Unit => {}
            BoundOp::Match {
                input,
                pattern,
                predicate,
                ..
            } => {
                exprs(input, out);
                for part in &pattern.parts {
                    for n in &part.nodes {
                        out.extend(n.props.iter().map(|(_, e)| e));
                    }
                    for r in &part.rels {
                        out.extend(r.props.iter().map(|(_, e)| e));
                    }
                }
                out.extend(predicate);
            }
            BoundOp::Unwind { input, expr, .. } => {
                exprs(input, out);
                out.push(expr);
            }
            BoundOp::Project { input, projection } => {
                exprs(input, out);
                out.extend(projection.items.iter().map(|i| &i.expr));
                out.extend(projection.order_by.iter().map(|k| &k.expr));
                out.extend(&projection.filter);
            }
            BoundOp::Sort { input, keys } => {
                exprs(input, out);
                out.extend(keys.iter().map(|k| &k.expr));
            }
            BoundOp::Skip { input, .. } | BoundOp::Limit { input, .. } => exprs(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| exprs(a, out)),
        }
    }
    let mut all = Vec::new();
    exprs(&stmt.plan, &mut all);
    let mut demand: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut add = |v: VarId, prop: &str| {
        demand.entry(v).or_default().insert(prop.to_string());
    };
    // A node or relationship the final RETURN returns whole: every property
    // (`v.*` is a property ref).
    if let BoundOp::Project { projection, .. } = &stmt.plan {
        for it in projection.items.iter().filter(|i| !i.aggregate) {
            if let LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) =
                &it.expr
            {
                if let Some(v) = parse_var(n).filter(|v| {
                    matches!(
                        stmt.bindings[v.0 as usize].kind,
                        BindingKind::Node { .. } | BindingKind::Rel { .. }
                    )
                }) {
                    add(v, ALL_PROPERTIES);
                }
            }
        }
    }
    for e in all {
        for (name, prop) in property_refs(e) {
            if let Some(v) = parse_var(&name) {
                add(v, &prop);
            }
        }
    }
    for part in pattern_parts(&stmt.plan) {
        for n in &part.nodes {
            n.props.iter().for_each(|(p, _)| add(n.var, p));
        }
        for r in &part.rels {
            r.props.iter().for_each(|(p, _)| add(r.var, p));
        }
    }
    // A pass-through item is a later binding than its source.
    for b in stmt.bindings.iter().rev() {
        if let BindingSource::Projection { of: Some(src), .. } = b.source {
            if let Some(props) = demand.get(&b.id).cloned() {
                demand.entry(src).or_default().extend(props);
            }
        }
    }
    demand
}

fn pattern_parts(op: &BoundOp) -> Vec<&PatternPart> {
    match op {
        BoundOp::Unit => Vec::new(),
        BoundOp::Match { input, pattern, .. } => {
            let mut v = pattern_parts(input);
            v.extend(&pattern.parts);
            v
        }
        BoundOp::Unwind { input, .. }
        | BoundOp::Project { input, .. }
        | BoundOp::Sort { input, .. }
        | BoundOp::Skip { input, .. }
        | BoundOp::Limit { input, .. } => pattern_parts(input),
        BoundOp::Union { arms, .. } => arms.iter().flat_map(pattern_parts).collect(),
    }
}

use crate::query_planner::logical_expr::LogicalExpr;

pub(crate) fn parse_var(generated: &str) -> Option<VarId> {
    generated.strip_prefix('v')?.parse().ok().map(VarId)
}

/// No column, variable or aggregate: the same value on every row.
fn is_constant(e: &RenderExpr) -> bool {
    match e {
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) => true,
        RenderExpr::List(xs) => xs.iter().all(is_constant),
        RenderExpr::MapLiteral(entries) => entries.iter().all(|(_, v)| is_constant(v)),
        RenderExpr::OperatorApplicationExp(op) => op.operands.iter().all(is_constant),
        _ => false,
    }
}

pub(crate) fn and_all(mut exprs: Vec<RenderExpr>) -> Option<RenderExpr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: exprs,
        })),
    }
}

pub(crate) fn or_all(mut exprs: Vec<RenderExpr>) -> RenderExpr {
    if exprs.len() == 1 {
        return exprs.pop().expect("one");
    }
    RenderExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Or,
        operands: exprs,
    })
}
