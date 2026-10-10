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
//!   is_standard_edge_table`); fixed length or variable length, directed or
//!   undirected (S6, S7a). A node of several possible labels is one relation
//!   of its labels' tables (`Scan::Labels`, S7b1), its rows carrying their
//!   label; a fixed-length relationship of several possible types or label
//!   pairs is one relation of its definitions' tables (`Scan::Rels`, S7b2),
//!   its rows carrying their type and their ends' labels. A variable-length
//!   relationship of one type of one definition between nodes of its one
//!   label walks its table; any other walks the union of its definitions
//!   between nodes keyed by label and id (`Walked::Union`, S7b3a), as a
//!   shortest path by a search over the same union (S7b3b);
//! * WITH and RETURN with aggregation, DISTINCT, ORDER BY, SKIP, LIMIT and
//!   (WITH) WHERE, evaluated in that order; free-standing ORDER BY, SKIP and
//!   LIMIT;
//! * UNWIND of a list of values (`unwind.rs`, S7c): like a SKIP / LIMIT it
//!   ends a segment, whose CTE repeats each row per element;
//! * UNION and UNION ALL (`union.rs`, S7d): each arm lowered on its own to a
//!   CTE, read through one `UNION ALL`;
//! * a final RETURN of values, whole nodes and relationships (`n`, `n.*`)
//!   and `id(n)`, with the result shape that Bolt, the HTTP graph output and
//!   embedded `query_graph` read (§4.13, [`ResultColumn`]).
//!
//! A node or relationship whose label / type set is empty matches nothing
//! (Cypher returns no rows; it is not an error): the query lowers to a
//! relation with no rows, and its properties read as NULL.

mod elements;
mod expr;
mod path;
#[cfg(test)]
mod tests;
mod union;
mod unwind;
mod value;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::{
    visit_render_expr_mut, ColumnAlias, Literal, MutVisit, Operator, OperatorApplication,
    PropertyAccess, RenderCase, RenderExpr, TableAlias,
};
use crate::render_plan::{
    ArrayJoin, ArrayJoinItem, Cte, CteContent, CteItems, FilterItems, FromTableItem,
    GroupByExpressions, Join, JoinItems, JoinType, LimitItem, OrderByItem, OrderByItems,
    OrderByOrder, RenderPlan, SelectItem, SelectItems, SkipItem, Union, UnionItems, UnionType,
    ViewTableRef,
};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::function_mapper::current_function_mapper;
use crate::utils::cte_column_naming::cte_column_name;

use super::expr::{calls_aggregate, property_refs, referenced_names};
use super::types::*;
pub use value::GraphType;
use value::{Carried, GraphRef, NODE_VALUES, PATH_KEY, REL_VALUES};

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
    /// A path, or a list of nodes or relationships, in Neo4j's JSON form
    /// (`value.rs`): column `name`.
    Graph(GraphType),
}

/// Lower a bound statement to a render plan.
pub fn lower_statement(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
) -> Result<Lowered, LowerError> {
    let demand = demand(stmt);
    if let BoundOp::Union {
        arms,
        arm_columns,
        all,
    } = &stmt.plan
    {
        return union::lower(
            &Query {
                stmt,
                schema,
                options,
                demand: &demand,
            },
            arms,
            arm_columns,
            *all,
        );
    }
    let mut ctes = Vec::new();
    let arm = Query {
        stmt,
        schema,
        options,
        demand: &demand,
    }
    .lower(&stmt.plan, &mut ctes)?;
    let mut plan = arm.plan;
    plan.ctes = CteItems(ctes);
    Ok(Lowered {
        plan,
        shape: arm.shape,
    })
}

/// What lowering one query (the statement, or an arm of a UNION) reads.
struct Query<'a> {
    stmt: &'a BoundStatement,
    schema: &'a GraphSchema,
    options: &'a LowerOptions,
    demand: &'a HashMap<VarId, BTreeSet<String>>,
}

/// A lowered query ending in RETURN ([`Query::lower`]).
struct LoweredQuery {
    /// The final SELECT, without CTEs.
    plan: RenderPlan,
    shape: Vec<ResultColumn>,
    /// The identity of each node, relationship or graph value the RETURN
    /// returns, by item name: what a DISTINCT keeps it apart by (a
    /// relationship's graph value is not: its `elementId` is its ends').
    identities: Vec<(String, Vec<RenderExpr>)>,
}

impl Query<'_> {
    /// Lower `op`, a query ending in RETURN, appending its CTEs to `ctes`
    /// (whose names it continues, so the CTEs of several queries never
    /// share one).
    fn lower(&self, op: &BoundOp, ctes: &mut Vec<Cte>) -> Result<LoweredQuery, LowerError> {
        let BoundOp::Project { input, projection } = op else {
            return unsupported("internal: a query that does not end in RETURN");
        };
        let mut l = Lowerer {
            schema: self.schema,
            bindings: &self.stmt.bindings,
            options: self.options,
            demand: self.demand.clone(),
            ctes: std::mem::take(ctes),
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
            graph_values: HashMap::new(),
            kinds: HashMap::new(),
            local_kinds: RefCell::new(HashMap::new()),
            collect_order: None,
            one_row: true,
            identities: Vec::new(),
        };
        let lowered = l
            .relation(input)
            .map(|()| l.finish_relation())
            .and_then(|()| l.project(projection));
        *ctes = std::mem::take(&mut l.ctes);
        let (plan, shape) = lowered?;
        Ok(LoweredQuery {
            plan,
            shape,
            identities: l.identities,
        })
    }
}

/// How a pattern element is read.
#[derive(Debug, Clone)]
enum Scan<'s> {
    Node {
        schema: &'s NodeSchema,
        label: String,
        at: At,
    },
    /// A node with several possible labels (§4.6 `Alternatives`): read
    /// from the CTE `cte` of one arm per label ([`Lowerer::label_union`]),
    /// whose rows carry their label (`LABEL_COLUMN`), identity
    /// (`__cg_id_{i}`) and the properties read of the node.
    Labels {
        arms: Vec<(String, &'s NodeSchema)>,
        cte: String,
        /// The node the CTE was built for: its columns are named after it.
        of: VarId,
        at: At,
    },
    Rel {
        schema: &'s RelationshipSchema,
        rel_type: String,
        at: At,
        /// Read at its table from this CTE of the table's rows in both
        /// directions ([`Lowerer::both_directions`]): an undirected
        /// relationship whose two directions are the one table.
        both: Option<String>,
    },
    /// A relationship of several possible types or label pairs (§4.6
    /// `Alternatives`): read from the CTE `cte` of one arm per definition
    /// and orientation ([`Lowerer::rel_union`]), whose rows carry their type,
    /// the labels and ids of their stored ends, their identity and the
    /// properties read of the relationship.
    Rels {
        arms: Vec<RelArm<'s>>,
        cte: String,
        /// The relationship the CTE was built for: its columns are named
        /// after it.
        of: VarId,
        at: At,
    },
    /// A variable-length relationship: a relation of paths (`path.rs`),
    /// read from the CTE `cte`.
    Path {
        /// The relationships it walks.
        walked: Walked<'s>,
        cte: String,
        at: At,
        /// It has a `path_edges` column.
        edges: bool,
        /// It has a `path_nodes` column (a shortest path's search has not,
        /// unless its paths are recovered).
        nodes: bool,
        /// The hop range it is generated for.
        range: (u32, Option<u32>),
        /// Of a `shortestPath` / `allShortestPaths` pattern: per pair of
        /// ends, the shortest paths only.
        shortest: Option<ShortestMode>,
        /// The walk starts at the pattern's right end: its order is the
        /// reverse of the path's.
        reversed: bool,
        /// It carries its nodes / relationships as values
        /// (`path_node_values` / `path_rel_values`, `value.rs`).
        node_values: bool,
        rel_values: bool,
    },
    /// An element whose label / type set is empty: it matches nothing.
    Impossible,
}

/// The relationships a variable-length relationship walks ([`Scan::Path`]).
#[derive(Debug, Clone)]
enum Walked<'s> {
    /// One definition between nodes of its one label: its table, walked by
    /// the generator (`path::path_cte`); a path's nodes are ids.
    One {
        schema: &'s RelationshipSchema,
        rel_type: String,
    },
    /// The definitions of its types, between nodes of any labels (§4.6
    /// `Alternatives`, S7b3a): a relation of them walked from node to node by
    /// label and id (`path::union_path_ctes`). `arms` are the definitions,
    /// in the orientations the walk follows them, that a path between its
    /// ends can use; decided with the walk (none before, and none when only
    /// the path of none can match).
    Union {
        types: BTreeSet<String>,
        arms: Vec<RelArm<'s>>,
    },
}

/// One arm of a relationship of several possible types or label pairs
/// ([`Scan::Rels`]): a definition, read as stored or reversed (its left end
/// is the stored `to`).
#[derive(Debug, Clone)]
struct RelArm<'s> {
    rel_type: String,
    schema: &'s RelationshipSchema,
    reversed: bool,
}

impl RelArm<'_> {
    /// The labels of the ends the arm's rows leave and enter.
    fn ends(&self) -> (&str, &str) {
        let (f, t) = (self.schema.from_node.as_str(), self.schema.to_node.as_str());
        if self.reversed {
            (t, f)
        } else {
            (f, t)
        }
    }
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
            Scan::Node { at, .. }
            | Scan::Labels { at, .. }
            | Scan::Rel { at, .. }
            | Scan::Rels { at, .. }
            | Scan::Path { at, .. } => Some(at),
            Scan::Impossible => None,
        }
    }

    /// The same element, read at `at`.
    fn with_at(self, at: At) -> Scan<'s> {
        match self {
            Scan::Node { schema, label, .. } => Scan::Node { schema, label, at },
            Scan::Labels { arms, cte, of, .. } => Scan::Labels { arms, cte, of, at },
            Scan::Rel {
                schema, rel_type, ..
            } => Scan::Rel {
                schema,
                rel_type,
                at,
                both: None,
            },
            Scan::Rels { arms, cte, of, .. } => Scan::Rels { arms, cte, of, at },
            Scan::Path {
                walked,
                cte,
                edges,
                nodes,
                range,
                shortest,
                reversed,
                node_values,
                rel_values,
                ..
            } => Scan::Path {
                walked,
                cte,
                at,
                edges,
                nodes,
                range,
                shortest,
                reversed,
                node_values,
                rel_values,
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
    graph_values: HashMap<VarId, Carried>,
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
    /// Graph values (`value.rs`) the segment's CTE carries, by variable;
    /// each is also in `values`.
    graph_values: HashMap<VarId, Carried>,
    /// What each projected or unwound value is (`unwind.rs`).
    kinds: HashMap<VarId, unwind::Kind>,
    /// What each comprehension parameter is: an element of its list. Filled
    /// in as [`Lowerer::kind`] reads the comprehension (variables are unique
    /// per query, so one map serves every scope).
    local_kinds: RefCell<HashMap<VarId, unwind::Kind>>,
    /// While a projection over rows in an order is lowered: the rows'
    /// number in that order, which `collect()` lists its values in.
    collect_order: Option<RenderExpr>,
    /// The rows so far are at most one (no MATCH yet, or an aggregation with
    /// no grouping item since).
    one_row: bool,
    /// The identity of each node, relationship or graph value the final
    /// RETURN returns, by item name ([`LoweredQuery::identities`]).
    identities: Vec<(String, Vec<RenderExpr>)>,
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
    /// first-seen order of their input; UNWIND of rows in no order keeps
    /// each row's elements together, in list order) that the SQL does not:
    /// a later clause that relies on it is not lowered.
    Lost,
}

/// `alias.column`.
pub(crate) fn col_at(alias: &str, column: &str) -> RenderExpr {
    RenderExpr::PropertyAccessExp(PropertyAccess {
        table_alias: TableAlias(alias.to_string()),
        column: PropertyValue::Column(column.to_string()),
    })
}

/// The columns of a relationship read in both directions
/// ([`Lowerer::both_directions`]) holding the identity of the node a row
/// leaves (`BOTH_START`) and enters (`BOTH_END`), one per identity column.
const BOTH_START: &str = "__cg_start";
const BOTH_END: &str = "__cg_end";

/// The columns of a node with several possible labels
/// ([`Lowerer::label_union`]): its label, and its identity, one column per
/// identity column (`LABEL_ID_{i}`).
const LABEL_COLUMN: &str = "__cg_label";
const LABEL_ID: &str = "__cg_id";

/// The columns of a relationship of several possible types or label pairs
/// ([`Lowerer::rel_union`]): its type, the labels of its stored ends, its
/// identity (`REL_ID_{i}`), the ids of its stored ends (`REL_FROM_{i}`,
/// `REL_TO_{i}`), and the label and id of the node a row leaves and enters
/// here (`REL_START_LABEL` / `BOTH_START_{i}`, `REL_END_LABEL` /
/// `BOTH_END_{i}`).
const REL_TYPE: &str = "__cg_type";
const REL_FROM_LABEL: &str = "__cg_from_label";
const REL_TO_LABEL: &str = "__cg_to_label";
const REL_ID: &str = "__cg_rid";
const REL_FROM: &str = "__cg_from";
const REL_TO: &str = "__cg_to";
const REL_START_LABEL: &str = "__cg_start_label";
const REL_END_LABEL: &str = "__cg_end_label";
/// The identities as texts (`value::node_key_text`, a walk's node `KEY`) of
/// the node a row of a walk's relationships leaves and enters, for a
/// shortest-path search over them (S7b3b).
const REL_START_KEY: &str = "__cg_start_key";
const REL_END_KEY: &str = "__cg_end_key";
/// The column of a relationship's orientation ([`Lowerer::turns`]): 1 where
/// a row reads it reversed.
const TURN: &str = "__cg_turn";
/// An element's identity as a text, with its label or definition
/// (`value::node_key_text` / `value::rel_key`), in the rows of a union of
/// relationships and of a walk's nodes: how a path's identity spells it.
const KEY: &str = "__cg_key";
/// An element's value (`value.rs`) in the rows of a walk's nodes and
/// relationships.
const ELEMENT_VALUE: &str = "__cg_value";

fn indexed_column(side: &str, i: usize) -> String {
    format!("{side}_{i}")
}

fn indexed_columns(side: &str, arity: usize) -> Vec<String> {
    (0..arity).map(|i| indexed_column(side, i)).collect()
}

/// The label a relationship's row gives the node at one of its ends.
#[derive(Debug, Clone)]
enum EndLabel {
    /// Always this one.
    Is(String),
    /// One of `labels`, by row: `value`.
    By {
        labels: Vec<String>,
        value: RenderExpr,
    },
}

impl EndLabel {
    /// One of `labels` (repeats allowed), by row `value`.
    fn of(mut labels: Vec<String>, value: RenderExpr) -> EndLabel {
        labels.sort();
        labels.dedup();
        if labels.len() == 1 {
            EndLabel::Is(labels.remove(0))
        } else {
            EndLabel::By { labels, value }
        }
    }

    fn labels(&self) -> Vec<String> {
        match self {
            EndLabel::Is(l) => vec![l.clone()],
            EndLabel::By { labels, .. } => labels.clone(),
        }
    }
}

/// The ends a relationship's columns tie: (end, the label it gives the end,
/// the relationship's columns equal to the end's identity).
type Ends = Vec<(VarId, EndLabel, Vec<RenderExpr>)>;

/// The widths of a relationship union's columns ([`Lowerer::rel_union`]).
struct RelUnionShape {
    /// Its identity: the widest arm's (a narrower one's is NULL-padded).
    identity: usize,
    /// The ids of its stored ends.
    from: usize,
    to: usize,
    /// The ids of the ends a row leaves and enters.
    start: usize,
    end: usize,
}

/// [`RelUnionShape`] of `arms`, whose ends' ids must have one arity each.
fn rel_union_shape(arms: &[RelArm]) -> Result<RelUnionShape, LowerError> {
    let shape = |a: &RelArm| {
        let (f, t) = (
            a.schema.from_id.columns().len(),
            a.schema.to_id.columns().len(),
        );
        let identity = a
            .schema
            .edge_id
            .as_ref()
            .map_or(f + t, |id| id.columns().len());
        let (start, end) = if a.reversed { (t, f) } else { (f, t) };
        (identity, (f, t, start, end))
    };
    let Some(first) = arms.first().map(shape) else {
        return unsupported("internal: a relationship union of no arms");
    };
    let mut identity = first.0;
    for a in arms {
        let (i, ends) = shape(a);
        if ends != first.1 {
            return unsupported("a relationship whose definitions' ends differ in id arity (S8)");
        }
        identity = identity.max(i);
    }
    let (from, to, start, end) = first.1;
    Ok(RelUnionShape {
        identity,
        from,
        to,
        start,
        end,
    })
}

/// The number of definitions (labels, relationship definitions) with a
/// value of a property, of each arm's index among them.
fn definitions_with_value(defs: &[Option<usize>]) -> usize {
    defs.iter().flatten().max().map_or(0, |k| k + 1)
}

/// The CTE columns holding property `p` of union element `of` whose arms'
/// values come from `definitions` definitions: one, or one per definition
/// (`…__cg{k}`, NULL outside its rows), so each keeps its own type
/// (`FunctionMapper::one_type_guard`).
fn union_property_columns(of: VarId, p: &str, definitions: usize) -> Vec<String> {
    let base = cte_column_name(&of.name(), p);
    if definitions <= 1 {
        return vec![base];
    }
    (0..definitions).map(|k| format!("{base}__cg{k}")).collect()
}

/// The SELECT items of property `p` in arm `arm` of a union for element
/// `of` ([`union_property_columns`]): the arm's value (`None`: NULL) in its
/// definition's column (`defs[arm]`), NULL in the others.
fn union_property_items(
    of: VarId,
    p: &str,
    defs: &[Option<usize>],
    arm: usize,
    value: Option<RenderExpr>,
) -> Vec<SelectItem> {
    let columns = union_property_columns(of, p, definitions_with_value(defs));
    let one = columns.len() == 1;
    columns
        .iter()
        .enumerate()
        .map(|(k, c)| {
            let e = match &value {
                Some(e) if one || defs[arm] == Some(k) => e.clone(),
                _ => RenderExpr::Literal(Literal::Null),
            };
            select(e, c)
        })
        .collect()
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
    /// Columns (by name) whose value follows from the others' and is not
    /// grouped by: a graph value (`value.rs`, ClickHouse refuses `Dynamic` in
    /// GROUP BY). When the rows are grouped, each is `any()` of its group.
    determined: Vec<String>,
    skip: Option<i64>,
    limit: Option<i64>,
    /// An UNWIND's lists, read in step (`unwind.rs`).
    array_join: Vec<ArrayJoin>,
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
                self.one_row = false;
                Ok(())
            }
            BoundOp::Project { input, projection } => {
                if projection.kind != ProjectionKind::With {
                    return unsupported("a RETURN that is not the last clause");
                }
                self.relation(input)?;
                self.finish_relation();
                for item in &projection.items {
                    let kind = self.kind(&item.expr);
                    self.kinds.insert(item.var, kind);
                }
                // Aggregating with no grouping item: one row.
                if projection.aggregates() && projection.items.iter().all(|i| i.aggregate) {
                    self.one_row = true;
                }
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
            BoundOp::Unwind { input, expr, var } => {
                self.relation(input)?;
                self.finish_relation();
                self.unwind(expr, *var)
            }
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
                Self::shortest_pattern(part)?;
            }
            for n in &part.nodes {
                self.node_scan(n.var)?;
            }
            for (i, r) in part.rels.iter().enumerate() {
                self.rel_scan(r, part.nodes[i].var, part.nodes[i + 1].var)?;
                if let Some(Scan::Path { shortest, .. }) = self.scans.get_mut(&r.var) {
                    *shortest = part.shortest;
                }
                // The relationships of a shortest path are not unique against
                // the clause's others (Neo4j): the search is the pattern's
                // own.
                if part.shortest.is_none() {
                    clause_rels.push(r.var);
                }
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
                self.written(n.var, &n.labels)?;
                conditions.extend(self.props(n.var, &n.props)?);
            }
            for r in &part.rels {
                self.written(r.var, &r.types)?;
                if r.length.is_none() {
                    conditions.extend(self.props(r.var, &r.props)?);
                }
            }
        }
        let filter = predicate
            .map(|p| self.expr(p, &HashMap::new()))
            .transpose()?;
        let own: Vec<RenderExpr> = conditions.iter().chain(&filter).cloned().collect();
        // A shortest path's conditions (§4.8): the WHERE conjuncts that read
        // its relation (the path, its relationships) hold of the paths the
        // search picks from, not of the picked ones.
        let mut in_search: HashMap<VarId, Vec<RenderExpr>> = HashMap::new();
        for part in pattern.parts.iter().filter(|p| p.shortest.is_some()) {
            let r = part.rels[0].var;
            let Some(Scan::Path { at, .. }) = self.scans.get(&r) else {
                continue; // it matches nothing
            };
            let alias = at.alias().to_string();
            for c in filter.iter().flat_map(conjuncts) {
                match read_aliases(c) {
                    Some(read) if !read.contains(&alias) => {}
                    Some(_) => in_search.entry(r).or_default().push(c.clone()),
                    None => {
                        return unsupported(
                            "a shortestPath whose WHERE has a subquery or raw SQL condition",
                        )
                    }
                }
            }
        }
        for part in &pattern.parts {
            self.emit(part.nodes[0].var)?;
            for (i, r) in part.rels.iter().enumerate() {
                if let Some(Scan::Path { .. }) = self.scans.get(&r.var) {
                    let (left, right) = (part.nodes[i].var, part.nodes[i + 1].var);
                    let search = in_search.get(&r.var).map(Vec::as_slice).unwrap_or(&[]);
                    self.build_path(r, left, right, &own, search)?;
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
        // A variable-length relationship walks from a node the drive holds;
        // in the anchored form a copied restriction need not reach it.
        let has_path = pattern
            .parts
            .iter()
            .flat_map(|p| &p.rels)
            .any(|r| r.length.is_some());
        let anchored = where_only.is_empty()
            && shared.iter().all(|v| {
                matches!(
                    self.scans.get(v),
                    Some(Scan::Node { .. } | Scan::Labels { .. })
                )
            })
            && (shared.is_empty() || (!has_path && (hops <= 1 || restricted)));
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
            let Some(scan @ (Scan::Node { .. } | Scan::Labels { .. })) = self.scans.get(v).cloned()
            else {
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
            q.scans.insert(*v, scan.with_at(at));
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
            if self.graph_values.contains_key(v) {
                return unsupported("a path's list read by an OPTIONAL MATCH");
            }
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
            // `Q` reads no value: a list's values stay out of `D`.
            let mut physical = physical;
            physical.retain(|c, _| !path::VALUE_COLUMNS.contains(&c.as_str()));
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
            let by_ends = match &scan {
                Scan::Rel { schema, .. } => schema.edge_id.is_none(),
                Scan::Rels { arms, .. } => arms.iter().any(|a| a.schema.edge_id.is_none()),
                _ => false,
            };
            if by_ends {
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
            for (i, c) in self
                .identity_physical(*v)
                .unwrap_or_default()
                .into_iter()
                .enumerate()
            {
                let column = physical[&c].clone();
                correlation.push(Correlated {
                    column: column.clone(),
                    outer: col_at(&w, &column),
                    inner: col_at(&d, &column),
                    // A union's identity past its definition's own arity is
                    // NULL; its first column is NULL exactly when it is.
                    null_safe: null_safe || (i > 0 && matches!(scan, Scan::Rels { .. })),
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
            let scan = self.export_element(*v, *v, q_alias, &mut body, &mut Vec::new())?;
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
        std::mem::swap(&mut self.graph_values, &mut s.graph_values);
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
        let mut arms = Vec::new();
        for label in labels {
            let Some(ns) = self.schema.node_schema_opt(label) else {
                return unsupported(format!("label {label} has no node schema"));
            };
            if !ns.is_standard_own_table() {
                return unsupported(format!("label {label} is not the standard layout (S8)"));
            }
            arms.push((label.clone(), ns));
        }
        let scan = match arms.len() {
            0 => Scan::Impossible,
            1 => {
                let (label, schema) = arms.remove(0);
                Scan::Node {
                    schema,
                    label,
                    at: At::Table(v.name()),
                }
            }
            _ => Scan::Labels {
                cte: self.label_union(v, &arms)?,
                arms,
                of: v,
                at: At::Table(v.name()),
            },
        };
        self.scans.insert(v, scan);
        Ok(())
    }

    /// The nodes of several labels, as one relation (§4.6 `Alternatives`):
    /// the CTE `{v}_labels`, one arm per label reading its table (with its
    /// `filter:`, view parameters and FINAL), its rows carrying
    ///
    /// * their label (`LABEL_COLUMN`) and identity (`LABEL_ID_{i}`): a node
    ///   is its label and its id, so two labels' equal ids are two nodes;
    /// * each property read of `v` (the demand pass), as the column
    ///   `cte_column_name(v, prop)`: the label's mapping, else NULL when
    ///   another label declares the property (Cypher: the node has no such
    ///   property), else the rule of an undeclared one
    ///   ([`Self::label_arm_value`]).
    ///
    /// A union's column takes its arms' common type, converting values
    /// (`Bool` and `UInt8`, `Float32` and `Float64`, `Date` and `DateTime`;
    /// review finding), or with none a `Variant`, which answers differently
    /// (its NULLs are counted, `8` and `8.0` are two values). A property
    /// several labels have a value of is one column per label
    /// ([`union_property_items`]), read through
    /// `FunctionMapper::one_type_guard`: an error unless their types differ
    /// only in what changes no value.
    fn label_union(
        &mut self,
        v: VarId,
        arms: &[(String, &'s NodeSchema)],
    ) -> Result<String, LowerError> {
        let name = format!("{}_labels", v.name());
        let arity = arms[0].1.id_physical_columns().len();
        if arms
            .iter()
            .any(|(_, ns)| ns.id_physical_columns().len() != arity)
        {
            return unsupported("a node whose labels' ids have different arities (S8)");
        }
        const ROW: &str = "e";
        let props = self.label_union_props(v, arms);
        let mut input = Vec::new();
        let definitions: Vec<(&String, Vec<Option<usize>>)> = props
            .iter()
            .map(|p| (p, self.label_value_definitions(p, arms)))
            .collect();
        for (arm, (label, ns)) in arms.iter().enumerate() {
            let mut items = vec![select(
                RenderExpr::Literal(Literal::String(label.clone())),
                LABEL_COLUMN,
            )];
            for (i, c) in ns.id_physical_columns().iter().enumerate() {
                items.push(select(col_at(ROW, c), &indexed_column(LABEL_ID, i)));
            }
            for (p, defs) in &definitions {
                let value = self.label_arm_value(ns, p, arms);
                items.extend(union_property_items(v, p, defs, arm, value));
            }
            let filters = match &ns.filter {
                Some(f) => match f.to_sql(ROW) {
                    Ok(sql) => Some(RenderExpr::Raw(format!("({sql})"))),
                    Err(e) => return unsupported(format!("schema filter: {e}")),
                },
                None => None,
            };
            let table = ViewTableRef::parameterized_name(
                &ns.full_table_name(),
                ns.view_parameters.as_deref(),
                self.options.view_parameter_values.as_ref(),
            );
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table,
                    alias: Some(ROW.to_string()),
                    use_final: ns.should_use_final(),
                })),
                filters: FilterItems(filters),
                ..empty_plan()
            });
        }
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input,
                union_type: UnionType::All,
                is_cypher_union: false,
            })),
            ..empty_plan()
        };
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(body)),
            false,
        ));
        Ok(name)
    }

    /// Property `p` in the arm of `ns` of a node of several labels
    /// ([`Self::label_union`]), as [`Self::arm_value`].
    fn label_arm_value(
        &self,
        ns: &NodeSchema,
        p: &str,
        arms: &[(String, &NodeSchema)],
    ) -> Option<RenderExpr> {
        let declared = arms
            .iter()
            .any(|(_, o)| o.property_mappings.contains_key(p));
        self.arm_value(&ns.property_mappings, ns.closed_properties, declared, p)
    }

    /// Property `p` in one arm of a union (read under `e`) whose element's
    /// label or definition maps properties by `mappings`: its mapping; else
    /// NULL (`None`) when another arm's declares it (`declared`: the element
    /// has no such property, as in Neo4j), for discovered columns (`closed`)
    /// and in Neo4j-compat mode; else, declared by no arm, the
    /// undeclared-property rule ([`Self::property`]: the same-named column, a
    /// missing one a ClickHouse error).
    fn arm_value(
        &self,
        mappings: &HashMap<String, PropertyValue>,
        closed: bool,
        declared: bool,
        p: &str,
    ) -> Option<RenderExpr> {
        match mappings.get(p) {
            Some(pv) => Some(RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("e".to_string()),
                column: pv.clone(),
            })),
            None if closed || self.options.neo4j_compat || declared => None,
            None => Some(col_at("e", p)),
        }
    }

    /// For each arm of a node of several labels, the index of its label
    /// among those with a value of property `p` ([`union_property_items`]).
    fn label_value_definitions(
        &self,
        p: &str,
        arms: &[(String, &NodeSchema)],
    ) -> Vec<Option<usize>> {
        let mut n = 0;
        arms.iter()
            .map(|(_, ns)| {
                self.label_arm_value(ns, p, arms).map(|_| {
                    n += 1;
                    n - 1
                })
            })
            .collect()
    }

    /// The properties the CTE of a node of several labels carries
    /// ([`Self::label_union`]): those read of it, every one when it is read
    /// whole.
    fn label_union_props(&self, v: VarId, arms: &[(String, &NodeSchema)]) -> BTreeSet<String> {
        self.union_props(v, || Self::label_property_names(arms))
    }

    /// The properties read of `v` (the demand pass), `all` of them when it
    /// is read whole.
    fn union_props(&self, v: VarId, all: impl Fn() -> BTreeSet<String>) -> BTreeSet<String> {
        let mut props = BTreeSet::new();
        for p in self.demand.get(&v).into_iter().flatten() {
            if p == ALL_PROPERTIES {
                props.extend(all());
            } else if !p.starts_with('#') {
                props.insert(p.clone());
            }
        }
        props
    }

    /// Every property some label of `arms` declares, by name.
    fn label_property_names(arms: &[(String, &NodeSchema)]) -> BTreeSet<String> {
        arms.iter()
            .flat_map(|(_, ns)| ns.property_mappings.keys().cloned())
            .collect()
    }

    /// The labels a node can have here: one for a table scan, several for a
    /// union of labels, none when it matches nothing.
    fn labels_of(&self, v: VarId) -> Result<Vec<String>, LowerError> {
        match self.scans.get(&v) {
            Some(Scan::Node { label, .. }) => Ok(vec![label.clone()]),
            Some(Scan::Labels { arms, .. }) => Ok(arms.iter().map(|(l, _)| l.clone()).collect()),
            Some(Scan::Impossible) => Ok(Vec::new()),
            _ => unsupported(format!("internal: {v} is not a node")),
        }
    }

    /// Node `v` has label `label` here (a relationship's end that only nodes
    /// of `label` can be): for a node of several possible labels, a filter on
    /// its label column.
    fn holds_label(&mut self, v: VarId, label: &str) -> Result<(), LowerError> {
        if let Some(Scan::Labels { arms, .. }) = self.scans.get(&v) {
            if !arms.iter().any(|(l, _)| l == label) {
                self.empty = true;
                return Ok(());
            }
            let column = self.physical(v, LABEL_COLUMN)?;
            self.filters.push(RenderExpr::OperatorApplicationExp(eq(
                column,
                RenderExpr::Literal(Literal::String(label.to_string())),
            )));
        }
        Ok(())
    }

    /// A node's identity without its label (a union of labels carries it
    /// first): what a relationship's endpoint columns equal.
    fn id_columns(&self, v: VarId) -> Result<Option<Vec<RenderExpr>>, LowerError> {
        let ids = self.identity(v)?;
        Ok(match self.scans.get(&v) {
            Some(Scan::Labels { .. }) => ids.map(|mut ids| ids.split_off(1)),
            _ => ids,
        })
    }

    /// Decide how a relationship is read (once per variable) and tie it to
    /// its endpoints: in the stored orientation, or, undirected, in either
    /// (§4.6 `Alternatives`).
    fn rel_scan(&mut self, r: &PatRel, left: VarId, right: VarId) -> Result<(), LowerError> {
        if let Some((min, max)) = r.length {
            let (from, to) = match r.direction {
                RelDirection::Left => (right, left),
                _ => (left, right),
            };
            return self.path_scan(r, from, to, min, max);
        }
        let (ends, turned) = if self.scans.contains_key(&r.var) {
            match r.direction {
                RelDirection::Right => (self.stored_ends(r.var, left, right)?, false),
                RelDirection::Left => (self.stored_ends(r.var, right, left)?, false),
                RelDirection::Either => (self.turned_ends(r.var, left, right)?, true),
            }
        } else {
            let BindingKind::Rel { types, .. } = &self.binding(r.var).kind else {
                return unsupported("a pattern relationship that is not a relationship binding");
            };
            let (ll, rl) = (self.labels_of(left)?, self.labels_of(right)?);
            let arms = self.rel_arms(types, r.direction, &ll, &rl)?;
            (self.decide_rel(r.var, arms, left, right)?, false)
        };
        for (end, label, cols) in ends {
            self.tie_end(r.var, end, label, cols, turned)?;
        }
        Ok(())
    }

    /// The arms of a relationship of one of `types` written in `direction`
    /// between nodes of the labels `ll` and `rl` (§4.6 `Alternatives`): each
    /// definition the schema has between a left and a right label, as stored
    /// (left to right) or reversed (right to left), as the direction allows.
    /// `get_rel_schema_with_nodes` cannot say which exist: it falls back to a
    /// type's first schema whatever its ends.
    fn rel_arms(
        &self,
        types: &BTreeSet<String>,
        direction: RelDirection,
        ll: &[String],
        rl: &[String],
    ) -> Result<Vec<RelArm<'s>>, LowerError> {
        let mut arms: Vec<RelArm<'s>> = Vec::new();
        for rel_type in types {
            for l in ll {
                for r in rl {
                    let orientations: &[bool] = match direction {
                        RelDirection::Right => &[false],
                        RelDirection::Left => &[true],
                        RelDirection::Either => &[false, true],
                    };
                    for &reversed in orientations {
                        let (from, to) = if reversed { (r, l) } else { (l, r) };
                        if self.defined(rel_type, from, to).is_none() {
                            continue;
                        }
                        let schema = self.edge_schema(rel_type, from, to)?;
                        if schema.from_node != *from || schema.to_node != *to {
                            return unsupported(format!(
                                "type {rel_type} is not the standard layout (S8)"
                            ));
                        }
                        let arm = RelArm {
                            rel_type: rel_type.clone(),
                            schema,
                            reversed,
                        };
                        let known = arms
                            .iter()
                            .any(|a| std::ptr::eq(a.schema, schema) && a.reversed == reversed);
                        if !known {
                            arms.push(arm);
                        }
                    }
                }
            }
        }
        Ok(arms)
    }

    /// The scan of an unbound relationship of these arms (between nodes
    /// `left` and `right`), and the ends its columns tie:
    /// * none: it matches nothing;
    /// * one: its table, in the arm's orientation;
    /// * a definition between nodes of one label, in both orientations (an
    ///   undirected relationship whose two directions are the one table):
    ///   [`Self::both_directions`], its left end tied to the node each row
    ///   leaves and its right end to the node it enters;
    /// * any others: [`Self::rel_union`], each row tying the nodes it leaves
    ///   and enters, with their labels.
    fn decide_rel(
        &mut self,
        r: VarId,
        arms: Vec<RelArm<'s>>,
        left: VarId,
        right: VarId,
    ) -> Result<Ends, LowerError> {
        let at = At::Table(r.name());
        let at_table = |c: &str| col_at(&r.name(), c);
        let scan = match arms.as_slice() {
            [] => Scan::Impossible,
            [arm] => {
                let scan = Scan::Rel {
                    schema: arm.schema,
                    rel_type: arm.rel_type.clone(),
                    at,
                    both: None,
                };
                let (from, to) = if arm.reversed {
                    (right, left)
                } else {
                    (left, right)
                };
                self.scans.insert(r, scan);
                return self.stored_ends(r, from, to);
            }
            [a, b]
                if std::ptr::eq(a.schema, b.schema)
                    && a.reversed != b.reversed
                    && a.schema.from_node == a.schema.to_node =>
            {
                let rs = a.schema;
                let both = self.both_directions(rs, r)?;
                let arity = rs.from_id.columns().len();
                let label = EndLabel::Is(rs.from_node.clone());
                let (starts, ends) = (
                    indexed_columns(BOTH_START, arity),
                    indexed_columns(BOTH_END, arity),
                );
                self.scans.insert(
                    r,
                    Scan::Rel {
                        schema: rs,
                        rel_type: a.rel_type.clone(),
                        at,
                        both: Some(both),
                    },
                );
                return Ok(vec![
                    (
                        left,
                        label.clone(),
                        starts.iter().map(|c| at_table(c)).collect(),
                    ),
                    (right, label, ends.iter().map(|c| at_table(c)).collect()),
                ]);
            }
            _ => {
                let cte = self.rel_union(r, &arms)?;
                let shape = rel_union_shape(&arms)?;
                let side = |left_end: bool, label: &str, ids: &str, arity: usize| {
                    let labels = arms
                        .iter()
                        .map(|a| {
                            let (l, r) = a.ends();
                            (if left_end { l } else { r }).to_string()
                        })
                        .collect();
                    let cols = indexed_columns(ids, arity)
                        .iter()
                        .map(|c| at_table(c))
                        .collect();
                    (EndLabel::of(labels, at_table(label)), cols)
                };
                let (ll, lc) = side(true, REL_START_LABEL, BOTH_START, shape.start);
                let (rl, rc) = side(false, REL_END_LABEL, BOTH_END, shape.end);
                self.scans.insert(
                    r,
                    Scan::Rels {
                        arms,
                        cte,
                        of: r,
                        at,
                    },
                );
                return Ok(vec![(left, ll, lc), (right, rl, rc)]);
            }
        };
        self.scans.insert(r, scan);
        Ok(Vec::new())
    }

    /// The ends a relationship read as stored ties: `from` by its stored
    /// `from` columns and label, `to` by its `to` ones.
    fn stored_ends(&self, r: VarId, from: VarId, to: VarId) -> Result<Ends, LowerError> {
        let physical = |cols: Vec<String>| -> Result<Vec<RenderExpr>, LowerError> {
            cols.iter().map(|c| self.physical(r, c)).collect()
        };
        Ok(match self.scans.get(&r) {
            Some(Scan::Rel { schema, .. }) => {
                let cols = |id: &crate::graph_catalog::config::Identifier| {
                    id.columns().iter().map(|c| c.to_string()).collect()
                };
                vec![
                    (
                        from,
                        EndLabel::Is(schema.from_node.clone()),
                        physical(cols(&schema.from_id))?,
                    ),
                    (
                        to,
                        EndLabel::Is(schema.to_node.clone()),
                        physical(cols(&schema.to_id))?,
                    ),
                ]
            }
            Some(Scan::Rels { arms, .. }) => {
                let shape = rel_union_shape(arms)?;
                let (fa, ta) = (shape.from, shape.to);
                let labels = |to_end: bool| {
                    arms.iter()
                        .map(|a| {
                            if to_end {
                                a.schema.to_node.clone()
                            } else {
                                a.schema.from_node.clone()
                            }
                        })
                        .collect()
                };
                vec![
                    (
                        from,
                        EndLabel::of(labels(false), self.physical(r, REL_FROM_LABEL)?),
                        physical(indexed_columns(REL_FROM, fa))?,
                    ),
                    (
                        to,
                        EndLabel::of(labels(true), self.physical(r, REL_TO_LABEL)?),
                        physical(indexed_columns(REL_TO, ta))?,
                    ),
                ]
            }
            // It matches nothing.
            _ => Vec::new(),
        })
    }

    /// The ends of a relationship bound before, matched undirected: each
    /// row of the relation so far, in both orientations ([`Self::turns`]),
    /// the left end tied to the stored `from` as stored and to the stored
    /// `to` reversed. A self-loop is read once, as Neo4j matches it once.
    fn turned_ends(&mut self, r: VarId, left: VarId, right: VarId) -> Result<Ends, LowerError> {
        if !self.is_emitted(r) {
            // Used twice in one MATCH: a relationship is not two of the
            // clause's (uniqueness), so nothing matches, as in Neo4j.
            self.empty = true;
            return Ok(Vec::new());
        }
        let stored = self.stored_ends(r, left, right)?;
        let Ok([(_, fl, fc), (_, tl, tc)]) = <[_; 2]>::try_from(stored) else {
            return Ok(Vec::new()); // it matches nothing
        };
        if fc.len() != tc.len() {
            return unsupported("an undirected relationship whose ends' ids differ in arity (S8)");
        }
        let turned = self.turns(r)?;
        let pick = |reversed: &RenderExpr, stored: &RenderExpr| {
            RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(turned.clone(), reversed.clone())],
                else_expr: Some(Box::new(stored.clone())),
            })
        };
        let label_value = |l: &EndLabel| match l {
            EndLabel::Is(one) => RenderExpr::Literal(Literal::String(one.clone())),
            EndLabel::By { value, .. } => value.clone(),
        };
        let (fv, tv) = (label_value(&fl), label_value(&tl));
        let both_labels: Vec<String> = fl.labels().into_iter().chain(tl.labels()).collect();
        // A row is a self-loop when its two ends are one node.
        let mut differ: Vec<RenderExpr> = fc
            .iter()
            .zip(&tc)
            .map(|(f, t)| {
                RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::NotEqual,
                    operands: vec![f.clone(), t.clone()],
                })
            })
            .collect();
        let loops = match (&fl, &tl) {
            (EndLabel::Is(a), EndLabel::Is(b)) => a == b,
            _ => {
                differ.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::NotEqual,
                    operands: vec![fv.clone(), tv.clone()],
                }));
                true
            }
        };
        if loops {
            differ.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![turned.clone()],
            }));
            self.filters.push(or_all(differ));
        }
        let end = |a: &[RenderExpr], b: &[RenderExpr]| -> Vec<RenderExpr> {
            a.iter().zip(b).map(|(x, y)| pick(y, x)).collect()
        };
        Ok(vec![
            (
                left,
                EndLabel::of(both_labels.clone(), pick(&tv, &fv)),
                end(&fc, &tc),
            ),
            (
                right,
                EndLabel::of(both_labels, pick(&fv, &tv)),
                end(&tc, &fc),
            ),
        ])
    }

    /// Join the relation so far to its two orientations: the CTE
    /// `{r}_turns{n}` of the rows `TURN` = 0 and 1, joined to every row.
    /// Returns the condition of the reversed one.
    fn turns(&mut self, r: VarId) -> Result<RenderExpr, LowerError> {
        let n = self.ctes.len() + 1;
        let (name, alias) = (
            format!("{}_turns{n}", r.name()),
            format!("{}_t{n}", r.name()),
        );
        let row = |turn: i64| {
            let mut plan = empty_plan();
            plan.select.items = vec![select(RenderExpr::Literal(Literal::Integer(turn)), TURN)];
            plan
        };
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input: vec![row(0), row(1)],
                union_type: UnionType::All,
                is_cypher_union: false,
            })),
            ..empty_plan()
        };
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(body)),
            false,
        ));
        if self.from.is_none() {
            return unsupported("internal: an orientation join with no relation");
        }
        let mut j = join(name, &alias);
        j.joining_on = vec![eq(
            RenderExpr::Literal(Literal::Integer(1)),
            RenderExpr::Literal(Literal::Integer(1)),
        )];
        self.joins.push(j);
        self.emitted.push(alias.clone());
        Ok(RenderExpr::OperatorApplicationExp(eq(
            col_at(&alias, TURN),
            RenderExpr::Literal(Literal::Integer(1)),
        )))
    }

    /// Tie relationship `r`'s columns `cols` to node `end`, and the label
    /// the relationship gives it to its label: an end of several possible
    /// labels holds that one (`Self::holds_label`), or equals the
    /// relationship's label column; one the relationship cannot be at
    /// makes the relation empty. A tie of a relationship read in both
    /// orientations (`turned`) to a node already joined is a WHERE: the
    /// orientation join comes after the node's.
    fn tie_end(
        &mut self,
        r: VarId,
        end: VarId,
        label: EndLabel,
        cols: Vec<RenderExpr>,
        turned: bool,
    ) -> Result<(), LowerError> {
        let labels = self.labels_of(end)?;
        if labels.is_empty() {
            return Ok(()); // an endpoint that matches nothing
        }
        let mut eqs = Vec::new();
        match label {
            EndLabel::Is(l) if labels.contains(&l) => self.holds_label(end, &l)?,
            EndLabel::By {
                labels: held,
                value,
            } if held.iter().any(|l| labels.contains(l)) => match self.scans.get(&end) {
                Some(Scan::Labels { .. }) => {
                    eqs.push((value, self.physical(end, LABEL_COLUMN)?));
                }
                _ => self.filters.push(RenderExpr::OperatorApplicationExp(eq(
                    value,
                    RenderExpr::Literal(Literal::String(labels[0].clone())),
                ))),
            },
            _ => {
                self.empty = true;
                return Ok(());
            }
        }
        let Some(ids) = self.id_columns(end)? else {
            return Ok(());
        };
        if ids.len() != cols.len() {
            return unsupported("endpoint id arity differs from the edge's");
        }
        eqs.extend(cols.into_iter().zip(ids));
        if turned && self.is_emitted(end) {
            self.filters.extend(
                eqs.into_iter()
                    .map(|(a, b)| RenderExpr::OperatorApplicationExp(eq(a, b))),
            );
        } else {
            self.tie(r, end, eqs);
        }
        Ok(())
    }

    /// A relationship of several possible types or label pairs, as one
    /// relation (§4.6 `Alternatives`): the CTE `{v}_rels`, one arm per
    /// definition and orientation ([`RelArm`]) reading its table (with its
    /// `filter:`, view parameters and FINAL), its rows carrying
    ///
    /// * their type and the labels of their stored ends, then their identity
    ///   (`REL_ID_{i}`: the `edge_id`, else the stored ends, #887; NULL past
    ///   an arm's own arity): a relationship is its definition and its
    ///   identity, so two definitions' equal ids are two relationships;
    /// * the ids of their stored ends (`REL_FROM_{i}`, `REL_TO_{i}`), and the
    ///   label and id of the node they leave and enter here;
    /// * each property read of `v` (the demand pass), as on a node of several
    ///   labels ([`Self::arm_value`]): one column per definition where several
    ///   have a value ([`union_property_items`]), read through
    ///   `one_type_guard`.
    ///
    /// A definition between nodes of one label read in both orientations
    /// (undirected) reads a self-loop once, as stored, as Neo4j matches it
    /// once.
    fn rel_union(&mut self, v: VarId, arms: &[RelArm<'s>]) -> Result<String, LowerError> {
        let props = self.rel_union_props(v, arms);
        self.rel_union_of(v, arms, &props, false, false)
    }

    /// [`Self::rel_union`] carrying the properties `props`, and with `values`
    /// each row's relationship as a value (`ELEMENT_VALUE`, for a walk).
    /// Each row also carries its identity as a text (`KEY`) where the
    /// dialect spells one, and, with `node_keys`, those of the nodes it
    /// leaves and enters (`REL_START_KEY` / `REL_END_KEY`, for a
    /// shortest-path search).
    fn rel_union_of(
        &mut self,
        v: VarId,
        arms: &[RelArm<'s>],
        props: &BTreeSet<String>,
        values: bool,
        node_keys: bool,
    ) -> Result<String, LowerError> {
        let name = format!("{}_rels", v.name());
        let shape = rel_union_shape(arms)?;
        const ROW: &str = "e";
        let g = current_function_mapper().graph_values();
        let string = |s: &str| RenderExpr::Literal(Literal::String(s.to_string()));
        let definitions: Vec<(&String, Vec<Option<usize>>)> = props
            .iter()
            .map(|p| (p, self.rel_value_definitions(p, arms)))
            .collect();
        let mut input = Vec::new();
        for (i, arm) in arms.iter().enumerate() {
            let rs = arm.schema;
            let (from, to) = (rs.from_id.columns(), rs.to_id.columns());
            let identity: Vec<&str> = match &rs.edge_id {
                Some(id) => id.columns(),
                None => from.iter().chain(&to).copied().collect(),
            };
            let (left, right) = arm.ends();
            let mut items = vec![
                select(string(&arm.rel_type), REL_TYPE),
                select(string(&rs.from_node), REL_FROM_LABEL),
                select(string(&rs.to_node), REL_TO_LABEL),
            ];
            for i in 0..shape.identity {
                let e = identity
                    .get(i)
                    .map_or(RenderExpr::Literal(Literal::Null), |c| col_at(ROW, c));
                items.push(select(e, &indexed_column(REL_ID, i)));
            }
            let (starts, ends) = if arm.reversed {
                (&to, &from)
            } else {
                (&from, &to)
            };
            for (side, cols) in [
                (REL_FROM, &from),
                (REL_TO, &to),
                (BOTH_START, starts),
                (BOTH_END, ends),
            ] {
                for (i, c) in cols.iter().enumerate() {
                    items.push(select(col_at(ROW, c), &indexed_column(side, i)));
                }
            }
            items.push(select(string(left), REL_START_LABEL));
            items.push(select(string(right), REL_END_LABEL));
            if let Some(g) = &g {
                let ids: Vec<String> = identity
                    .iter()
                    .map(|c| render_expr_to_sql_plain(&col_at(ROW, c)))
                    .collect();
                items.push(select(
                    RenderExpr::Raw(value::rel_key(
                        g,
                        &arm.rel_type,
                        &rs.from_node,
                        &rs.to_node,
                        &ids,
                    )),
                    KEY,
                ));
                if values {
                    items.push(select(
                        RenderExpr::Raw(value::table_rel_object(g, rs, &arm.rel_type, ROW)?),
                        ELEMENT_VALUE,
                    ));
                }
                if node_keys {
                    for (label, cols, column) in
                        [(left, starts, REL_START_KEY), (right, ends, REL_END_KEY)]
                    {
                        let [id] = cols.as_slice() else {
                            return unsupported(
                                "a variable-length relationship between composite ids (S8)",
                            );
                        };
                        items.push(select(
                            RenderExpr::Raw(value::node_key_text(
                                g,
                                &value::string(label),
                                &render_expr_to_sql_plain(&col_at(ROW, id)),
                            )),
                            column,
                        ));
                    }
                }
            } else if node_keys {
                return unsupported(
                    "a shortestPath over several labels or types in this SQL dialect",
                );
            }
            for (p, defs) in &definitions {
                let value = self.rel_arm_value(rs, p, arms);
                items.extend(union_property_items(v, p, defs, i, value));
            }
            let mut filters = Vec::new();
            if let Some(f) = &rs.filter {
                match f.to_sql(ROW) {
                    Ok(sql) => filters.push(RenderExpr::Raw(format!("({sql})"))),
                    Err(e) => return unsupported(format!("schema filter: {e}")),
                }
            }
            let read_both_ways = arms
                .iter()
                .any(|o| std::ptr::eq(o.schema, rs) && o.reversed != arm.reversed);
            if arm.reversed && read_both_ways && rs.from_node == rs.to_node {
                let differ = from
                    .iter()
                    .zip(&to)
                    .map(|(f, t)| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::NotEqual,
                            operands: vec![col_at(ROW, f), col_at(ROW, t)],
                        })
                    })
                    .collect();
                filters.push(or_all(differ));
            }
            let table = ViewTableRef::parameterized_name(
                &rs.full_table_name(),
                rs.view_parameters.as_deref(),
                self.options.view_parameter_values.as_ref(),
            );
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table,
                    alias: Some(ROW.to_string()),
                    use_final: rs.should_use_final(),
                })),
                filters: FilterItems(and_all(filters)),
                ..empty_plan()
            });
        }
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input,
                union_type: UnionType::All,
                is_cypher_union: false,
            })),
            ..empty_plan()
        };
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(body)),
            false,
        ));
        Ok(name)
    }

    /// Property `p` in the arm of definition `rs` of a relationship union
    /// ([`Self::rel_union`]), as [`Self::arm_value`].
    fn rel_arm_value(
        &self,
        rs: &RelationshipSchema,
        p: &str,
        arms: &[RelArm],
    ) -> Option<RenderExpr> {
        let declared = arms
            .iter()
            .any(|a| a.schema.property_mappings.contains_key(p));
        self.arm_value(&rs.property_mappings, rs.closed_properties, declared, p)
    }

    /// For each arm of a relationship union, the index of its definition
    /// among those with a value of property `p` (the two orientations of
    /// one definition share it; [`union_property_items`]).
    fn rel_value_definitions(&self, p: &str, arms: &[RelArm]) -> Vec<Option<usize>> {
        let mut valued: Vec<&RelationshipSchema> = Vec::new();
        arms.iter()
            .map(|a| {
                self.rel_arm_value(a.schema, p, arms)?;
                Some(
                    match valued.iter().position(|s| std::ptr::eq(*s, a.schema)) {
                        Some(k) => k,
                        None => {
                            valued.push(a.schema);
                            valued.len() - 1
                        }
                    },
                )
            })
            .collect()
    }

    /// The properties the CTE of a relationship union carries
    /// ([`Self::rel_union`]): those read of it, every one when it is read
    /// whole.
    fn rel_union_props(&self, v: VarId, arms: &[RelArm]) -> BTreeSet<String> {
        self.union_props(v, || Self::rel_property_names(arms))
    }

    /// Every property some definition of `arms` declares, by name.
    fn rel_property_names(arms: &[RelArm]) -> BTreeSet<String> {
        arms.iter()
            .flat_map(|a| a.schema.property_mappings.keys().cloned())
            .collect()
    }

    /// The table of an undirected relationship whose two directions are the
    /// one table, read in both (§4.6 `Alternatives`): the CTE `{v}_both` of
    /// its rows, each once as stored and once reversed, with the identities
    /// of the node the row leaves and enters (`__cg_start_{i}` /
    /// `__cg_end_{i}`). The stored columns it carries keep their names, so a
    /// row's identity, endpoints and properties are the relationship's,
    /// whichever way it is read. A self-loop is read once (reversed, it is
    /// the same row), as Neo4j matches it once.
    ///
    /// It carries the columns named, not `*`: ClickHouse leaves ALIAS and
    /// MATERIALIZED columns out of `*`. They are the identity and endpoint
    /// columns, those of every property mapping, and the same-named column of
    /// each undeclared property read of `v` ([`Self::property`]).
    fn both_directions(
        &mut self,
        rs: &'s RelationshipSchema,
        v: VarId,
    ) -> Result<String, LowerError> {
        let name = format!("{}_both", v.name());
        let (from, to) = (rs.from_id.columns(), rs.to_id.columns());
        if from.len() != to.len() {
            return unsupported("endpoint id arity differs from the edge's");
        }
        const ROW: &str = "e";
        let mut carried: BTreeSet<String> = from.iter().chain(&to).map(|c| c.to_string()).collect();
        if let Some(id) = &rs.edge_id {
            carried.extend(id.columns().iter().map(|c| c.to_string()));
        }
        for pv in rs.property_mappings.values() {
            carried.extend(pv.get_columns());
        }
        if !rs.closed_properties && !self.options.neo4j_compat {
            let read = self.demand.get(&v).into_iter().flatten();
            // Not the demand pass's markers: every property (`*`, read through
            // the mappings) or a path's (`#…`).
            let undeclared = |p: &&String| {
                p.as_str() != ALL_PROPERTIES
                    && !p.starts_with('#')
                    && !rs.property_mappings.contains_key(*p)
            };
            carried.extend(read.filter(undeclared).cloned());
        }
        let filter = match &rs.filter {
            Some(f) => match f.to_sql(ROW) {
                Ok(sql) => Some(RenderExpr::Raw(format!("({sql})"))),
                Err(e) => return unsupported(format!("schema filter: {e}")),
            },
            None => None,
        };
        let table = ViewTableRef::parameterized_name(
            &rs.full_table_name(),
            rs.view_parameters.as_deref(),
            self.options.view_parameter_values.as_ref(),
        );
        let arm = |forward: bool| {
            let mut items: Vec<SelectItem> =
                carried.iter().map(|c| select(col_at(ROW, c), c)).collect();
            let (starts, ends) = if forward { (&from, &to) } else { (&to, &from) };
            for (i, c) in starts.iter().enumerate() {
                items.push(select(col_at(ROW, c), &indexed_column(BOTH_START, i)));
            }
            for (i, c) in ends.iter().enumerate() {
                items.push(select(col_at(ROW, c), &indexed_column(BOTH_END, i)));
            }
            let mut filters: Vec<RenderExpr> = filter.iter().cloned().collect();
            if !forward {
                // A self-loop is read forward.
                let differ = from
                    .iter()
                    .zip(&to)
                    .map(|(f, t)| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::NotEqual,
                            operands: vec![col_at(ROW, f), col_at(ROW, t)],
                        })
                    })
                    .collect();
                filters.push(or_all(differ));
            }
            RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table.clone(),
                    alias: Some(ROW.to_string()),
                    use_final: rs.should_use_final(),
                })),
                filters: FilterItems(and_all(filters)),
                ..empty_plan()
            }
        };
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input: vec![arm(true), arm(false)],
                union_type: UnionType::All,
                is_cypher_union: false,
            })),
            ..empty_plan()
        };
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(body)),
            false,
        ));
        Ok(name)
    }

    /// Decide how a variable-length relationship is read: a relation of
    /// paths (`path.rs`), generated by [`Self::build_path`] once the clause's
    /// conditions are known, and tied to its endpoints. A relationship of
    /// one type of one definition between nodes of its one label is the
    /// generator's walk of its table (`Walked::One`); any other is a walk of
    /// its definitions between nodes of any labels (`Walked::Union`, S7b3a).
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
        let types = types.clone();
        let impossible = |v: VarId| matches!(self.scans.get(&v), Some(Scan::Impossible));
        let path = |walked: Walked<'s>, range: (u32, Option<u32>)| Scan::Path {
            walked,
            cte: String::new(),
            at: At::Table(r.var.name()),
            edges: false,
            nodes: false,
            range,
            shortest: None,
            reversed: false,
            node_values: false,
            rel_values: false,
        };
        let scan = if max.is_some_and(|m| m < min) || impossible(from) || impossible(to) {
            Scan::Impossible
        } else if let Some(scan) = self.one_definition_path(r.var, &types, from, to, min, max)? {
            scan
        } else if types.is_empty() && min > 0 {
            // No feasible type: only the path of none could match.
            Scan::Impossible
        } else {
            path(
                Walked::Union {
                    types,
                    arms: Vec::new(),
                },
                (min, max),
            )
        };
        if let Scan::Path { .. } = &scan {
            for end in [from, to] {
                if self.id_columns(end)?.is_some_and(|ids| ids.len() != 1) {
                    return unsupported(
                        "a variable-length relationship between composite ids (S8)",
                    );
                }
            }
        }
        self.scans.insert(r.var, scan);
        Ok(())
    }

    /// The scan of a variable-length relationship of one type of one
    /// definition between nodes of its one label (`Walked::One`, the
    /// generator), or `None` for any other.
    fn one_definition_path(
        &self,
        r: VarId,
        types: &BTreeSet<String>,
        from: VarId,
        to: VarId,
        min: u32,
        max: Option<u32>,
    ) -> Result<Option<Scan<'s>>, LowerError> {
        let types: Vec<&String> = types.iter().collect();
        let ([rel_type], Some(fl), Some(tl)) = (
            types.as_slice(),
            self.single_label(from),
            self.single_label(to),
        ) else {
            return Ok(None);
        };
        let rel_type = (*rel_type).clone();
        let schemas = self.schema.rel_schemas_for_type(&rel_type);
        let [only] = schemas.as_slice() else {
            return Ok(None);
        };
        // Every node of a path of one or more relationships has the edge's
        // one label.
        if only.from_node != only.to_node {
            return Ok(None);
        }
        let rs = match self.defined(&rel_type, &fl, &tl) {
            Some(_) => self.edge_schema(&rel_type, &fl, &tl)?,
            // The type does not join these labels: only the path of none can
            // match (below), generated over its table.
            None => Self::standard_edge(&rel_type, only)?,
        };
        // With another label at either end only the path of none is left: a
        // node to itself.
        let walks = fl == rs.from_node && tl == rs.to_node;
        if !walks && (fl != tl || min > 0) {
            return Ok(Some(Scan::Impossible));
        }
        let range = if walks { (min, max) } else { (0, Some(0)) };
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
        Ok(Some(Scan::Path {
            walked: Walked::One {
                schema: rs,
                rel_type,
            },
            cte: String::new(),
            at: At::Table(r.name()),
            edges: false,
            nodes: false,
            range,
            shortest: None,
            reversed: false,
            node_values: false,
            rel_values: false,
        }))
    }

    /// Generate the relation of paths of a variable-length relationship
    /// (decided by [`Self::path_scan`]) when it joins the rows so far (its
    /// `left` node is joined), and tie it to its endpoints: `start_id` is
    /// the node the walk starts at, `end_id` the other. The walk starts at a
    /// restricted end (§4.8 d): one whose identity equals a constant, else
    /// one carried from a CTE, else one with conjuncts over its own columns,
    /// else one the rows so far restrict, the left one first; it follows the relationships forward or backward
    /// from there. Inside the search go:
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
        own: &[RenderExpr],
        in_search: &[RenderExpr],
    ) -> Result<(), LowerError> {
        let Some(Scan::Path {
            walked,
            at,
            range: (min, max),
            shortest,
            ..
        }) = self.scans.get(&r.var).cloned()
        else {
            return Ok(()); // it matches nothing
        };
        let (edge, rel_type) = match walked {
            Walked::One { schema, rel_type } => (schema, rel_type),
            Walked::Union { types, .. } => {
                let ends = (left, right);
                return self.build_union_path(
                    r,
                    types,
                    at,
                    (min, max),
                    shortest,
                    ends,
                    own,
                    in_search,
                );
            }
        };
        // The `from` end of the relationships (stored orientation). An
        // undirected relationship is walked in both directions, from the
        // first end, whichever it is.
        let undirected = r.direction == RelDirection::Either;
        let from_end = if r.direction == RelDirection::Left {
            right
        } else {
            left
        };
        let first = self.walk_first(left, right, own);
        let last = if first == left { right } else { left };
        let backward = !undirected && first != from_end;
        // Its nodes / relationships read as values (the demand pass): the
        // search carries them. A shortest path's are recovered from its
        // search also when its identity is read (`PATH_KEY`).
        let wants = |name: &str| self.demand.get(&r.var).is_some_and(|d| d.contains(name));
        let (node_values, rel_values) = (wants(NODE_VALUES), wants(REL_VALUES));
        let walked = node_values || rel_values || wants(PATH_KEY);
        let Some(Scan::Node {
            schema: node,
            label,
            ..
        }) = self.scans.get(&first).cloned()
        else {
            return unsupported("internal: a path endpoint is not a node");
        };
        let Some(start) = self.restriction(first, own, path::START)? else {
            return Ok(()); // it matches nothing
        };
        // A shortest-path search ends once it has reached every value the
        // last node can have.
        let end = match shortest {
            Some(_) => match self.restriction(last, own, path::END)? {
                Some(end) => end,
                None => return Ok(()),
            },
            None => Vec::new(),
        };
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
        let var = r.var.name();
        let both = match undirected {
            true => Some(self.both_directions(edge, r.var)?),
            false => None,
        };
        let call = path::PathCall {
            var: &var,
            rel_type: &rel_type,
            edge,
            label: &label,
            node,
            min,
            max,
            backward,
            both: both.as_deref(),
            start,
            end,
            rel,
            node_values,
            rel_values,
        };
        let (ctes, cte, edges) = match shortest {
            None => {
                let built = path::path_cte(self.schema, call)?;
                let cte = built.cte.cte_name.clone();
                (vec![built.cte], cte, built.edges)
            }
            Some(mode) => {
                match self.shortest_relation(
                    path::Walk::One(call),
                    mode,
                    walked,
                    at.alias(),
                    first,
                    last,
                    in_search,
                )? {
                    Some(relation) => relation,
                    None => {
                        // Its conditions allow no length of its range.
                        self.scans.insert(r.var, Scan::Impossible);
                        return Ok(());
                    }
                }
            }
        };
        self.ctes.extend(ctes);
        self.scans.insert(
            r.var,
            Scan::Path {
                walked: Walked::One {
                    schema: edge,
                    rel_type,
                },
                cte,
                at,
                edges,
                nodes: shortest.is_none() || walked,
                range: (min, max),
                shortest,
                // The walk follows the stored direction unless `backward`;
                // the path's order follows it when the pattern points right.
                // Undirected, it goes from the first end.
                reversed: match undirected {
                    true => first != left,
                    false => backward != (r.direction == RelDirection::Left),
                },
                node_values,
                rel_values,
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

    /// Generate the relation of paths of a variable-length relationship of
    /// several types, definitions or labels (`Walked::Union`, §4.6
    /// `Alternatives`, S7b3a) and tie it to its endpoints. The walk starts at
    /// the end [`Self::walk_first`] picks, restricted as a directed walk's
    /// first node is ([`Self::walk_restriction`]), and follows the arms
    /// ([`Self::walk_arms`]) in the orientations that lead away from it: a
    /// relationship leaves the node the one before it entered, by label and
    /// id. Its nodes are each a row of the nodes it can visit
    /// ([`Self::walk_nodes`]), its relationships each a row of their union
    /// ([`Self::rel_union_of`]), which keeps the relationship's property map
    /// inside the walk (one value per definition, as a property of a union
    /// is read).
    ///
    /// A `shortestPath` / `allShortestPaths` (S7b3b) is the search of
    /// [`Self::shortest_relation`] over the same nodes and relationships, a
    /// node identified by its identity as a text (`path::UnionWalk::search`),
    /// its last node restricted as its first is; its pairs' ends are then
    /// the nodes of those identities (`path::union_ends_cte`).
    #[allow(clippy::too_many_arguments)]
    fn build_union_path(
        &mut self,
        r: &PatRel,
        types: BTreeSet<String>,
        at: At,
        (min, max): (u32, Option<u32>),
        shortest: Option<ShortestMode>,
        (left, right): (VarId, VarId),
        own: &[RenderExpr],
        in_search: &[RenderExpr],
    ) -> Result<(), LowerError> {
        let first = self.walk_first(left, right, own);
        let last = if first == left { right } else { left };
        let orientations: &[bool] = match (r.direction, first == left) {
            (RelDirection::Either, _) => &[false, true],
            (RelDirection::Right, true) | (RelDirection::Left, false) => &[false],
            _ => &[true],
        };
        let (first_labels, last_labels) = (self.labels_of(first)?, self.labels_of(last)?);
        let arms = match max {
            Some(0) => Vec::new(),
            _ => self.walk_arms(&types, orientations, &first_labels, &last_labels)?,
        };
        if arms.is_empty() && min > 0 {
            self.scans.insert(r.var, Scan::Impossible);
            return Ok(());
        }
        let mut labels = first_labels.clone();
        for a in &arms {
            let (s, e) = a.ends();
            labels.extend([s.to_string(), e.to_string()]);
        }
        labels.sort();
        labels.dedup();
        let Some(start) = self.walk_restriction(first, own, path::START, &labels)? else {
            self.scans.insert(r.var, Scan::Impossible);
            return Ok(());
        };
        // A shortest-path search ends once it has reached every value the
        // last node can have.
        let end = match shortest {
            Some(_) => match self.walk_restriction(last, own, path::END, &labels)? {
                Some(end) => end,
                None => {
                    self.scans.insert(r.var, Scan::Impossible);
                    return Ok(());
                }
            },
            None => Vec::new(),
        };
        let wants = |name: &str| self.demand.get(&r.var).is_some_and(|d| d.contains(name));
        let (node_values, rel_values) = (wants(NODE_VALUES), wants(REL_VALUES));
        // A shortest path's are recovered from its search also when its
        // identity is read.
        let walked = node_values || rel_values || wants(PATH_KEY);
        let nodes = self.walk_nodes(r.var, &labels, node_values)?;
        let props: BTreeSet<String> = r.props.iter().map(|(p, _)| p.clone()).collect();
        let rels = match arms.is_empty() {
            true => None,
            false => {
                Some(self.rel_union_of(r.var, &arms, &props, rel_values, shortest.is_some())?)
            }
        };
        let mut rel = Vec::new();
        for (prop, value) in &r.props {
            let defs = self.rel_value_definitions(prop, &arms);
            let column = self.one_type(path::REL, r.var, prop, &defs)?;
            let value = self.expr(value, &HashMap::new())?;
            if !is_constant(&value) {
                return unsupported(
                    "a variable-length relationship's property map reading a variable",
                );
            }
            rel.push(RenderExpr::OperatorApplicationExp(eq(column, value)));
        }
        let var = r.var.name();
        let walk = path::UnionWalk {
            var: &var,
            nodes: &nodes,
            rels: rels.as_deref(),
            min,
            max,
            start,
            end,
            rel,
            node_values,
            rel_values,
            keyed: shortest.is_some(),
        };
        let alias = at.alias().to_string();
        let (ctes, cte, edges) = match shortest {
            None => {
                let (ctes, cte) = path::union_path_ctes(&walk)?;
                (ctes, cte, true)
            }
            Some(mode) => {
                let columns = match walked {
                    true => walk.search().walked_columns(),
                    false => Vec::new(),
                };
                let found = self.shortest_relation(
                    path::Walk::Union(walk),
                    mode,
                    walked,
                    &alias,
                    first,
                    last,
                    in_search,
                )?;
                let Some((mut ctes, keyed, edges)) = found else {
                    // Its conditions allow no length of its range.
                    self.scans.insert(r.var, Scan::Impossible);
                    return Ok(());
                };
                let (ends, cte) = path::union_ends_cte(&var, &nodes, &keyed, &columns);
                ctes.push(ends);
                (ctes, cte, edges)
            }
        };
        self.ctes.extend(ctes);
        self.scans.insert(
            r.var,
            Scan::Path {
                walked: Walked::Union { types, arms },
                cte,
                at,
                edges,
                nodes: shortest.is_none() || walked,
                range: (min, max),
                shortest,
                // The walk's order is the path's from its left end.
                reversed: first != left,
                node_values,
                rel_values,
            },
        );
        let column = |c: &str| col_at(&alias, c);
        let ends = [
            (first, first_labels, path::START_LABEL, "start_id"),
            (last, labels, path::END_LABEL, "end_id"),
        ];
        for (end, held, label, id) in ends {
            let label = EndLabel::of(held, column(label));
            self.tie_end(r.var, end, label, vec![column(id)], false)?;
        }
        Ok(())
    }

    /// The arms a walk of a relationship of one of `types` follows
    /// (`Walked::Union`): every definition of each type, in each of
    /// `orientations` (reversed or as stored), that a path from a node of
    /// `from_labels` to a node of `to_labels` can use: its rows leave a label
    /// a walk from `from_labels` reaches, and enter one from which it reaches
    /// `to_labels` (a relationship leaves a node of the label the one before
    /// entered). The others match nothing here, whatever their layout.
    fn walk_arms(
        &self,
        types: &BTreeSet<String>,
        orientations: &[bool],
        from_labels: &[String],
        to_labels: &[String],
    ) -> Result<Vec<RelArm<'s>>, LowerError> {
        let mut all: Vec<RelArm<'s>> = Vec::new();
        for rel_type in types {
            let definitions = self.schema.rel_schemas_for_type(rel_type);
            if definitions.is_empty() {
                return unsupported(format!("type {rel_type} has no schema"));
            }
            for rs in definitions {
                for &reversed in orientations {
                    if !all
                        .iter()
                        .any(|a| std::ptr::eq(a.schema, rs) && a.reversed == reversed)
                    {
                        all.push(RelArm {
                            rel_type: rel_type.clone(),
                            schema: rs,
                            reversed,
                        });
                    }
                }
            }
        }
        // Which labels a definition joins is what the pruning below reads:
        // one whose rows carry their own (a polymorphic edge) is not lowered
        // whether or not a path could use it.
        if let Some(a) = all.iter().find(|a| !a.schema.has_fixed_endpoint_labels()) {
            return unsupported(format!(
                "type {} is not the standard layout (S8)",
                a.rel_type
            ));
        }
        // The labels reached from `from_labels` (forward), and those that
        // reach `to_labels` (backward).
        let closure = |seed: &[String], forward: bool| {
            let mut held: BTreeSet<String> = seed.iter().cloned().collect();
            loop {
                let mut more = false;
                for a in &all {
                    let (s, e) = a.ends();
                    let (at, to) = if forward { (s, e) } else { (e, s) };
                    if held.contains(at) && held.insert(to.to_string()) {
                        more = true;
                    }
                }
                if !more {
                    return held;
                }
            }
        };
        let (reached, reaching) = (closure(from_labels, true), closure(to_labels, false));
        let arms: Vec<RelArm<'s>> = all
            .iter()
            .filter(|a| {
                let (s, e) = a.ends();
                reached.contains(s) && reaching.contains(e)
            })
            .cloned()
            .collect();
        for a in &arms {
            Self::standard_edge(&a.rel_type, a.schema)?;
            for label in [&a.schema.from_node, &a.schema.to_node] {
                match self.schema.node_schema_opt(label) {
                    Some(ns) if ns.is_standard_own_table() => {}
                    Some(_) => {
                        return unsupported(format!(
                            "label {label} is not the standard layout (S8)"
                        ))
                    }
                    None => return unsupported(format!("label {label} has no node schema")),
                }
            }
        }
        if !arms.is_empty() {
            let shape = rel_union_shape(&arms)?;
            if shape.start != 1 || shape.end != 1 {
                return unsupported("a variable-length relationship between composite ids (S8)");
            }
        }
        Ok(arms)
    }

    /// The nodes a walk of `v` can visit (`Walked::Union`), of `labels`, as
    /// one relation: the CTE `vlp_{v}_nodes`, an arm per label reading its
    /// table (with its `filter:`, view parameters and FINAL), its rows
    /// carrying their label (`LABEL_COLUMN`), id (`LABEL_ID_0`), identity as
    /// a text (`KEY`: as a path's identity spells a node) and, with `values`,
    /// their value (`ELEMENT_VALUE`). A walk joins a node of each
    /// relationship it follows here, so it visits only nodes there are.
    fn walk_nodes(
        &mut self,
        v: VarId,
        labels: &[String],
        values: bool,
    ) -> Result<String, LowerError> {
        let g = value::spelling()?;
        let name = format!("vlp_{}_nodes", v.name());
        const ROW: &str = "e";
        let mut input = Vec::new();
        for label in labels {
            let Some(ns) = self.schema.node_schema_opt(label) else {
                return unsupported(format!("label {label} has no node schema"));
            };
            if !ns.is_standard_own_table() {
                return unsupported(format!("label {label} is not the standard layout (S8)"));
            }
            let [id] = ns.id_physical_columns().try_into().map_err(|_| {
                LowerError::Unsupported(
                    "a variable-length relationship between composite ids (S8)".to_string(),
                )
            })?;
            let id = col_at(ROW, &id);
            let mut items = vec![
                select(
                    RenderExpr::Literal(Literal::String(label.clone())),
                    LABEL_COLUMN,
                ),
                select(id.clone(), &indexed_column(LABEL_ID, 0)),
                select(
                    RenderExpr::Raw(value::node_key_text(
                        &g,
                        &value::string(label),
                        &render_expr_to_sql_plain(&id),
                    )),
                    KEY,
                ),
            ];
            if values {
                items.push(select(
                    RenderExpr::Raw(value::table_node_object(&g, ns, label, ROW)?),
                    ELEMENT_VALUE,
                ));
            }
            let filters = match &ns.filter {
                Some(f) => match f.to_sql(ROW) {
                    Ok(sql) => Some(RenderExpr::Raw(format!("({sql})"))),
                    Err(e) => return unsupported(format!("schema filter: {e}")),
                },
                None => None,
            };
            let table = ViewTableRef::parameterized_name(
                &ns.full_table_name(),
                ns.view_parameters.as_deref(),
                self.options.view_parameter_values.as_ref(),
            );
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    source: Arc::new(LogicalPlan::Empty),
                    name: table,
                    alias: Some(ROW.to_string()),
                    use_final: ns.should_use_final(),
                })),
                filters: FilterItems(filters),
                ..empty_plan()
            });
        }
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input,
                union_type: UnionType::All,
                is_cypher_union: false,
            })),
            ..empty_plan()
        };
        self.ctes.push(Cte::new(
            name.clone(),
            CteContent::Structured(Box::new(body)),
            false,
        ));
        Ok(name)
    }

    /// Conditions on a row of a walk's nodes ([`Self::walk_nodes`], of
    /// labels `among`) read under `alias` that hold of the values node `v`
    /// has in the result: one of its labels (unless it has each of
    /// `among`), and, when the rows so far or its own conjuncts restrict it,
    /// `(label, id) IN (SELECT DISTINCT …)` of those ([`Self::rows_holding`],
    /// else `v`'s own relation under those conjuncts). `None` when it
    /// matches nothing.
    fn walk_restriction(
        &self,
        v: VarId,
        own: &[RenderExpr],
        alias: &str,
        among: &[String],
    ) -> Result<Option<Vec<RenderExpr>>, LowerError> {
        let labels = self.labels_of(v)?;
        if labels.is_empty() {
            return Ok(None);
        }
        let (label_column, id_column) = (
            col_at(alias, LABEL_COLUMN),
            col_at(alias, &indexed_column(LABEL_ID, 0)),
        );
        let mut conds = Vec::new();
        if among.iter().any(|l| !labels.contains(l)) {
            conds.push(or_all(
                labels
                    .iter()
                    .map(|l| {
                        RenderExpr::OperatorApplicationExp(eq(
                            label_column.clone(),
                            RenderExpr::Literal(Literal::String(l.clone())),
                        ))
                    })
                    .collect(),
            ));
        }
        let rows = match self.rows_holding(v, own) {
            Some(rows) => Some(rows),
            None => {
                let conjuncts: Vec<RenderExpr> =
                    self.own_conjuncts(v, own).into_iter().cloned().collect();
                let source = match self.scans.get(&v) {
                    Some(Scan::Node {
                        schema,
                        at: At::Table(a),
                        ..
                    }) => Some((
                        ViewTableRef::parameterized_name(
                            &schema.full_table_name(),
                            schema.view_parameters.as_deref(),
                            self.options.view_parameter_values.as_ref(),
                        ),
                        a.clone(),
                        schema.should_use_final(),
                    )),
                    Some(Scan::Labels {
                        cte,
                        at: At::Table(a),
                        ..
                    }) => Some((cte.clone(), a.clone(), false)),
                    _ => None,
                };
                match (conjuncts.is_empty(), source) {
                    (false, Some((name, a, use_final))) => {
                        let mut rows = empty_plan();
                        rows.from = FromTableItem(Some(ViewTableRef {
                            source: Arc::new(LogicalPlan::Empty),
                            name,
                            alias: Some(a),
                            use_final,
                        }));
                        rows.filters = FilterItems(and_all(conjuncts));
                        Some(rows)
                    }
                    _ => None,
                }
            }
        };
        if let Some(mut rows) = rows {
            let Some(ids) = self.identity(v)? else {
                return Ok(None);
            };
            let (label, id) = match (self.scans.get(&v), ids.as_slice()) {
                (Some(Scan::Labels { .. }), [label, id]) => (label.clone(), id.clone()),
                (Some(Scan::Node { label, .. }), [id]) => (
                    RenderExpr::Literal(Literal::String(label.clone())),
                    id.clone(),
                ),
                _ => {
                    return unsupported("a variable-length relationship between composite ids (S8)")
                }
            };
            rows.select = SelectItems {
                items: vec![select(label, "label"), select(id, "id")],
                distinct: true,
            };
            conds.push(RenderExpr::Raw(format!(
                "({}, {}) IN ({})",
                render_expr_to_sql_plain(&label_column),
                render_expr_to_sql_plain(&id_column),
                crate::sql_generator::emitters::clickhouse::to_sql_query::render_plan_to_sql_plain(
                    rows
                )
                .trim_end()
            )));
        }
        Ok(Some(conds))
    }

    /// The end a variable-length relationship between `left` and `right` is
    /// walked from: a restricted one (§4.8 d). Without table statistics
    /// (P-5): an end whose identity equals a constant is one node; an end
    /// carried from a CTE (a WITH, the OPTIONAL drive) holds values already
    /// narrowed; then an end with conjuncts over its own columns; then one
    /// tied to the rows so far; the left one first.
    fn walk_first(&self, left: VarId, right: VarId, own: &[RenderExpr]) -> VarId {
        let pinned = |v: VarId| self.pinned(v, own);
        let carried = |v: VarId| {
            self.is_emitted(v)
                && matches!(
                    self.scans.get(&v).and_then(Scan::at),
                    Some(At::Exported { .. })
                )
        };
        let own_columns = |v| !self.own_conjuncts(v, own).is_empty();
        [
            &pinned as &dyn Fn(VarId) -> bool,
            &carried,
            &own_columns,
            &|v| self.joined_rows_restrict(v),
        ]
        .iter()
        .find_map(|restricts| {
            if restricts(left) {
                Some(left)
            } else if restricts(right) {
                Some(right)
            } else {
                None
            }
        })
        .unwrap_or(left)
    }

    /// Conditions every value node `v` has in the result satisfies, over its
    /// node table read under `alias` (§4.11): the conjuncts of the segment
    /// and `own` over its own columns, and, when the rows so far restrict
    /// it, `alias.id IN (SELECT DISTINCT <its identity> FROM <those rows>)`
    /// (`Self::rows_holding`). `None` when it matches nothing.
    fn restriction(
        &self,
        v: VarId,
        own: &[RenderExpr],
        alias: &str,
    ) -> Result<Option<Vec<RenderExpr>>, LowerError> {
        let Some(Scan::Node { schema, at, .. }) = self.scans.get(&v) else {
            return Ok(None);
        };
        let mut conds = Vec::new();
        if let At::Table(own_alias) = at {
            for c in self.own_conjuncts(v, own) {
                conds.push(realias(c, own_alias, alias));
            }
        }
        if let Some(mut rows) = self.rows_holding(v, own) {
            let Some(id) = self.identity(v)? else {
                return Ok(None);
            };
            rows.select = SelectItems {
                items: vec![select(id[0].clone(), "id")],
                distinct: true,
            };
            let id_column = schema.id_physical_columns();
            conds.push(RenderExpr::Raw(format!(
                "{} IN ({})",
                render_expr_to_sql_plain(&col_at(alias, &id_column[0])),
                crate::sql_generator::emitters::clickhouse::to_sql_query::render_plan_to_sql_plain(
                    rows
                )
                .trim_end()
            )));
        }
        Ok(Some(conds))
    }

    /// The relation of a `shortestPath` / `allShortestPaths` (`mode`) whose
    /// paths are `walk`'s (of one table, or of a union, whose relation's ends
    /// are then identities as texts), read under `alias`, from `first` to `last` (its
    /// CTEs, its name, whether it has `path_edges`), or `None` when its
    /// conditions allow no length of its range. The search picks, per pair
    /// of ends, the shortest paths that satisfy `in_search` (§4.8, #1312):
    /// the WHERE conjuncts that read the path, and only it and its ends. In
    /// S6b those depend on a path only through its length.
    /// * A bound on the length from above (`length(p) < k`, `<= k`, `= k`)
    ///   bounds the search.
    /// * A breadth-first search (`path::search_cte`) finds each pair's
    ///   distance: a shortest path is a shortest walk. Without other
    ///   conditions (and a lower bound of at most 1), its pairs are the
    ///   relation (`path::reached_cte`).
    /// * Otherwise a pair whose distance satisfies them has its shortest
    ///   paths; for the others the pick is among the trails of the range that
    ///   satisfy them (`path::pick_cte`), from only their first nodes. A
    ///   condition such as `length(p) > 1` can make a trail that revisits a
    ///   node the shortest (as Neo4j's exhaustive search does), and a lower
    ///   bound above 1 is such a condition.
    ///
    /// A pair whose two ends are one node has a path only when the range
    /// starts at 0 (the path of none): Neo4j raises an error for such a pair
    /// otherwise, unless `cypher.forbid_shortestpath_common_nodes` is off, when
    /// it has no path.
    ///
    /// When `walked` (its values or its identity are read, S6d), the paths
    /// themselves are recovered from the search (`path::walk_ctes`): the
    /// relation has `path_nodes`, `path_edges` and the values `call` asks
    /// for, and a pair of `allShortestPaths` has a row per path, not copies.
    #[allow(clippy::too_many_arguments)]
    fn shortest_relation(
        &self,
        mut walk: path::Walk<'_>,
        mode: ShortestMode,
        walked: bool,
        alias: &str,
        first: VarId,
        last: VarId,
        in_search: &[RenderExpr],
    ) -> Result<Option<(Vec<Cte>, String, bool)>, LowerError> {
        if current_function_mapper().shortest_path_search().is_none() {
            return unsupported("shortestPath in this SQL dialect");
        }
        let all = mode == ShortestMode::AllShortest;
        // A bound on the length from above is a bound on the search: no
        // longer path satisfies it.
        let mut conditions = Vec::new();
        for c in in_search {
            match length_bound(c, alias) {
                Some((bound, exact)) => {
                    if bound < i64::from(walk.min()) {
                        return Ok(None);
                    }
                    let bound = u32::try_from(bound).unwrap_or(u32::MAX);
                    let max = walk.max_mut();
                    *max = Some(max.map_or(bound, |m| m.min(bound)));
                    if !exact {
                        conditions.push(c.clone());
                    }
                }
                None => conditions.push(c.clone()),
            }
        }
        // A lower bound above 1 is a condition on the length.
        let min = walk.min();
        if min > 1 {
            conditions.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThanEqual,
                operands: vec![
                    col_at(alias, "hop_count"),
                    RenderExpr::Literal(Literal::Integer(i64::from(min))),
                ],
            }));
        }
        let mut search = walk.search(self.schema)?;
        search.min = min.min(1);
        let var = search.var.clone();
        // The paths are walked back over the levels the search keeps: it
        // need not count them, and it keeps the parents a walk follows.
        let counted = all && !walked;
        let parents = match (walked, all) {
            (false, _) => path::Parents::None,
            (true, false) => path::Parents::Least,
            (true, true) => path::Parents::All,
        };
        if conditions.is_empty() {
            let name = format!("vlp_{var}_path");
            let mut ctes = vec![path::search_cte(&search, counted, parents)?];
            if walked {
                let ends = path::ends_reached(&search);
                ctes.extend(path::walk_ctes(&search, &name, &ends, all)?);
            } else {
                ctes.push(path::reached_cte(&search, &name, all, true)?);
            }
            return Ok(Some((ctes, name, walked)));
        }
        // The ends the conditions read are joined to the paths under their
        // own aliases. A condition reading anything else (another element,
        // a CTE's column) would need a pick per row of it.
        let read: Vec<String> = conditions
            .iter()
            .flat_map(|c| read_aliases(c).unwrap_or_default())
            .collect();
        let mut ends = Vec::new();
        let mut readable = vec![alias.to_string()];
        for (end, column) in [(first, "start_id"), (last, "end_id")] {
            // Its relation, label and id, joined by its identity as the
            // search spells a node: a union's search by label and id.
            let (table, label, id, a) = match self.scans.get(&end) {
                Some(Scan::Node {
                    schema,
                    label,
                    at: At::Table(a),
                    ..
                }) => (
                    self.node_relation_sql(schema)?,
                    value::string(label),
                    format!("{a}.{}", schema.id_physical_columns()[0]),
                    a,
                ),
                Some(Scan::Labels {
                    cte,
                    at: At::Table(a),
                    ..
                }) if matches!(walk, path::Walk::Union(_)) => (
                    cte.clone(),
                    format!("{a}.{LABEL_COLUMN}"),
                    format!("{a}.{}", indexed_column(LABEL_ID, 0)),
                    a,
                ),
                _ => continue,
            };
            if !self.elided.contains_key(&end) && read.contains(a) {
                let key = match &walk {
                    path::Walk::One(_) => id,
                    path::Walk::Union(_) => value::node_key_text(&value::spelling()?, &label, &id),
                };
                ends.push(path::PickEnd {
                    alias: a.clone(),
                    table,
                    key,
                    column,
                });
                readable.push(a.clone());
            }
        }
        if read.iter().any(|a| !readable.contains(a)) {
            return unsupported(
                "a shortestPath condition that reads a variable other than the path and its ends",
            );
        }
        // The trails start only where a pair's distance fails the conditions.
        let near = format!("vlp_{var}_near");
        walk.start_mut().push(RenderExpr::Raw(format!(
            "{}.{} IN (SELECT start_id FROM ({}))",
            path::START,
            search.id,
            path::failing_pairs(&var, alias, &ends, &conditions),
        )));
        let columns = search.walked_columns();
        let (trails, trail_edges) = walk.trails(self.schema)?;
        let (pick, name) = path::pick_cte(
            &var,
            alias,
            &ends,
            &conditions,
            min >= 1,
            all,
            walked.then_some((columns.as_slice(), trail_edges)),
        )?;
        let mut ctes = vec![
            path::search_cte(&search, counted, parents)?,
            path::reached_cte(&search, &near, counted, false)?,
        ];
        if walked {
            // The pairs whose distance satisfies the conditions.
            let ends = path::passing_ends(&var, alias, &ends, &conditions);
            let walked_name = format!("vlp_{var}_walked");
            ctes.extend(path::walk_ctes(&search, &walked_name, &ends, all)?);
        }
        ctes.extend(trails);
        ctes.push(pick);
        Ok(Some((ctes, name, walked)))
    }

    /// The relation of the nodes of `schema`'s label, as SQL to read under
    /// an alias: its table with its view parameters, FINAL and `filter:` (a
    /// subquery when it has either of the last two), so each node is one
    /// row there is.
    fn node_relation_sql(&self, schema: &NodeSchema) -> Result<String, LowerError> {
        let table = ViewTableRef::parameterized_name(
            &schema.full_table_name(),
            schema.view_parameters.as_deref(),
            self.options.view_parameter_values.as_ref(),
        );
        let use_final = schema.should_use_final();
        if schema.filter.is_none() && !use_final {
            return Ok(table);
        }
        const ROW: &str = "e";
        let filter = match &schema.filter {
            Some(f) => match f.to_sql(ROW) {
                Ok(sql) => format!(" WHERE ({sql})"),
                Err(e) => return unsupported(format!("schema filter: {e}")),
            },
            None => String::new(),
        };
        let fin = if use_final { " FINAL" } else { "" };
        Ok(format!("(SELECT * FROM {table} AS {ROW}{fin}{filter})"))
    }

    /// A `shortestPath` / `allShortestPaths` pattern as Neo4j takes it: one
    /// variable-length relationship, between two node variables.
    fn shortest_pattern(part: &PatternPart) -> Result<(), LowerError> {
        let [r] = part.rels.as_slice() else {
            return unsupported("shortestPath over other than one relationship");
        };
        if r.length.is_none() {
            return unsupported("shortestPath over a fixed-length relationship");
        }
        if part.nodes[0].var == part.nodes[1].var {
            // Neo4j raises an error for it.
            return unsupported("shortestPath from a node to itself");
        }
        Ok(())
    }

    /// Conjuncts of the segment's filters and `own` that read only node
    /// `v`'s own relation (it is read from its table, or its union of
    /// labels): they restrict `v`.
    fn own_conjuncts<'e>(&'e self, v: VarId, own: &'e [RenderExpr]) -> Vec<&'e RenderExpr> {
        let (Some(Scan::Node {
            at: At::Table(alias),
            ..
        })
        | Some(Scan::Labels {
            at: At::Table(alias),
            ..
        })) = self.scans.get(&v)
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

    /// Node `v` is read from its table, and a conjunct of the segment or
    /// `own` equates its identity with a constant: it is at most one node.
    fn pinned(&self, v: VarId, own: &[RenderExpr]) -> bool {
        let Some(Scan::Node {
            schema,
            at: At::Table(alias),
            ..
        }) = self.scans.get(&v)
        else {
            return false;
        };
        let id = col_at(alias, &schema.id_physical_columns()[0]);
        self.own_conjuncts(v, own).into_iter().any(|c| {
            matches!(c, RenderExpr::OperatorApplicationExp(op)
                if op.operator == Operator::Equal
                    && op.operands.len() == 2
                    && op.operands.contains(&id)
                    && op.operands.iter().any(is_constant))
        })
    }

    /// The rows joined so far restrict node `v`: [`Self::rows_holding`].
    fn joined_rows_restrict(&self, v: VarId) -> bool {
        self.rows_holding(v, &[]).is_some()
    }

    /// The rows so far that restrict node `v` (no SELECT yet): the relations
    /// of the join tree connected to `v`'s by ties, with the conjuncts of the
    /// segment's filters and `own` over them. Each value of `v` in the
    /// result is one of theirs. `None` when they are only `v`'s own table
    /// (its conjuncts are [`Self::own_conjuncts`]) or `v` is not joined yet.
    /// Relations not tied to `v` (a cross join) restrict nothing.
    fn rows_holding(&self, v: VarId, own: &[RenderExpr]) -> Option<RenderPlan> {
        if self.empty || !self.is_emitted(v) {
            return None;
        }
        let at = self.scans.get(&v)?.at()?;
        let from = self.from.as_ref()?;
        let mut relations: Vec<(String, String, Vec<OperatorApplication>, JoinType)> = vec![(
            from.alias.clone().unwrap_or_default(),
            from.name.clone(),
            Vec::new(),
            JoinType::Join,
        )];
        for j in &self.joins {
            relations.push((
                j.table_alias.clone(),
                j.table_name.clone(),
                j.joining_on.clone(),
                j.join_type.clone(),
            ));
        }
        // Ties: ON conjuncts, and conjuncts placed in WHERE.
        let mut links: Vec<Vec<String>> = relations
            .iter()
            .flat_map(|(_, _, on, _)| on)
            .map(|c| table_aliases(&RenderExpr::OperatorApplicationExp(c.clone())))
            .collect();
        links.extend(self.filters.iter().flat_map(conjuncts).map(table_aliases));
        let mut held = vec![at.alias().to_string()];
        loop {
            let more: Vec<String> = links
                .iter()
                .filter(|l| l.iter().any(|a| held.contains(a)))
                .flatten()
                .filter(|a| !held.contains(*a) && relations.iter().any(|(r, ..)| r == *a))
                .cloned()
                .collect();
            if more.is_empty() {
                break;
            }
            held.extend(more);
        }
        if held.len() == 1 && matches!(at, At::Table(_)) {
            return None;
        }
        let mut rows = empty_plan();
        let mut where_ = Vec::new();
        for (i, (alias, table, on, kind)) in relations
            .into_iter()
            .filter(|(a, ..)| held.contains(a))
            .enumerate()
        {
            if i == 0 {
                rows.from = FromTableItem(Some(if from.alias.as_deref() == Some(&alias) {
                    from.clone()
                } else {
                    table_ref(table, &alias)
                }));
                where_.extend(on.into_iter().map(RenderExpr::OperatorApplicationExp));
            } else {
                let mut j = join(table, &alias);
                j.joining_on = on;
                j.join_type = kind;
                rows.joins.0.push(j);
            }
        }
        where_.extend(
            self.filters
                .iter()
                .chain(own)
                .flat_map(conjuncts)
                .filter(|c| reads_only(c, &held))
                .cloned(),
        );
        rows.filters = FilterItems(and_all(where_));
        Some(rows)
    }

    /// The edge definition of `(from_label)-[:rel_type]->(to_label)`, when
    /// it is lowered (the standard layout).
    fn edge_schema(
        &self,
        rel_type: &str,
        from_label: &str,
        to_label: &str,
    ) -> Result<&'s RelationshipSchema, LowerError> {
        let Some(rs) = self.defined(rel_type, from_label, to_label) else {
            return unsupported(format!(
                "no schema for ({from_label})-[:{rel_type}]->({to_label})"
            ));
        };
        Self::standard_edge(rel_type, rs)
    }

    /// `rs` (of `rel_type`) if it is the standard layout.
    fn standard_edge(
        rel_type: &str,
        rs: &'s RelationshipSchema,
    ) -> Result<&'s RelationshipSchema, LowerError> {
        if !rs.is_standard_edge_table() {
            return unsupported(format!("type {rel_type} is not the standard layout (S8)"));
        }
        if rs.constraints.is_some() {
            return unsupported("an edge `constraints:` expression (S8)");
        }
        Ok(rs)
    }

    /// The schema of `rel_type` from `from_label` to `to_label`, if the
    /// schema defines it (`get_rel_schema_with_nodes` falls back to the
    /// type's first schema whatever its ends).
    fn defined(
        &self,
        rel_type: &str,
        from_label: &str,
        to_label: &str,
    ) -> Option<&'s RelationshipSchema> {
        let all = self.schema.get_relationships_schemas();
        let found: Vec<&'s RelationshipSchema> = self
            .schema
            .expand_generic_relationship_type(rel_type, Some(from_label), Some(to_label))
            .iter()
            .filter_map(|key| all.get(key))
            .collect();
        let exact = found
            .iter()
            .find(|rs| rs.from_node == from_label && rs.to_node == to_label);
        exact.or(found.first()).copied()
    }

    /// Written labels / types are alternatives the element must have. A
    /// table's label is static, so this is decided here: a variable from an
    /// earlier clause written with a label it does not have (`MATCH (a:User)
    /// MATCH (a:Post)`) makes the clause match nothing. A node of several
    /// possible labels is filtered on its label column.
    fn written(&mut self, v: VarId, written: &[String]) -> Result<(), LowerError> {
        if written.is_empty() {
            return Ok(());
        }
        let scan = self.scans[&v].clone();
        let holds = match &scan {
            Scan::Node { label, .. } => written.contains(label),
            // Of several possible labels, the written ones.
            Scan::Labels { arms, .. } => {
                let held: Vec<String> = arms
                    .iter()
                    .map(|(l, _)| l.clone())
                    .filter(|l| written.contains(l))
                    .collect();
                if !held.is_empty() && held.len() < arms.len() {
                    let column = self.physical(v, LABEL_COLUMN)?;
                    let is = held
                        .into_iter()
                        .map(|l| {
                            RenderExpr::OperatorApplicationExp(eq(
                                column.clone(),
                                RenderExpr::Literal(Literal::String(l)),
                            ))
                        })
                        .collect();
                    self.filters.push(or_all(is));
                    true
                } else {
                    !held.is_empty()
                }
            }
            // Of several possible types, the written ones.
            Scan::Rels { arms, .. } => {
                let types: BTreeSet<&String> = arms.iter().map(|a| &a.rel_type).collect();
                let held: Vec<String> = types
                    .iter()
                    .filter(|t| written.contains(t))
                    .map(|t| t.to_string())
                    .collect();
                let holds = !held.is_empty();
                if holds && held.len() < types.len() {
                    let column = self.physical(v, REL_TYPE)?;
                    let is = held
                        .into_iter()
                        .map(|t| {
                            RenderExpr::OperatorApplicationExp(eq(
                                column.clone(),
                                RenderExpr::Literal(Literal::String(t)),
                            ))
                        })
                        .collect();
                    self.filters.push(or_all(is));
                }
                holds
            }
            Scan::Rel { rel_type, .. }
            | Scan::Path {
                walked: Walked::One { rel_type, .. },
                ..
            } => written.contains(rel_type),
            // Unbound before (a list is not re-matched): its types are the
            // binder's feasible ones of the written. With none (an unknown
            // type), only the path of none can match, and it has no
            // relationship to check.
            Scan::Path {
                walked: Walked::Union { types, .. },
                ..
            } => types.iter().all(|t| written.contains(t)),
            Scan::Impossible => false,
        };
        if !holds {
            self.empty = true;
        }
        Ok(())
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
            // Its label first: NULL exactly when an OPTIONAL MATCH left it
            // NULL, as a table's first identity column is.
            Scan::Labels { arms, .. } => {
                let arity = arms[0].1.id_physical_columns().len();
                Some(
                    std::iter::once(LABEL_COLUMN.to_string())
                        .chain(indexed_columns(LABEL_ID, arity))
                        .collect(),
                )
            }
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
            // Its definition first (its type, NULL exactly when an OPTIONAL
            // MATCH left it NULL), then its identity.
            Scan::Rels { arms, .. } => {
                let identity = rel_union_shape(arms).ok()?.identity;
                Some(
                    [REL_TYPE, REL_FROM_LABEL, REL_TO_LABEL]
                        .iter()
                        .map(|c| c.to_string())
                        .chain(indexed_columns(REL_ID, identity))
                        .collect(),
                )
            }
            // The walk's first node and relationships: the path relation's
            // rows differ in them (a path of none has no relationship). Its
            // id first, which a shortest path's search relations have too
            // (as a text): a condition there reads it for NULL.
            Scan::Path {
                walked: Walked::Union { .. },
                edges,
                ..
            } => Some(
                ["start_id", path::START_LABEL, "path_edges"][..if *edges { 3 } else { 2 }]
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
            ),
            Scan::Path { edges, .. } => Some(
                ["start_id", "path_edges"][..if *edges { 2 } else { 1 }]
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
            ),
            Scan::Impossible => None,
        }
    }

    /// The columns a path relation exports through a CTE: its identity,
    /// ends, length, nodes, and the values it carries.
    fn path_physical(&self, v: VarId) -> Vec<String> {
        let Some(Scan::Path {
            walked,
            edges,
            nodes,
            node_values,
            rel_values,
            ..
        }) = self.scans.get(&v)
        else {
            return Vec::new();
        };
        let mut cols: Vec<&str> = path::PATH_COLUMNS.to_vec();
        if let Walked::Union { .. } = walked {
            cols.extend([path::START_LABEL, path::END_LABEL]);
        }
        if *nodes {
            cols.push("path_nodes");
        }
        if *edges {
            cols.push("path_edges");
        }
        if *node_values {
            cols.push(path::VALUE_COLUMNS[0]);
        }
        if *rel_values {
            cols.push(path::VALUE_COLUMNS[1]);
        }
        cols.into_iter().map(str::to_string).collect()
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
                let walk = |v: &VarId| {
                    matches!(
                        self.scans.get(v),
                        Some(Scan::Path {
                            walked: Walked::Union { .. },
                            ..
                        })
                    )
                };
                if walk(a) || walk(b) {
                    if let Some(differs) = self.key_differs(*a, *b)? {
                        self.filters.push(differs);
                    }
                    continue;
                }
                let union = |v: &VarId| matches!(self.scans.get(v), Some(Scan::Rels { .. }));
                if union(a) || union(b) {
                    if let Some(differs) = self.union_differs(*a, *b)? {
                        self.filters.push(differs);
                    }
                    continue;
                }
                let table = |v: &VarId| match self.scans.get(v) {
                    Some(Scan::Rel { schema, .. }) => Some(schema.full_table_name()),
                    // A shortest path's relationships are its own (and
                    // their identities texts).
                    Some(Scan::Path {
                        walked: Walked::One { schema, .. },
                        edges: true,
                        shortest: None,
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
                    (Scan::Rel { schema, at, .. }, Scan::Path { at: p, .. })
                    | (Scan::Path { at: p, .. }, Scan::Rel { schema, at, .. }) => {
                        let At::Table(edge) = at else {
                            return unsupported(
                                "a relationship from an earlier clause and a variable-length \
                                 relationship of the same table in one MATCH",
                            );
                        };
                        let Some(identity) = path::edge_identity_sql(schema, edge) else {
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

    /// Relationships `a` and `b` of one MATCH, one of several possible types
    /// or label pairs (`Scan::Rels`), differ: in their definition, or in
    /// their identity (a relationship is not on a path). `None` when they
    /// have no definition in common.
    fn union_differs(&self, a: VarId, b: VarId) -> Result<Option<RenderExpr>, LowerError> {
        if !self.share_definition(a, b) {
            return Ok(None);
        }
        let differ = |x: RenderExpr, y: RenderExpr| {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::NotEqual,
                operands: vec![x, y],
            })
        };
        let string = |s: &str| RenderExpr::Literal(Literal::String(s.to_string()));
        let path = |v: VarId| match self.scans.get(&v) {
            Some(Scan::Path {
                walked: Walked::One { schema, rel_type },
                at,
                ..
            }) => Some((*schema, rel_type.clone(), at.alias().to_string())),
            _ => None,
        };
        if let Some((r, (ps, rel_type, p))) = path(b).map(|p| (a, p)).or(path(a).map(|p| (b, p))) {
            // Rows of the path's definition: their identity, spelled as the
            // path's `path_edges` spell it, is not on the path.
            let arity = match &ps.edge_id {
                Some(id) => id.columns().len(),
                None if ps.from_id.columns().len() == 1 && ps.to_id.columns().len() == 1 => 2,
                None => return unsupported("a composite relationship endpoint (S8)"),
            };
            let ids = indexed_columns(REL_ID, arity)
                .iter()
                .map(|c| self.physical(r, c).map(|e| render_expr_to_sql_plain(&e)))
                .collect::<Result<Vec<_>, _>>()?;
            let identity = match ids.as_slice() {
                [one] => one.clone(),
                _ => format!(
                    "{}({})",
                    current_function_mapper().tuple_constructor(),
                    ids.join(", ")
                ),
            };
            let mut differs = vec![
                differ(self.physical(r, REL_TYPE)?, string(&rel_type)),
                differ(self.physical(r, REL_FROM_LABEL)?, string(&ps.from_node)),
                differ(self.physical(r, REL_TO_LABEL)?, string(&ps.to_node)),
            ];
            differs.push(path::path_avoids_edge(&p, &identity));
            return Ok(Some(or_all(differs)));
        }
        // Definition, then identity, column by column. Past a definition's
        // own arity a union's identity is NULL in both (each has its arms).
        let (ia, ib) = (self.definition_identity(a)?, self.definition_identity(b)?);
        Ok(Some(or_all(
            ia.into_iter().zip(ib).map(|(x, y)| differ(x, y)).collect(),
        )))
    }

    /// The definitions relationship `v` of a MATCH can be (none for a
    /// shortest path: its relationships are its own).
    fn definitions(&self, v: VarId) -> Vec<&'s RelationshipSchema> {
        match self.scans.get(&v) {
            Some(Scan::Rel { schema, .. }) => vec![*schema],
            Some(Scan::Rels { arms, .. }) => arms.iter().map(|a| a.schema).collect(),
            Some(Scan::Path {
                walked: Walked::One { schema, .. },
                edges: true,
                shortest: None,
                ..
            }) => vec![*schema],
            Some(Scan::Path {
                walked: Walked::Union { arms, .. },
                shortest: None,
                ..
            }) => arms.iter().map(|a| a.schema).collect(),
            _ => Vec::new(),
        }
    }

    /// Relationships `a` and `b` can be one relationship: they have a
    /// definition in common.
    fn share_definition(&self, a: VarId, b: VarId) -> bool {
        let (da, db) = (self.definitions(a), self.definitions(b));
        da.iter().any(|x| db.iter().any(|y| std::ptr::eq(*x, *y)))
    }

    /// Relationships `a` and `b` of one MATCH, one a walk over several
    /// definitions (`Walked::Union`), differ: by their identities as texts
    /// (`value::rel_key`), a relationship's not on a path's, two paths'
    /// apart. `None` when they have no definition in common.
    fn key_differs(&self, a: VarId, b: VarId) -> Result<Option<RenderExpr>, LowerError> {
        if !self.share_definition(a, b) {
            return Ok(None);
        }
        let g = value::spelling()?;
        let m = current_function_mapper();
        let sql = |e: RenderExpr| render_expr_to_sql_plain(&e);
        // One relationship's text, or a path's list of them.
        let keys = |v: VarId| -> Result<Result<String, String>, LowerError> {
            Ok(match self.scans.get(&v) {
                Some(Scan::Rel {
                    schema, rel_type, ..
                }) => {
                    let ids: Vec<String> = self
                        .identity(v)?
                        .unwrap_or_default()
                        .into_iter()
                        .map(sql)
                        .collect();
                    Ok(value::rel_key(
                        &g,
                        rel_type,
                        &schema.from_node,
                        &schema.to_node,
                        &ids,
                    ))
                }
                Some(Scan::Rels { .. }) => Ok(sql(self.physical(v, KEY)?)),
                Some(Scan::Path {
                    walked: Walked::One { schema, rel_type },
                    ..
                }) => Err((g.prefixed_texts)(
                    &value::rel_key_prefix(rel_type, &schema.from_node, &schema.to_node),
                    &sql(self.physical(v, "path_edges")?),
                )),
                Some(Scan::Path { .. }) => Err(sql(self.physical(v, "path_edges")?)),
                _ => return unsupported(format!("internal: {v} is not a relationship scan")),
            })
        };
        let differs = match (keys(a)?, keys(b)?) {
            (Ok(one), Err(list)) | (Err(list), Ok(one)) => {
                format!("NOT {}({list}, {one})", m.array_contains())
            }
            (Err(x), Err(y)) => format!("NOT {}({x}, {y})", m.arrays_overlap()),
            (Ok(_), Ok(_)) => return unsupported("internal: no walk to compare"),
        };
        Ok(Some(RenderExpr::Raw(differs)))
    }

    /// A relationship's definition (type, labels of its stored ends) and
    /// identity, as a union of several ([`Self::rel_union`]) carries them.
    fn definition_identity(&self, v: VarId) -> Result<Vec<RenderExpr>, LowerError> {
        let ids = self.identity(v)?.unwrap_or_default();
        match self.scans.get(&v) {
            Some(Scan::Rel {
                schema, rel_type, ..
            }) => {
                let string = |s: &str| RenderExpr::Literal(Literal::String(s.to_string()));
                let mut all = vec![
                    string(rel_type),
                    string(&schema.from_node),
                    string(&schema.to_node),
                ];
                all.extend(ids);
                Ok(all)
            }
            Some(Scan::Rels { .. }) => Ok(ids),
            _ => unsupported(format!("internal: {v} is not a relationship scan")),
        }
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
            // Its CTE reads the tables with their options and filters.
            Scan::Labels {
                cte,
                at: At::Table(alias),
                ..
            } => (alias.clone(), cte.clone(), None, false, None),
            Scan::Rel {
                at: At::Table(alias),
                both: Some(both),
                ..
            } => (alias.clone(), both.clone(), None, false, None),
            Scan::Rels {
                cte,
                at: At::Table(alias),
                ..
            } => (alias.clone(), cte.clone(), None, false, None),
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
            | Scan::Labels {
                at: At::Exported { .. },
                ..
            }
            | Scan::Rel {
                at: At::Exported { .. },
                ..
            }
            | Scan::Rels {
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
        if keys.iter().any(|k| self.holds_elements(&k.expr)) {
            // A tuple would sort by its columns.
            return unsupported("ORDER BY a node or relationship of a list");
        }
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
        // What each item is, for this projection's ORDER BY / WHERE (and the
        // next segment's reads of a WITH's).
        for it in &p.items {
            let kind = self.kind(&it.expr);
            self.kinds.insert(it.var, kind);
        }
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
        // Path elements a WITH already exports (`WITH p, p AS q`).
        let mut exported: Vec<VarId> = Vec::new();
        for it in &p.items {
            if let Some(g) = self.graph_ref(&it.expr).filter(|_| !it.aggregate) {
                let handled = self.graph_item(
                    g,
                    it,
                    cte,
                    aggregating,
                    &mut body,
                    &mut exports,
                    &mut shape,
                    &mut exported,
                )?;
                if handled {
                    continue;
                }
            }
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
                    return unsupported(format!("internal: path {src} not projected as a value"));
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
                let out = self.export_element(src, it.var, alias, &mut body, &mut keys)?;
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
                    if let Some(Scan::Labels { .. } | Scan::Rels { .. }) = self.scans.get(&src) {
                        return unsupported(
                            "`v.*` of an element of several possible labels or types",
                        );
                    }
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
            // A list's node or relationship, or a list of them, returned: their
            // values (`elements.rs`); a WITH carries the tuples.
            if cte.is_none() {
                if let Some((ty, value)) = self.elements_value(&it.expr, &e)? {
                    if body.distinct && it.aggregate {
                        return unsupported("DISTINCT of an aggregated list of nodes");
                    }
                    let column = body.column(RenderExpr::Raw(value), &it.name);
                    if !it.aggregate {
                        // Grouped (above) or made distinct by its tuples.
                        body.determined.push(column.clone());
                        if body.distinct && !aggregating {
                            body.distinct_keys.push(e.clone());
                        }
                    }
                    self.identities.push((it.name.clone(), vec![e]));
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::Graph(ty),
                        columns: vec![(it.name.clone(), column)],
                    });
                    continue;
                }
            }
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
                            "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
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
        &mut self,
        src: VarId,
        name: &str,
        aggregating: bool,
        body: &mut Body,
    ) -> Result<ResultColumn, LowerError> {
        // Its label / type differs by row: returned as a value, grouped by
        // its identity and its properties (which an ORDER BY can read; they
        // are the identity's).
        let union = match self.scans.get(&src) {
            Some(Scan::Labels { arms, of, .. }) => {
                Some((self.labeled_node(src)?, self.label_union_props(*of, arms)))
            }
            Some(Scan::Rels { arms, of, .. }) => {
                Some((self.labeled_rel(src)?, self.rel_union_props(*of, arms)))
            }
            _ => None,
        };
        if let Some((v, props)) = union {
            let column = body.column(v.value, name);
            body.determined.push(column.clone());
            self.identities.push((name.to_string(), v.keys.clone()));
            let mut keys = v.keys;
            for prop in props {
                keys.push(self.property(src, &prop)?);
            }
            if aggregating {
                body.group_by.extend(keys);
            } else if body.distinct {
                body.distinct_keys.extend(keys);
            }
            return Ok(ResultColumn {
                name: name.to_string(),
                kind: ResultKind::Graph(v.ty),
                columns: vec![(name.to_string(), column)],
            });
        }
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
        self.identities.push((name.to_string(), identity.clone()));
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
            // Its value (`Lowerer::labeled_node` / `labeled_rel`).
            Some(Scan::Labels { .. } | Scan::Rels { .. }) => {
                return unsupported("internal: an element of several labels or types as columns")
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
            Some(Scan::Labels { arms, .. }) => {
                return Self::label_property_names(arms).into_iter().collect()
            }
            Some(Scan::Rels { arms, .. }) => {
                return Self::rel_property_names(arms).into_iter().collect()
            }
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
        body: &mut Body,
        keys: &mut Vec<RenderExpr>,
    ) -> Result<Scan<'s>, LowerError> {
        let Some(scan) = self.scans.get(&src).cloned() else {
            return unsupported(format!("internal: {src} has no scan"));
        };
        let mut physical_cols = match &scan {
            Scan::Path { .. } => self.path_physical(src),
            _ => self.identity_physical(src).unwrap_or_default(),
        };
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
        // Its stored ends' ids (a later clause ties them, and its value
        // reads them).
        if let Scan::Rels { arms, .. } = &scan {
            let shape = rel_union_shape(arms)?;
            physical_cols.extend(indexed_columns(REL_FROM, shape.from));
            physical_cols.extend(indexed_columns(REL_TO, shape.to));
            // Its identity as a text, where the dialect spells one (a path's
            // identity reads it).
            if current_function_mapper().graph_values().is_some() {
                physical_cols.push(KEY.to_string());
            }
        }
        if matches!(scan, Scan::Impossible) {
            return Ok(Scan::Impossible);
        }
        let mut physical = HashMap::new();
        for c in &physical_cols {
            let name = format!("{out}__{c}");
            let e = self.physical(src, c)?;
            body.select.push(select(e.clone(), &name));
            match &scan {
                // Grouped by as a list: by its relationships (`value.rs`);
                // its other columns follow from them where they are read.
                Scan::Path { .. } if c != "path_edges" => body.determined.push(name.clone()),
                // Follows from its identity.
                Scan::Rels { .. } if c == KEY => body.determined.push(name.clone()),
                _ => keys.push(e),
            }
            physical.insert(c.clone(), name);
        }
        if matches!(scan, Scan::Path { .. }) {
            if self.binding(src).nullable {
                // An OPTIONAL MATCH's list that did not match is NULL, not
                // the empty list: grouped apart.
                let e = RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::IsNull,
                    operands: vec![self.physical(src, "start_id")?],
                });
                body.select
                    .push(select(e.clone(), &format!("{out}__unmatched")));
                keys.push(e);
            }
            // A list of relationships has no properties.
            return Ok(scan.with_at(At::Exported {
                alias: alias.to_string(),
                physical,
                props: HashMap::new(),
            }));
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
            body.select.push(select(e.clone(), &name));
            keys.push(e);
            props.insert(prop.clone(), col_at(alias, &name));
        }
        Ok(scan.with_at(At::Exported {
            alias: alias.to_string(),
            physical,
            props,
        }))
    }

    /// Before projection `p`: `collect` lists its input rows in their order,
    /// whatever the projection's own ORDER BY does to its output. Over rows
    /// in an order, the rows are numbered in it first (a CTE), and
    /// `collect` lists its values in the order of that number
    /// (`collect_order`).
    fn order_collect(&mut self, p: &Projection) -> Result<(), LowerError> {
        self.collect_order = None;
        if !p.aggregates() || !p.items.iter().any(|i| calls_aggregate(&i.expr, "collect")) {
            return Ok(());
        }
        match &self.order {
            RowOrder::Unordered => Ok(()),
            RowOrder::Keys(_) => {
                self.number_rows()?;
                let RowOrder::Keys(keys) = &self.order else {
                    return unsupported("internal: numbered rows in no order");
                };
                self.collect_order = keys.first().map(|k| k.expression.clone());
                Ok(())
            }
            RowOrder::Lost => unsupported("collect() over rows in an order the SQL does not keep"),
        }
    }

    /// A WITH: the rows so far become a CTE whose columns are the WITH's
    /// output scope, and a new segment reads from it.
    fn with(&mut self, p: &Projection) -> Result<(), LowerError> {
        self.order_collect(p)?;
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
                return unsupported(
                    "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
                )
            }
        };
        let mut body = Body {
            order_by,
            skip,
            limit,
            ..Body::default()
        };
        let mut exports = Exports::default();
        self.export_scope(&alias, &mut body, &mut exports)?;
        self.close_segment(body, exports, alias)
    }

    /// Export the scope unchanged from the CTE aliased `alias` of the rows
    /// so far: every named element and value of this segment, and the
    /// elements of a named path (its length reads them).
    fn export_scope(
        &self,
        alias: &str,
        body: &mut Body,
        exports: &mut Exports<'s>,
    ) -> Result<(), LowerError> {
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
                if let Some(c) = self.graph_values.get(&v).cloned() {
                    let value = value::GraphValue {
                        value: e,
                        keys: c.keys,
                        ty: c.ty,
                        nullable: c.nullable,
                    };
                    let carried = value::export_graph(v, value, alias, body, false);
                    exports.values.push((v, col_at(alias, &v.name())));
                    exports.graph.push((v, carried));
                    continue;
                }
                if is_constant(&e) {
                    exports.values.push((v, e));
                } else {
                    body.select.push(select(e, &v.name()));
                    exports.values.push((v, col_at(alias, &v.name())));
                }
                continue;
            }
            let scan = self.export_element(v, v, alias, body, &mut Vec::new())?;
            exports.scans.push((v, scan));
        }
        Ok(())
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
        self.graph_values = exports.graph.into_iter().collect();
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
        self.order_collect(p)?;
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
        let mut grouped = body.grouped;
        let mut select = body.select;
        let determined = |i: &SelectItem| {
            i.col_alias
                .as_ref()
                .is_some_and(|a| body.determined.contains(&a.0))
        };
        if distinct && (!body.distinct_keys.is_empty() || select.iter().any(determined)) {
            // DISTINCT by the returned columns and keys that are not returned:
            // a GROUP BY of both (a plain projection, so no aggregate).
            group_by = select
                .iter()
                .filter(|i| !determined(i))
                .map(|i| i.expression.clone())
                .chain(body.distinct_keys)
                .collect();
            distinct = false;
            grouped = true;
        }
        group_by.retain(|g| !is_constant(g));
        if grouped || !group_by.is_empty() {
            if let Some(g) = current_function_mapper().graph_values() {
                for i in select.iter_mut().filter(|i| determined(i)) {
                    i.expression =
                        RenderExpr::Raw((g.any)(&render_expr_to_sql_plain(&i.expression)));
                }
            }
        }
        if grouped && group_by.is_empty() {
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
                items: select,
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
            array_join: ArrayJoinItem(body.array_join),
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
    /// Graph values among `values`.
    graph: Vec<(VarId, Carried)>,
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

/// The table aliases an expression reads.
fn table_aliases(e: &RenderExpr) -> Vec<String> {
    // Every column read, however deep (a `CASE` of a relationship read in
    // both orientations, `turned_ends`; review finding: one missed placed a
    // tie before the relation it reads).
    let mut read = Vec::new();
    let mut e = e.clone();
    visit_render_expr_mut(&mut e, &mut |x| match x {
        RenderExpr::PropertyAccessExp(pa) => {
            read.push(pa.table_alias.0.clone());
            MutVisit::Stop
        }
        _ => MutVisit::Recurse,
    });
    read
}

/// `alias.hop_count < k` (or `<=`, `=`, either way round) for an integer
/// `k`: the longest length it allows, and whether that is all it says.
fn length_bound(c: &RenderExpr, alias: &str) -> Option<(i64, bool)> {
    let RenderExpr::OperatorApplicationExp(op) = c else {
        return None;
    };
    let is_length = |e: &RenderExpr| {
        matches!(e, RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias,
            column: PropertyValue::Column(col),
        }) if table_alias.0 == alias && col == "hop_count")
    };
    let [x, y] = op.operands.as_slice() else {
        return None;
    };
    let (k, length_left) = match (x, y) {
        (l, RenderExpr::Literal(Literal::Integer(k))) if is_length(l) => (*k, true),
        (RenderExpr::Literal(Literal::Integer(k)), l) if is_length(l) => (*k, false),
        _ => return None,
    };
    match (op.operator, length_left) {
        (Operator::LessThan, true) | (Operator::GreaterThan, false) => Some((k - 1, true)),
        (Operator::LessThanEqual, true) | (Operator::GreaterThanEqual, false) => Some((k, true)),
        (Operator::Equal, _) => Some((k, false)),
        _ => None,
    }
}

/// Every table alias `e` reads, or `None` when a part of it reads what is
/// not visible here (a subquery, raw SQL). The names a `reduce` binds are
/// its own, not reads.
fn read_aliases(e: &RenderExpr) -> Option<Vec<String>> {
    let mut read = Vec::new();
    let mut bound = Vec::new();
    let mut opaque = false;
    let mut e = e.clone();
    visit_render_expr_mut(&mut e, &mut |x| match x {
        RenderExpr::ReduceExpr(r) => {
            bound.push(r.accumulator.clone());
            bound.push(r.variable.clone());
            MutVisit::Recurse
        }
        RenderExpr::PropertyAccessExp(pa) => {
            read.push(pa.table_alias.0.clone());
            MutVisit::Stop
        }
        RenderExpr::TableAlias(t) => {
            read.push(t.0.clone());
            MutVisit::Stop
        }
        RenderExpr::Raw(_)
        | RenderExpr::Column(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::InSubquery(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_) => {
            opaque = true;
            MutVisit::Stop
        }
        _ => MutVisit::Recurse,
    });
    read.retain(|a| !bound.contains(a));
    (!opaque).then_some(read)
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
    // A WITH item that passes a variable through reads nothing of it: what
    // later clauses read of it is demanded of its source (below).
    fn pass_through<'a>(op: &'a BoundOp, out: &mut Vec<&'a LogicalExpr>) {
        match op {
            BoundOp::Project { input, projection } => {
                pass_through(input, out);
                if projection.kind == ProjectionKind::With {
                    out.extend(
                        projection
                            .items
                            .iter()
                            .map(|i| &i.expr)
                            .filter(|e| matches!(e, LogicalExpr::TableAlias(_))),
                    );
                }
            }
            BoundOp::Match { input, .. }
            | BoundOp::Unwind { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Skip { input, .. }
            | BoundOp::Limit { input, .. } => pass_through(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| pass_through(a, out)),
            BoundOp::Unit => {}
        }
    }
    let mut passed = Vec::new();
    pass_through(&stmt.plan, &mut passed);
    // A path or list a DISTINCT or aggregating WITH passes through is
    // grouped by: its identity is read.
    fn keyed<'a>(op: &'a BoundOp, out: &mut Vec<&'a LogicalExpr>) {
        match op {
            BoundOp::Project { input, projection } => {
                keyed(input, out);
                if projection.kind == ProjectionKind::With
                    && (projection.distinct || projection.aggregates())
                {
                    out.extend(
                        projection
                            .items
                            .iter()
                            .filter(|i| !i.aggregate)
                            .map(|i| &i.expr),
                    );
                }
            }
            BoundOp::Match { input, .. }
            | BoundOp::Unwind { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Skip { input, .. }
            | BoundOp::Limit { input, .. } => keyed(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| keyed(a, out)),
            BoundOp::Unit => {}
        }
    }
    let mut grouped = Vec::new();
    keyed(&stmt.plan, &mut grouped);
    let mut demand: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let mut add = |v: VarId, prop: &str| {
        demand.entry(v).or_default().insert(prop.to_string());
    };
    // A node or relationship a final RETURN (the statement's, or a UNION
    // arm's) returns whole: every property (`v.*` is a property ref).
    let finals = match &stmt.plan {
        BoundOp::Union { arms, .. } => arms.iter().collect(),
        op => vec![op],
    };
    for op in finals {
        let BoundOp::Project { projection, .. } = op else {
            continue;
        };
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
    for e in &all {
        for (name, prop) in property_refs(e) {
            if let Some(v) = parse_var(&name) {
                add(v, &prop);
            }
        }
        // A node or relationship put in a list carries every property
        // (`elements.rs`: a list's elements are tuples of them).
        for v in listed_elements(e, &stmt.bindings) {
            add(v, ALL_PROPERTIES);
        }
        // A path or list read as a value needs its elements' values.
        if passed.iter().any(|x| std::ptr::eq(*x, *e)) {
            continue;
        }
        let mut uses = Vec::new();
        graph_demand(e, &stmt.bindings, &mut uses);
        for (v, what) in uses {
            add(v, what);
        }
    }
    for e in grouped {
        if let Some(GraphRef::Path(v) | GraphRef::List(v)) = value::graph_ref(e, &stmt.bindings) {
            add(v, PATH_KEY);
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
    // What is read of a pass-through item is read of its source (a later
    // binding than its source): a node of several labels carries it from
    // its first scan ([`Lowerer::label_union`]).
    let pass_through = |demand: &mut HashMap<VarId, BTreeSet<String>>| {
        for b in stmt.bindings.iter().rev() {
            if let BindingSource::Projection { of: Some(src), .. } = b.source {
                if let Some(props) = demand.get(&b.id).cloned() {
                    demand.entry(src).or_default().extend(props);
                }
            }
        }
    };
    pass_through(&mut demand);
    // A path's value is its elements': every property of a fixed node or
    // relationship, the carried values of a variable-length one.
    let mut elements: Vec<(VarId, &str)> = Vec::new();
    for part in pattern_parts(&stmt.plan) {
        let Some(wanted) = part.path_var.and_then(|p| demand.get(&p)) else {
            continue;
        };
        let (nodes, rels) = (wanted.contains(NODE_VALUES), wanted.contains(REL_VALUES));
        let key = wanted.contains(PATH_KEY);
        if nodes {
            elements.extend(part.nodes.iter().map(|n| (n.var, ALL_PROPERTIES)));
        }
        for r in &part.rels {
            match r.length {
                Some(_) => {
                    if key {
                        elements.push((r.var, PATH_KEY));
                    }
                    if nodes {
                        elements.push((r.var, NODE_VALUES));
                    }
                    if rels {
                        elements.push((r.var, REL_VALUES));
                    }
                }
                None if rels => elements.push((r.var, ALL_PROPERTIES)),
                None => {}
            }
        }
    }
    for (v, what) in elements {
        demand.entry(v).or_default().insert(what.to_string());
    }
    pass_through(&mut demand);
    demand
}

/// The nodes and relationships an expression puts in a list: `collect(a)`
/// and `[a, …]` items.
fn listed_elements(e: &LogicalExpr, bindings: &[Binding]) -> Vec<VarId> {
    use crate::query_planner::logical_expr::{Operator, TableAlias};
    let element = |x: &LogicalExpr| match x {
        LogicalExpr::TableAlias(TableAlias(n)) => parse_var(n).filter(|v| {
            matches!(
                bindings[v.0 as usize].kind,
                BindingKind::Node { .. } | BindingKind::Rel { length: None, .. }
            )
        }),
        _ => None,
    };
    let mut out = Vec::new();
    let mut visit = vec![e];
    while let Some(x) = visit.pop() {
        match x {
            LogicalExpr::AggregateFnCall(f) if f.name.eq_ignore_ascii_case("collect") => {
                for a in &f.args {
                    let a = match a {
                        LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)
                            if op.operator == Operator::Distinct && op.operands.len() == 1 =>
                        {
                            &op.operands[0]
                        }
                        a => a,
                    };
                    out.extend(element(a));
                }
            }
            LogicalExpr::List(items) => out.extend(items.iter().filter_map(element)),
            _ => {}
        }
        visit.extend(crate::bound_plan::expr::children(x));
    }
    out
}

/// The graph values (`value.rs`) an expression reads: (variable, what of
/// it). `length(p)` and `size()` of a list are counted from the path's
/// structure and read none.
fn graph_demand(e: &LogicalExpr, bindings: &[Binding], out: &mut Vec<(VarId, &'static str)>) {
    let structural = match e {
        LogicalExpr::ScalarFnCall(f) => match f.args.as_slice() {
            [arg] if f.name.eq_ignore_ascii_case("size") => {
                value::graph_ref(arg, bindings).is_some()
            }
            [arg] if f.name.eq_ignore_ascii_case("length") => {
                matches!(value::graph_ref(arg, bindings), Some(GraphRef::Path(_)))
            }
            _ => false,
        },
        _ => false,
    };
    if structural {
        return;
    }
    match value::graph_ref(e, bindings) {
        Some(GraphRef::Path(p)) => out.extend([(p, NODE_VALUES), (p, REL_VALUES)]),
        Some(GraphRef::Nodes(p)) => out.push((p, NODE_VALUES)),
        Some(GraphRef::Rels(p)) => out.push((p, REL_VALUES)),
        Some(GraphRef::List(r)) => out.push((r, REL_VALUES)),
        Some(GraphRef::Carried(_)) | None => {
            for c in crate::bound_plan::expr::children(e) {
                graph_demand(c, bindings, out);
            }
        }
    }
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
