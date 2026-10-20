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
//! * MATCH and OPTIONAL MATCH over labels with their own tables and types
//!   whose table rows are relationships (`NodeSchema::is_standard_own_table`,
//!   `RelationshipSchema::is_edge_row_table`: an edge table or, S8a, an FK
//!   edge's node table); fixed length or variable length, directed or
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
//! * subquery expressions (`subquery.rs`, S9): `EXISTS`, pattern
//!   predicates, `size(pattern)`, pattern comprehensions, each a LEFT JOIN
//!   of its matches grouped by the variables it reads;
//! * a final RETURN of values, whole nodes and relationships (`n`, `n.*`)
//!   and `id(n)`, with the result shape that Bolt, the HTTP graph output and
//!   embedded `query_graph` read (§4.13, [`ResultColumn`]).
//!
//! A node or relationship whose label / type set is empty matches nothing
//! (Cypher returns no rows; it is not an error): the query lowers to a
//! relation with no rows, and its properties read as NULL.

mod elements;
mod embedded;
mod expr;
mod null_list;
mod path;
mod subquery;
#[cfg(test)]
mod tests;
mod union;
mod unwind;
mod value;
pub mod write;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::graph_catalog::schema_types::{KeyPartType, TextSpelling};
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
use value::{Carried, GraphRef, NODE_TUPLES, NODE_VALUES, PATH_KEY, REL_TUPLES, REL_VALUES};

/// Why a bound plan was not lowered.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LowerError {
    /// Not lowered: an error on ClickHouse, the legacy pipeline on dialects
    /// that still fall back (`crate::translate`).
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
    /// A property of a node embedded in edge tables is an error when the
    /// rows of one id hold several values of it (#531); off: any of them.
    pub check_node_properties: bool,
    /// The server's reading of an integer compared with `id(n)`
    /// (`ReadOptions::id_mapper`); `None` (library, `cg`): `id(n)` is the
    /// node's key.
    pub id_lookup: Option<IdLookup>,
    /// Nodes a write statement deleted (its RETURN, S13d): returned whole
    /// without properties (Neo4j's deleted node: its identity alone).
    pub deleted: BTreeSet<VarId>,
    /// The dialect this query renders in. Supplied by the caller rather than
    /// read from the query context: `bound_plan` must not reach into
    /// `server` (see the boundary test), and a type name inside a CAST is
    /// dialect-specific, so it has to come in the same way as every other
    /// rendering input.
    pub dialect: crate::sql_generator::SqlDialect,
}

/// The node an integer `id()` value names on the server: its label and key
/// (a composite key's parts joined by `|`), `None` for none
/// (`id_mapper::resolve_node_id`).
#[derive(Clone)]
pub struct IdLookup(pub Arc<dyn Fn(i64) -> Option<(String, String)> + Send + Sync>);

impl std::fmt::Debug for IdLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IdLookup")
    }
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
    /// `id(r)` of a relationship (S11l): column `name` holds its element id
    /// (`TYPE:from->to`, as Bolt makes it); the server and the library
    /// return Bolt's number for it (`IdMapper::compute_deterministic_id`),
    /// SQL handed out as text keeps the element id.
    RelId,
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
    // FK edges as tables of relationships (S8a): a self-referencing one's
    // ends as its rows relate nodes (#632), its table read as its node's.
    match schema.lowering_view() {
        Some(read) => lower_over(stmt, &read, options),
        None => lower_over(stmt, schema, options),
    }
}

fn lower_over(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
) -> Result<Lowered, LowerError> {
    let constraints = constraint_reads(stmt, schema);
    let demand = demand(stmt, &constraints);
    let (lowered, unstable) = lower_pass(stmt, schema, options, &demand, false)?;
    // ClickHouse evaluates a CTE once per read (#1395): two reads of rows
    // that depend on the order an evaluation produces rows in (a SKIP /
    // LIMIT over ties, a `collect()` listing values in it) can disagree,
    // and a join of the reads (an OPTIONAL MATCH's or a subquery's drive, a
    // path's seed) then matches the wrong rows. Such CTEs are those the
    // stable lowering spells otherwise; when one is read more than once,
    // directly or through others, the stable lowering is the plan.
    if !unwind::rereads(&lowered.plan, &unwind::cte_names(&lowered.plan)) {
        return Ok(lowered);
    }
    let (stable, stable_unstable) = lower_pass(stmt, schema, options, &demand, true)?;
    let mut order_dependent = unwind::changed_ctes(&lowered.plan, &stable.plan);
    order_dependent.extend(unstable);
    if !unwind::rereads(&lowered.plan, &order_dependent) {
        return Ok(lowered);
    }
    unwind::reject_rereads_of_numbered_rows(&stable.plan, &stable_unstable)?;
    Ok(stable)
}

/// One lowering of the statement ([`Lowerer::stable`] when `stable`), and
/// the CTEs whose rows depend on how rows in no order were numbered.
fn lower_pass(
    stmt: &BoundStatement,
    schema: &GraphSchema,
    options: &LowerOptions,
    demand: &HashMap<VarId, BTreeSet<String>>,
    stable: bool,
) -> Result<(Lowered, BTreeSet<String>), LowerError> {
    let unstable = RefCell::new(BTreeSet::new());
    let query = Query {
        stmt,
        schema,
        options,
        demand,
        as_values: BTreeSet::new(),
        unstable: &unstable,
        stable,
    };
    if let BoundOp::Union {
        arms,
        arm_columns,
        all,
    } = &stmt.plan
    {
        let lowered = union::lower(&query, arms, arm_columns, *all)?;
        return Ok((lowered, unstable.take()));
    }
    let mut ctes = Vec::new();
    let arm = query.lower(&stmt.plan, &mut ctes)?;
    let mut plan = arm.plan;
    plan.ctes = CteItems(ctes);
    let lowered = Lowered {
        plan,
        shape: arm.shape,
    };
    Ok((lowered, unstable.take()))
}

/// What lowering one query (the statement, or an arm of a UNION) reads.
struct Query<'a> {
    stmt: &'a BoundStatement,
    schema: &'a GraphSchema,
    options: &'a LowerOptions,
    demand: &'a HashMap<VarId, BTreeSet<String>>,
    /// The RETURN items returned as values even when they are a node of one
    /// label or a relationship of one definition (UNION columns whose arms
    /// hold different ones, `union.rs`).
    as_values: BTreeSet<String>,
    /// The CTEs, by name, whose rows depend on how rows in no order were
    /// numbered ([`Lowerer::unstable`]), of every query lowered.
    unstable: &'a RefCell<BTreeSet<String>>,
    /// Lower what depends on the order an evaluation produces rows in to
    /// the same rows and values on every evaluation ([`Lowerer::stable`]).
    stable: bool,
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
        let mut l = self.lowerer(ctes);
        let lowered = l
            .relation(input)
            .and_then(|()| l.finish_relation())
            .and_then(|()| l.project(projection));
        *ctes = std::mem::take(&mut l.ctes);
        self.unstable
            .borrow_mut()
            .extend(std::mem::take(&mut l.unstable));
        let (plan, shape) = lowered?;
        Ok(LoweredQuery {
            plan,
            shape,
            identities: l.identities,
        })
    }

    /// A lowerer of this query, continuing the CTEs `ctes` (taken).
    fn lowerer(&self, ctes: &mut Vec<Cte>) -> Lowerer<'_> {
        Lowerer {
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
            key_targets: HashMap::new(),
            key_targets_by_label: HashMap::new(),
            grouped_paths: HashSet::new(),
            graph_values: HashMap::new(),
            map_keys: HashMap::new(),
            list_nulls: HashMap::new(),
            held_items: RefCell::new(HashMap::new()),
            no_column_kinds: std::cell::Cell::new(false),
            comparable_cache: RefCell::new(HashMap::new()),
            text_parts: RefCell::new(None),
            kinds: HashMap::new(),
            kinds_before: HashMap::new(),
            never_boolean: HashSet::new(),
            local_kinds: RefCell::new(HashMap::new()),
            local_lists: RefCell::new(HashMap::new()),
            collect_order: None,
            one_row: true,
            identities: Vec::new(),
            subqueries: &self.stmt.subqueries,
            projected: projected_items(&self.stmt.plan),
            as_values: self.as_values.clone(),
            ordered_unwinds: ordered_unwinds(&self.stmt.plan),
            numbers: HashSet::new(),
            id_values: HashMap::new(),
            rel_id_vars: HashSet::new(),
            unordered_numbers: HashSet::new(),
            unstable: BTreeSet::new(),
            stable: self.stable,
            final_items: false,
            collect_by_any: false,
            grouped_with: None,
        }
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
        /// It carries them as a list's elements (`path_node_tuples` /
        /// `path_rel_tuples`, `elements.rs`).
        node_tuples: bool,
        rel_tuples: bool,
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

    /// This arm's discovered types for the end `end` (0 = the row's start,
    /// 1 = its end), oriented as the arm walks: from-columns unless reversed.
    fn end_part_types(&self, end: usize) -> &[KeyPartType] {
        let (from, to) = if self.reversed {
            (
                &self.schema.to_node_id_part_types,
                &self.schema.from_node_id_part_types,
            )
        } else {
            (
                &self.schema.from_node_id_part_types,
                &self.schema.to_node_id_part_types,
            )
        };
        if end == 0 {
            from
        } else {
            to
        }
    }

    /// The label at `end` (0 = start, 1 = end), matching [`RelArm::end_part_types`].
    fn end_label(&self, end: usize) -> &str {
        let (f, t) = self.ends();
        if end == 0 {
            f
        } else {
            t
        }
    }
}

impl Lowerer<'_> {
    /// #1360: the CAST target for each part of `label`'s walk key, folded
    /// across every arm of this walk.
    ///
    /// A walk's node key is a single CTE shared by all its relationship types,
    /// so the sides cannot each choose their own target: if one arm widened a
    /// `Date` to `DateTime64(3)`, the arms whose ends are `Date` would stop
    /// joining it. Folding the whole set makes that agreement structural.
    fn walk_key_targets(&self, label: &str, arms: &[RelArm]) -> Vec<Option<String>> {
        let Some(ns) = self.schema.node_schema_opt(label) else {
            return Vec::new();
        };
        let sides: Vec<&[KeyPartType]> = arms
            .iter()
            .flat_map(|arm| {
                let mut out = Vec::new();
                if arm.end_label(0) == label {
                    out.push(arm.end_part_types(0));
                }
                if arm.end_label(1) == label {
                    out.push(arm.end_part_types(1));
                }
                out
            })
            .collect();
        self.key_part_targets(ns, &sides)
    }

    /// The per-part CAST target of a key built over `node`'s id, folded
    /// across every set of part types `sides` (an arm's two ends), spelled by
    /// the dialect. An empty entry means "no cast": the types already agree,
    /// or the mix is outside the table, in which case the SQL is exactly what
    /// it was before.
    fn key_part_targets(&self, node: &NodeSchema, sides: &[&[KeyPartType]]) -> Vec<Option<String>> {
        let arity = node
            .id_physical_columns()
            .len()
            .max(node.node_id.part_types.len());
        (0..arity)
            .map(|i| {
                let mut set: Vec<KeyPartType> = node
                    .node_id
                    .part_types
                    .get(i)
                    .cloned()
                    .into_iter()
                    .collect();
                for side in sides {
                    set.extend(side.get(i).cloned());
                }
                // An unknown part (one of a partly known id) tells nothing:
                // read as its `Integer` default, it would be cast by the
                // `Bool` / `Integer` rule.
                set.retain(|t| !t.raw.is_empty());
                // As an id's text is spelled where labels mix (I2): times as
                // instants of one zone, a `Date` with times of several zones
                // as a day or a time (`value::DAY_OR_TIME`).
                match crate::graph_catalog::schema_types::text_spelling(&set) {
                    TextSpelling::In(t) => Some(self.options.dialect.cast_target(&t)),
                    TextSpelling::DayOrTime => Some(value::DAY_OR_TIME.to_string()),
                    TextSpelling::Own => None,
                }
            })
            .collect()
    }

    /// Whether the nodes of a walk over `labels` (of the walk being lowered,
    /// whose key folds `key_targets_by_label` holds) hold their ids as texts,
    /// as its relationships' ends do ([`Self::union_holds_texts`]): of several
    /// labels, or of one whose `Date` id its relationships end at in times of
    /// several zones (its key fold `value::DAY_OR_TIME`).
    fn walk_holds_texts(&self, labels: &[String]) -> bool {
        labels.len() > 1
            || labels.iter().any(|l| {
                self.key_targets_by_label
                    .get(l)
                    .is_some_and(|t| t.iter().flatten().any(|t| t == value::DAY_OR_TIME))
            })
    }

    /// Whether a part of `node`'s id is a `Date` met by times of several
    /// zones among `sides` (`TextSpelling::DayOrTime`).
    fn day_or_time(&self, node: &NodeSchema, sides: &[&[KeyPartType]]) -> bool {
        (0..node.node_id.part_types.len()).any(|i| {
            let mut set: Vec<KeyPartType> = node
                .node_id
                .part_types
                .get(i)
                .cloned()
                .into_iter()
                .collect();
            for side in sides {
                set.extend(side.get(i).cloned());
            }
            crate::graph_catalog::schema_types::text_spelling(&set) == TextSpelling::DayOrTime
        })
    }

    /// Whether a union of relationships `arms` holds its ends' ids as texts
    /// ([`Self::compared_as`] `across`): where it mixes labels
    /// ([`arms_mix_labels`]), and between nodes of one label whose `Date` id
    /// part it ends at in times of several zones (`TextSpelling::DayOrTime`):
    /// typed, one column of them takes one zone, and `=` reads the `Date` at
    /// midnight in it, not in each end's own.
    fn union_holds_texts(&self, arms: &[RelArm]) -> bool {
        if arms_mix_labels(arms) {
            return true;
        }
        let Some(ns) = arms
            .first()
            .and_then(|a| self.schema.node_schema_opt(&a.schema.from_node))
        else {
            return false;
        };
        let sides: Vec<&[KeyPartType]> = arms
            .iter()
            .flat_map(|a| {
                [
                    a.schema.from_node_id_part_types.as_slice(),
                    a.schema.to_node_id_part_types.as_slice(),
                ]
            })
            .collect();
        self.day_or_time(ns, &sides)
    }

    /// #1360 (item 6, S8d2a): the CAST target for each part of a
    /// single-definition walk's BFS key, folded across the node and the
    /// edge's both ends.
    ///
    /// Same fold a union walk's node CTE makes (`walk_key_targets`), so a
    /// walk's every key is one type per part: the BFS builds several of them
    /// (the node's, and each relationship end's) and compares them as texts.
    /// A single-column id's BFS joins those columns typed, so it needs none —
    /// its SQL stays exactly what it was.
    fn path_key_targets(
        &self,
        node: &NodeSchema,
        edge: &RelationshipSchema,
    ) -> Vec<Option<String>> {
        if node.id_physical_columns().len() <= 1 {
            return Vec::new();
        }
        self.key_part_targets(
            node,
            &[&edge.from_node_id_part_types, &edge.to_node_id_part_types],
        )
    }

    /// ELEMENT_IDENTITY.md I1 (#1358): the type each id part of `label` is
    /// compared in, wherever a node of it meets a relationship's end: the
    /// fold of the node's id part type and every relationship end at a
    /// `label` node in the schema, where the backend has no common type for
    /// them (a `UInt64` id and an `Int64` end: joins and unions of them
    /// fail, Code 386; the dialect's `common_type_where_none`), spelled as a
    /// CAST target. Every producer of an identity part for comparison casts
    /// to it ([`Self::comparable`]); `across`: in a column that mixes labels,
    /// where every label's integer ids of at most 64 bits meet in one type.
    /// The two spellings meet as ClickHouse's integer supertypes allow
    /// (`Int128` with any integer of at most 64 bits). `None`: no cast, the
    /// SQL as before. Computed once per lowering.
    fn comparable_targets(&self, label: &str, across: bool) -> Vec<Option<String>> {
        let key = (label.to_string(), across);
        if let Some(t) = self.comparable_cache.borrow().get(&key) {
            return t.clone();
        }
        let targets = self.comparable_fold(label, across);
        self.comparable_cache
            .borrow_mut()
            .insert(key, targets.clone());
        targets
    }

    /// The id part types where nodes of each label meet relationship ends:
    /// the node's own, and every end at it, by label and part.
    fn id_part_sets(&self) -> std::collections::BTreeMap<String, Vec<Vec<KeyPartType>>> {
        let mut by_label: std::collections::BTreeMap<String, Vec<Vec<KeyPartType>>> =
            std::collections::BTreeMap::new();
        let mut add = |label: &str, types: &[KeyPartType]| {
            let sets = by_label.entry(label.to_string()).or_default();
            for (i, t) in types.iter().enumerate() {
                if sets.len() <= i {
                    sets.resize(i + 1, Vec::new());
                }
                if !t.raw.is_empty() {
                    sets[i].push(t.clone());
                }
            }
        };
        for (l, ns) in self.schema.all_node_schemas() {
            add(l.as_str(), &ns.node_id.part_types);
        }
        for rs in self.schema.get_relationships_schemas().values() {
            add(rs.from_node.as_str(), &rs.from_node_id_part_types);
            add(rs.to_node.as_str(), &rs.to_node_id_part_types);
        }
        by_label
    }

    /// [`Self::comparable_targets`], computed.
    fn comparable_fold(&self, label: &str, across: bool) -> Vec<Option<String>> {
        let by_label = self.id_part_sets();
        // Integers of at most 64 bits: `Int128` holds every one of them.
        let integers = |set: &[KeyPartType]| {
            set.iter().all(|t| {
                t.schema == crate::graph_catalog::schema_types::SchemaType::Integer
                    && !t.is_wider_than_64_bits()
            })
        };
        let own = by_label.get(label).cloned().unwrap_or_default();
        own.iter()
            .enumerate()
            .map(|(i, set)| {
                // In a column that mixes labels (a node of several labels, a
                // walk over them, a union of relationships between them), an
                // integer part meets every other label's integer parts: one
                // type for all of them. Elsewhere, the label's own (a cast
                // join key keeps ClickHouse from pushing a filter into the
                // other table's key: review finding).
                let set: Vec<KeyPartType> = match across && !set.is_empty() && integers(set) {
                    true => by_label
                        .values()
                        .filter_map(|sets| sets.get(i))
                        .filter(|s| integers(s))
                        .flatten()
                        .cloned()
                        .collect(),
                    false => set.clone(),
                };
                self.options
                    .dialect
                    .common_type_where_none(&set)
                    .map(|t| self.options.dialect.cast_target(&t))
            })
            .collect()
    }

    /// Id part `i` of a node of `label` (or a relationship end at one), `e`,
    /// as it is compared ([`Self::comparable_targets`]).
    pub(super) fn comparable(&self, label: &str, i: usize, e: RenderExpr) -> RenderExpr {
        self.compared_as(label, i, e, false)
    }

    /// [`Self::comparable`] in a column that mixes labels (`across`): there,
    /// an id part held as a text ([`Self::held_as_text`]) is its text.
    fn compared_as(&self, label: &str, i: usize, e: RenderExpr, across: bool) -> RenderExpr {
        if across && self.held_as_text(i) {
            return self.part_text(label, i, e);
        }
        match self
            .comparable_targets(label, across)
            .get(i)
            .cloned()
            .flatten()
        {
            Some(target) => RenderExpr::Cast {
                expr: Box::new(e),
                target,
            },
            None => e,
        }
    }

    /// ELEMENT_IDENTITY.md I2 (#1393): whether id part `i`, in a column that
    /// mixes labels (a node of several labels, a walk over them, a union of
    /// relationships between them), is held as a text: where the schema's
    /// labels hold that part in values of several kinds (an integer id and a
    /// string one), which one column has no common type for (ClickHouse
    /// makes it a `Variant`, and reads a string `'01'` into it as `1`). Over
    /// every label, as the cross-label fold is (I1), so any two such columns
    /// agree. Unknown types tell nothing: the SQL as before.
    fn held_as_text(&self, i: usize) -> bool {
        if let Some(parts) = self.text_parts.borrow().as_ref() {
            return parts.get(i).copied().unwrap_or(false);
        }
        let mut sets: Vec<Vec<KeyPartType>> = Vec::new();
        for label_sets in self.id_part_sets().into_values() {
            for (i, set) in label_sets.into_iter().enumerate() {
                if sets.len() <= i {
                    sets.resize(i + 1, Vec::new());
                }
                sets[i].extend(set);
            }
        }
        let parts: Vec<bool> = sets
            .iter()
            .map(|set| self.options.dialect.ids_held_as_text(set))
            .collect();
        let held = parts.get(i).copied().unwrap_or(false);
        *self.text_parts.borrow_mut() = Some(parts);
        held
    }

    /// Id part `i` of a node of `label` (or a relationship end at one), `e`,
    /// as a text ([`Self::held_as_text`]): spelled as its label's ids and ends
    /// spell theirs (`schema_types::text_spelling`), so a node and the ends at
    /// it spell one text exactly where `=` holds. Nullable: a typed side may be
    /// an OPTIONAL MATCH's NULL.
    fn part_text(&self, label: &str, i: usize, e: RenderExpr) -> RenderExpr {
        let set = self.id_part_set(label, i);
        // A string id is its text.
        if !set.is_empty() && set.iter().all(|t| t.raw.eq_ignore_ascii_case("String")) {
            return e;
        }
        let text = |e: RenderExpr| RenderExpr::Cast {
            expr: Box::new(e),
            target: self.text_target(true),
        };
        match crate::graph_catalog::schema_types::text_spelling(&set) {
            TextSpelling::Own => text(e),
            TextSpelling::In(t) => text(RenderExpr::Cast {
                expr: Box::new(e),
                target: self.nullable_target(&t),
            }),
            TextSpelling::DayOrTime => {
                RenderExpr::Raw(value::day_or_time(&render_expr_to_sql_plain(&e)))
            }
        }
    }

    /// Text `text` of id part `i` of `label` ([`Self::part_text`]) read back
    /// in type `own`, one of that part's columns: through the type it was
    /// spelled in, whose text it parses exactly (a time's in its zone; a
    /// day's text is midnight in `own`'s).
    fn read_back(&self, label: &str, i: usize, own: &KeyPartType, text: RenderExpr) -> RenderExpr {
        let text = match self.text_spelling(label, i) {
            TextSpelling::In(t) => RenderExpr::Cast {
                expr: Box::new(text),
                target: self.nullable_target(&t),
            },
            TextSpelling::Own | TextSpelling::DayOrTime => text,
        };
        RenderExpr::Cast {
            expr: Box::new(text),
            target: self.nullable_target(own),
        }
    }

    /// The CAST target of type `t`, nullable where the dialect allows it of
    /// that type (a NULL of an OPTIONAL MATCH).
    fn nullable_target(&self, t: &KeyPartType) -> String {
        match t.raw.to_ascii_lowercase().starts_with("lowcardinality(") {
            true => self.options.dialect.cast_target(t),
            false => self.options.dialect.cast_target(&KeyPartType {
                nullable: true,
                ..t.clone()
            }),
        }
    }

    /// The types id part `i` of `label` is held in: its nodes' and the ends'
    /// at it ([`Self::id_part_sets`]).
    fn id_part_set(&self, label: &str, i: usize) -> Vec<KeyPartType> {
        self.id_part_sets()
            .remove(label)
            .and_then(|mut sets| (i < sets.len()).then(|| sets.swap_remove(i)))
            .unwrap_or_default()
    }

    /// How [`Self::part_text`] spells id part `i` of `label`.
    fn text_spelling(&self, label: &str, i: usize) -> TextSpelling {
        crate::graph_catalog::schema_types::text_spelling(&self.id_part_set(label, i))
    }

    /// Whether [`Self::part_text`] spells id part `i` of `label` other than
    /// as its own text, so that a reader of it reads it back.
    fn read_back_differs(&self, label: &str, i: usize) -> bool {
        self.text_spelling(label, i) != TextSpelling::Own
    }

    /// `e`, of any type and maybe NULL, as a text.
    fn any_text(&self, e: RenderExpr) -> RenderExpr {
        RenderExpr::Cast {
            expr: Box::new(e),
            target: self.text_target(true),
        }
    }

    /// The CAST target of a text, nullable or not.
    fn text_target(&self, nullable: bool) -> String {
        self.options.dialect.cast_target(&KeyPartType {
            schema: crate::graph_catalog::schema_types::SchemaType::String,
            nullable,
            raw: "String".to_string(),
        })
    }

    /// Id part `i` of a node of `label` (or a relationship end at one), `e`,
    /// spelled as it is compared with a column that mixes labels: its text
    /// where that column holds one ([`Self::held_as_text`]; over `e` as its
    /// label compares it, which spells the same text), else `e` (the two
    /// types meet, I1).
    fn as_held(&self, label: &str, i: usize, e: RenderExpr) -> RenderExpr {
        match self.held_as_text(i) {
            true => self.part_text(label, i, e),
            false => e,
        }
    }

    /// The id parts `ids` of node `v` (without its label) as their own
    /// labels' texts: a node of several labels holds a part as a text folded
    /// with its relationships' ends (`held_as_text`, `part_text`), which is
    /// read back in each row's label's type and spelled from it there (its
    /// element id, `id()`'s value, are a node of one label's).
    fn own_texts(&self, v: VarId, ids: Vec<RenderExpr>) -> Result<Vec<RenderExpr>, LowerError> {
        let Some(Scan::Labels { arms, .. }) = self.scans.get(&v) else {
            return Ok(ids);
        };
        let label = self.physical(v, LABEL_COLUMN)?;
        Ok(ids
            .into_iter()
            .enumerate()
            .map(|(i, id)| {
                if !self.held_as_text(i) {
                    return id;
                }
                // Only a text spelled in another type than its own differs.
                let when_then: Vec<(RenderExpr, RenderExpr)> = arms
                    .iter()
                    .filter(|(l, _)| self.read_back_differs(l, i))
                    .filter_map(|(l, ns)| {
                        let own = ns.node_id.part_types.get(i).filter(|t| !t.raw.is_empty())?;
                        let back = RenderExpr::Cast {
                            expr: Box::new(self.read_back(l, i, own, id.clone())),
                            target: self.text_target(true),
                        };
                        Some((
                            RenderExpr::OperatorApplicationExp(eq(
                                label.clone(),
                                RenderExpr::Literal(Literal::String(l.clone())),
                            )),
                            back,
                        ))
                    })
                    .collect();
                match when_then.is_empty() {
                    true => id,
                    false => RenderExpr::Case(RenderCase {
                        expr: None,
                        when_then,
                        else_expr: Some(Box::new(id)),
                    }),
                }
            })
            .collect())
    }

    /// Id part `i` of the `to` (else `from`) ends of union of relationships
    /// `v` of `arms`, `id`, as its own end's text: a union that mixes labels
    /// holds a part as a text folded with its label's ids and ends
    /// (`held_as_text`, `part_text`), read back in each row's end column's
    /// type where that differs (its element id is a relationship of one
    /// definition's).
    fn own_end_text(
        &self,
        v: VarId,
        arms: &[RelArm],
        to: bool,
        i: usize,
        id: RenderExpr,
    ) -> Result<RenderExpr, LowerError> {
        if !self.union_holds_texts(arms) || !self.held_as_text(i) {
            return Ok(id);
        }
        let mut when_then = Vec::new();
        let mut seen = BTreeSet::new();
        for arm in arms {
            let rs = arm.schema;
            let (label, types) = match to {
                true => (&rs.to_node, &rs.to_node_id_part_types),
                false => (&rs.from_node, &rs.from_node_id_part_types),
            };
            let Some(own) = types.get(i).filter(|t| !t.raw.is_empty()) else {
                continue;
            };
            if !self.read_back_differs(label, i) || !seen.insert(std::ptr::from_ref(rs)) {
                continue;
            }
            let is = |c: &str, value: &str| -> Result<RenderExpr, LowerError> {
                Ok(RenderExpr::OperatorApplicationExp(eq(
                    self.physical(v, c)?,
                    RenderExpr::Literal(Literal::String(value.to_string())),
                )))
            };
            let definition = and_all(vec![
                is(REL_TYPE, &arm.rel_type)?,
                is(REL_FROM_LABEL, &rs.from_node)?,
                is(REL_TO_LABEL, &rs.to_node)?,
            ])
            .expect("a definition has columns");
            when_then.push((
                definition,
                RenderExpr::Cast {
                    expr: Box::new(self.read_back(label, i, own, id.clone())),
                    target: self.text_target(true),
                },
            ));
        }
        Ok(match when_then.is_empty() {
            true => id,
            false => RenderExpr::Case(RenderCase {
                expr: None,
                when_then,
                else_expr: Some(Box::new(id)),
            }),
        })
    }

    /// Whether the end columns relationship `r` is tied by mix labels
    /// ([`Self::compared_as`] `across`): a union's of several labels, or a
    /// relationship between two labels read in both orientations (`turned`,
    /// [`Self::turned_ends`]).
    fn ends_mix_labels(&self, r: VarId, turned: bool) -> bool {
        match self.scans.get(&r) {
            Some(Scan::Rel { schema, .. }) => turned && schema.from_node != schema.to_node,
            Some(Scan::Rels { arms, .. }) => self.union_holds_texts(arms),
            _ => false,
        }
    }

    /// Which identity columns (`REL_ID_{i}`) of a union of relationships
    /// `arms` are texts (I2): those where some arm's stored end holds an id
    /// part as a text there ([`Self::held_as_text`], in a union that mixes
    /// labels); every arm's is then a text, an `edge_id`'s too.
    fn rel_id_texts(&self, arms: &[RelArm]) -> Vec<bool> {
        let Ok(shape) = rel_union_shape(arms) else {
            return Vec::new();
        };
        let mut texts = vec![false; shape.identity];
        if !self.union_holds_texts(arms) {
            return texts;
        }
        for rs in arms.iter().map(|a| a.schema) {
            if rs.edge_id.is_some() {
                continue;
            }
            let from = rs.from_id.columns().len();
            for (i, text) in texts
                .iter_mut()
                .enumerate()
                .take(from + rs.to_id.columns().len())
            {
                let part = if i < from { i } else { i - from };
                *text |= self.held_as_text(part);
            }
        }
        texts
    }

    /// [`Self::rel_id_texts`] of relationship `v`'s scan (none but a union's).
    fn rel_scan_id_texts(&self, v: VarId) -> Vec<bool> {
        match self.scans.get(&v) {
            Some(Scan::Rels { arms, .. }) => self.rel_id_texts(arms),
            _ => Vec::new(),
        }
    }

    /// Whether some id part of `label` is compared in a cast type.
    fn compared_cast(&self, label: &str) -> bool {
        self.comparable_targets(label, false)
            .iter()
            .any(Option::is_some)
    }
}

/// A label's comparable id part types, as CAST targets
/// ([`Lowerer::comparable_targets`]).
type ComparableTargets = Vec<Option<String>>;

/// How [`Lowerer::edge_ends`] reads an edge table's rows.
#[derive(Clone, Copy, PartialEq)]
enum Orientation {
    /// Each row from its stored `from` to its `to`.
    Forward,
    /// Each row from its stored `to` to its `from`.
    Backward,
    /// Each row both ways (an undirected relationship).
    Both,
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
                node_tuples,
                rel_tuples,
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
                node_tuples,
                rel_tuples,
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
    map_keys: HashMap<VarId, MapKeys>,
    list_nulls: HashMap<VarId, Option<null_list::NullList>>,
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
    /// #1360: the CAST target of each key part of a walked node's identity,
    /// recorded by [`Lowerer::walk_nodes`] under the walked variable.
    ///
    /// The shortest-path tie-back spells the same identity again, from the
    /// node's own table rather than from that CTE, and it has to use the same
    /// target: a `Date` read raw stringifies as `2020-01-01` against the
    /// widened key's `2020-01-01 00:00:00.000`, and the two never join.
    /// Recorded rather than re-derived, because folding needs the walk's arms
    /// and that site has only the `Walk`. Keyed by the variable rather than the
    /// label so that two walks over one label, with different arms and so
    /// different targets, cannot overwrite each other.
    key_targets: HashMap<VarId, Vec<Option<String>>>,
    /// The same folds, by label — the fallback [`Self::walk_key`] reads when
    /// the key it spells belongs to an endpoint variable of the walk, which
    /// never carries an entry of [`Self::key_targets`]. Its entries live only
    /// WHILE the walk that records them is being built
    /// ([`Self::build_union_path`]: recorded under that walk's arms before its
    /// first restriction is spelled, dropped when it returns), so a later walk
    /// over a shared label never reads another walk's fold — a filtered seed
    /// must spell this walk's fold, which before was a previous walk's or none
    /// at all.
    key_targets_by_label: HashMap<String, Vec<Option<String>>>,
    /// Paths a grouping RETURN (DISTINCT, an aggregation) returns: its
    /// ORDER BY reads their lengths as any of a group's, which the path
    /// (the group's key) determines.
    grouped_paths: HashSet<VarId>,
    /// Graph values (`value.rs`) the segment's CTE carries, by variable;
    /// each is also in `values`.
    graph_values: HashMap<VarId, Carried>,
    /// Map values the segment's CTE carries with each key's value (S11m):
    /// a map literal a WITH projected; each is also in `values`.
    map_keys: HashMap<VarId, MapKeys>,
    /// When each list value the segment's CTE carries is NULL, and the
    /// list held so (`null_list.rs`): its `{v}__null` and `{v}__held`
    /// columns (`None`: never NULL). A list not here is not known to be
    /// either.
    list_nulls: HashMap<VarId, Option<null_list::NullList>>,
    /// Of the projection being lowered, each list item that may be NULL as
    /// its WHERE / ORDER BY read it (`null_list.rs`): when it is NULL and
    /// the list held so, of its group where it groups.
    held_items: RefCell<HashMap<VarId, null_list::NullList>>,
    /// While set, a property's kind is not read from its column's type
    /// (VALUE_TYPES.md T2a): what a check written before the column types
    /// were known asks ([`Self::kind_before_column_types`]).
    no_column_kinds: std::cell::Cell<bool>,
    /// Each label's comparable id part types (`comparable_targets`), by
    /// label and whether across labels: computed once per lowering.
    comparable_cache: RefCell<HashMap<(String, bool), ComparableTargets>>,
    /// Which id parts a column that mixes labels holds as texts
    /// (`held_as_text`): computed once per lowering.
    text_parts: RefCell<Option<Vec<bool>>>,
    /// What each projected, unwound, subquery or collected-map value is
    /// (`unwind.rs`, set by `set_kind` only).
    kinds: HashMap<VarId, unwind::Kind>,
    /// [`Self::kinds`] as it was with properties' column types left
    /// unread, where it differs: what `kind_before_column_types` reads of a
    /// variable. Both are set together, by `set_kind` only.
    kinds_before: HashMap<VarId, unwind::Kind>,
    /// Projected values that are never a boolean (`Lowerer::never_boolean`).
    never_boolean: HashSet<VarId>,
    /// What each comprehension parameter is: an element of its list. Filled
    /// in as [`Lowerer::kind`] reads the comprehension (variables are unique
    /// per query, so one map serves every scope).
    local_kinds: RefCell<HashMap<VarId, unwind::Kind>>,
    /// Each comprehension parameter's list, as SQL, while the comprehension
    /// is lowered (`Lowerer::in_members`).
    local_lists: RefCell<HashMap<VarId, String>>,
    /// While a projection over rows in an order is lowered: the rows'
    /// number in that order, which `collect()` lists its values in.
    collect_order: Option<RenderExpr>,
    /// The rows so far are at most one (no MATCH yet, or an aggregation with
    /// no grouping item since).
    one_row: bool,
    /// The identity of each node, relationship or graph value the final
    /// RETURN returns, by item name ([`LoweredQuery::identities`]).
    identities: Vec<(String, Vec<RenderExpr>)>,
    /// The statement's subquery expressions (`subquery.rs`).
    subqueries: &'s [Subquery],
    /// The expression each WITH / RETURN item was projected from.
    projected: HashMap<VarId, &'s LogicalExpr>,
    /// RETURN items returned as values ([`Query::as_values`]).
    as_values: BTreeSet<String>,
    /// The UNWINDs (by variable) whose elements a later clause takes in
    /// order ([`ordered_unwinds`]).
    ordered_unwinds: HashSet<VarId>,
    /// Sort-key columns (as SQL) that are numbers the lowering made: a
    /// row's number, an element's position. Never NULL; sorted ascending,
    /// they need no numbering of their own for `collect()`
    /// ([`Lowerer::order_collect`]).
    numbers: HashSet<String>,
    /// Values that are `id(n)` of a node of this label (S11l): a WITH item
    /// `id(n)`, or one passing such a value on.
    id_values: HashMap<VarId, String>,
    /// RETURN items that are `id(r)` of a relationship (S11l): Bolt's number
    /// is computed after the query, so nothing in the SQL orders by it.
    rel_id_vars: HashSet<VarId>,
    /// Those of [`Lowerer::numbers`] that number rows in no order
    /// (`Unwind::any_row_number`), or derive from such a number: ClickHouse
    /// numbers the rows anew each time it evaluates the CTE.
    unordered_numbers: HashSet<String>,
    /// The CTEs, by name, whose rows depend on such a number: a SKIP /
    /// LIMIT by it, a `collect()` in its order. Read more than once, their
    /// reads can disagree (`unwind::reject_rereads_of_numbered_rows`).
    unstable: BTreeSet<String>,
    /// Lower what depends on the order an evaluation produces rows in to
    /// the same rows and values on every evaluation (#1395): ClickHouse
    /// evaluates a CTE once per read, and two reads joined must agree. A
    /// `collect()` over rows in no order lists its values sorted; rows
    /// numbered, or taken by SKIP / LIMIT, are ordered by every column
    /// after their own keys (ties broken). Set for a second lowering when
    /// the first reads such a CTE more than once ([`lower_over`]).
    stable: bool,
    /// The final SELECT's items are being lowered.
    final_items: bool,
    /// The projection being lowered collects in the order of such a number.
    collect_by_any: bool,
    /// The last WITH, when it aggregates: its CTE's alias, its grouping
    /// keys and its aggregated values (each determined by the keys).
    grouped_with: Option<(String, Vec<VarId>, Vec<VarId>)>,
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
/// 1 on a row of a relationship read in its stored direction, 0 reversed:
/// where an edge constraint reads which end is `from` (S11h).
const FORWARD: &str = "__cg_forward";
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
/// A node's identity as a text, as a walk by keys, a union's search and a
/// path's identity spell it: its label (SQL) and its id's columns (SQL, the
/// column or their tuple, whose text no two ids share, S8d2b).
fn walk_key_sql(
    g: &crate::sql_generator::function_mapper::GraphValues,
    label: &str,
    ids: &[String],
) -> String {
    (g.text)(&[
        label.to_string(),
        value::string(":"),
        value::identity_text(g, ids),
    ])
}

/// Whether a union of relationships `arms` has columns that mix labels: its
/// arms' ends are of several.
fn arms_mix_labels(arms: &[RelArm]) -> bool {
    arms.iter()
        .flat_map(|a| [a.schema.from_node.as_str(), a.schema.to_node.as_str()])
        .collect::<BTreeSet<_>>()
        .len()
        > 1
}

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
                self.finish_relation()?;
                for item in &projection.items {
                    let kind = self.kind(&item.expr);
                    self.record_kind(item.var, &item.expr, kind);
                    if self.never_boolean(&item.expr) {
                        self.never_boolean.insert(item.var);
                    }
                }
                // Aggregating with no grouping item: one row.
                if projection.aggregates() && projection.items.iter().all(|i| i.aggregate) {
                    self.one_row = true;
                }
                self.with(projection)
            }
            BoundOp::Sort { input, keys } => {
                self.relation(input)?;
                let exprs: Vec<&LogicalExpr> = keys.iter().map(|k| &k.expr).collect();
                self.apply_subqueries(&exprs)?;
                let keys = self.sort_keys(keys, &HashMap::new(), None)?;
                // Sorting by constants keeps the order the rows had.
                if !keys.is_empty() {
                    self.order = RowOrder::Keys(keys);
                }
                Ok(())
            }
            BoundOp::Skip { input, count } => {
                self.relation(input)?;
                self.finish_relation()?;
                self.page(Some(*count), None)
            }
            BoundOp::Limit { input, count } => {
                self.relation(input)?;
                self.finish_relation()?;
                self.page(None, Some(*count))
            }
            BoundOp::Unwind { input, expr, var } => {
                self.relation(input)?;
                self.finish_relation()?;
                self.apply_subqueries(&[expr])?;
                self.unwind(expr, *var)
            }
            BoundOp::Filter { input, predicate } => {
                self.relation(input)?;
                self.apply_subqueries(&[predicate])?;
                let e = self.condition(predicate, &HashMap::new())?;
                self.filters.push(e);
                Ok(())
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
        // The WHERE conjuncts that read a subquery are applied to the
        // clause's rows once they are built (`subquery.rs`).
        let (plain, deferred) = Self::split_subquery_conjuncts(predicate);
        let predicate = plain.as_ref();
        // Decide every element's scan first (a relationship's table depends
        // on its endpoints' labels), then join them in path order: node,
        // relationship, node, … so each scan joins on the one before it.
        let mut clause_rels: Vec<VarId> = Vec::new();
        // A node of the clause may be read from the row of the first
        // relationship it is an end of (S8b, `Lowerer::embed_end`): not
        // from a later one (the first's tie reads it before), and not when a
        // variable-length relationship of the clause starts or ends at it
        // (the walk ties to it).
        let mut walk_ends: Vec<VarId> = Vec::new();
        let mut embeddable: Vec<(VarId, VarId)> = Vec::new();
        for p in &pattern.parts {
            for (i, r) in p.rels.iter().enumerate() {
                for end in [p.nodes[i].var, p.nodes[i + 1].var] {
                    if r.length.is_some() {
                        walk_ends.push(end);
                    } else if !embeddable.iter().any(|(n, _)| *n == end) {
                        embeddable.push((end, r.var));
                    }
                }
            }
        }
        // Nor a node from an earlier clause: in an OPTIONAL MATCH's
        // matches it is read again from its table, where the earlier
        // clause's conditions on it are re-applied (review finding).
        let earlier: Vec<VarId> = pattern
            .parts
            .iter()
            .flat_map(|p| p.nodes.iter().filter(|n| n.bound_before).map(|n| n.var))
            .collect();
        embeddable.retain(|(n, _)| !walk_ends.contains(n) && !earlier.contains(n));
        for part in &pattern.parts {
            if part.shortest.is_some() {
                Self::shortest_pattern(part)?;
            }
            for n in &part.nodes {
                self.node_scan(n.var)?;
            }
            for (i, r) in part.rels.iter().enumerate() {
                self.rel_scan(r, part.nodes[i].var, part.nodes[i + 1].var, &embeddable)?;
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
            .map(|p| self.condition(p, &HashMap::new()))
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
            // From a node the relation has already (an earlier clause's or
            // part's): a part whose first node is new but whose last is not
            // is joined from its last, each step tied to the one before (from
            // the first it would start as a product with the relation).
            let last = part.nodes.len() - 1;
            let backward = last > 0
                && !self.is_emitted(part.nodes[0].var)
                && self.is_emitted(part.nodes[last].var);
            let steps: Vec<usize> = match backward {
                true => (0..part.rels.len()).rev().collect(),
                false => (0..part.rels.len()).collect(),
            };
            self.emit(part.nodes[if backward { last } else { 0 }].var)?;
            for i in steps {
                let r = &part.rels[i];
                if let Some(Scan::Path { .. }) = self.scans.get(&r.var) {
                    let (left, right) = (part.nodes[i].var, part.nodes[i + 1].var);
                    let search = in_search.get(&r.var).map(Vec::as_slice).unwrap_or(&[]);
                    self.build_path(r, left, right, &own, search)?;
                }
                self.emit(r.var)?;
                self.emit(part.nodes[if backward { i } else { i + 1 }].var)?;
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
                self.filters.push(null_test(&self.presence(v, &id), false));
            }
        }
        self.uniqueness(&clause_rels)?;
        self.filters.extend(filter);
        if !deferred.is_empty() {
            // A shortest path's WHERE conjunct reading the path holds in its
            // search (§4.8), where a subquery is not applied.
            let path_vars: Vec<String> = pattern
                .parts
                .iter()
                .filter(|p| p.shortest.is_some())
                .flat_map(|p| p.path_var.into_iter().chain(p.rels.iter().map(|r| r.var)))
                .map(|v| v.name())
                .collect();
            if deferred.iter().any(|c| {
                referenced_names(c, false)
                    .iter()
                    .any(|n| path_vars.contains(n))
            }) {
                return unsupported(
                    "a shortestPath's WHERE conjunct reading a subquery and the path",
                );
            }
            let deferred: Vec<&LogicalExpr> = deferred.iter().collect();
            self.apply_subqueries(&deferred)?;
            for c in deferred {
                let e = self.condition(c, &HashMap::new())?;
                self.filters.push(e);
            }
        }
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
        // And what its WHERE's subqueries read (`subquery.rs`).
        let nested = self.nested_subquery_exprs(&read);
        read.extend(nested);
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
        // And what its relationships' edge constraints read of their ends
        // (S11h): such an end is read from its table or exported by the
        // drive, never from an edge's endpoint columns.
        let rels: Vec<(VarId, RelDirection, VarId, VarId)> = pattern
            .parts
            .iter()
            .flat_map(|part| {
                part.rels
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| r.length.is_none() && !r.bound_before)
                    .map(|(i, r)| (r.var, r.direction, part.nodes[i].var, part.nodes[i + 1].var))
            })
            .collect();
        for e in rel_constraint_exprs(self.schema, self.bindings, &rels) {
            read_props.extend(
                property_refs(&e)
                    .into_iter()
                    .filter_map(|(n, p)| Some((parse_var(&n)?, p))),
            );
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

    /// A drive's rows read right after an aggregating WITH (`at_with`, the
    /// segment reads its CTE) whose aggregated values it reads: those
    /// values, determined by the WITH's grouping keys, and the keys the
    /// drive does not read (carried, and joined on NULL-safely, a key's
    /// group can be NULL's). The rows are joined on the keys, not on the
    /// aggregated values (#1395): ClickHouse evaluates the WITH's CTE again
    /// for the drive, and a `collect()` in another order, a float sum
    /// added in another order, would join no match; a list of maps cannot
    /// be joined on at all. Nothing when a key cannot be carried (a graph
    /// value) or joined on (a map).
    fn grouped_drive(
        &self,
        shared: &[VarId],
        where_only: &[VarId],
        at_with: bool,
    ) -> (Vec<VarId>, Vec<VarId>) {
        let Some((alias, keys, aggregated)) = &self.grouped_with else {
            return (Vec::new(), Vec::new());
        };
        let read = |v: &VarId| shared.contains(v) || where_only.contains(v);
        if !at_with || self.emitted.first() != Some(alias) || !aggregated.iter().any(read) {
            return (Vec::new(), Vec::new());
        }
        // A map (a `Dynamic`'s) is no join key.
        fn joinable(k: &unwind::Kind) -> bool {
            match k {
                unwind::Kind::Map(_) => false,
                unwind::Kind::List(k) => joinable(k),
                _ => true,
            }
        }
        let mut carried = Vec::new();
        for k in keys.iter().filter(|k| !read(k)) {
            match self.values.get(k) {
                Some(e) if is_constant(e) => continue,
                Some(_)
                    if !self.graph_values.contains_key(k)
                        && self.var_kind(*k).is_some_and(|kind| joinable(&kind)) =>
                {
                    carried.push(*k)
                }
                None if self.scans.contains_key(k) => carried.push(*k),
                _ => return (Vec::new(), Vec::new()),
            }
        }
        (aggregated.clone(), carried)
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
                // An end compared in a cast type is read and tied, cast (I1):
                // the elided node's place would compare its raw columns.
                if self.compared_cast(&fl) || self.compared_cast(&tl) {
                    return None;
                }
                let rel_type = types.iter().next().expect("one type");
                // No such edge: the pattern matches nothing (decided later).
                let Ok(rs) = self.edge_schema(rel_type, &fl, &tl) else {
                    return None;
                };
                // Its edge constraint reads the node's properties: the
                // node is read from its table (S11h).
                if rs.constraints.is_some() {
                    return None;
                }
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
        self.finish_relation()?;
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
        let (determined, keys) = self.grouped_drive(shared, where_only, only_a_cte);
        let where_only: Vec<VarId> = where_only.iter().chain(&keys).copied().collect();
        let where_only = where_only.as_slice();
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
                columns.push(column.clone());
                // When a list is NULL (`null_list.rs`): its column too, and
                // joined on (a NULL list and `[]` are held alike).
                let mut null_columns: Vec<(String, RenderExpr)> = Vec::new();
                match self.list_nulls.get(v).cloned() {
                    Some(Some(n)) => {
                        let mut import = |e: RenderExpr| -> Result<RenderExpr, LowerError> {
                            if is_constant(&e) {
                                return Ok(e);
                            }
                            let c = column_of(&e, &w)?;
                            columns.push(c.clone());
                            if c != column {
                                null_columns.push((c.clone(), e));
                            }
                            Ok(col_at(&d, &c))
                        };
                        let flag = import(n.flag)?;
                        let held = import(n.held)?;
                        q.list_nulls
                            .insert(*v, Some(null_list::NullList { flag, held }));
                    }
                    Some(None) => {
                        q.list_nulls.insert(*v, None);
                    }
                    None => {}
                }
                if determined.contains(v) {
                    continue;
                }
                correlation.push(Correlated {
                    column: column.clone(),
                    outer: e.clone(),
                    inner: col_at(&d, &column),
                    // A value's nullability is not tracked.
                    null_safe: where_only.contains(v),
                });
                for (n_column, n) in null_columns {
                    correlation.push(Correlated {
                        inner: col_at(&d, &n_column),
                        outer: n,
                        column: n_column,
                        null_safe: true,
                    });
                }
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
            physical.retain(|c, _| {
                !path::VALUE_COLUMNS.contains(&c.as_str())
                    && !path::TUPLE_COLUMNS.contains(&c.as_str())
            });
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
        self.finish_relation()?;
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
        std::mem::swap(&mut self.map_keys, &mut s.map_keys);
        std::mem::swap(&mut self.list_nulls, &mut s.list_nulls);
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
        for (_, ns) in arms {
            self.node_table(ns)?;
        }
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
            // Each arm's id as its label's is compared (I1).
            for (i, c) in ns.id_physical_columns().iter().enumerate() {
                items.push(select(
                    self.compared_as(label, i, col_at(ROW, c), true),
                    &indexed_column(LABEL_ID, i),
                ));
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
    fn rel_scan(
        &mut self,
        r: &PatRel,
        left: VarId,
        right: VarId,
        embeddable: &[(VarId, VarId)],
    ) -> Result<(), LowerError> {
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
            let ends = self.decide_rel(r.var, arms, left, right)?;
            // Ends embedded in its rows are read there (S8b): before the
            // ties, which read the ends' identities.
            if let [(from, ..), (to, ..)] = ends.as_slice() {
                let (from, to) = (*from, *to);
                for (end, is_from) in [(from, true), (to, false)] {
                    if embeddable.contains(&(end, r.var)) {
                        self.embed_end(r.var, end, is_from)?;
                    }
                }
            }
            if let Some(c) = self.rel_constraint(r.var, left, right, &ends)? {
                self.filters.push(c);
            }
            (ends, false)
        };
        let mixed = self.ends_mix_labels(r.var, turned);
        for (end, label, cols) in ends {
            self.tie_end(r.var, end, label, cols, turned, mixed)?;
        }
        Ok(())
    }

    /// The condition the edge `constraints:` of the definitions relationship
    /// `r` is read from put on its rows (S11h), or `None`: a relationship
    /// exists only where its definition's constraint holds of its two
    /// nodes. `ends` are its ties ([`Self::decide_rel`]): of one definition
    /// in one orientation, its stored `from` then `to`; of one definition in
    /// both, its `left` and `right`, each row saying its orientation
    /// ([`FORWARD`]).
    fn rel_constraint(
        &mut self,
        r: VarId,
        left: VarId,
        right: VarId,
        ends: &Ends,
    ) -> Result<Option<RenderExpr>, LowerError> {
        let forward = |l: &Self| -> Result<RenderExpr, LowerError> {
            Ok(RenderExpr::OperatorApplicationExp(eq(
                l.physical(r, FORWARD)?,
                RenderExpr::Literal(Literal::Integer(1)),
            )))
        };
        let case = |when_then: Vec<(RenderExpr, RenderExpr)>, otherwise: RenderExpr| {
            RenderExpr::Case(RenderCase {
                expr: None,
                when_then,
                else_expr: Some(Box::new(otherwise)),
            })
        };
        match self.scans.get(&r).cloned() {
            Some(Scan::Rel {
                schema, both: None, ..
            }) => {
                let [(from, ..), (to, ..)] = ends.as_slice() else {
                    return Ok(None); // it matches nothing
                };
                self.edge_constraint(schema, *from, *to)
            }
            Some(Scan::Rel {
                schema,
                both: Some(_),
                ..
            }) => {
                let (Some(stored), Some(reversed)) = (
                    self.edge_constraint(schema, left, right)?,
                    self.edge_constraint(schema, right, left)?,
                ) else {
                    return Ok(None);
                };
                Ok(Some(case(vec![(forward(self)?, stored)], reversed)))
            }
            // A union's arms keep only the rows their constraints hold of
            // (`Self::constrained_rows`).
            _ => Ok(None),
        }
    }

    /// The edge `constraints:` of `rs`, read of a relationship from `from` to
    /// `to` (its stored ends), or `None` when it has none.
    fn edge_constraint(
        &self,
        rs: &RelationshipSchema,
        from: VarId,
        to: VarId,
    ) -> Result<Option<RenderExpr>, LowerError> {
        let Some(text) = &rs.constraints else {
            return Ok(None);
        };
        // Read as everywhere else a constraint is (the generator, a
        // union's arms): of the nodes' declared column properties.
        let node = |label: &str| self.schema.node_schema_opt(label);
        let (Some(from_ns), Some(to_ns)) = (node(&rs.from_node), node(&rs.to_node)) else {
            return unsupported("an edge constraint between labels without node schemas");
        };
        if let Err(e) = crate::graph_catalog::constraint_compiler::compile_constraint(
            text, from_ns, to_ns, "from", "to",
        ) {
            return unsupported(format!("edge constraint `{text}`: {e}"));
        }
        let bound = crate::bound_plan::expr::bind_edge_constraint(text, from, to)
            .map_err(|e| LowerError::Unsupported(e.to_string()))?;
        Ok(Some(self.expr(&bound, &HashMap::new())?))
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
                // Each end as it is compared with its nodes (I1).
                let compared = |label: &str, cols: Vec<RenderExpr>| -> Vec<RenderExpr> {
                    cols.into_iter()
                        .enumerate()
                        .map(|(i, c)| self.comparable(label, i, c))
                        .collect()
                };
                vec![
                    (
                        from,
                        EndLabel::Is(schema.from_node.clone()),
                        compared(&schema.from_node, physical(cols(&schema.from_id))?),
                    ),
                    (
                        to,
                        EndLabel::Is(schema.to_node.clone()),
                        compared(&schema.to_node, physical(cols(&schema.to_id))?),
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
        // Each end is either one: of two labels, a column that mixes them,
        // held as such (I2; a union's are already).
        let (fc, tc) = match (&fl, &tl) {
            (EndLabel::Is(a), EndLabel::Is(b)) if a != b => {
                let held = |label: &str, cols: Vec<RenderExpr>| -> Vec<RenderExpr> {
                    cols.into_iter()
                        .enumerate()
                        .map(|(i, c)| self.as_held(label, i, c))
                        .collect()
                };
                (held(a, fc), held(b, tc))
            }
            _ => (fc, tc),
        };
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
    /// orientation join comes after the node's. `mixed`: `cols` mix labels
    /// ([`Self::ends_mix_labels`]).
    fn tie_end(
        &mut self,
        r: VarId,
        end: VarId,
        label: EndLabel,
        cols: Vec<RenderExpr>,
        turned: bool,
        mixed: bool,
    ) -> Result<(), LowerError> {
        let labels = self.labels_of(end)?;
        if labels.is_empty() {
            return Ok(()); // an endpoint that matches nothing
        }
        let end_labels = label.labels();
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
        // The node's id as it is compared (I1); the relationship's columns
        // are, where they were read. A several-labels node's id columns are
        // its arms', each cast there (`label_union`). Where only one side
        // mixes labels, the other is spelled as that side holds its ids (I2).
        let node_mixed = matches!(self.scans.get(&end), Some(Scan::Labels { .. }));
        let ids: Vec<RenderExpr> = match (labels.as_slice(), node_mixed) {
            (_, true) => ids,
            ([one], false) => ids
                .into_iter()
                .enumerate()
                .map(|(i, id)| {
                    let id = self.comparable(one, i, id);
                    match mixed {
                        true => self.as_held(one, i, id),
                        false => id,
                    }
                })
                .collect(),
            _ => ids,
        };
        let cols: Vec<RenderExpr> = match (node_mixed && !mixed, end_labels.as_slice()) {
            (true, [one]) => cols
                .into_iter()
                .enumerate()
                .map(|(i, c)| self.as_held(one, i, c))
                .collect(),
            _ => cols,
        };
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

    /// [`Self::tie_end`] for a walk by keys (S8d2b): `key`, its column of an
    /// end's identity as a text, equals the end's ([`Self::walk_key`]).
    fn tie_end_by_key(
        &mut self,
        r: VarId,
        end: VarId,
        label: EndLabel,
        key: RenderExpr,
    ) -> Result<(), LowerError> {
        let labels = self.labels_of(end)?;
        if labels.is_empty() {
            return Ok(()); // an endpoint that matches nothing
        }
        // The label: as `tie_end` ties it (the key holds it too).
        match &label {
            EndLabel::Is(l) if labels.contains(l) => self.holds_label(end, l)?,
            EndLabel::By { labels: held, .. } if held.iter().any(|l| labels.contains(l)) => {}
            _ => {
                self.empty = true;
                return Ok(());
            }
        }
        let Some(own) = self.walk_key(end)? else {
            return Ok(());
        };
        self.tie(r, end, vec![(key, own)]);
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
        self.rel_union_of(v, arms, &props, false, false, false)
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
        by_keys: bool,
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
        let mixed_labels = self.union_holds_texts(arms);
        let id_texts = self.rel_id_texts(arms);
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
            // Ids as they are compared (I1): an identity of stored ends as
            // its ends are (an `edge_id`'s is compared with itself only).
            let id_label = |i: usize| match (&rs.edge_id, i < from.len()) {
                (Some(_), _) => None,
                (None, true) => Some((rs.from_node.as_str(), i)),
                (None, false) => Some((rs.to_node.as_str(), i - from.len())),
            };
            for i in 0..shape.identity {
                // A text where some arm's is one (I2).
                let text = id_texts.get(i).copied().unwrap_or(false);
                let e = identity
                    .get(i)
                    .map_or(RenderExpr::Literal(Literal::Null), |c| {
                        match (id_label(i), text) {
                            (Some((label, part)), true) => {
                                self.part_text(label, part, col_at(ROW, c))
                            }
                            (Some((label, part)), false) => {
                                self.compared_as(label, part, col_at(ROW, c), mixed_labels)
                            }
                            (None, true) => self.any_text(col_at(ROW, c)),
                            (None, false) => col_at(ROW, c),
                        }
                    });
                items.push(select(e, &indexed_column(REL_ID, i)));
            }
            let (starts, ends) = if arm.reversed {
                (&to, &from)
            } else {
                (&from, &to)
            };
            // A walk by keys (S8d2b) joins its rows by their ends' keys.
            let both: [(&str, &Vec<&str>, &str); 2] =
                [(BOTH_START, starts, left), (BOTH_END, ends, right)];
            let walked_by = match by_keys {
                true => &both[..0],
                false => &both[..],
            };
            // Each end as its label's ids are compared (I1).
            for (side, cols, label) in [
                (REL_FROM, &from, rs.from_node.as_str()),
                (REL_TO, &to, rs.to_node.as_str()),
            ]
            .into_iter()
            .chain(walked_by.iter().copied())
            {
                for (i, c) in cols.iter().enumerate() {
                    items.push(select(
                        self.compared_as(label, i, col_at(ROW, c), mixed_labels),
                        &indexed_column(side, i),
                    ));
                }
            }
            // #1360: a walk key is compared as TEXT, so both sides must
            // stringify identically. The target for each part is folded across
            // every arm of this walk (`walk_key_targets`) so the shared node
            // key and every arm's ends agree; each side is cast to it, or left
            // alone when the types already agree.
            let node_targets = |label: &str| self.walk_key_targets(label, arms);
            let end_key = |g: &crate::sql_generator::function_mapper::GraphValues,
                           label: &str,
                           targets: &[Option<String>],
                           cols: &[&str]| {
                let ids: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let sql = render_expr_to_sql_plain(&col_at(ROW, c));
                        value::cast_key_part(&sql, targets.get(i).and_then(|t| t.as_deref()))
                    })
                    .collect();
                walk_key_sql(g, &value::string(label), &ids)
            };
            if by_keys {
                let Some(g) = &g else {
                    return unsupported(
                        "a variable-length relationship between composite ids in this SQL dialect",
                    );
                };
                for (label, targets, cols, side) in [
                    (left, node_targets(left), starts, BOTH_START),
                    (right, node_targets(right), ends, BOTH_END),
                ] {
                    items.push(select(
                        RenderExpr::Raw(end_key(g, label, &targets, cols)),
                        &indexed_column(side, 0),
                    ));
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
                    for (label, targets, cols, column) in [
                        (left, node_targets(left), starts, REL_START_KEY),
                        (right, node_targets(right), ends, REL_END_KEY),
                    ] {
                        items.push(select(
                            RenderExpr::Raw(end_key(g, label, &targets, cols)),
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
            // A row whose end has a NULL part relates no node (its key's text
            // would equal a node's of the same parts: `=` decides, S8d2b).
            // Each composite end's parts (review finding: a composite `to`
            // after a single-id `from` was not filtered).
            if by_keys {
                for c in [&from, &to].into_iter().filter(|e| e.len() > 1).flatten() {
                    filters.push(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::IsNotNull,
                        operands: vec![col_at(ROW, c)],
                    }));
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
            let joins = self.constrained_rows(rs, ROW, &mut filters)?;
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
                    name: table,
                    alias: Some(ROW.to_string()),
                    use_final: rs.should_use_final(),
                })),
                joins: JoinItems(joins),
                filters: FilterItems(and_all(filters)),
                ..empty_plan()
            });
        }
        let body = RenderPlan {
            union: UnionItems(Some(Union {
                input,
                union_type: UnionType::All,
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

    /// The rows of definition `rs` (its table read as `row`) its edge
    /// `constraints:` keeps (S11h): joined to its two nodes' tables, the
    /// constraint among `filters`. No joins when it has none.
    fn constrained_rows(
        &self,
        rs: &RelationshipSchema,
        row: &str,
        filters: &mut Vec<RenderExpr>,
    ) -> Result<Vec<Join>, LowerError> {
        let Some(text) = &rs.constraints else {
            return Ok(Vec::new());
        };
        let (from_ns, to_ns) = (
            self.schema.node_schema_opt(&rs.from_node),
            self.schema.node_schema_opt(&rs.to_node),
        );
        let (Some(from_ns), Some(to_ns)) = (from_ns, to_ns) else {
            return unsupported("an edge constraint between labels without node schemas");
        };
        if !from_ns.is_standard_own_table() || !to_ns.is_standard_own_table() {
            return unsupported(
                "an edge constraint of a definition read with others whose nodes are not \
                 each in their own table",
            );
        }
        const FROM: &str = "__cg_cfrom";
        const TO: &str = "__cg_cto";
        let mut joins = Vec::new();
        for (ns, label, id, alias) in [
            (from_ns, rs.from_node.as_str(), &rs.from_id, FROM),
            (to_ns, rs.to_node.as_str(), &rs.to_id, TO),
        ] {
            let mut j = join(self.node_relation_sql(ns)?, alias);
            let node_id = ns.id_physical_columns();
            if node_id.len() != id.columns().len() {
                return unsupported("an edge constraint between ids of another number of columns");
            }
            // Both sides as the label's ids are compared (I1).
            j.joining_on = node_id
                .iter()
                .zip(id.columns())
                .enumerate()
                .map(|(i, (n, e))| {
                    eq(
                        self.comparable(label, i, col_at(alias, n)),
                        self.comparable(label, i, col_at(row, e)),
                    )
                })
                .collect();
            joins.push(j);
        }
        let sql = crate::graph_catalog::constraint_compiler::compile_constraint(
            text, from_ns, to_ns, FROM, TO,
        )
        .map_err(|e| LowerError::Unsupported(format!("edge constraint `{text}`: {e}")))?;
        filters.push(RenderExpr::Raw(format!("({sql})")));
        Ok(joins)
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
        self.edge_ends(rs, v, Orientation::Both)
    }

    /// The edge of directed variable-length relationship `v` over `rs` where
    /// its ends are compared in a cast type (I1, [`Self::comparable_targets`]):
    /// the CTE `{v}_ends` of its rows read one way (`backward`: from `to` to
    /// `from`), as [`Self::both_directions`] reads them. `None` where no end
    /// is cast: the walk reads the table itself, as before.
    fn typed_ends(
        &mut self,
        rs: &'s RelationshipSchema,
        v: VarId,
        backward: bool,
    ) -> Result<Option<String>, LowerError> {
        if !self.compared_cast(&rs.from_node) && !self.compared_cast(&rs.to_node) {
            return Ok(None);
        }
        let orientation = match backward {
            true => Orientation::Backward,
            false => Orientation::Forward,
        };
        self.edge_ends(rs, v, orientation).map(Some)
    }

    /// The rows of `rs` read in `orientation` ([`Self::both_directions`],
    /// [`Self::typed_ends`]).
    fn edge_ends(
        &mut self,
        rs: &'s RelationshipSchema,
        v: VarId,
        orientation: Orientation,
    ) -> Result<String, LowerError> {
        let name = match orientation {
            Orientation::Both => format!("{}_both", v.name()),
            _ => format!("{}_ends", v.name()),
        };
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
            let (start_label, end_label) = match forward {
                true => (rs.from_node.as_str(), rs.to_node.as_str()),
                false => (rs.to_node.as_str(), rs.from_node.as_str()),
            };
            if rs.constraints.is_some() && orientation == Orientation::Both {
                // Which end the edge constraint reads as `from` (S11h).
                let flag = RenderExpr::Literal(Literal::Integer(i64::from(forward)));
                items.push(select(flag, FORWARD));
            }
            // Each end as its label's ids are compared (I1).
            for (i, c) in starts.iter().enumerate() {
                items.push(select(
                    self.comparable(start_label, i, col_at(ROW, c)),
                    &indexed_column(BOTH_START, i),
                ));
            }
            for (i, c) in ends.iter().enumerate() {
                items.push(select(
                    self.comparable(end_label, i, col_at(ROW, c)),
                    &indexed_column(BOTH_END, i),
                ));
            }
            let mut filters: Vec<RenderExpr> = filter.iter().cloned().collect();
            if !forward && orientation == Orientation::Both {
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
                    name: table.clone(),
                    alias: Some(ROW.to_string()),
                    use_final: rs.should_use_final(),
                })),
                filters: FilterItems(and_all(filters)),
                ..empty_plan()
            }
        };
        let body = match orientation {
            Orientation::Both => RenderPlan {
                union: UnionItems(Some(Union {
                    input: vec![arm(true), arm(false)],
                    union_type: UnionType::All,
                })),
                ..empty_plan()
            },
            Orientation::Forward => arm(true),
            Orientation::Backward => arm(false),
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
            node_tuples: false,
            rel_tuples: false,
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
        // A walk of one definition between composite ids walks them by their
        // columns (S8d); a union's walks them by their keys (S8d2b).
        if let Scan::Path {
            walked: Walked::Union { .. },
            ..
        } = &scan
        {
            for end in [from, to] {
                let _ = self.id_columns(end)?;
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
            None => self.standard_edge(&rel_type, only)?,
        };
        // With another label at either end only the path of none is left: a
        // node to itself.
        let walks = fl == rs.from_node && tl == rs.to_node;
        if !walks && (fl != tl || min > 0) {
            return Ok(Some(Scan::Impossible));
        }
        let range = if walks { (min, max) } else { (0, Some(0)) };
        let ns = self.schema.node_schema_opt(&fl).expect("a scanned label");
        // The generator applies the edge constraint (S11h), compiling it as
        // here; what it cannot compile it would drop.
        if let Some(text) = &rs.constraints {
            if let Err(e) = crate::graph_catalog::constraint_compiler::compile_constraint(
                text, ns, ns, "from", "to",
            ) {
                return unsupported(format!(
                    "a variable-length relationship whose edge constraint `{text}` \
                     does not compile: {e}"
                ));
            }
        }
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
            // The generator reads no table options: the walk of its
            // definitions reads them (S8c: a polymorphic table's rows of one
            // type are a filter).
            return Ok(None);
        }
        // A condition of the MATCH reads them before the walk is built
        // (`build_path`, which decides alike).
        let node_tuples = self.walk_carries(r, NODE_TUPLES, &fl, ns, &rel_type, rs);
        let rel_tuples = self.walk_carries(r, REL_TUPLES, &fl, ns, &rel_type, rs);
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
            node_tuples,
            rel_tuples,
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
        // An edge constraint (S11h): the generator reads its `from` at the
        // node a step leaves, so it walks the stored direction, from the
        // stored `from` end. Read both ways, searched for shortest paths, or
        // restricted only at its `to` end (walked forward, it would expand
        // from every node: review), the walk of its definitions follows only
        // the relationships the constraint keeps (`Self::constrained_rows`).
        if edge.constraints.is_some()
            && (undirected || shortest.is_some() || self.walk_first(left, right, own) != from_end)
        {
            return self.build_union_path(
                r,
                BTreeSet::from([rel_type]),
                at,
                (min, max),
                shortest,
                (left, right),
                own,
                in_search,
            );
        }
        let first = match edge.constraints {
            Some(_) => from_end,
            None => self.walk_first(left, right, own),
        };
        let last = if first == left { right } else { left };
        let backward = !undirected && first != from_end;
        // Its nodes / relationships read as values (the demand pass): the
        // search carries them. A shortest path's are recovered from its
        // search also when its identity is read (`PATH_KEY`).
        let wants = |name: &str| self.demand.get(&r.var).is_some_and(|d| d.contains(name));
        let (node_values, rel_values) = (wants(NODE_VALUES), wants(REL_VALUES));
        let Some(Scan::Node {
            schema: node,
            label,
            ..
        }) = self.scans.get(&first).cloned()
        else {
            return unsupported("internal: a path endpoint is not a node");
        };
        // Read as a list's elements (`elements.rs`): carried as tuples.
        let (node_tuples, rel_tuples) = (
            self.walk_carries(r.var, NODE_TUPLES, &label, node, &rel_type, edge),
            self.walk_carries(r.var, REL_TUPLES, &label, node, &rel_type, edge),
        );
        let walked = node_values || rel_values || node_tuples || rel_tuples || wants(PATH_KEY);
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
        // A directed walk whose ends are compared in a cast type reads its
        // edge through a CTE of them cast, as an undirected one does (I1).
        let both = match undirected {
            true => Some(self.both_directions(edge, r.var)?),
            false => self.typed_ends(edge, r.var, backward)?,
        };
        self.node_table(node)?;
        // #1360 (item 6): the walk's BFS keys are texts (S8d2a) and need the
        // fold of the node and both of the edge's ends, spelled now (single-
        // column ids walk typed joins and get none).
        let key_casts = self.path_key_targets(node, edge);
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
            node_tuples,
            rel_tuples,
            key_casts: key_casts.clone(),
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
                node_tuples,
                rel_tuples,
            },
        );
        // A composite id is tied by its columns (`start_<col>` /
        // `end_<col>`, S8d): its text in `start_id` / `end_id` two ids can
        // share. A shortest path's search keys it by the text of its tuple
        // (`path::composite_key`, S8d2).
        let id_columns = node.id_physical_columns();
        for (end, column, side) in [(first, "start_id", "start"), (last, "end_id", "end")] {
            let Some(ids) = self.identity(end)? else {
                continue; // an endpoint that matches nothing
            };
            let eqs = match (ids.as_slice(), id_columns.as_slice()) {
                ([id], [_]) => vec![(col_at(&r.var.name(), column), id.clone())],
                (ids, cols) if ids.len() == cols.len() && shortest.is_some() => {
                    // #1360 (item 6): the search keys a node by its folded,
                    // casted tuple, so this tie spells the same — or the tie
                    // compares the raw text against the casted one and drops
                    // the path the search found.
                    let rendered: Vec<String> = ids
                        .iter()
                        .enumerate()
                        .map(|(i, id)| {
                            value::cast_key_part(
                                &render_expr_to_sql_plain(id),
                                key_casts.get(i).and_then(|t| t.as_deref()),
                            )
                        })
                        .collect();
                    let Some(key) = path::composite_key(&rendered) else {
                        return unsupported(
                            "a shortestPath between composite ids in this SQL dialect",
                        );
                    };
                    vec![(col_at(&r.var.name(), column), RenderExpr::Raw(key))]
                }
                (ids, cols) if ids.len() == cols.len() => cols
                    .iter()
                    .zip(ids)
                    .map(|(c, id)| (col_at(&r.var.name(), &format!("{side}_{c}")), id.clone()))
                    .collect(),
                _ => return unsupported("internal: a path's end of another arity than its id"),
            };
            self.tie(r.var, end, eqs);
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
        // #1360, review round 3: a filtered or bound end spells its rows
        // THROUGH `walk_key`, whose (label, arms) fold is read from this map —
        // so record every label's fold under THIS walk's arms before the first
        // restriction is spelled, and drop the entries again when the walk is
        // built (see the returns below). Recorded `walk_nodes`-time, the seed
        // read an entry a previous walk had left over a shared label — or none
        // at all — and spelled a raw (or foreign-widened) key against this
        // walk's casted node CTE, which matched nothing: the issue's own
        // filtered query returns rows none, and queries main gets right lose
        // them. Scoped to the call: what a later walk reads, it computes from
        // its own (label, arms).
        for label in &labels {
            let targets = self.walk_key_targets(label, &arms);
            self.key_targets_by_label.insert(label.clone(), targets);
        }
        let Some(start) = self.walk_restriction(first, own, path::START, &labels)? else {
            self.unrecord_walk_folds(&labels);
            self.scans.insert(r.var, Scan::Impossible);
            return Ok(());
        };
        // A shortest-path search ends once it has reached every value the
        // last node can have.
        let end = match shortest {
            Some(_) => match self.walk_restriction(last, own, path::END, &labels)? {
                Some(end) => end,
                None => {
                    self.unrecord_walk_folds(&labels);
                    self.scans.insert(r.var, Scan::Impossible);
                    return Ok(());
                }
            },
            None => Vec::new(),
        };
        let wants = |name: &str| self.demand.get(&r.var).is_some_and(|d| d.contains(name));
        let (node_values, rel_values) = (wants(NODE_VALUES), wants(REL_VALUES));
        // Its nodes or relationships, of several labels or types, have no one
        // tuple type (`elements.rs`): it carries none, and reading them as a
        // list's elements is not lowered (`Lowerer::path_elem`).
        // A shortest path's are recovered from its search also when its
        // identity is read.
        let walked = node_values || rel_values || wants(PATH_KEY);
        let by_keys = self.walks_by_keys(&labels);
        let nodes = self.walk_nodes(r.var, &labels, node_values, &arms)?;
        let props: BTreeSet<String> = r.props.iter().map(|(p, _)| p.clone()).collect();
        let rels = match arms.is_empty() {
            true => None,
            false => Some(self.rel_union_of(
                r.var,
                &arms,
                &props,
                rel_values,
                shortest.is_some(),
                by_keys,
            )?),
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
                    self.unrecord_walk_folds(&labels);
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
                node_tuples: false,
                rel_tuples: false,
            },
        );
        let column = |c: &str| col_at(&alias, c);
        let ends = [
            (first, first_labels, path::START_LABEL, "start_id"),
            (last, labels.clone(), path::END_LABEL, "end_id"),
        ];
        let walk_mixes = self.walk_holds_texts(&labels);
        for (end, held, label, id) in ends {
            let label = EndLabel::of(held, column(label));
            match by_keys {
                // Its id is its key (S8d2b).
                true => self.tie_end_by_key(r.var, end, label, column(id))?,
                // Its ends' ids mix labels where its nodes are of several.
                false => self.tie_end(r.var, end, label, vec![column(id)], false, walk_mixes)?,
            }
        }
        // The walk is built — its ties were spelled while its folds still
        // held — so a later walk over a shared label spells only ever its own.
        self.unrecord_walk_folds(&labels);
        Ok(())
    }

    /// Drop [`Self::key_targets_by_label`]'s entries for `labels`: a walk
    /// records its folds only while it is being built (`build_union_path`),
    /// from its own arms, so a later walk never borrows them.
    fn unrecord_walk_folds(&mut self, labels: &[String]) {
        for label in labels {
            self.key_targets_by_label.remove(label);
        }
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
            self.standard_edge(&a.rel_type, a.schema)?;
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
            // Its definitions' ends of one arity (a walk by keys, S8d2b, when
            // composite).
            rel_union_shape(&arms)?;
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
    /// Whether a walk over nodes of `labels` goes by their keys (S8d2b): one
    /// of them has a composite id. Its nodes' ids (`LABEL_ID_0`) and its
    /// relationships' ends (`BOTH_START_0` / `BOTH_END_0`) are then each
    /// node's identity as a text ([`walk_key_sql`]), one column whatever the
    /// id's arity, which every label's and definition's rows spell alike.
    fn walks_by_keys(&self, labels: &[String]) -> bool {
        labels.iter().any(|l| {
            self.schema
                .node_schema_opt(l)
                .is_some_and(|ns| ns.id_physical_columns().len() > 1)
        })
    }

    /// Node `v`'s identity as a text, as a walk by keys and a union's search
    /// key a node (`KEY`): its label and id ([`walk_key_sql`]).
    fn walk_key(&self, v: VarId) -> Result<Option<RenderExpr>, LowerError> {
        let g = value::spelling()?;
        let Some(ids) = self.identity(v)? else {
            return Ok(None);
        };
        let (label, ids) = match (self.scans.get(&v), ids.split_first()) {
            (Some(Scan::Labels { .. }), Some((label, ids))) => {
                (render_expr_to_sql_plain(label), ids.to_vec())
            }
            (Some(Scan::Node { label, .. }), _) => (value::string(label), ids),
            _ => return unsupported(format!("internal: {v} is not a node")),
        };
        // #1360: spell each part as the walk's node CTE does, or this key
        // joins `toString(<raw Date>)` against the widened one and matches
        // nothing. The targets are the ones `walk_nodes` recorded -- keyed by
        // the walked variable, whose entry a walk's endpoint variable does
        // not have: the CTE is built under the walk's variable, and the
        // shortest-path search ties `first`/`last` back to their own tables
        // after it. Those fall back to the label's fold, the same (label,
        // arms) fold the CTE spelled. An endpoint read through its label CTE
        // (`Scan::Labels`, `label_union`) holds its raw id columns, so it
        // spells each row by its own label's fold.
        let fold = |label: &str| {
            self.key_targets_by_label
                .get(label)
                .cloned()
                .unwrap_or_default()
        };
        // A node of several labels may hold an id part as a text (I2,
        // `held_as_text`): each row's is read back in its label's type (`of`),
        // which its key spells, as the walk's nodes spell it.
        // A text of its own type is its key's, as one part (a part of a
        // composite one is spelled as a tuple's, its type's).
        let read_back_of = |of: &str, i: usize| {
            self.held_as_text(i) && (ids.len() > 1 || self.read_back_differs(of, i))
        };
        let held = match self.scans.get(&v) {
            Some(Scan::Labels { arms, .. }) => arms
                .iter()
                .any(|(l, _)| (0..ids.len()).any(|i| read_back_of(l, i))),
            _ => false,
        };
        let read_back = |of: &str, i: usize, id: &RenderExpr| -> RenderExpr {
            let own = self
                .schema
                .node_schema_opt(of)
                .and_then(|ns| ns.node_id.part_types.get(i))
                .filter(|t| !t.raw.is_empty());
            match (read_back_of(of, i), own) {
                (true, Some(t)) => self.read_back(of, i, t, id.clone()),
                _ => id.clone(),
            }
        };
        let spell = |targets: &[Option<String>], of: Option<&str>| {
            let ids: Vec<String> = ids
                .iter()
                .enumerate()
                .map(|(i, id)| {
                    let id = match of {
                        Some(of) => read_back(of, i, id),
                        None => id.clone(),
                    };
                    value::cast_key_part(
                        &render_expr_to_sql_plain(&id),
                        targets.get(i).and_then(|t| t.as_deref()),
                    )
                })
                .collect();
            walk_key_sql(&g, &label, &ids)
        };
        let targets = match (self.key_targets.get(&v).cloned(), self.scans.get(&v)) {
            (Some(targets), _) => targets,
            (None, Some(Scan::Node { label, .. })) => fold(label),
            (None, Some(Scan::Labels { arms, .. })) => {
                let folds: Vec<(&String, Vec<Option<String>>)> =
                    arms.iter().map(|(l, _)| (l, fold(l))).collect();
                match folds.split_first() {
                    Some(((_, first), rest)) if !held && rest.iter().all(|(_, f)| f == first) => {
                        first.clone()
                    }
                    _ => {
                        let whens: Vec<String> = folds
                            .iter()
                            .map(|(l, f)| {
                                format!(
                                    "WHEN {label} = {} THEN {}",
                                    value::string(l),
                                    spell(f, Some(l))
                                )
                            })
                            .collect();
                        return Ok(Some(RenderExpr::Raw(format!(
                            "CASE {} END",
                            whens.join(" ")
                        ))));
                    }
                }
            }
            (None, _) => Vec::new(),
        };
        Ok(Some(RenderExpr::Raw(spell(&targets, None))))
    }

    fn walk_nodes(
        &mut self,
        v: VarId,
        labels: &[String],
        values: bool,
        arms: &[RelArm],
    ) -> Result<String, LowerError> {
        let by_keys = self.walks_by_keys(labels);
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
            // #1360: this CTE is shared by every relationship type in the
            // walk, so its key has to be cast to the same target as all of
            // their ends -- otherwise an arm that widens leaves the others
            // unable to join it.
            let targets = self.walk_key_targets(label, arms);
            // The shortest-path tie-back spells this key again from the node's
            // own table; record the target so it spells it the same way.
            self.key_targets.insert(v, targets.clone());
            // (The label-keyed folds are `build_union_path`'s: recorded under
            // its own arms before its first restriction is spelled and
            // dropped when it returns, so `walk_key`'s label fallback never
            // crosses walks.)
            let ids: Vec<String> = ns
                .id_physical_columns()
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let sql = render_expr_to_sql_plain(&col_at(ROW, c));
                    value::cast_key_part(&sql, targets.get(i).and_then(|t| t.as_deref()))
                })
                .collect();
            let key = RenderExpr::Raw(walk_key_sql(&g, &value::string(label), &ids));
            // By keys, its id is its key (S8d2b); otherwise its one column.
            let id = match (by_keys, ns.id_physical_columns().as_slice()) {
                (true, _) => key.clone(),
                // As its label's ids are compared (I1): the walk's steps join
                // it to its relationships' ends, cast so.
                (false, [id]) => {
                    self.compared_as(label, 0, col_at(ROW, id), self.walk_holds_texts(labels))
                }
                (false, _) => return unsupported("internal: a composite id not walked by keys"),
            };
            let mut items = vec![
                select(
                    RenderExpr::Literal(Literal::String(label.clone())),
                    LABEL_COLUMN,
                ),
                select(id, &indexed_column(LABEL_ID, 0)),
                select(key, KEY),
            ];
            if values {
                items.push(select(
                    RenderExpr::Raw(value::table_node_object(&g, self.schema, ns, label, ROW)?),
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
                &self.node_table(ns)?,
                ns.view_parameters.as_deref(),
                self.options.view_parameter_values.as_ref(),
            );
            input.push(RenderPlan {
                select: SelectItems {
                    items,
                    distinct: false,
                },
                from: FromTableItem(Some(ViewTableRef {
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
                // A walk by keys holds a node's key as its id (S8d2b).
                (_, _) if self.walks_by_keys(among) => {
                    let label = match self.scans.get(&v) {
                        Some(Scan::Node { label, .. }) => {
                            RenderExpr::Literal(Literal::String(label.clone()))
                        }
                        _ => ids[0].clone(),
                    };
                    let Some(key) = self.walk_key(v)? else {
                        return Ok(None);
                    };
                    (label, key)
                }
                (Some(Scan::Labels { .. }), [label, id]) => (label.clone(), id.clone()),
                // As the walk's nodes hold it where they mix labels (I2).
                (Some(Scan::Node { label: l, .. }), [id]) => (
                    RenderExpr::Literal(Literal::String(l.clone())),
                    match self.walk_holds_texts(among) {
                        true => self.as_held(l, 0, id.clone()),
                        false => id.clone(),
                    },
                ),
                _ => {
                    return unsupported("a variable-length relationship between composite ids (S8)")
                }
            };
            // A node of several labels holds its id as the walk's nodes do
            // where they mix labels; else as that one label's, its walk's.
            let id_column = match (self.walks_by_keys(among), self.scans.get(&v), among) {
                (false, Some(Scan::Labels { .. }), [one]) if !self.walk_holds_texts(among) => {
                    self.as_held(one, 0, id_column)
                }
                _ => id_column,
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
            let id_column = schema.id_physical_columns();
            if id.len() != id_column.len() {
                return unsupported("internal: a node's identity of another arity than its id");
            }
            rows.select = SelectItems {
                items: id
                    .iter()
                    .enumerate()
                    .map(|(k, c)| match id.len() {
                        1 => select(c.clone(), "id"),
                        _ => select(c.clone(), &format!("id_{k}")),
                    })
                    .collect(),
                distinct: true,
            };
            // A composite id (S8d): its columns as a tuple.
            let columns: Vec<String> = id_column
                .iter()
                .map(|c| render_expr_to_sql_plain(&col_at(alias, c)))
                .collect();
            let held = match columns.as_slice() {
                [one] => one.clone(),
                _ => format!("({})", columns.join(", ")),
            };
            conds.push(RenderExpr::Raw(format!(
                "{held} IN ({})",
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
        &mut self,
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
        // Where a step of a recursive CTE cannot group (Spark) the search
        // counts no paths: `allShortestPaths` walks them back, a row each
        // (the relation as a walk's, its paths' columns unread).
        let read_paths = walked;
        let walked = walked
            || (all
                && current_function_mapper()
                    .shortest_path_search()
                    .is_some_and(|s| !s.grouped_steps));
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
        // Between composite ids (S8d2) the search and the trails key a node
        // by the text of its id's tuple (`path::composite_key`; the
        // generator's too, S8d2c).
        let composite = match &walk {
            path::Walk::One(call) => {
                let cols = call.node.id_physical_columns();
                (cols.len() > 1).then_some(cols)
            }
            path::Walk::Union(_) => None,
        };
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
            // Walked for its count alone: the paths, without their columns.
            if walked && !read_paths {
                let ends = path::ends_reached(&search);
                let ctes = path::counted_paths_ctes(&search, &name, &ends)?;
                return Ok(Some((ctes, name, read_paths)));
            }
            let mut ctes = vec![path::search_cte(&search, counted, parents)?];
            if walked {
                let ends = path::ends_reached(&search);
                ctes.extend(path::walk_ctes(&search, &name, &ends, all)?);
            } else {
                ctes.push(path::reached_cte(&search, &name, all, true)?);
            }
            return Ok(Some((ctes, name, read_paths)));
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
        for end in [first, last] {
            if let Some(Scan::Node { schema, .. }) = self.scans.get(&end) {
                let schema: &'s NodeSchema = schema;
                self.node_table(schema)?;
            }
        }
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
                let key = match (&walk, &composite) {
                    (path::Walk::One(_), Some(cols)) => {
                        // #1360 (item 6): spell each part as the search's BFS
                        // keys do -- the same folded targets (`walk`'s node
                        // and edge ends) the BFS's keys were pre-spelled
                        // with, so this tie-back joins text for text. It
                        // read `key_targets` by an endpoint variable that no
                        // single-definition walk records and spelled the raw
                        // tuple, which the searched keys then never matched.
                        let at: Vec<String> = cols
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                let target = search.casts().get(i).and_then(|t| t.as_deref());
                                value::cast_key_part(&format!("{a}.{c}"), target)
                            })
                            .collect();
                        match path::composite_key(&at) {
                            Some(key) => key,
                            None => {
                                return unsupported(
                                    "a shortestPath between composite ids in this SQL dialect",
                                )
                            }
                        }
                    }
                    (path::Walk::One(_), None) => id,
                    // As the union's walk keys it (its label and id, or
                    // the tuple of a composite id's columns, S8d2b).
                    (path::Walk::Union(_), _) => match self.walk_key(end)? {
                        Some(key) => render_expr_to_sql_plain(&key),
                        None => value::node_key_text(&value::spelling()?, &label, &id),
                    },
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
        // The trails start only where a pair's distance fails the conditions
        // (where the dialect lets them read the search; the pick keeps those
        // pairs either way).
        let near = format!("vlp_{var}_near");
        let nested = current_function_mapper()
            .shortest_path_search()
            .is_some_and(|s| s.nested_recursion);
        if nested {
            // The trails' first node, keyed as the search keys it.
            let first_key = search.node_key(path::START);
            walk.start_mut().push(RenderExpr::Raw(format!(
                "{first_key} IN (SELECT start_id FROM ({}))",
                path::failing_pairs(&var, alias, &ends, &conditions),
            )));
        }
        let columns = search.walked_columns();
        let (trails, trail_edges, trails_name) = walk.trails(self.schema)?;
        let (pick, name) = path::pick_cte(
            &var,
            &trails_name,
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
        Ok(Some((ctes, name, read_paths)))
    }

    /// The relation of the nodes of `schema`'s label, as SQL to read under
    /// an alias: its table with its view parameters, FINAL and `filter:` (a
    /// subquery when it has either of the last two), so each node is one
    /// row there is.
    fn node_relation_sql(&self, schema: &NodeSchema) -> Result<String, LowerError> {
        // A node relation's CTE is added when the end is scanned.
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
        // Through the dialect: its FINAL, and `*` reading every column a
        // property can map to (ALIAS / MATERIALIZED ones: S11k review).
        let m = current_function_mapper();
        let fin = if use_final { m.final_modifier() } else { "" };
        let settings = m.all_columns_settings();
        Ok(format!(
            "(SELECT * FROM {table} AS {ROW}{fin}{filter}{settings})"
        ))
    }

    /// A `shortestPath` / `allShortestPaths` pattern as Neo4j takes it: one
    /// variable-length relationship, between two node variables.
    fn shortest_pattern(part: &PatternPart) -> Result<(), LowerError> {
        let [r] = part.rels.as_slice() else {
            return unsupported("shortestPath over other than one relationship");
        };
        // One written without a length binds as `*1..1` (S11i).
        if r.length.is_none() {
            return unsupported("internal: shortestPath over a fixed-length relationship");
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
        // Each relation with its FINAL (S11k), which it keeps wherever it
        // is placed below.
        type Relation = (String, String, Vec<OperatorApplication>, JoinType, bool);
        let mut relations: Vec<Relation> = vec![(
            from.alias.clone().unwrap_or_default(),
            from.name.clone(),
            Vec::new(),
            JoinType::Join,
            from.use_final,
        )];
        for j in &self.joins {
            relations.push((
                j.table_alias.clone(),
                j.table_name.clone(),
                j.joining_on.clone(),
                j.join_type.clone(),
                j.use_final,
            ));
        }
        // Ties: ON conjuncts, and conjuncts placed in WHERE.
        let mut links: Vec<Vec<String>> = relations
            .iter()
            .flat_map(|(_, _, on, ..)| on)
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
        for (i, (alias, table, on, kind, use_final)) in relations
            .into_iter()
            .filter(|(a, ..)| held.contains(a))
            .enumerate()
        {
            if i == 0 {
                rows.from = FromTableItem(Some(if from.alias.as_deref() == Some(&alias) {
                    from.clone()
                } else {
                    ViewTableRef {
                        use_final,
                        ..table_ref(table, &alias)
                    }
                }));
                where_.extend(on.into_iter().map(RenderExpr::OperatorApplicationExp));
            } else {
                let mut j = join(table, &alias);
                j.joining_on = on;
                j.join_type = kind;
                j.use_final = use_final;
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
        self.standard_edge(rel_type, rs)
    }

    /// `rs` (of `rel_type`) if its table's rows are its relationships, each
    /// end's columns paired with its node's id as the schema plainly means.
    fn standard_edge(
        &self,
        rel_type: &str,
        rs: &'s RelationshipSchema,
    ) -> Result<&'s RelationshipSchema, LowerError> {
        if !rs.is_edge_row_table() {
            return unsupported(format!("type {rel_type} is not the standard layout (S8)"));
        }
        // An end's columns are tied to its node's id position by position:
        // the same columns in another order would cross them (#672/#1010;
        // legacy refuses it too).
        for (end, label) in [(&rs.from_id, &rs.from_node), (&rs.to_id, &rs.to_node)] {
            let Some(ns) = self.schema.node_schema_opt(label) else {
                return unsupported(format!("label {label} has no node schema"));
            };
            let id = Identifier::from(ns.id_physical_columns());
            // Its columns pair with the id's one for one (S8d review: an end
            // of fewer columns tied part of the id).
            if end.columns().len() != id.columns().len() {
                return unsupported(format!(
                    "type {rel_type} pairs {:?} with {label}'s id {:?}: another number of \
                     columns",
                    end.columns(),
                    id.columns()
                ));
            }
            if end.crosses_positionally(&id) {
                return unsupported(format!(
                    "type {rel_type} pairs {:?} with {label}'s id {:?}: the same \
                     columns in another order (#672/#1010)",
                    end.columns(),
                    id.columns()
                ));
            }
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
                // A boolean and a number or a string are never equal, as in
                // a WHERE (#1470).
                let read = LogicalExpr::PropertyAccessExp(
                    crate::query_planner::logical_expr::PropertyAccess {
                        table_alias: crate::query_planner::logical_expr::TableAlias(format!(
                            "v{}",
                            v.0
                        )),
                        column: PropertyValue::Column(prop.clone()),
                    },
                );
                let equal = crate::query_planner::logical_expr::OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![read, value.clone()],
                };
                if let Some(e) = self.boolean_operator(&equal, &HashMap::new())? {
                    return Ok(e);
                }
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
        scan_identity(self.scans.get(&v)?)
    }

    /// Whether walk `r` carries its nodes (`NODE_TUPLES`) or relationships
    /// (`REL_TUPLES`) as element tuples: when they are read as a list's
    /// elements and have a tuple layout (not a composite id). Without one
    /// it carries none, and such a read is not lowered; its values (S6c)
    /// still are.
    fn walk_carries(
        &self,
        r: VarId,
        what: &str,
        label: &str,
        node: &'s NodeSchema,
        rel_type: &str,
        edge: &'s RelationshipSchema,
    ) -> bool {
        if !self.demand.get(&r).is_some_and(|d| d.contains(what)) {
            return false;
        }
        match what {
            NODE_TUPLES => elements::node_layout(label, node).is_ok(),
            _ => elements::rel_layout(rel_type, edge).is_ok(),
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
            node_tuples,
            rel_tuples,
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
        if *node_tuples {
            cols.push(path::TUPLE_COLUMNS[0]);
        }
        if *rel_tuples {
            cols.push(path::TUPLE_COLUMNS[1]);
        }
        cols.into_iter().map(str::to_string).collect()
    }

    /// The columns of `v`'s identity `ids` that are all NULL exactly when
    /// `v` is (`null_test`): a composite node id's, every one (a part of a
    /// real node's can be NULL); otherwise the first (a label union's label,
    /// a relationship's first end or `edge_id` column, a path's start).
    pub(super) fn presence(&self, v: VarId, ids: &[RenderExpr]) -> Vec<RenderExpr> {
        match self.scans.get(&v) {
            Some(Scan::Node { .. }) if ids.len() > 1 => ids.to_vec(),
            _ => ids.iter().take(1).cloned().collect(),
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
                // Relationships of different definitions (types, or label
                // pairs) are never equal, even in one table (S8a / S8b: FK
                // or embedding edges of several types on one table's rows).
                let definition = |v: &VarId| match self.scans.get(v) {
                    Some(Scan::Rel {
                        schema, rel_type, ..
                    }) => Some((rel_type, &schema.from_node, &schema.to_node)),
                    // A shortest path's relationships are its own (and
                    // their identities texts).
                    Some(Scan::Path {
                        walked: Walked::One { schema, rel_type },
                        edges: true,
                        shortest: None,
                        ..
                    }) => Some((rel_type, &schema.from_node, &schema.to_node)),
                    _ => None,
                };
                let (Some(da), Some(db)) = (definition(a), definition(b)) else {
                    continue;
                };
                if da != db {
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
                        let identity = path::edge_identity_sql(schema, edge);
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
                None => ps.from_id.columns().len() + ps.to_id.columns().len(),
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
        let texts = self.definition_texts(a, b);
        let (ia, ib) = (
            self.definition_identity(a, &texts)?,
            self.definition_identity(b, &texts)?,
        );
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
                    path::edge_arity(schema),
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
    fn definition_identity(&self, v: VarId, texts: &[bool]) -> Result<Vec<RenderExpr>, LowerError> {
        let ids = self.identity(v)?.unwrap_or_default();
        let text = |i: usize| texts.get(i).copied().unwrap_or(false);
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
                // As a union's arm of its definition spells it where that
                // union's are texts (`rel_union_of`).
                let from = schema.from_id.columns().len();
                all.extend(ids.into_iter().enumerate().map(|(i, id)| {
                    match (text(i), &schema.edge_id, i < from) {
                        (false, ..) => id,
                        (true, Some(_), _) => self.any_text(id),
                        (true, None, true) => self.part_text(&schema.from_node, i, id),
                        (true, None, false) => self.part_text(&schema.to_node, i - from, id),
                    }
                }));
                Ok(all)
            }
            // Its identity after its definition's three columns.
            Some(Scan::Rels { .. }) => {
                let own = self.rel_scan_id_texts(v);
                Ok(ids
                    .into_iter()
                    .enumerate()
                    .map(|(i, id)| match i.checked_sub(3) {
                        Some(j) if text(j) && !own.get(j).copied().unwrap_or(false) => {
                            self.any_text(id)
                        }
                        _ => id,
                    })
                    .collect())
            }
            _ => unsupported(format!("internal: {v} is not a relationship scan")),
        }
    }

    /// The identity columns relationships `a` and `b` compare as texts:
    /// where either's are ([`Self::rel_scan_id_texts`]).
    fn definition_texts(&self, a: VarId, b: VarId) -> Vec<bool> {
        let (ta, tb) = (self.rel_scan_id_texts(a), self.rel_scan_id_texts(b));
        (0..ta.len().max(tb.len()))
            .map(|i| ta.get(i).copied().unwrap_or(false) || tb.get(i).copied().unwrap_or(false))
            .collect()
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
        if let Some(Scan::Node { schema, .. }) = self.scans.get(&v) {
            let schema: &'s NodeSchema = schema;
            self.node_table(schema)?;
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
                name,
                alias: Some(alias.clone()),
                use_final,
            });
        } else {
            // A joined table read with FINAL (S11k): `JOIN t AS a FINAL`.
            let mut j = join(name, &alias);
            j.use_final = use_final;
            self.joins.push(j);
        }
        self.emitted.push(alias);
        // Ties that this scan completes go in its ON (a node read from this
        // relationship's columns is emitted with it: its ties too).
        let mut still = Vec::new();
        for t in std::mem::take(&mut self.pending) {
            if self.is_emitted(t.a) && self.is_emitted(t.b) {
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

    fn finish_relation(&mut self) -> Result<(), LowerError> {
        // A tie to an impossible scan never completes; the relation is empty.
        // Otherwise every tie is placed: one left would join its scans as a
        // product (wrong rows, silently).
        if !self.empty && !self.pending.is_empty() {
            return unsupported("internal: a tie between scans of the relation was not placed");
        }
        self.pending.clear();
        Ok(())
    }

    // ------------------------------------------------------------- segments

    /// `ORDER BY` keys over the current relation (and a projection's items).
    /// `of_group`: of a DISTINCT that is no aggregation, the items it groups
    /// by a key (lists, maps: `items` holds them as they are); a sort key
    /// reading one is any one of its group's.
    fn sort_keys(
        &self,
        keys: &[SortKey],
        items: &HashMap<VarId, RenderExpr>,
        of_group: Option<&HashSet<VarId>>,
    ) -> Result<Vec<OrderByItem>, LowerError> {
        if keys.iter().any(|k| self.holds_elements(&k.expr)) {
            // A tuple would sort by its columns.
            return unsupported("ORDER BY a node or relationship of a list");
        }
        if keys.iter().any(|k| self.holds_maps(&k.expr)) {
            // A map's values are of any type: they have no order.
            return unsupported("ORDER BY a map");
        }
        // A relationship's id is Bolt's number for it, computed after the
        // query (S11l review): the SQL holds its element id, whose order is
        // not the number's.
        for k in keys {
            let item = match &k.expr {
                LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) => {
                    parse_var(n).is_some_and(|v| self.rel_id_vars.contains(&v))
                }
                _ => false,
            };
            if item || self.rel_id_item(&k.expr)?.is_some() {
                return unsupported("ORDER BY a relationship's id()");
            }
        }
        let mut sorted = Vec::new();
        for k in keys {
            let order = if k.descending {
                OrderByOrder::Desc
            } else {
                OrderByOrder::Asc
            };
            // `id(n)` of a node of several labels (the Neo4j Browser's node
            // expand: `ORDER BY id(o)`): by its label, then its key, as its
            // identity is (no one number: keys of two labels may be equal).
            let labeled = match &k.expr {
                LogicalExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("id") => self
                    .entity_arg(&f.args)
                    .filter(|v| matches!(self.scans.get(v), Some(Scan::Labels { .. }))),
                _ => None,
            };
            if let Some(v) = labeled {
                for e in self.identity(v)?.unwrap_or_default() {
                    sorted.push(OrderByItem {
                        expression: e,
                        order: order.clone(),
                    });
                }
                continue;
            }
            let e = match self.node_id_item(&k.expr) {
                // As an `id(n)` item: by the node's key (S11e).
                Some((v, _)) => self.identity_value(v)?,
                // (A list held where its NULL is known: `null_list.rs`.)
                None => self.held_list(&k.expr, items)?,
            };
            // A key reading an item of a DISTINCT's group: any one of the
            // group's, one value by DISTINCT (not a grouping key).
            let reads_group = of_group.is_some_and(|vars| {
                let mut found = false;
                crate::bound_plan::expr::visit(&k.expr, &mut |x| {
                    if let LogicalExpr::TableAlias(
                        crate::query_planner::logical_expr::TableAlias(n),
                    ) = x
                    {
                        found |= parse_var(n).is_some_and(|v| vars.contains(&v));
                    }
                });
                found
            });
            let e = match (reads_group, current_function_mapper().cypher_union()) {
                (true, Some(u)) => RenderExpr::Raw((u.any)(&render_expr_to_sql_plain(&e))),
                (true, None) => return unsupported("ORDER BY a grouped list in this SQL dialect"),
                (false, _) => e,
            };
            // A list that may be NULL, held as `[]` (`null_list.rs`): NULL
            // last ascending, first descending, as Neo4j sorts.
            if matches!(self.kind(&k.expr), unwind::Kind::List(_)) {
                if let null_list::ListNull::When(c) = self.list_null(&k.expr, items)? {
                    sorted.push(OrderByItem {
                        expression: c,
                        order: order.clone(),
                    });
                }
            }
            sorted.push(OrderByItem {
                expression: e,
                order,
            });
        }
        let keys = sorted;
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
            self.record_kind(it.var, &it.expr, kind);
            if self.never_boolean(&it.expr) {
                self.never_boolean.insert(it.var);
            }
            // A WITH item holding `id(n)` (S11l): returned later, it is that
            // node's id (Bolt encodes it).
            if let (Some(_), Some(label), false) = (cte, self.node_id_of(&it.expr), it.aggregate) {
                self.id_values.insert(it.var, label);
            }
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
        // List items that may be NULL: when, and the list held so, over the
        // input rows and of a group (`Lowerer::held_items`).
        let mut held_items: Vec<(VarId, RenderExpr, RenderExpr, RenderExpr)> = Vec::new();
        // Each item as it is (no group's), and those a DISTINCT keys by a key.
        let mut raw_env: HashMap<VarId, RenderExpr> = HashMap::new();
        let mut keyed_items: HashSet<VarId> = HashSet::new();
        // `id(n)` items: their column is the node's key, not the id returned.
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
            // `head(collect({…}))` / `collect({…})[i]` carried by a WITH
            // (LDBC IC 7, BI 14): the map of one row, its keys each its own
            // column (S11m), from the collected tuples of its values.
            if let (Some(alias), Some((entries, index))) = (cte, collected_map(&it.expr)) {
                let keys = self.collected_map_keys(entries, index, it.var)?;
                // The map's kind: each key's (a node a key holds is read as
                // a list's element, `elements.rs`).
                let mut kinds: Vec<(String, unwind::Kind)> = keys
                    .iter()
                    .map(|(k, _)| match entries.iter().find(|(e, _)| e == k) {
                        Some((_, v)) => (k.clone(), self.kind(v)),
                        // A property of a key's node (`k.p`).
                        None => (k.clone(), unwind::Kind::Unknown),
                    })
                    .collect();
                kinds.sort_by(|a, b| a.0.cmp(&b.0));
                let mut before: Vec<(String, unwind::Kind)> = keys
                    .iter()
                    .map(|(k, _)| match entries.iter().find(|(e, _)| e == k) {
                        Some((_, v)) => (k.clone(), self.kind_before_column_types(v)),
                        None => (k.clone(), unwind::Kind::Unknown),
                    })
                    .collect();
                before.sort_by(|a, b| a.0.cmp(&b.0));
                self.set_kind(it.var, unwind::Kind::Map(kinds), unwind::Kind::Map(before));
                self.map_keys.insert(it.var, keys.clone());
                let keys = export_map_keys(it.var, keys, alias, &mut body);
                exports.maps.push((it.var, keys));
                continue;
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
                    if self.as_values.contains(name) {
                        return unsupported(
                            "`v.*` in a UNION column of different labels or definitions",
                        );
                    }
                    shape.push(self.return_element(src, name, aggregating, &mut body)?);
                    continue;
                }
                // An `id(n)` carried from a WITH (S11l): its value is the key.
                if let (Some(label), None) =
                    (self.node_id_of(&it.expr), self.node_id_item(&it.expr))
                {
                    let e = self.expr(&it.expr, &HashMap::new())?;
                    if aggregating && !it.aggregate {
                        body.group_by.push(e.clone());
                    }
                    items_env.insert(it.var, e.clone());
                    let column = body.column(e, &it.name);
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::NodeId { label },
                        columns: vec![(it.name.clone(), column)],
                    });
                    continue;
                }
                // `id(r)` of a relationship of one definition (S11l).
                if let Some(e) = self.rel_id_item(&it.expr)? {
                    if aggregating && !it.aggregate {
                        body.group_by.push(e.clone());
                    }
                    items_env.insert(it.var, e.clone());
                    let column = body.column(e, &it.name);
                    self.rel_id_vars.insert(it.var);
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::RelId,
                        columns: vec![(it.name.clone(), column)],
                    });
                    continue;
                }
                if let Some((v, label)) = self.node_id_item(&it.expr) {
                    let e = self.identity_value(v)?;
                    if aggregating {
                        body.group_by.push(e.clone());
                    }
                    // What this projection's ORDER BY reads of it.
                    items_env.insert(it.var, e.clone());
                    let column = body.column(e, &it.name);
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::NodeId { label },
                        columns: vec![(it.name.clone(), column)],
                    });
                    continue;
                }
            }
            // A list that may be NULL, held as `[]` (`null_list.rs`): when
            // it is NULL is grouped and made distinct by too; a WITH carries
            // it, the final RETURN returns NULL where it holds. (`None`: not
            // known; `Some(None)`: never NULL.)
            let list_null = match self.kind(&it.expr) {
                unwind::Kind::List(_) => match self.list_null(&it.expr, &HashMap::new())? {
                    null_list::ListNull::Never => Some(None),
                    null_list::ListNull::When(c) => Some(Some(c)),
                    null_list::ListNull::Unknown => None,
                },
                _ => None,
            };
            // The list held (its NULL as `[]`, `Lowerer::held_list`): what a
            // WITH carries beside it, and grouping / DISTINCT key on; anything
            // else reads the list as it is lowered.
            let held = match &list_null {
                Some(Some(_)) => Some(self.held_list(&it.expr, &HashMap::new())?),
                _ => None,
            };
            let e = self.expr(&it.expr, &HashMap::new())?;
            // A map or a list of maps (of `Dynamic` values) is grouped, and
            // made distinct, by its key (`CypherUnion::distinct_key`, as a
            // UNION's): ClickHouse groups no `Dynamic`, and compares `1` and
            // `1.0` as different. A dialect without the key reads a map that
            // is neither grouped nor made distinct as it is.
            // So is a list of values, whose items may be of several types (a
            // `Variant`, which ClickHouse groups no more than `Dynamic`;
            // LDBC IC 1's `[uni.name, studyAt.classYear, …]`), by the same
            // key (`[1]` and `[1.0]` are one group, as in Cypher).
            // A property never NULL whose column is an `Array` of one scalar
            // type groups as itself (VALUE_TYPES.md T2a; review finding: it
            // was keyed as a list of mixed values).
            let one_typed = matches!(&it.expr, LogicalExpr::PropertyAccessExp(_))
                && matches!(&list_null, Some(None))
                && matches!(
                    self.kind(&it.expr),
                    unwind::Kind::List(inner)
                        if matches!(*inner, unwind::Kind::Scalar | unwind::Kind::Boolean)
                );
            let value_list = !one_typed
                && matches!(
                    self.kind(&it.expr),
                    unwind::Kind::List(inner) if !matches!(*inner, unwind::Kind::Element(_))
                );
            let map_key = match (self.holds_maps(&it.expr) || value_list) && !it.aggregate {
                true => match current_function_mapper().cypher_union() {
                    Some(u) => Some(RenderExpr::Raw((u.distinct_key)(
                        &render_expr_to_sql_plain(held.as_ref().unwrap_or(&e)),
                    ))),
                    None if !aggregating && !body.distinct => None,
                    None => return unsupported("a grouped or DISTINCT map in this SQL dialect"),
                },
                false => None,
            };
            if body.distinct && it.aggregate && self.holds_maps(&it.expr) {
                return unsupported("DISTINCT of an aggregated list of maps");
            }
            if aggregating && !it.aggregate {
                body.group_by
                    .push(map_key.clone().unwrap_or_else(|| e.clone()));
            }
            if let (Some(k), true) = (&map_key, body.distinct) {
                body.distinct_keys.push(k.clone());
            }
            // Grouped by its key, the value is any one of its group's (as its
            // column is): what this projection's ORDER BY reads.
            let read = match (&map_key, aggregating || body.distinct) {
                (Some(_), true) => match current_function_mapper().cypher_union() {
                    Some(u) => RenderExpr::Raw((u.any)(&render_expr_to_sql_plain(&e))),
                    None => return unsupported("a map in this SQL dialect"),
                },
                _ => e.clone(),
            };
            items_env.insert(it.var, read);
            raw_env.insert(it.var, e.clone());
            if map_key.is_some() || held.is_some() {
                keyed_items.insert(it.var);
            }
            if let (Some(Some(flag)), Some(held)) = (&list_null, &held) {
                // (An aggregate is its group's already.)
                let grouped = match (aggregating || body.distinct) && !it.aggregate {
                    true => match current_function_mapper().cypher_union() {
                        Some(u) => RenderExpr::Raw((u.any)(&render_expr_to_sql_plain(held))),
                        None => return unsupported("a grouped list in this SQL dialect"),
                    },
                    false => held.clone(),
                };
                held_items.push((it.var, flag.clone(), held.clone(), grouped));
            }
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
            if let (Some(Some(c)), true) = (&list_null, aggregating && !it.aggregate) {
                body.group_by.push(c.clone());
            }
            match cte {
                // A constant needs no column: the next segment uses it as is.
                Some(_) if is_constant(&e) && held.is_none() => {
                    if let Some(None) = list_null {
                        exports.list_nulls.push((it.var, None));
                    }
                    exports.values.push((it.var, e));
                }
                Some(alias) => {
                    let name = it.var.name();
                    body.select.push(select(e.clone(), &name));
                    let null = match (list_null, held.clone()) {
                        (Some(Some(flag)), Some(held)) => {
                            Some(Some(null_list::NullList { flag, held }))
                        }
                        (Some(None), _) => Some(None),
                        _ => None,
                    };
                    if let Some(null) = null {
                        let (null, added) = export_list_null(it.var, null, &e, alias, &mut body);
                        exports.list_nulls.push((it.var, null));
                        // Grouped by its key, as the list is.
                        if let (Some(added), true) = (added, map_key.is_some()) {
                            body.determined.push(added);
                        }
                    }
                    // Grouped by its key, not itself.
                    if map_key.is_some() {
                        body.determined.push(name.clone());
                    }
                    exports.values.push((it.var, col_at(alias, &name)));
                    // A map's keys, each its own column (S11m), which the
                    // map's key determines as it does the map.
                    if let Some(keys) = self.item_map_keys(&it.expr)? {
                        // This projection's WHERE / ORDER BY read the item's
                        // keys over its input rows; grouped by the map's key
                        // (an aggregation: its WHERE is a HAVING), any one of
                        // its group's (S11m review). Made distinct, its
                        // WHERE reads the input rows.
                        let own: MapKeys = match (&map_key, aggregating) {
                            (Some(_), true) => {
                                let Some(u) = current_function_mapper().cypher_union() else {
                                    return unsupported("a map in this SQL dialect");
                                };
                                keys.iter()
                                    .map(|(k, e)| {
                                        let e = match is_constant(e) {
                                            true => e.clone(),
                                            false => RenderExpr::Raw((u.any)(
                                                &render_expr_to_sql_plain(e),
                                            )),
                                        };
                                        (k.clone(), e)
                                    })
                                    .collect()
                            }
                            _ => keys.clone(),
                        };
                        self.map_keys.insert(it.var, own);
                        let first = body.select.len();
                        let keys = export_map_keys(it.var, keys, alias, &mut body);
                        if map_key.is_some() {
                            let added = body.select[first..]
                                .iter()
                                .filter_map(|s| s.col_alias.as_ref().map(|a| a.0.clone()))
                                .collect::<Vec<_>>();
                            body.determined.extend(added);
                        }
                        exports.maps.push((it.var, keys));
                    }
                }
                None => {
                    let (e, null) = match (list_null, held) {
                        (Some(Some(c)), Some(held)) => {
                            let Some(spelling) = current_function_mapper().lists() else {
                                return unsupported("a list that may be NULL in this SQL dialect");
                            };
                            // DISTINCT by the list (or its key) and whether
                            // it is NULL; the column follows from them.
                            if body.distinct && !aggregating {
                                body.distinct_keys
                                    .push(map_key.clone().unwrap_or_else(|| held.clone()));
                                body.distinct_keys.push(c.clone());
                            }
                            let value = (spelling.null_list)(
                                &render_expr_to_sql_plain(&c),
                                &render_expr_to_sql_plain(&held),
                            );
                            (RenderExpr::Raw(value), true)
                        }
                        _ => (e, false),
                    };
                    let column = body.column(e, &it.name);
                    // (An aggregate is its group's already.)
                    if map_key.is_some() || (null && !it.aggregate) {
                        body.determined.push(column.clone());
                    }
                    shape.push(ResultColumn {
                        name: it.name.clone(),
                        kind: ResultKind::Value,
                        columns: vec![(it.name.clone(), column)],
                    });
                }
            }
        }
        // The ORDER BY reads a grouping projection's groups; its WHERE
        // (unless an aggregation's HAVING) the input rows.
        let set_held = |grouped: bool| {
            let mut m = self.held_items.borrow_mut();
            m.clear();
            for (v, flag, raw, of_group) in &held_items {
                let held = if grouped { of_group } else { raw };
                m.insert(
                    *v,
                    null_list::NullList {
                        flag: flag.clone(),
                        held: held.clone(),
                    },
                );
            }
        };
        // (A DISTINCT that is no aggregation reads its items as they are,
        // a key reading one it groups by a key being any one of its group's.)
        set_held(aggregating);
        body.order_by = match body.distinct && !aggregating {
            true => self.sort_keys(&p.order_by, &raw_env, Some(&keyed_items))?,
            false => self.sort_keys(&p.order_by, &items_env, None)?,
        };
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
            let e = self.condition(f, &items_env)?;
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
        // A UNION ALL column holding another label or definition in another
        // arm (`Query::as_values`): a value too.
        let as_value = self.as_values.contains(name);
        let union = match self.scans.get(&src) {
            Some(Scan::Labels { arms, of, .. }) => {
                Some((self.labeled_node(src)?, self.label_union_props(*of, arms)))
            }
            Some(Scan::Rels { arms, of, .. }) => {
                Some((self.labeled_rel(src)?, self.rel_union_props(*of, arms)))
            }
            Some(Scan::Node { .. }) if as_value => Some((
                self.labeled_node(src)?,
                self.all_property_names(src).into_iter().collect(),
            )),
            Some(Scan::Rel { .. }) if as_value => Some((
                self.labeled_rel(src)?,
                self.all_property_names(src).into_iter().collect(),
            )),
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
            let e = match self.options.deleted.contains(&v) {
                true => RenderExpr::Literal(Literal::Null),
                false => self.property(v, &prop)?,
            };
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
    /// Its value, and its order in an ORDER BY, is the node's key, as the
    /// legacy pipeline reads it: the id HTTP returns. Bolt returns it encoded
    /// (`IdMapper`: its label's code over a non-negative integer key, which
    /// keeps the key's order; a string key's hash, which does not).
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

    /// `e` as an `id(r)` RETURN item of a relationship of one definition: its
    /// element id (`value::rel_element_id`), NULL where it is. `None`: not
    /// one.
    fn rel_id_item(&self, e: &LogicalExpr) -> Result<Option<RenderExpr>, LowerError> {
        let LogicalExpr::ScalarFnCall(f) = e else {
            return Ok(None);
        };
        if !f.name.eq_ignore_ascii_case("id") {
            return Ok(None);
        }
        let Some(r) = self.entity_arg(&f.args) else {
            return Ok(None);
        };
        let Some(Scan::Rel {
            schema, rel_type, ..
        }) = self.scans.get(&r)
        else {
            return Ok(None);
        };
        let (schema, rel_type): (&RelationshipSchema, String) = (schema, rel_type.clone());
        let g = value::spelling()?;
        let end = |cols: Vec<&str>| -> Result<String, LowerError> {
            let ids: Vec<String> = cols
                .iter()
                .map(|c| Ok(render_expr_to_sql_plain(&self.physical(r, c)?)))
                .collect::<Result<_, LowerError>>()?;
            Ok(value::element_id_sql(&g, &ids))
        };
        let text = value::rel_element_id(
            &g,
            &rel_type,
            &end(schema.from_id.columns())?,
            &end(schema.to_id.columns())?,
        );
        Ok(Some(self.unless_null(&[r], RenderExpr::Raw(text))?))
    }

    /// The label of the node whose `id()` `e` is: `id(n)` of a node of one
    /// label, or a variable a WITH bound to one (`Lowerer::id_values`).
    fn node_id_of(&self, e: &LogicalExpr) -> Option<String> {
        if let Some((_, label)) = self.node_id_item(e) {
            return Some(label);
        }
        match e {
            LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) => {
                self.id_values.get(&parse_var(n)?).cloned()
            }
            _ => None,
        }
    }

    /// The keys of the map a WITH item `e` is, each with its value (S11m): a
    /// map literal, or a map value this segment carries with its keys.
    /// The keys of `collect(MAP)[index]` ([`collected_map`]): each, its
    /// value of that row, from the collected tuples of the map's values (in
    /// the rows' order where they have one, `Lowerer::collect_order`). A
    /// map literal is never NULL: every row is collected.
    ///
    /// A key holding a node or relationship of several labels or types (no
    /// list element's tuple, `elements.rs`) carries the properties the
    /// statement reads of it (`m.k.p`, as keys `k.p`); read whole, it is not
    /// lowered.
    fn collected_map_keys(
        &self,
        entries: &[(String, LogicalExpr)],
        index: usize,
        var: VarId,
    ) -> Result<MapKeys, LowerError> {
        let m = current_function_mapper();
        let Some(spelling) = m.lists() else {
            return unsupported("a collected map in this SQL dialect");
        };
        let demanded = self.demand.get(&var);
        let mut names = Vec::new();
        let mut values = Vec::new();
        for (k, v) in entries {
            let several = self.entity(v).filter(|e| {
                matches!(
                    self.scans.get(e),
                    Some(Scan::Labels { .. } | Scan::Rels { .. })
                )
            });
            if let Some(e) = several {
                let read = demanded.into_iter().flatten();
                if read.clone().any(|p| p == k || p == ALL_PROPERTIES) {
                    return unsupported(
                        "a node or relationship of several labels or types in a collected map, \
                         read whole",
                    );
                }
                let prefix = format!("{k}.");
                for p in read.filter_map(|p| p.strip_prefix(&prefix)) {
                    names.push(collected_key(k, Some(p)));
                    values.push(render_expr_to_sql_plain(&self.property(e, p)?));
                }
                continue;
            }
            // A list or map would read `[]` / `{}` where the row is missing
            // (ClickHouse holds no NULL array, #1454).
            // A value made of `Array` columns' properties (VALUE_TYPES.md
            // T2a) reads as it did before they were known to be lists: its
            // missing row is NULL (review findings: refused once the kind
            // was known).
            if matches!(
                self.kind_before_column_types(v),
                unwind::Kind::List(_) | unwind::Kind::Map(_)
            ) {
                return unsupported("a list or map in a collected map");
            }
            names.push(k.clone());
            values.push(render_expr_to_sql_plain(&self.element(v, &HashMap::new())?));
        }
        let tuple = (spelling.tuple)(&values);
        let list = match &self.collect_order {
            Some(key) => (spelling.ordered_collect)(&tuple, &render_expr_to_sql_plain(key), "true"),
            None => render_expr_to_sql_plain(
                &self.in_fixed_order(RenderExpr::Raw((spelling.collect_all)(&tuple)))?,
            ),
        };
        let row = m.array_element_or_null(&list, &(index + 1).to_string());
        // The row may be missing (an index past the list's end, `head` of
        // none): its values NULL, not a tuple's defaults. A node's tuple
        // cannot be NULL: its reads check the row (`collected_key(k, None)`).
        let present = format!("{} > {index}", m.list_size(&list));
        let mut keys: MapKeys = Vec::new();
        for (i, k) in names.into_iter().enumerate() {
            let value = (spelling.element)(&row, i + 1);
            let element = entries
                .iter()
                .find(|(e, _)| *e == k)
                .is_some_and(|(_, v)| matches!(self.kind(v), unwind::Kind::Element(_)));
            if element {
                keys.push((collected_key(&k, None), RenderExpr::Raw(present.clone())));
                keys.push((k, RenderExpr::Raw(value)));
            } else {
                keys.push((
                    k,
                    RenderExpr::Raw(format!("CASE WHEN {present} THEN {value} ELSE NULL END")),
                ));
            }
        }
        Ok(keys)
    }

    fn item_map_keys(&self, e: &LogicalExpr) -> Result<Option<MapKeys>, LowerError> {
        match e {
            LogicalExpr::MapLiteral(entries) => {
                let mut keys = Vec::new();
                for (k, v) in entries {
                    keys.push((k.clone(), self.expr(v, &HashMap::new())?));
                }
                Ok(Some(keys))
            }
            LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(n)) => {
                Ok(parse_var(n).and_then(|v| self.map_keys.get(&v).cloned()))
            }
            _ => Ok(None),
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
        let demanded = self.demand.get(&out);
        let mut names: BTreeSet<String> = demanded.into_iter().flatten().cloned().collect();
        if names.remove(ALL_PROPERTIES) {
            names.extend(self.all_property_names(src));
        }
        self.export_element_with(src, out, alias, body, keys, &names)
    }

    /// [`Self::export_element`] with the properties `names`.
    fn export_element_with(
        &self,
        src: VarId,
        out: VarId,
        alias: &str,
        body: &mut Body,
        keys: &mut Vec<RenderExpr>,
        names: &BTreeSet<String>,
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
        for prop in names {
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
        self.collect_by_any = false;
        if !p.aggregates() || !p.items.iter().any(|i| calls_aggregate(&i.expr, "collect")) {
            return Ok(());
        }
        if let RowOrder::Keys(keys) = &self.order {
            self.collect_by_any = keys.iter().any(|k| self.is_unordered_number(k));
        }
        match &self.order {
            RowOrder::Unordered => Ok(()),
            // Keys that are the lowering's own numbers, ascending: the
            // values in the order of the keys themselves.
            RowOrder::Keys(keys) if keys.iter().all(|k| self.is_number(k)) => {
                self.collect_order = match keys.as_slice() {
                    [one] => Some(one.expression.clone()),
                    _ => Some(RenderExpr::ScalarFnCall(
                        crate::render_plan::render_expr::ScalarFnCall {
                            name: current_function_mapper().tuple_constructor().to_string(),
                            args: keys.iter().map(|k| k.expression.clone()).collect(),
                        },
                    )),
                };
                Ok(())
            }
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

    /// `k` sorts ascending by a number the lowering made ([`Lowerer::numbers`]).
    pub(super) fn is_number(&self, k: &OrderByItem) -> bool {
        matches!(k.order, OrderByOrder::Asc)
            && self
                .numbers
                .contains(&render_expr_to_sql_plain(&k.expression))
    }

    /// `k` sorts by a number of rows in no order ([`Lowerer::unordered_numbers`]).
    pub(super) fn is_unordered_number(&self, k: &OrderByItem) -> bool {
        self.unordered_numbers
            .contains(&render_expr_to_sql_plain(&k.expression))
    }

    /// A WITH: the rows so far become a CTE whose columns are the WITH's
    /// output scope, and a new segment reads from it.
    fn with(&mut self, p: &Projection) -> Result<(), LowerError> {
        self.order_collect(p)?;
        let collects_by_any = self.collect_by_any;
        // After the rows are numbered for `collect` (a CTE).
        self.projection_subqueries(p)?;
        let alias = self.next_cte_alias();
        let (body, exports, _) = self.projection_body(p, Some(&alias))?;
        if collects_by_any {
            self.unstable.insert(format!("with_{alias}"));
        }
        self.grouped_with = p.aggregates().then(|| {
            let (aggregated, keys): (Vec<&ProjItem>, Vec<&ProjItem>) =
                p.items.iter().partition(|i| i.aggregate);
            (
                alias.clone(),
                keys.iter().map(|i| i.var).collect(),
                aggregated.iter().map(|i| i.var).collect(),
            )
        });
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
                    exports.values.push((v, e.clone()));
                } else {
                    body.select.push(select(e.clone(), &v.name()));
                    exports.values.push((v, col_at(alias, &v.name())));
                }
                if let Some(keys) = self.map_keys.get(&v).cloned() {
                    let keys = export_map_keys(v, keys, alias, body);
                    exports.maps.push((v, keys));
                }
                if let Some(null) = self.list_nulls.get(&v).cloned() {
                    let (null, _) = export_list_null(v, null, &e, alias, body);
                    exports.list_nulls.push((v, null));
                }
                continue;
            }
            let scan = self.export_element(v, v, alias, body, &mut Vec::new())?;
            exports.scans.push((v, scan));
        }
        Ok(())
    }

    /// Sort keys ordering rows by every column of `select` but constants
    /// (#1395): rows that tie on them are alike. A node, or a relationship
    /// with `edge_id`, exported (`exports`) by its identity columns as they
    /// are (its other columns follow from them); another column by its
    /// `CypherUnion::order_key` (of a value of any type). By the columns' names when
    /// `by_name` (an ORDER BY reads the SELECT's, an aggregate's too), else
    /// their expressions (a window's ORDER BY).
    fn tie_breaks(
        &self,
        select: &[SelectItem],
        exports: &Exports<'_>,
        by_name: bool,
    ) -> Result<Vec<OrderByItem>, LowerError> {
        let Some(union) = current_function_mapper().cypher_union() else {
            return unsupported("rows read more than once in this SQL dialect");
        };
        // Identity columns as they are; those of a node of several labels
        // keyed (their type may be a `Dynamic`, which does not sort).
        let mut identity: HashSet<&str> = HashSet::new();
        let mut determined: HashSet<String> = HashSet::new();
        for (_, scan) in &exports.scans {
            let raw = match scan {
                Scan::Node { .. } => true,
                Scan::Rel { schema, .. } => {
                    if schema.edge_id.is_none() {
                        continue;
                    }
                    true
                }
                Scan::Labels { .. } => false,
                _ => continue,
            };
            let Some(ids) = scan_identity(scan) else {
                continue;
            };
            let Some(At::Exported {
                alias,
                physical,
                props,
            }) = scan.at()
            else {
                continue;
            };
            if !ids.iter().all(|c| physical.contains_key(c)) {
                continue;
            }
            if raw {
                identity.extend(ids.iter().map(|c| physical[c].as_str()));
            }
            // Its other columns follow from those.
            determined.extend(
                physical
                    .iter()
                    .filter(|(c, _)| !ids.contains(c))
                    .map(|(_, e)| e.clone()),
            );
            for e in props.values().filter(|e| !is_constant(e)) {
                determined.insert(column_of(e, alias)?);
            }
        }
        let m = current_function_mapper();
        let mut ties = Vec::new();
        for i in select.iter().filter(|i| !is_constant(&i.expression)) {
            let name = i.col_alias.as_ref().map(|a| a.0.as_str());
            let value = match (name, by_name) {
                (Some(a), true) => m.quote_alias(a),
                _ => render_expr_to_sql_plain(&i.expression),
            };
            let expression = match name {
                Some(a) if identity.contains(a) => RenderExpr::Raw(value),
                Some(a) if determined.contains(a) => continue,
                _ => RenderExpr::Raw((union.order_key)(&value)),
            };
            ties.push(OrderByItem {
                expression,
                order: OrderByOrder::Asc,
            });
        }
        Ok(ties)
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
        let paged = body.skip.is_some() || body.limit.is_some();
        if paged && body.order_by.iter().any(|k| self.is_unordered_number(k)) {
            self.unstable.insert(format!("with_{alias}"));
        }
        // Lowered stable: the rows taken are those first by every column
        // after the keys (#1395), by name (an aggregate's too).
        let ties = match self.stable && paged {
            true => self.tie_breaks(&body.select, &exports, true)?,
            false => Vec::new(),
        };
        let mut keys = Vec::new();
        for (i, k) in body.order_by.iter().enumerate() {
            let name = format!("__o{i}");
            body.select.push(select(k.expression.clone(), &name));
            let column = render_expr_to_sql_plain(&col_at(&alias, &name));
            if self.is_unordered_number(k) {
                self.unordered_numbers.insert(column.clone());
            }
            if self.is_number(k) {
                self.numbers.insert(column);
            }
            keys.push(OrderByItem {
                expression: col_at(&alias, &name),
                order: k.order.clone(),
            });
        }
        if body.skip.is_none() && body.limit.is_none() {
            body.order_by.clear();
        }
        body.order_by.extend(ties);
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
        self.map_keys = exports.maps.into_iter().collect();
        self.list_nulls = exports.list_nulls.into_iter().collect();
        self.emitted = vec![alias.clone()];
        self.from = Some(table_ref(name, &alias));
        self.joins = Vec::new();
        self.pending = Vec::new();
        self.filters = exports.keep.into_iter().collect();
        self.empty = false;
        self.order = order;
        Ok(())
    }

    /// Apply the subqueries a projection reads to its input rows: its items',
    /// and (when it neither aggregates nor is DISTINCT, so they are evaluated
    /// per input row) its ORDER BY's and WHERE's, if they read only the input.
    fn projection_subqueries(&mut self, p: &Projection) -> Result<(), LowerError> {
        let items: Vec<&LogicalExpr> = p.items.iter().map(|i| &i.expr).collect();
        self.apply_subqueries(&items)?;
        let modifiers: Vec<&LogicalExpr> = p
            .order_by
            .iter()
            .map(|k| &k.expr)
            .chain(&p.filter)
            .filter(|e| !subquery::subquery_free(e))
            .collect();
        if modifiers.is_empty() {
            return Ok(());
        }
        if p.aggregates() || p.distinct || !modifiers.iter().all(|e| self.subqueries_readable(e)) {
            return unsupported(
                "a subquery expression in the ORDER BY / WHERE of a projection reading a \
                 projected name, or of a DISTINCT or aggregating projection",
            );
        }
        self.apply_subqueries(&modifiers)
    }

    /// The final RETURN.
    fn project(&mut self, p: &Projection) -> Result<(RenderPlan, Vec<ResultColumn>), LowerError> {
        if p.filter.is_some() {
            return unsupported("internal: a RETURN with a WHERE");
        }
        self.order_collect(p)?;
        self.projection_subqueries(p)?;
        // The final SELECT is read once: its lists keep their order.
        self.final_items = true;
        let projected = self.projection_body(p, None);
        self.final_items = false;
        let (body, _, shape) = projected?;
        Ok((self.render(body), shape))
    }

    /// Join order (§4.6.4) for ClickHouse, which builds a hash table of each
    /// joined relation and streams the FROM rows through them: a path
    /// relation, the largest of a clause in every shape measured (a path
    /// multiplies its start rows by the walks from each), goes first. The
    /// others follow in their order, each after a relation it is tied to,
    /// and every ON conjunct moves to the ON of the later of its relations
    /// (the tie rule of [`Self::place`]). Only a join of inner joins is
    /// reordered (an equality of inner joins holds in any order); each
    /// relation keeps its FINAL wherever it is placed (S11k).
    fn path_first(&mut self) {
        if self.joins.iter().any(|j| j.join_type != JoinType::Join) || self.from.is_none() {
            return;
        }
        // A conjunct whose columns are not all visible (SQL text: a composite
        // id's key, S8d2) cannot be moved to the relations it reads: the joins
        // stay as placed (review finding: a key tie moved before its node).
        if self.joins.iter().any(|j| {
            j.joining_on
                .iter()
                .any(|c| read_aliases(&RenderExpr::OperatorApplicationExp(c.clone())).is_none())
        }) {
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
        let mut relations = vec![(
            from.alias.clone().unwrap_or_default(),
            (from.name.clone(), from.use_final),
        )];
        let mut conjuncts: Vec<(String, OperatorApplication)> = Vec::new();
        for j in std::mem::take(&mut self.joins) {
            relations.push((j.table_alias.clone(), (j.table_name.clone(), j.use_final)));
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
        let mut joins: Vec<Join> = order[1..]
            .iter()
            .map(|a| {
                let (name, use_final) = table(a);
                Join {
                    use_final,
                    ..join(name, a)
                }
            })
            .collect();
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
        let (name, use_final) = table(&order[0]);
        self.from = Some(ViewTableRef {
            use_final,
            ..table_ref(name, &order[0])
        });
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
            // (A column that is any one of its group's values — a sort key
            // of a list item, `null_list.rs` — is no key.)
            let any_of_group = current_function_mapper()
                .cypher_union()
                .map(|u| (u.any)("").trim_end_matches(')').to_string());
            let aggregate = |e: &RenderExpr| match (&any_of_group, e) {
                (Some(prefix), RenderExpr::Raw(sql)) => sql.starts_with(prefix.as_str()),
                _ => false,
            };
            group_by = select
                .iter()
                .filter(|i| !determined(i) && !aggregate(&i.expression))
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
    /// Each key's value of the map values among `values` (S11m).
    maps: Vec<(VarId, MapKeys)>,
    /// When each list value among `values` is NULL (`null_list.rs`).
    list_nulls: Vec<(VarId, Option<null_list::NullList>)>,
}

/// A map value's keys and each one's value, in its own type (S11m): what
/// `m.k` / `m['k']` reads.
type MapKeys = Vec<(String, RenderExpr)>;

/// A collected map's key of what a key `k` holds, no Cypher key spells (a
/// NUL in it): `k\0p`, property `p` of the node of several labels it holds;
/// `k\0`, whether the row of the node it holds is there.
pub(super) fn collected_key(k: &str, p: Option<&str>) -> String {
    format!("{k}\u{0}{}", p.unwrap_or(""))
}

/// `head(collect(MAP))` / `collect(MAP)[i]` of a map literal: its entries
/// and the row's index (`i` ≥ 0); not of DISTINCT (a map's values have no
/// order or equality here).
fn collected_map(e: &LogicalExpr) -> Option<(&[(String, LogicalExpr)], usize)> {
    use crate::query_planner::logical_expr::Literal as L;
    fn entries(c: &LogicalExpr) -> Option<&[(String, LogicalExpr)]> {
        match c {
            LogicalExpr::AggregateFnCall(f) if f.name.eq_ignore_ascii_case("collect") => {
                match f.args.as_slice() {
                    [LogicalExpr::MapLiteral(entries)] => Some(entries.as_slice()),
                    _ => None,
                }
            }
            _ => None,
        }
    }
    match e {
        LogicalExpr::ScalarFnCall(f) if f.name.eq_ignore_ascii_case("head") => {
            match f.args.as_slice() {
                [c] => entries(c).map(|es| (es, 0)),
                _ => None,
            }
        }
        LogicalExpr::ArraySubscript { array, index } => match index.as_ref() {
            LogicalExpr::Literal(L::Integer(i)) if *i >= 0 => {
                entries(array).map(|es| (es, usize::try_from(*i).unwrap_or(0)))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Export when list `out` (exported as the column `{out}`, of `value`) is
/// NULL and the list held so (`null`, `None`: never NULL) from a CTE
/// aliased `alias`: columns `{out}__null` and `{out}__held` (a constant
/// stays as it is; a held list that is the value is its column). Returns
/// how the next segment reads them, and the held column it added.
fn export_list_null(
    out: VarId,
    null: Option<null_list::NullList>,
    value: &RenderExpr,
    alias: &str,
    body: &mut Body,
) -> (Option<null_list::NullList>, Option<String>) {
    let Some(null) = null else {
        return (None, None);
    };
    let flag = match is_constant(&null.flag) {
        true => null.flag,
        false => {
            let name = format!("{}__null", out.name());
            body.select.push(select(null.flag, &name));
            col_at(alias, &name)
        }
    };
    let (held, added) = if render_expr_to_sql_plain(&null.held) == render_expr_to_sql_plain(value) {
        (col_at(alias, &out.name()), None)
    } else if is_constant(&null.held) {
        (null.held, None)
    } else {
        let name = format!("{}__held", out.name());
        body.select.push(select(null.held, &name));
        (col_at(alias, &name), Some(name))
    };
    (Some(null_list::NullList { flag, held }), added)
}

/// Export map `v`'s keys `keys` as `out`'s from a CTE aliased `alias`: each
/// key's value a column `{out}__k{i}` (a constant stays as it is). Returns
/// how the next segment reads them.
fn export_map_keys(out: VarId, keys: MapKeys, alias: &str, body: &mut Body) -> MapKeys {
    keys.into_iter()
        .enumerate()
        .map(|(i, (k, e))| {
            if is_constant(&e) {
                return (k, e);
            }
            let name = format!("{}__k{i}", out.name());
            body.select.push(select(e, &name));
            (k, col_at(alias, &name))
        })
        .collect()
}

fn join(table_name: String, alias: &str) -> Join {
    Join {
        table_name,
        table_alias: alias.to_string(),
        joining_on: Vec::new(),
        join_type: JoinType::Join,
        use_final: false,
    }
}

/// `name AS alias` in a FROM.
fn table_ref(name: String, alias: &str) -> ViewTableRef {
    ViewTableRef {
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
        RenderExpr::Raw(_) | RenderExpr::Column(_) | RenderExpr::ColumnAlias(_) => {
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
/// The edge `constraints:` each single relationship of the statement's
/// patterns can be matched under, bound over its ends (S11h): what they read
/// of the ends is demanded of them. Each definition of its types between
/// labels its ends can have, in each orientation its direction allows.
fn constraint_reads(stmt: &BoundStatement, schema: &GraphSchema) -> Vec<LogicalExpr> {
    fn walk(op: &BoundOp, out: &mut Vec<(VarId, RelDirection, VarId, VarId)>) {
        match op {
            BoundOp::Match { input, pattern, .. } => {
                walk(input, out);
                for part in &pattern.parts {
                    for (i, r) in part.rels.iter().enumerate() {
                        if r.length.is_none() && !r.bound_before {
                            let (left, right) = (part.nodes[i].var, part.nodes[i + 1].var);
                            out.push((r.var, r.direction, left, right));
                        }
                    }
                }
            }
            BoundOp::Unwind { input, .. }
            | BoundOp::Project { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Filter { input, .. }
            | BoundOp::Skip { input, .. }
            | BoundOp::Limit { input, .. } => walk(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| walk(a, out)),
            BoundOp::Unit => {}
        }
    }
    let mut rels = Vec::new();
    walk(&stmt.plan, &mut rels);
    for sq in &stmt.subqueries {
        walk(&sq.plan, &mut rels);
    }
    rel_constraint_exprs(schema, &stmt.bindings, &rels)
}

/// The edge constraints of each relationship `(r, direction, left, right)`
/// bound over its ends ([`constraint_reads`]).
fn rel_constraint_exprs(
    schema: &GraphSchema,
    bindings: &[Binding],
    rels: &[(VarId, RelDirection, VarId, VarId)],
) -> Vec<LogicalExpr> {
    let binding = |v: VarId| bindings.iter().find(|b| b.id == v);
    let labels = |v: VarId| match binding(v).map(|b| &b.kind) {
        Some(BindingKind::Node { labels }) => labels.clone(),
        _ => BTreeSet::new(),
    };
    let mut out = Vec::new();
    for &(r, direction, left, right) in rels {
        let Some(BindingKind::Rel { types, .. }) = binding(r).map(|b| &b.kind) else {
            continue;
        };
        let orientations = match direction {
            RelDirection::Right => vec![(left, right)],
            RelDirection::Left => vec![(right, left)],
            RelDirection::Either => vec![(left, right), (right, left)],
        };
        for t in types {
            for rs in schema.rel_schemas_for_type(t) {
                let Some(text) = &rs.constraints else {
                    continue;
                };
                for &(from, to) in &orientations {
                    if labels(from).contains(&rs.from_node) && labels(to).contains(&rs.to_node) {
                        // A constraint that does not bind fails where the
                        // relationship is lowered.
                        if let Ok(e) = crate::bound_plan::expr::bind_edge_constraint(text, from, to)
                        {
                            out.push(e);
                        }
                    }
                }
            }
        }
    }
    out
}

/// Every expression of `op` (patterns' property maps, predicates,
/// projections, sort keys, UNWIND lists), into `out`.
pub(crate) fn op_exprs<'a>(op: &'a BoundOp, out: &mut Vec<&'a LogicalExpr>) {
    match op {
        BoundOp::Unit => {}
        BoundOp::Match {
            input,
            pattern,
            predicate,
            ..
        } => {
            op_exprs(input, out);
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
            op_exprs(input, out);
            out.push(expr);
        }
        BoundOp::Project { input, projection } => {
            op_exprs(input, out);
            out.extend(projection.items.iter().map(|i| &i.expr));
            out.extend(projection.order_by.iter().map(|k| &k.expr));
            out.extend(&projection.filter);
        }
        BoundOp::Sort { input, keys } => {
            op_exprs(input, out);
            out.extend(keys.iter().map(|k| &k.expr));
        }
        BoundOp::Filter { input, predicate } => {
            op_exprs(input, out);
            out.push(predicate);
        }
        BoundOp::Skip { input, .. } | BoundOp::Limit { input, .. } => op_exprs(input, out),
        BoundOp::Union { arms, .. } => arms.iter().for_each(|a| op_exprs(a, out)),
    }
}

fn demand(stmt: &BoundStatement, extra: &[LogicalExpr]) -> HashMap<VarId, BTreeSet<String>> {
    let mut all = Vec::new();
    op_exprs(&stmt.plan, &mut all);
    // The edge constraints a relationship of a pattern is matched under.
    all.extend(extra);
    // What a subquery reads of the variables it is correlated with is read
    // where it is applied.
    for sq in &stmt.subqueries {
        op_exprs(&sq.plan, &mut all);
        match &sq.kind {
            SubqueryKind::Collect(p) => all.push(p),
            // Its list, read where it is applied (`subquery.rs`).
            SubqueryKind::Members { of: Some(of), .. } => all.push(of),
            _ => {}
        }
    }
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
            | BoundOp::Filter { input, .. }
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
            | BoundOp::Filter { input, .. }
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
        graph_demand(e, &stmt.bindings, &mut uses, false);
        for (v, what) in uses {
            add(v, what);
        }
    }
    // Read as a list's elements (`elements.rs`): an UNWIND's list, and a
    // WITH item (which carries tuples when its nodes are of one label).
    for e in element_contexts(&stmt.plan) {
        let mut uses = Vec::new();
        graph_demand(e, &stmt.bindings, &mut uses, true);
        for (v, what) in uses {
            add(v, what);
        }
    }
    for e in grouped {
        if let Some(GraphRef::Path(v) | GraphRef::List(v)) = value::graph_ref(e, &stmt.bindings) {
            add(v, PATH_KEY);
        }
    }
    let subquery_parts = stmt
        .subqueries
        .iter()
        .flat_map(|sq| pattern_parts(&sq.plan));
    for part in pattern_parts(&stmt.plan).into_iter().chain(subquery_parts) {
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
        let (node_tuples, rel_tuples) = (wanted.contains(NODE_TUPLES), wanted.contains(REL_TUPLES));
        let key = wanted.contains(PATH_KEY);
        if nodes || node_tuples {
            elements.extend(part.nodes.iter().map(|n| (n.var, ALL_PROPERTIES)));
        }
        for r in &part.rels {
            match r.length {
                Some(_) => {
                    if key {
                        elements.push((r.var, PATH_KEY));
                    }
                    for (wanted, what) in [
                        (nodes, NODE_VALUES),
                        (rels, REL_VALUES),
                        (node_tuples, NODE_TUPLES),
                        (rel_tuples, REL_TUPLES),
                    ] {
                        if wanted {
                            elements.push((r.var, what));
                        }
                    }
                }
                None if rels || rel_tuples => elements.push((r.var, ALL_PROPERTIES)),
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
            // A map's value (`head(collect({msg: m}))`, S11o8): as a list's.
            LogicalExpr::MapLiteral(entries) => {
                out.extend(entries.iter().filter_map(|(_, v)| element(v)))
            }
            _ => {}
        }
        visit.extend(crate::bound_plan::expr::children(x));
    }
    out
}

/// The graph values (`value.rs`) an expression reads: (variable, what of
/// it). `length(p)` and `size()` of a list are counted from the path's
/// structure and read none.
/// The expressions whose paths' nodes / relationships are read as a list's
/// elements: an UNWIND's list and a WITH's items.
fn element_contexts(op: &BoundOp) -> Vec<&LogicalExpr> {
    match op {
        BoundOp::Unit => Vec::new(),
        BoundOp::Unwind { input, expr, .. } => {
            let mut v = element_contexts(input);
            v.push(expr);
            v
        }
        BoundOp::Project { input, projection } => {
            let mut v = element_contexts(input);
            // A variable passed through reads nothing of it.
            if projection.kind == ProjectionKind::With {
                v.extend(
                    projection
                        .items
                        .iter()
                        .map(|i| &i.expr)
                        .filter(|e| !matches!(e, LogicalExpr::TableAlias(_))),
                );
            }
            v
        }
        BoundOp::Match { input, .. }
        | BoundOp::Sort { input, .. }
        | BoundOp::Filter { input, .. }
        | BoundOp::Skip { input, .. }
        | BoundOp::Limit { input, .. } => element_contexts(input),
        BoundOp::Union { arms, .. } => arms.iter().flat_map(element_contexts).collect(),
    }
}

/// The expression each projection item of `op` was projected from.
fn projected_items(op: &BoundOp) -> HashMap<VarId, &LogicalExpr> {
    let mut out = HashMap::new();
    fn walk<'a>(op: &'a BoundOp, out: &mut HashMap<VarId, &'a LogicalExpr>) {
        match op {
            BoundOp::Unit => {}
            BoundOp::Project { input, projection } => {
                walk(input, out);
                for it in &projection.items {
                    out.insert(it.var, &it.expr);
                }
            }
            BoundOp::Match { input, .. }
            | BoundOp::Unwind { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Filter { input, .. }
            | BoundOp::Skip { input, .. }
            | BoundOp::Limit { input, .. } => walk(input, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| walk(a, out)),
        }
    }
    walk(op, &mut out);
    out
}

/// The UNWINDs (by variable) whose output order a later clause relies on,
/// with no ORDER BY between: a SKIP / LIMIT or a `collect()`. Over rows in
/// no order such an UNWIND numbers them first (in any order), so each
/// row's elements stay together in list order, as Neo4j returns them;
/// other UNWINDs leave their rows in no order.
fn ordered_unwinds(op: &BoundOp) -> HashSet<VarId> {
    fn walk(op: &BoundOp, relied: bool, out: &mut HashSet<VarId>) {
        match op {
            BoundOp::Unit => {}
            BoundOp::Unwind { input, var, .. } => {
                if relied {
                    out.insert(*var);
                }
                walk(input, relied, out);
            }
            BoundOp::Project { input, projection } => {
                // A `collect()` reads its input's order; otherwise, without
                // an ORDER BY of its own, a projection passes its input's
                // order on (DISTINCT and aggregation as first seen), and a
                // SKIP / LIMIT takes it.
                let collects = projection
                    .items
                    .iter()
                    .any(|i| calls_aggregate(&i.expr, "collect"));
                let paged = projection.skip.is_some() || projection.limit.is_some();
                let relies = collects || (projection.order_by.is_empty() && (relied || paged));
                walk(input, relies, out);
            }
            BoundOp::Sort { input, .. } => walk(input, false, out),
            BoundOp::Skip { input, .. } | BoundOp::Limit { input, .. } => walk(input, true, out),
            // A MATCH leaves its rows in no order (`Lowerer::relation`).
            BoundOp::Match { input, .. } => walk(input, false, out),
            BoundOp::Filter { input, .. } => walk(input, relied, out),
            BoundOp::Union { arms, .. } => arms.iter().for_each(|a| walk(a, false, out)),
        }
    }
    let mut out = HashSet::new();
    walk(op, false, &mut out);
    out
}

/// `nested`: `e` is inside another expression (a comprehension's list, …),
/// where a path's nodes / relationships are a list's elements; otherwise a
/// value (a RETURN item).
fn graph_demand(
    e: &LogicalExpr,
    bindings: &[Binding],
    out: &mut Vec<(VarId, &'static str)>,
    nested: bool,
) {
    // A subquery's placeholder: its arguments name what it reads, which its
    // own expressions demand (`demand`).
    if crate::bound_plan::expr::subquery_var(e).is_some() {
        return;
    }
    let structural = match e {
        LogicalExpr::ScalarFnCall(f) => match f.args.as_slice() {
            [arg] if f.name.eq_ignore_ascii_case("size") => {
                value::graph_ref(arg, bindings).is_some()
            }
            // A path's, or its nodes' / relationships' (their size:
            // `Lowerer::scalar_fn`).
            [arg] if f.name.eq_ignore_ascii_case("length") => matches!(
                value::graph_ref(arg, bindings),
                Some(GraphRef::Path(_) | GraphRef::Nodes(_) | GraphRef::Rels(_))
            ),
            _ => false,
        },
        _ => false,
    };
    if structural {
        return;
    }
    // `count(g)` reads no value; `count(DISTINCT g)` its identities
    // (`Lowerer::count_graph_value`).
    if let LogicalExpr::AggregateFnCall(f) = e {
        if let (true, [arg]) = (f.name.eq_ignore_ascii_case("count"), f.args.as_slice()) {
            let (arg, distinct) = match arg {
                LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op)
                    if op.operator == crate::query_planner::logical_expr::Operator::Distinct
                        && op.operands.len() == 1 =>
                {
                    (&op.operands[0], true)
                }
                other => (other, false),
            };
            match value::graph_ref(arg, bindings) {
                Some(
                    GraphRef::Path(v) | GraphRef::Nodes(v) | GraphRef::Rels(v) | GraphRef::List(v),
                ) => {
                    if distinct {
                        out.push((v, PATH_KEY));
                    }
                    return;
                }
                Some(GraphRef::Carried(_)) => return,
                None => {}
            }
        }
    }
    match (value::graph_ref(e, bindings), nested) {
        (Some(GraphRef::Path(p)), _) => out.extend([(p, NODE_VALUES), (p, REL_VALUES)]),
        (Some(GraphRef::Nodes(p)), false) => out.push((p, NODE_VALUES)),
        (Some(GraphRef::Rels(p)), false) => out.push((p, REL_VALUES)),
        (Some(GraphRef::List(r)), false) => out.push((r, REL_VALUES)),
        (Some(GraphRef::Nodes(p)), true) => out.push((p, NODE_TUPLES)),
        (Some(GraphRef::Rels(p)), true) => out.push((p, REL_TUPLES)),
        (Some(GraphRef::List(r)), true) => out.push((r, REL_TUPLES)),
        (Some(GraphRef::Carried(_)) | None, _) => {
            for c in crate::bound_plan::expr::children(e) {
                graph_demand(c, bindings, out, true);
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
        | BoundOp::Filter { input, .. }
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

/// Whether an element is NULL (`null`) or not (an OPTIONAL MATCH's
/// unmatched element, a NULL list element), by `ids`, its columns that are
/// all NULL exactly then ([`Lowerer::presence`]): one column's
/// `IS [NOT] NULL`, or all of a composite node id's (a part of a real
/// node's composite id can be NULL, S8d2c review).
pub(crate) fn null_test(ids: &[RenderExpr], null: bool) -> RenderExpr {
    let test = |e: &RenderExpr| {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: if null {
                Operator::IsNull
            } else {
                Operator::IsNotNull
            },
            operands: vec![e.clone()],
        })
    };
    match ids {
        [one] => test(one),
        _ if null => and_all(ids.iter().map(test).collect())
            .unwrap_or(RenderExpr::Literal(Literal::Boolean(true))),
        _ => or_all(ids.iter().map(test).collect()),
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

/// The physical identity columns of an element read by `scan`
/// ([`Lowerer::identity_physical`]).
fn scan_identity(scan: &Scan<'_>) -> Option<Vec<String>> {
    match scan {
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
