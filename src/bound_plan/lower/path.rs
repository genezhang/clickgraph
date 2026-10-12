//! Variable-length relationships (P-4c S6, `docs/design/EXPLICIT_SCOPE.md`
//! §4.11): a `PathScan` is a relation of paths, one row per path, built by
//! the recursive-CTE generator (`cte_manager`) behind one call, [`path_cte`].
//!
//! The call takes everything it uses as arguments: the edge's and endpoints'
//! access strategies (`PatternSchemaContext`), the hop range, the name of the
//! CTE, and the conditions evaluated inside the search (a pushed-down start
//! predicate, §4.8 d, and the relationship's own property map, which holds
//! of every relationship of the path). It returns the CTE, whose columns are
//! the contract the lowering reads:
//! * `start_id`, `end_id`: the identities of the node the walk starts at and
//!   the node it ends at (it follows the relationships `from` → `to`, or
//!   `to` → `from` when [`PathCall::backward`]);
//! * `hop_count`: the number of relationships;
//! * `path_edges`: the identities of its relationships, spelled as
//!   [`edge_identity_sql`] spells one, whichever way the walk goes (when
//!   [`PathCte::edges`]);
//! * `path_nodes`: the identities of its nodes.
//!
//! The generator's side channels are not used: the FROM alias it reports (the
//! query-wide `vlp_from_alias()`) is replaced by the caller's alias for the
//! relation; `outer_where_filters` (set by the denormalized strategy only) is
//! refused. A composite identity (S8d) is in `start_id` / `end_id` as a text,
//! which two ids can share; the walk carries its columns too
//! (`start_<col>` / `end_<col>`), which each step joins by
//! (`CteGenerationContext::composite_by_columns`) and the caller ties to.
//!
//! The walk is a trail: no relationship twice (the generator's
//! `NOT has(path_edges, …)`), as Cypher's relationship uniqueness requires.
//! An unbounded range (`*`, `*2..`) is unbounded: the walk ends when no trail
//! extends, or the query fails at ClickHouse's recursion limit
//! (`max_recursive_cte_evaluation_depth`, the server's `max_cte_depth`). It
//! is never cut at a depth of its own (the generator's default bound for a
//! missing maximum, which drops longer paths silently).
//!
//! A relationship of several types, definitions or labels (S7b3a) is walked
//! here instead ([`union_path_ctes`]): over the union of its definitions,
//! from node to node by label and id, its relationships and nodes kept as
//! texts (`value::rel_key`, `value::node_key_text`), with the same columns
//! and the labels of its ends. A shortest-path search reads either walk's
//! relations through [`Search`] (S7b3b: a union's nodes identified by their
//! identities as texts, its pairs' ends then recovered, [`union_ends_cte`]).

use std::sync::Arc;

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};
use crate::graph_catalog::pattern_schema::{
    EdgeAccessStrategy, JoinStrategy, NodeAccessStrategy, PatternSchemaContext,
};
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::cte_generation::CteGenerationContext;
use crate::render_plan::cte_manager::CteManager;
use crate::render_plan::render_expr::RenderExpr;
use crate::render_plan::{CategorizedFilters, Cte, CteContent};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain;
use crate::sql_generator::emitters::clickhouse::variable_length_cte::{
    spell_edge_identity, NodeProperty, PathValues,
};
use crate::sql_generator::function_mapper::current_function_mapper;

use super::{unsupported, LowerError};

/// The alias of the path's first node in the generator's base case: a pushed
/// start predicate reads it.
pub(super) const START: &str = "start_node";
/// The alias of a path's last node, in a condition on it.
pub(super) const END: &str = "end_node";
/// The alias of the relationship of each hop: the relationship's property
/// map reads it.
pub(super) const REL: &str = "rel";

/// The bound passed for a missing maximum: beyond any recursion ClickHouse
/// evaluates.
const UNBOUNDED: u32 = i32::MAX as u32;

/// The columns of a walk over several labels or types (`union_path_ctes`)
/// holding the labels of its first and last nodes.
pub(super) const START_LABEL: &str = "start_label";
pub(super) const END_LABEL: &str = "end_label";

/// Columns of a path relation that stand for it where an element's identity
/// would (carried through a CTE, tested for NULL). A path is not an element:
/// two paths can agree on all three.
pub(super) const PATH_COLUMNS: [&str; 3] = ["start_id", "end_id", "hop_count"];

/// The columns of a path relation that carry its nodes and relationships as
/// values (`value.rs`), when [`PathCall::node_values`] / `rel_values`.
pub(super) const VALUE_COLUMNS: [&str; 2] = ["path_node_values", "path_rel_values"];

/// The columns of a path relation that carry its nodes and relationships as
/// a list's elements (`elements.rs`), when [`PathCall::node_tuples`] /
/// `rel_tuples`.
pub(super) const TUPLE_COLUMNS: [&str; 2] = ["path_node_tuples", "path_rel_tuples"];

/// What one variable-length relationship needs from the generator.
#[derive(Clone)]
pub(super) struct PathCall<'a> {
    /// The relationship's binding name (`v{N}`): the CTE is `vlp_v{N}_path`.
    pub var: &'a str,
    pub rel_type: &'a str,
    pub edge: &'a RelationshipSchema,
    /// The label of every node of the path (the edge's `from` and `to`).
    pub label: &'a str,
    pub node: &'a NodeSchema,
    pub min: u32,
    pub max: Option<u32>,
    /// Walk against the relationships' direction (`to` → `from`).
    pub backward: bool,
    /// Walk this relation of the edge table's rows in both directions
    /// (`Lowerer::both_directions`: an undirected relationship), from the
    /// node each row leaves to the node it enters, in place of the table.
    pub both: Option<&'a str>,
    /// Conjuncts over the first node, aliased [`START`].
    pub start: Vec<RenderExpr>,
    /// Conjuncts over the last node, aliased [`END`] (the ends whose
    /// `allShortestPaths` rows are repeated).
    pub end: Vec<RenderExpr>,
    /// Conjuncts every relationship of the path satisfies, aliased [`REL`].
    pub rel: Vec<RenderExpr>,
    /// Carry each path's nodes / relationships as values
    /// (`path_node_values` / `path_rel_values`, `value.rs`).
    pub node_values: bool,
    pub rel_values: bool,
    /// Carry them as a list's elements (`path_node_tuples` /
    /// `path_rel_tuples`, `elements.rs`).
    pub node_tuples: bool,
    pub rel_tuples: bool,
}

impl PathCall<'_> {
    /// The node / relationship element tuples a walk carries, when asked.
    fn tuples(
        &self,
    ) -> Result<
        (
            Option<super::elements::WalkTuples>,
            Option<super::elements::WalkTuples>,
        ),
        LowerError,
    > {
        let node = match self.node_tuples {
            true => Some(super::elements::walk_tuples(
                &super::elements::node_layout(self.label, self.node)?,
                &self.node.full_table_name(),
                START,
                Some(END),
            )?),
            false => None,
        };
        let rel = match self.rel_tuples {
            true => Some(super::elements::walk_tuples(
                &super::elements::rel_layout(self.rel_type, self.edge)?,
                &self.edge.full_table_name(),
                REL,
                None,
            )?),
            false => None,
        };
        Ok((node, rel))
    }
}

/// The generated relation.
pub(super) struct PathCte {
    pub cte: Cte,
    /// It has a `path_edges` column (a path that can have relationships).
    pub edges: bool,
}

/// The schema context of `call`'s edge and nodes, walked from `from_id` to
/// `to_id` (exchanged when [`PathCall::backward`]; over [`PathCall::both`],
/// from the node a row leaves to the node it enters): the standard layout
/// (the nodes and the edge each in their own table; a composite identity
/// walked by its columns, S8d), or `Unsupported`.
fn standard_layout(
    schema: &GraphSchema,
    call: &PathCall<'_>,
) -> Result<PatternSchemaContext, LowerError> {
    let mut ctx = PatternSchemaContext::analyze(
        call.var,
        "path",
        call.node,
        call.node,
        call.edge,
        schema,
        call.var,
        vec![call.rel_type.to_string()],
        None,
        None,
    )
    .map_err(|e| LowerError::Unsupported(format!("variable-length relationship: {e}")))?;
    // An FK edge's table (a node table, S8a) holds one relationship per row,
    // as an edge table does (`RelationshipSchema::is_edge_row_table`): it is
    // walked as one, joining each relationship's row to the nodes it names.
    // (The generator's own FK-edge walk, `FkEdgeJoin`, reads the referenced
    // node's row in place of the relationship's, and keeps node uniqueness
    // at a zero lower bound, #902.)
    if matches!(ctx.join_strategy, JoinStrategy::FkEdgeJoin { .. })
        && matches!(ctx.edge, EdgeAccessStrategy::SeparateTable { .. })
    {
        ctx.join_strategy = JoinStrategy::Traditional {
            left_join_col: call.edge.from_id.clone(),
            right_join_col: call.edge.to_id.clone(),
        };
    }
    // The walk joins `rel.from_id` to the node it is at and moves to
    // `rel.to_id`.
    if let EdgeAccessStrategy::SeparateTable { from_id, to_id, .. } = &mut ctx.edge {
        if call.both.is_some() {
            // One column per identity column (a composite id's, S8d).
            let arity = call.edge.from_id.columns().len();
            *from_id = super::indexed_columns(super::BOTH_START, arity).join(", ");
            *to_id = super::indexed_columns(super::BOTH_END, arity).join(", ");
        } else if call.backward {
            std::mem::swap(from_id, to_id);
        }
    }
    let own_table = |n: &NodeAccessStrategy| matches!(n, NodeAccessStrategy::OwnTable { .. });
    if !own_table(&ctx.left_node)
        || !own_table(&ctx.right_node)
        || !matches!(ctx.edge, EdgeAccessStrategy::SeparateTable { .. })
        || !matches!(ctx.join_strategy, JoinStrategy::Traditional { .. })
    {
        return unsupported("a variable-length relationship outside the standard layout (S8)");
    }
    Ok(ctx)
}

/// `(c1) AND (c2) …`, or `None` for no conjunct.
fn conjunction(cs: &[RenderExpr]) -> Option<String> {
    (!cs.is_empty()).then(|| {
        cs.iter()
            .map(|c| format!("({})", render_expr_to_sql_plain(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    })
}

/// Build the relation of paths of `call` (the standard layout: the nodes and
/// the edge each in their own table, single-column identities).
pub(super) fn path_cte(schema: &GraphSchema, call: PathCall<'_>) -> Result<PathCte, LowerError> {
    // A composite id's columns are carried as `start_<col>` / `end_<col>`:
    // one named `id` is the generator's `start_id` (S8d review).
    let id = call.node.id_physical_columns();
    if id.len() > 1 && id.iter().any(|c| c == "id") {
        return unsupported(
            "a variable-length relationship between composite ids with a column `id`",
        );
    }
    let start_alias = call.var.to_string();
    let end_alias = "path".to_string();
    let ctx = standard_layout(schema, &call)?;
    let mut context = CteGenerationContext::new()
        .with_spec(VariableLengthSpec {
            min_hops: Some(call.min),
            max_hops: Some(call.max.unwrap_or(UNBOUNDED)),
        })
        .with_schema_owned(schema.clone())
        .with_relationship_types(Some(vec![call.rel_type.to_string()]))
        .with_edge_id(Some(edge_identity(call.edge)))
        .with_relationship_cypher_alias(Some(call.var.to_string()))
        .with_node_labels(Some(call.label.to_string()), Some(call.label.to_string()));
    context.needs_path_relationships = false;
    context.walk_relation = call.both.map(str::to_string);
    // A composite identity: each step joins the path's last node by its
    // columns, not by the ids' texts (S8d).
    context.composite_by_columns = true;
    let (node_tuple, rel_tuple) = call.tuples()?;
    context.path_values.node_tuple = node_tuple.map(|t| (t.one, t.other.unwrap_or_default()));
    context.path_values.rel_tuple = rel_tuple.map(|t| (t.one, t.empty));
    if call.node_values || call.rel_values {
        let g = super::value::spelling()?;
        let node =
            |alias: &str| super::value::table_node_object(&g, schema, call.node, call.label, alias);
        context.path_values = PathValues {
            node_tuple: context.path_values.node_tuple.take(),
            rel_tuple: context.path_values.rel_tuple.take(),
            node: match call.node_values {
                true => Some((node(START)?, node(END)?)),
                false => None,
            },
            rel: match call.rel_values {
                true => Some((
                    super::value::table_rel_object(&g, call.edge, call.rel_type, REL)?,
                    (g.list)(&[]),
                )),
                false => None,
            },
        };
    }
    let filters = CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
        path_function_filters: None,
        both_endpoint_filters: None,
        start_sql: conjunction(&call.start),
        end_sql: None,
        relationship_sql: conjunction(&call.rel),
        both_endpoint_sql: None,
    };
    let carried = constraint_carried(&call)?;
    let result = CteManager::with_context(Arc::new(schema.clone()), context)
        .generate_vlp_cte(&ctx, &carried, &filters)
        .map_err(|e| LowerError::Unsupported(format!("variable-length relationship: {e}")))?;
    let name = format!("vlp_{start_alias}_{end_alias}");
    if result.outer_where_filters.is_some()
        || result.cte_name != name
        || !result.sql.starts_with(&name)
    {
        return unsupported("internal: the path generator's output is not one relation");
    }
    Ok(PathCte {
        cte: Cte::new(name, CteContent::RawSql(result.sql), true),
        // `*0..0` has no recursion and no relationship.
        edges: call.max != Some(0),
    })
}

/// The properties an edge constraint reads of its `from` node (S11h): the
/// generator carries them of each path's last node (`end_<alias>`, the
/// alias prefixed so it never meets the generator's own `end_id`), where the
/// next step's constraint reads them. It rewrites `from.<property>` by text,
/// so a property another referenced one starts with is refused.
fn constraint_carried(call: &PathCall<'_>) -> Result<Vec<NodeProperty>, LowerError> {
    let Some(text) = &call.edge.constraints else {
        return Ok(Vec::new());
    };
    let names: Vec<&String> = call.node.property_mappings.keys().collect();
    let read = |prefix: &str| -> Vec<&str> {
        text.match_indices(prefix)
            .filter(|(at, _)| {
                !text[..*at].ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
            })
            .map(|(at, _)| {
                let rest = &text[at + prefix.len()..];
                let end = rest
                    .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .unwrap_or(rest.len());
                &rest[..end]
            })
            .collect()
    };
    let mut carried = Vec::new();
    for prefix in ["from.", "to."] {
        for prop in read(prefix) {
            if names
                .iter()
                .any(|n| n.as_str() != prop && prop.starts_with(n.as_str()))
            {
                return unsupported(format!(
                    "a variable-length relationship whose edge constraint `{text}` reads \
                     `{prefix}{prop}`, which another property's name starts"
                ));
            }
            if prefix == "to." {
                continue;
            }
            let Some(crate::graph_catalog::expression_parser::PropertyValue::Column(column)) =
                call.node.property_mappings.get(prop)
            else {
                return unsupported(format!(
                    "a variable-length relationship whose edge constraint `{text}` reads \
                     `from.{prop}`, which is not a column"
                ));
            };
            let alias = format!("cg_{prop}");
            if !carried.iter().any(|p: &NodeProperty| p.alias == alias) {
                carried.push(NodeProperty {
                    cypher_alias: "path".to_string(),
                    column_name: column.clone(),
                    alias,
                });
            }
        }
    }
    Ok(carried)
}

/// What a shortest-path search reads (§4.11): the relations of its nodes
/// and relationships, each node identified by one column, the range and the
/// conditions. Of one table ([`PathCall::search`], walked as
/// [`standard_layout`] walks it) or of a union of definitions
/// ([`UnionWalk::search`], S7b3b: its nodes identified by their identities
/// as texts, as a path's identity spells them).
#[derive(Clone)]
pub(super) struct Search {
    pub var: String,
    /// The nodes, and the columns identifying one (several: a composite id,
    /// keyed by [`composite_key`], S8d2).
    node_table: String,
    id: Vec<String>,
    /// The relationships, from the node in `from_id` to the node in `to_id`
    /// (none: only the path of none can match).
    edge_table: Option<String>,
    from_id: Vec<String>,
    to_id: Vec<String>,
    pub min: u32,
    pub max: Option<u32>,
    /// Conjuncts over the first node, aliased [`START`].
    pub start: Vec<RenderExpr>,
    /// Conjuncts over the last node, aliased [`END`].
    pub end: Vec<RenderExpr>,
    /// Conjuncts every relationship satisfies, aliased [`REL`].
    pub rel: Vec<RenderExpr>,
    /// A relationship's identity as a text, over [`REL`] (`None`: not
    /// spelled, a path is not recovered).
    edge_text: Option<String>,
    /// A node's value over [`START`] and over [`END`], and a relationship's
    /// over [`REL`], when the paths carry them.
    node_value: Option<(String, String)>,
    rel_value: Option<String>,
    /// A node's / relationship's element tuples, when the paths carry them.
    node_tuple: Option<super::elements::WalkTuples>,
    rel_tuple: Option<super::elements::WalkTuples>,
}

impl Search {
    /// The key of columns `cols` over `alias` (none: unqualified): the column,
    /// or the [`composite_key`] of several.
    fn key(cols: &[String], alias: &str) -> String {
        let at: Vec<String> = cols
            .iter()
            .map(|c| match alias {
                "" => c.clone(),
                a => format!("{a}.{c}"),
            })
            .collect();
        match at.as_slice() {
            [one] => one.clone(),
            _ => composite_key(&at).unwrap_or_else(|| format!("({})", at.join(", "))),
        }
    }

    /// A node's key over `alias`, as the search identifies a node.
    pub(super) fn node_key(&self, alias: &str) -> String {
        Self::key(&self.id, alias)
    }

    /// The keys of a relationship's `from` / `to` node over `alias`.
    fn rel_start_key(&self, alias: &str) -> String {
        Self::key(&self.from_id, alias)
    }

    fn rel_end_key(&self, alias: &str) -> String {
        Self::key(&self.to_id, alias)
    }

    /// The columns of a walked relation (`walk_ctes`) after `start_id`,
    /// `end_id` and `hop_count`: its nodes' identities, its relationships'
    /// (as texts), and the values it carries.
    pub(super) fn walked_columns(&self) -> Vec<&'static str> {
        let mut columns = vec!["path_nodes", "path_edges"];
        if self.node_value.is_some() {
            columns.push(VALUE_COLUMNS[0]);
        }
        if self.rel_value.is_some() {
            columns.push(VALUE_COLUMNS[1]);
        }
        if self.node_tuple.is_some() {
            columns.push(TUPLE_COLUMNS[0]);
        }
        if self.rel_tuple.is_some() {
            columns.push(TUPLE_COLUMNS[1]);
        }
        columns
    }

    /// The values the last node can have (`end`), as a SELECT of one column,
    /// or `None` when nothing restricts it.
    fn targets(&self) -> Option<String> {
        conjunction(&self.end).map(|c| {
            format!(
                "SELECT {key} FROM {table} AS {END} WHERE {c}",
                key = match self.id.as_slice() {
                    [_] => self.node_key(END),
                    _ => format!("{} AS {TARGET}", self.node_key(END)),
                },
                table = self.node_table
            )
        })
    }

    /// The column of [`Self::targets`], over `alias`.
    fn target_column(&self, alias: &str) -> String {
        match self.id.as_slice() {
            [one] => format!("{alias}.{one}"),
            _ => format!("{alias}.{TARGET}"),
        }
    }
}

impl PathCall<'_> {
    /// The search of these paths, with `end` the conjuncts over the last
    /// node.
    pub(super) fn search(&self, schema: &GraphSchema) -> Result<Search, LowerError> {
        let ctx = standard_layout(schema, self)?;
        let (
            NodeAccessStrategy::OwnTable {
                table: node_table,
                id_column,
                ..
            },
            EdgeAccessStrategy::SeparateTable {
                table: edge_table,
                from_id,
                to_id,
                ..
            },
        ) = (&ctx.left_node, &ctx.edge)
        else {
            return unsupported("internal: a shortestPath outside the standard layout");
        };
        let edge_table = self.both.unwrap_or(edge_table);
        // A node is identified by one value: a composite id (S8d2) by the
        // text of the tuple of its columns ([`composite_key`]), which no two
        // ids share (unlike the generator's `|`-joined text), computed where
        // the search reads it (the tables are read as they are, every column
        // there: a subquery's `*` has no MATERIALIZED / ALIAS column).
        let columns =
            |list: &str| -> Vec<String> { list.split(", ").map(str::to_string).collect() };
        let (id, from, to) = match id_column {
            Identifier::Single(id) => {
                (vec![id.clone()], vec![from_id.clone()], vec![to_id.clone()])
            }
            Identifier::Composite(cols) => {
                let (from, to) = (columns(from_id), columns(to_id));
                if from.len() != cols.len() || to.len() != cols.len() {
                    return unsupported(
                        "internal: a relationship's end of another arity than its id",
                    );
                }
                if composite_key(cols).is_none() {
                    return unsupported("a shortestPath between composite ids in this SQL dialect");
                }
                (cols.clone(), from, to)
            }
        };
        // A row whose end has a NULL part relates no node (the tables'
        // columns join by `=`; a key's text of NULL would equal itself).
        let mut rel = self.rel.clone();
        if id.len() > 1 {
            for c in from.iter().chain(&to) {
                rel.push(RenderExpr::Raw(format!("{REL}.{c} IS NOT NULL")));
            }
        }
        let g = current_function_mapper().graph_values();
        let values = |wanted: bool| -> Result<_, LowerError> {
            match (wanted, &g) {
                (false, _) => Ok(None),
                (true, None) => unsupported("a shortestPath's path in this SQL dialect"),
                (true, Some(g)) => Ok(Some(g)),
            }
        };
        let node_value = match values(self.node_values)? {
            Some(g) => Some((
                super::value::table_node_object(g, schema, self.node, self.label, START)?,
                super::value::table_node_object(g, schema, self.node, self.label, END)?,
            )),
            None => None,
        };
        let rel_value = match values(self.rel_values)? {
            Some(g) => Some(super::value::table_rel_object(
                g,
                self.edge,
                self.rel_type,
                REL,
            )?),
            None => None,
        };
        let (node_tuple, rel_tuple) = self.tuples()?;
        Ok(Search {
            node_tuple,
            rel_tuple,
            var: self.var.to_string(),
            node_table: node_table.clone(),
            id,
            edge_table: Some(edge_table.to_string()),
            from_id: from,
            to_id: to,
            min: self.min,
            max: self.max,
            start: self.start.clone(),
            end: self.end.clone(),
            rel,
            edge_text: g.as_ref().map(|g| {
                let cols: Vec<String> = edge_identity(self.edge)
                    .columns()
                    .into_iter()
                    .map(|c| format!("{REL}.{c}"))
                    .collect();
                super::value::identity_text(g, &cols)
            }),
            node_value,
            rel_value,
        })
    }
}

/// A composite id of the columns (SQL) `cols` as one value a shortest-path
/// search keys a node by (S8d2): the dialect's text of their tuple, which
/// tells two ids apart however their parts read (ClickHouse quotes a
/// string part: `('B|1','2')` is not `('B','1|2')`). `None` where the dialect
/// spells no text of a value.
pub(super) fn composite_key(cols: &[String]) -> Option<String> {
    let g = current_function_mapper().graph_values()?;
    Some((g.tuple_text)(cols))
}

/// The column of a search's targets (`Search::targets`).
const TARGET: &str = "__cg_target";

/// The parents a shortest-path search keeps of each node (`search_cte`),
/// for a walk back over its levels (`walk_ctes`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Parents {
    None,
    /// The least node one level nearer with a relationship to it
    /// (`parent`).
    Least,
    /// Every such node, once (`parents`). An array is kept only here: read
    /// through `ARRAY JOIN`, it made a pinned pair's walk twice as slow.
    All,
}

/// The breadth-first search of `call` (§4.11), `vlp_{var}_bfs`: for each
/// first node `start_id`, the nodes `node` it reaches and their distance
/// `depth`, from `call.min` (0 or 1) to `call.max`; with `all`, the number of
/// shortest paths to each (`paths`); otherwise, with `parents`, the nodes
/// one level nearer with a relationship to it that a walk back follows
/// ([`Parents`]: `parent`, or the array `parents`; the first node's is
/// itself).
///
/// ClickHouse gives each step of a recursive CTE only the rows of the step
/// before, so each step carries the nodes reached so far (`new = 0`) along
/// with those reached first at this depth (`new = 1`), and a node already
/// reached is not reached again: the search visits each node once per first
/// node, and ends when a depth adds no node (one step later, when the carried
/// rows stop). A node is reached through a relationship of the type that
/// satisfies its property map, and exists in the node table. `paths` is the
/// sum of the counts of the nodes one level nearer with a relationship to
/// it, once per relationship. A first node whose search has reached every
/// value the last node can have (`call.end`) goes no further: the farther
/// nodes are no path's end (a deep graph would otherwise reach ClickHouse's
/// recursion limit before a near end is known to be all).
pub(super) fn search_cte(call: &Search, all: bool, parents: Parents) -> Result<Cte, LowerError> {
    let Some(spelling) = current_function_mapper().shortest_path_search() else {
        return unsupported("shortestPath in this SQL dialect");
    };
    let (depth, flag, count) = (spelling.depth, spelling.flag, spelling.count);
    if !spelling.grouped_steps {
        if all {
            return unsupported("internal: a count of shortest paths where steps cannot group");
        }
        if parents != Parents::None {
            return path_search_cte(call, &spelling, true);
        }
    }
    let node_table = &call.node_table;
    let (start_key, end_key) = (call.node_key("start_node"), call.node_key("end_node"));
    let (rel_from, rel_to) = (call.rel_start_key("rel"), call.rel_end_key("rel"));
    let bfs = format!("vlp_{}_bfs", call.var);
    // A row whose id is NULL is no node: none starts a search (Spark's
    // `(a, b) NOT IN` of a row holding NULL rejects every pair, and the
    // search would end at its first nodes).
    let seed_where = format!(
        "\n    WHERE {}",
        std::iter::once(format!("{start_key} IS NOT NULL"))
            .chain(conjunction(&call.start))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let seed_paths = if all {
        format!(", CAST(1 AS {count}) AS paths")
    } else if parents == Parents::Least {
        format!(", {start_key} AS parent")
    } else if parents == Parents::All {
        format!(", [{start_key}] AS parents")
    } else {
        String::new()
    };
    let mut search = format!(
        "{bfs} AS (\n    \
         SELECT DISTINCT {start_key} AS start_id, {start_key} AS node, \
         CAST(0 AS {depth}) AS depth{seed_paths}, CAST(1 AS {flag}) AS new\n    \
         FROM {node_table} AS start_node{seed_where}"
    );
    // `*0..0`: the first nodes only (the edge may join other labels).
    if let (Some(edge_table), false) = (&call.edge_table, call.max == Some(0)) {
        let mut step = vec!["f.new = 1".to_string()];
        if let Some(max) = call.max {
            step.push(format!("f.depth < {max}"));
        }
        step.extend(conjunction(&call.rel));
        step.push(format!(
            "(f.start_id, {end_key}) NOT IN (SELECT start_id, node FROM {bfs})"
        ));
        if let (Some(targets), true) = (call.targets(), spelling.stops_early) {
            step.push(format!(
                "f.start_id NOT IN (SELECT start_id FROM {bfs} GROUP BY start_id \
                 HAVING {count_if}(node IN ({targets})) >= \
                 (SELECT count(DISTINCT {target}) FROM ({targets}) AS {END}))",
                count_if = current_function_mapper().count_if(),
                target = call.target_column(END),
            ));
        }
        // `shortestPath` needs a node once; `allShortestPaths` the number of
        // shortest paths to it, summed over the relationships reaching it.
        let (distinct, step_paths, reach, carried) = if all {
            (
                "",
                format!(", CAST(sum(f.paths) AS {count}) AS paths"),
                format!("\n    GROUP BY f.start_id, {end_key}, f.depth"),
                ", paths",
            )
        } else if parents != Parents::None {
            let (kept, column) = match parents {
                Parents::All => ((spelling.distinct_list)("f.node"), "parents"),
                _ => ("min(f.node)".to_string(), "parent"),
            };
            (
                "",
                format!(", {kept} AS {column}"),
                format!("\n    GROUP BY f.start_id, {end_key}, f.depth"),
                if column == "parent" {
                    ", parent"
                } else {
                    ", parents"
                },
            )
        } else {
            ("DISTINCT ", String::new(), String::new(), "")
        };
        let reached = format!(
            "SELECT {distinct}f.start_id AS start_id, {end_key} AS node, \
             CAST(f.depth + 1 AS {depth}) AS depth{step_paths}, CAST(1 AS {flag}) AS new\n    \
             FROM {bfs} AS f\n    \
             JOIN {edge_table} AS rel ON {rel_from} = f.node\n    \
             JOIN {node_table} AS end_node ON {end_key} = {rel_to}\n    \
             WHERE {}{reach}",
            step.join("\n      AND "),
        );
        let kept = format!(
            "SELECT start_id, node, depth{carried}, CAST(0 AS {flag}) AS new\n    \
             FROM {bfs}\n    \
             WHERE start_id IN (SELECT start_id FROM {bfs} WHERE new = 1)"
        );
        search.push_str(&format!(
            "\n    UNION ALL\n    {}",
            (spelling.recursive_steps)(&[reached, kept])
        ));
    }
    search.push_str("\n)");
    Ok(Cte::new(bfs, CteContent::RawSql(search), true))
}

/// [`search_cte`] where a step of a recursive CTE cannot group (Spark) and
/// the paths are recovered (`walk_ctes`): the search keeps them, a row per
/// shortest path to each node it reaches, with the [`carried_columns`] after
/// its first node. A node already reached is not reached again, so only a
/// shortest path to a node goes on: each row of a depth is a shortest path,
/// once per relationship of parallel ones (as the walk back has them). The
/// first rows of a node are `new = 1`; each step carries the nodes reached
/// so far (`new = 0`, once each, their paths empty) for the next to tell
/// them.
///
/// Without `carry` the rows carry no path columns: a row is still a path
/// (`allShortestPaths` counted, [`counted_paths_ctes`]).
fn path_search_cte(
    call: &Search,
    spelling: &crate::sql_generator::function_mapper::ShortestPathSearch,
    carry: bool,
) -> Result<Cte, LowerError> {
    let Some(g) = current_function_mapper().graph_values() else {
        return unsupported("a shortestPath's path in this SQL dialect");
    };
    let (depth, flag) = (spelling.depth, spelling.flag);
    let node_table = &call.node_table;
    let (start_key, end_key) = (call.node_key(START), call.node_key(END));
    let (rel_from, rel_to) = (call.rel_start_key(REL), call.rel_end_key(REL));
    let bfs = format!("vlp_{}_bfs", call.var);
    let seed_where = std::iter::once(format!("{start_key} IS NOT NULL"))
        .chain(conjunction(&call.start))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Each carried column, after a comma.
    let carried = |node: &str| match carry {
        true => carried_columns(call, node),
        false => Ok(Vec::new()),
    };
    let empty = |node: &str| -> Result<String, LowerError> {
        Ok(carried(node)?
            .iter()
            .map(|(c, e, _)| format!(", {e} AS {c}"))
            .collect())
    };
    let mut search = format!(
        "{bfs} AS (\n    \
         SELECT s.start_id AS start_id, s.start_id AS node, CAST(0 AS {depth}) AS depth\
         {}, CAST(1 AS {flag}) AS new\n    \
         FROM (SELECT DISTINCT {start_key} AS start_id FROM {node_table} AS {START} \
         WHERE {seed_where}) AS s",
        empty("s.start_id")?
    );
    // `*0..0`: the first nodes only (the edge may join other labels).
    if let (Some(edge_table), false) = (&call.edge_table, call.max == Some(0)) {
        let mut step = vec!["f.new = 1".to_string()];
        if let Some(max) = call.max {
            step.push(format!("f.depth < {max}"));
        }
        step.extend(conjunction(&call.rel));
        step.push(format!(
            "(f.start_id, {end_key}) NOT IN (SELECT start_id, node FROM {bfs})"
        ));
        let stepped: String = carried("node")?
            .iter()
            .map(|(c, _, one)| {
                format!(
                    ",\n      {} AS {c}",
                    (g.concat)(&[format!("f.{c}"), (g.list)(std::slice::from_ref(one))])
                )
            })
            .collect();
        let reached = format!(
            "SELECT f.start_id AS start_id, {end_key} AS node, \
             CAST(f.depth + 1 AS {depth}) AS depth{},\n      \
             CAST(1 AS {flag}) AS new\n    \
             FROM {bfs} AS f\n    \
             JOIN {edge_table} AS {REL} ON {rel_from} = f.node\n    \
             JOIN {node_table} AS {END} ON {end_key} = {rel_to}\n    \
             WHERE {}",
            stepped,
            step.join("\n      AND "),
        );
        // The nodes reached, once each (a node's paths are many rows).
        let kept = format!(
            "SELECT start_id, node, depth{}, CAST(0 AS {flag}) AS new FROM (\n      \
             SELECT DISTINCT start_id, node, depth FROM {bfs}\n      \
             WHERE start_id IN (SELECT start_id FROM {bfs} WHERE new = 1)\n    \
             ) AS reached",
            empty("node")?
        );
        search.push_str(&format!(
            "\n    UNION ALL\n    {}",
            (spelling.recursive_steps)(&[reached, kept])
        ));
    }
    search.push_str("\n)");
    Ok(Cte::new(bfs, CteContent::RawSql(search), true))
}

/// The shortest paths of `call` to the rows of its search that satisfy
/// `ends`, a row each, as the relation `name` (`start_id`, `end_id`,
/// `hop_count`), where a step of a recursive CTE cannot group (Spark): the
/// search keeps the paths, without their columns (none reads them).
pub(super) fn counted_paths_ctes(
    call: &Search,
    name: &str,
    ends: &str,
) -> Result<Vec<Cte>, LowerError> {
    let Some(spelling) = current_function_mapper().shortest_path_search() else {
        return unsupported("shortestPath in this SQL dialect");
    };
    let bfs = format!("vlp_{}_bfs", call.var);
    let sql = format!(
        "{name} AS (\n    \
         SELECT start_id, node AS end_id, depth AS hop_count FROM {bfs}\n    \
         WHERE new = 1 AND {ends}\n)"
    );
    Ok(vec![
        path_search_cte(call, &spelling, false)?,
        Cte::new(name.to_string(), CteContent::RawSql(sql), false),
    ])
}

/// The condition a row of the search of `call` (`search_cte`: `start_id`,
/// `node`, `depth`) meets when its node is a path's last: at a distance in
/// the range, and a value the last node can have.
pub(super) fn ends_reached(call: &Search) -> String {
    let mut reached = format!("depth >= {}", call.min);
    if let Some(targets) = call.targets() {
        reached.push_str(&format!("\n      AND node IN ({targets})"));
    }
    reached
}

/// The pairs the search of `call` reaches (`search_cte`), under `name`:
/// `start_id`, `end_id` (a value the last node can have), `hop_count` (the
/// distance). With `all` and `copies`, a pair's row is repeated once per
/// shortest path (a count beyond the dialect's array size fails, it does not
/// wrap); with `all` alone, the count is the column `paths`.
pub(super) fn reached_cte(
    call: &Search,
    name: &str,
    all: bool,
    copies: bool,
) -> Result<Cte, LowerError> {
    let Some(spelling) = current_function_mapper().shortest_path_search() else {
        return unsupported("shortestPath in this SQL dialect");
    };
    let bfs = format!("vlp_{}_bfs", call.var);
    let reached = format!("new = 1 AND {}", ends_reached(call));
    let (paths, copy) = match (all, copies) {
        (true, true) => (
            "",
            format!(" ARRAY JOIN {} AS copy", (spelling.copies)("paths")),
        ),
        (true, false) => (", paths", String::new()),
        (false, _) => ("", String::new()),
    };
    let sql = format!(
        "{name} AS (\n    \
         SELECT start_id, node AS end_id, depth AS hop_count{paths} FROM {bfs}{copy}\n    \
         WHERE {reached}\n)"
    );
    Ok(Cte::new(name.to_string(), CteContent::RawSql(sql), false))
}

/// The shortest paths of `call` to the rows of its search (`search_cte`, run
/// without counting) that satisfy `ends`, recovered (§4.11, S6d), as the
/// relation `name`: `start_id`, `end_id`, `hop_count` and
/// [`walked_columns`], in walk order from `start_id`. `path_edges` holds
/// each relationship's identity as a text (`edge_identity_sql`), whatever
/// the edge table's column types: it only tells paths apart.
///
/// The search keeps each node's distance from each first node, its level.
/// A shortest path to `t` at distance `d` steps from `t` to a node at
/// `d - 1` with a relationship to `t`, and on down to the first node, so the
/// walk goes back over the levels, one per step (`vlp_{var}_walk`). It is
/// one recursive CTE that reads the search once: its first rows are the
/// search's (`level = 1`), with the paths' last nodes marked
/// (`frontier = 1`); each step moves every path one level back and carries
/// the levels along while a path has more than one step left. (A CTE read
/// inside a recursive step is evaluated again in each step: a walk joined to
/// the search ran it once per level.)
/// Each step follows a node's `parents` (the search ran with them), by every
/// relationship from each to the node:
/// * `all` (with [`Parents::All`]): every path, a row each. Joining every
///   relationship into the node and keeping those from the level before
///   multiplied the paths by the nodes' in-degree first: 940K paths from one
///   start at scale 100 ran out of memory.
/// * Else (with [`Parents::Least`]): one parent, and one relationship of
///   parallel ones (the least identity): one path per pair, and a value is
///   built only for it.
///
/// Where a step of a recursive CTE cannot group (Spark), the search keeps no
/// parents: they are read after it (`vlp_{var}_parents`: a node's at one
/// level nearer with a relationship to it; one path: the least, by its least
/// relationship), and the walk joins them in each step.
pub(super) fn walk_ctes(
    call: &Search,
    name: &str,
    ends: &str,
    all: bool,
) -> Result<Vec<Cte>, LowerError> {
    let m = current_function_mapper();
    let (Some(spelling), Some(g)) = (m.shortest_path_search(), m.graph_values()) else {
        return unsupported("a shortestPath's path in this SQL dialect");
    };
    let Some(edge_text) = &call.edge_text else {
        return unsupported("a variable-length relationship between composite ids (S8)");
    };
    let (depth, flag) = (spelling.depth, spelling.flag);
    let node_table = &call.node_table;
    let end_key = call.node_key("end_node");
    let (rel_from, rel_to, to_bare) = (
        call.rel_start_key("rel"),
        call.rel_end_key("rel"),
        call.rel_end_key(""),
    );
    let bfs = format!("vlp_{}_bfs", call.var);
    let walk = format!("vlp_{}_walk", call.var);
    let columns = call.walked_columns();
    let carried = carried_columns(call, "node")?;
    let empty: Vec<String> = carried
        .iter()
        .map(|(c, e, _)| format!("{e} AS {c}"))
        .collect();
    let stepped: Vec<String> = carried
        .iter()
        .map(|(c, _, one)| {
            format!(
                "{} AS {c}",
                (g.concat)(&[(g.list)(std::slice::from_ref(one)), format!("w.{c}")])
            )
        })
        .collect();
    let mut step = vec!["w.frontier = 1".to_string(), "w.depth > 0".to_string()];
    // The relationships are read only into the frontier's nodes: joined to
    // the whole edge table, a walk of one path cost twice as much (an
    // `allShortestPaths` pair at scale 100: 710 ms against 432 ms).
    step.extend(conjunction(&call.rel));
    if !spelling.grouped_steps {
        // The search kept the paths (`search_cte`): a row each, its last
        // node `node`; one per pair, as the walk back picks it (the least
        // parent, by its least relationship, at each level from the last).
        let names: Vec<&str> = carried.iter().map(|(c, _, _)| *c).collect();
        let found = format!("SELECT * FROM {bfs} WHERE new = 1 AND {ends}");
        let found = match all {
            true => found,
            false => format!(
                "SELECT * FROM (\n        \
                 SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id, node \
                 ORDER BY {}, {}) AS pick\n        \
                 FROM ({found}) AS found\n    \
                 ) AS picked WHERE pick = 1",
                (g.reverse)("path_nodes"),
                (g.reverse)("path_edges"),
            ),
        };
        let walk_sql = format!(
            "{walk} AS (\n    \
             SELECT start_id, node AS end_id, depth AS hop_count, {}\n    \
             FROM ({found}) AS w\n)",
            names.join(", ")
        );
        return Ok(vec![
            Cte::new(walk.clone(), CteContent::RawSql(walk_sql), false),
            walked_paths(call, name, &walk, "true")?,
        ]);
    }
    // The parents, each a row of `lv`; the pick of one path of parallel
    // relationships.
    let (parents, levels) = if all {
        (
            "parents",
            format!("SELECT start_id, node, parent FROM {walk} ARRAY JOIN parents AS parent WHERE level = 1"),
        )
    } else {
        (
            "parent",
            format!("SELECT start_id, node, parent FROM {walk} WHERE level = 1"),
        )
    };
    let (pick, picked) = if all {
        (String::new(), String::new())
    } else {
        (
            format!(
                ",\n          ROW_NUMBER() OVER (PARTITION BY w.start_id, w.end_id ORDER BY {edge_text}) AS pick",
            ),
            " WHERE pick = 1".to_string(),
        )
    };
    let mut walk_sql = format!(
        "{walk} AS (\n    \
         SELECT start_id, node, depth, node AS end_id, depth AS hop_count, {empty}, {parents}, \
         CAST(1 AS {flag}) AS level, CAST({ends} AS {flag}) AS frontier\n    \
         FROM {bfs}\n    \
         WHERE new = 1",
        empty = empty.join(", "),
    );
    // Without relationships the paths are those of none: the first rows.
    if let Some(edge_table) = &call.edge_table {
        walk_sql.push_str(&format!(
            "\n    \
         UNION ALL\n    \
         SELECT start_id, node, depth, end_id, hop_count, {columns}, {parents}, level, frontier FROM (\n        \
         SELECT w.start_id AS start_id, lv.parent AS node, CAST(w.depth - 1 AS {depth}) AS depth, \
         w.end_id AS end_id, w.hop_count AS hop_count,\n          {stepped}, w.{parents} AS {parents},\n          \
         CAST(0 AS {flag}) AS level, CAST(1 AS {flag}) AS frontier{pick}\n        \
         FROM {walk} AS w\n        \
         JOIN ({levels}) AS lv\n          \
         ON lv.start_id = w.start_id AND lv.node = w.node\n        \
         JOIN (SELECT * FROM {edge_table} WHERE {to_bare} IN \
         (SELECT node FROM {walk} WHERE frontier = 1 AND depth > 0)) AS rel\n          \
         ON {rel_from} = lv.parent AND {rel_to} = w.node\n        \
         JOIN {node_table} AS end_node ON {end_key} = w.node\n        \
         WHERE {step}\n    \
         ){picked}\n    \
         UNION ALL\n    \
         SELECT start_id, node, depth, end_id, hop_count, {emptied}, {parents}, level, CAST(0 AS {flag}) AS frontier\n    \
         FROM {walk}\n    \
         WHERE level = 1 AND start_id IN (SELECT start_id FROM {walk} WHERE frontier = 1 AND depth > 1)",
            columns = columns.join(", "),
            stepped = stepped.join(",\n          "),
            step = step.join(" AND "),
            // A level row's values are empty.
            emptied = std::iter::once("path_nodes".to_string())
                .chain(carried[1..].iter().map(|(c, e, _)| format!("{e} AS {c}")))
                .collect::<Vec<_>>()
                .join(", "),
        ));
    }
    walk_sql.push_str("\n)");
    Ok(vec![
        Cte::new(walk.clone(), CteContent::RawSql(walk_sql), true),
        walked_paths(call, name, &walk, "w.frontier = 1 AND w.depth = 0")?,
    ])
}

/// The values a recovered path carries after its first node (`walk_ctes`;
/// a search that keeps the paths, `search_cte`), each with its value when
/// empty (`node`: a node's key, read for its type) and one step's (over
/// [`END`] and [`REL`]): the nodes' keys, the relationships' identities as
/// texts, and the values and tuples `call` asks for.
fn carried_columns(
    call: &Search,
    node: &str,
) -> Result<Vec<(&'static str, String, String)>, LowerError> {
    let m = current_function_mapper();
    let Some(g) = m.graph_values() else {
        return unsupported("a shortestPath's path in this SQL dialect");
    };
    let Some(edge_text) = &call.edge_text else {
        return unsupported("a variable-length relationship between composite ids (S8)");
    };
    let mut carried: Vec<(&'static str, String, String)> = vec![
        (
            "path_nodes",
            m.array_slice(&m.array_literal(node), "1", Some("0")),
            call.node_key(END),
        ),
        ("path_edges", (g.texts)(&[]), edge_text.clone()),
    ];
    if let Some((_, end)) = &call.node_value {
        carried.push((VALUE_COLUMNS[0], (g.list)(&[]), end.clone()));
    }
    if let Some(rel) = &call.rel_value {
        carried.push((VALUE_COLUMNS[1], (g.list)(&[]), rel.clone()));
    }
    if let Some(t) = &call.node_tuple {
        let end = t.other.clone().unwrap_or_default();
        carried.push((TUPLE_COLUMNS[0], t.empty.clone(), end));
    }
    if let Some(t) = &call.rel_tuple {
        carried.push((TUPLE_COLUMNS[1], t.empty.clone(), t.one.clone()));
    }
    Ok(carried)
}

/// The paths of a walk `walk` (`walk_ctes`), as the relation `name`: the
/// rows that satisfy `done` (the first node reached), the first node
/// before the rest.
fn walked_paths(call: &Search, name: &str, walk: &str, done: &str) -> Result<Cte, LowerError> {
    let Some(g) = current_function_mapper().graph_values() else {
        return unsupported("a shortestPath's path in this SQL dialect");
    };
    let node_table = &call.node_table;
    // The first node, before the rest.
    let mut first = vec![format!(
        "{} AS path_nodes",
        (g.concat)(&[
            (g.list)(&["w.start_id".to_string()]),
            "w.path_nodes".to_string()
        ])
    )];
    first.push("w.path_edges AS path_edges".to_string());
    let mut from = format!("{walk} AS w");
    if let Some((start, _)) = &call.node_value {
        first.push(format!(
            "{} AS {}",
            (g.concat)(&[
                (g.list)(std::slice::from_ref(start)),
                format!("w.{}", VALUE_COLUMNS[0]),
            ]),
            VALUE_COLUMNS[0]
        ));
    }
    if call.rel_value.is_some() {
        first.push(format!("w.{c} AS {c}", c = VALUE_COLUMNS[1]));
    }
    if let Some(t) = &call.node_tuple {
        first.push(format!(
            "{} AS {}",
            (g.concat)(&[
                (g.list)(std::slice::from_ref(&t.one)),
                format!("w.{}", TUPLE_COLUMNS[0])
            ]),
            TUPLE_COLUMNS[0]
        ));
    }
    if call.rel_tuple.is_some() {
        first.push(format!("w.{c} AS {c}", c = TUPLE_COLUMNS[1]));
    }
    // The first node's value / tuple read it.
    if call.node_value.is_some() || call.node_tuple.is_some() {
        from.push_str(&format!(
            "\n    JOIN {node_table} AS {START} ON {} = w.start_id",
            call.node_key(START)
        ));
    }
    let paths_sql = format!(
        "{name} AS (\n    \
         SELECT w.start_id AS start_id, w.end_id AS end_id, w.hop_count AS hop_count, {}\n    \
         FROM {from}\n    \
         WHERE {done}\n)",
        first.join(", ")
    );
    Ok(Cte::new(
        name.to_string(),
        CteContent::RawSql(paths_sql),
        false,
    ))
}

/// The rows of the search of `var` whose node is the last of a path that
/// satisfies `conditions` at its distance (the pairs of `vlp_{var}_near`
/// that do), as a condition over `start_id` and `node`.
pub(super) fn passing_ends(
    var: &str,
    alias: &str,
    ends: &[PickEnd],
    conditions: &[RenderExpr],
) -> String {
    format!(
        "(start_id, node) IN (SELECT {alias}.start_id, {alias}.end_id FROM {} WHERE {})",
        with_ends(&format!("vlp_{var}_near"), alias, ends),
        conjunction(conditions).unwrap_or_else(|| "true".to_string()),
    )
}

/// How the trails a shortestPath picks among (`pick_cte`) have their
/// relationships: none (`*0..0`), as the generator's typed identities (a
/// table's walk), or as texts already (a union's, `union_path_ctes`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TrailEdges {
    None,
    /// The relationships' identities, of this many columns.
    Typed(usize),
    Texts,
}

/// An end node of a path read by a condition of a shortestPath search:
/// joined to the paths under its own alias.
pub(super) struct PickEnd {
    pub alias: String,
    pub table: String,
    /// Its identity over `alias`, as the search spells a node ([`Search::id`]).
    pub key: String,
    /// `start_id` or `end_id`.
    pub column: &'static str,
}

/// `FROM <relation> AS <alias>` and the ends a condition reads.
fn with_ends(relation: &str, alias: &str, ends: &[PickEnd]) -> String {
    let mut from = format!("{relation} AS {alias}");
    for e in ends {
        from.push_str(&format!(
            "\n        JOIN {} AS {} ON {} = {alias}.{}",
            e.table, e.alias, e.key, e.column
        ));
    }
    from
}

/// The pairs the search of `var` reaches (`vlp_{var}_near`) whose distance
/// fails `conditions` (or leaves them unknown): `start_id`, `end_id`. Only
/// for them is a longer path the shortest that satisfies them.
pub(super) fn failing_pairs(
    var: &str,
    alias: &str,
    ends: &[PickEnd],
    conditions: &[RenderExpr],
) -> String {
    format!(
        "SELECT {alias}.start_id AS start_id, {alias}.end_id AS end_id FROM {} \
         WHERE NOT coalesce({}, false)",
        with_ends(&format!("vlp_{var}_near"), alias, ends),
        conjunction(conditions).unwrap_or_else(|| "true".to_string()),
    )
}

/// The relation of `shortestPath` (`all`: `allShortestPaths`) paths of `var`
/// that satisfy `conditions` (§4.11, #1312), read under `alias`: per
/// `(start_id, end_id)`, one of the shortest (every shortest). The
/// conditions read the path (`alias`) and the `ends`; in S6b they depend on
/// a path only through its length.
/// * A pair whose distance (`vlp_{var}_near`, the search) satisfies them has
///   its shortest paths: no path is shorter.
/// * For the others (`failing_pairs`) the pick is among the trails
///   (`vlp_{var}_path`) that satisfy them, before it: the shortest, or all of
///   the shortest length. With `distinct_ends`, a node is no path's other
///   end.
///
/// With `walked` (the [`Search::walked_columns`], and how the trails spell
/// their relationships), the paths are recovered: the first pairs' are
/// `vlp_{var}_walked` (`walk_ctes`, walked from only them), and the trails
/// carry theirs.
#[allow(clippy::too_many_arguments)]
pub(super) fn pick_cte(
    var: &str,
    trails: &str,
    alias: &str,
    ends: &[PickEnd],
    conditions: &[RenderExpr],
    distinct_ends: bool,
    all: bool,
    walked: Option<(&[&str], TrailEdges)>,
) -> Result<(Cte, String), LowerError> {
    let m = current_function_mapper();
    let Some(spelling) = m.shortest_path_search() else {
        return unsupported("shortestPath in this SQL dialect");
    };
    let name = format!("vlp_{var}_shortest");
    let holds = conjunction(conditions).unwrap_or_else(|| "true".to_string());
    // The recovered paths' columns: a trail's relationships as texts, as the
    // walk has them.
    let mut carried = String::new();
    let mut of_trail = String::new();
    if let Some((columns, trail_edges)) = walked {
        let Some(g) = m.graph_values() else {
            return unsupported("a shortestPath's path in this SQL dialect");
        };
        for c in columns {
            carried.push_str(&format!(", {c}"));
            let value = match (*c, trail_edges) {
                ("path_edges", TrailEdges::Typed(arity)) => {
                    (g.prefixed_texts)("''", &format!("{alias}.{c}"), arity)
                }
                // `*0..0`: no relationship.
                ("path_edges", TrailEdges::None) => (g.texts)(&[]),
                _ => format!("{alias}.{c}"),
            };
            of_trail.push_str(&format!(", {value} AS {c}"));
        }
    }
    let (paths, copies) = match (all, walked) {
        (true, None) => (
            format!(", {alias}.paths AS paths"),
            format!(" ARRAY JOIN {} AS copy", (spelling.copies)("paths")),
        ),
        _ => (String::new(), String::new()),
    };
    let first = match walked {
        Some(_) => format!("SELECT start_id, end_id, hop_count{carried} FROM vlp_{var}_walked"),
        None => format!(
            "SELECT start_id, end_id, hop_count FROM (\n        \
             SELECT {alias}.start_id AS start_id, {alias}.end_id AS end_id, \
             {alias}.hop_count AS hop_count{paths}\n        \
             FROM {near}\n        \
             WHERE {holds}\n    \
             ){copies}",
            near = with_ends(&format!("vlp_{var}_near"), alias, ends),
        ),
    };
    let mut longer = vec![
        holds.clone(),
        format!(
            "({alias}.start_id, {alias}.end_id) IN ({})",
            failing_pairs(var, alias, ends, conditions)
        ),
    ];
    if distinct_ends {
        longer.push(format!("{alias}.start_id <> {alias}.end_id"));
    }
    let (rank, keep) = if all {
        (
            format!("MIN({alias}.hop_count) OVER (PARTITION BY {alias}.start_id, {alias}.end_id)"),
            "hop_count = shortest",
        )
    } else {
        (
            format!(
                "ROW_NUMBER() OVER (PARTITION BY {alias}.start_id, {alias}.end_id \
                 ORDER BY {alias}.hop_count)"
            ),
            "shortest = 1",
        )
    };
    let sql = format!(
        "{name} AS (\n    \
         {first}\n    \
         UNION ALL\n    \
         SELECT start_id, end_id, hop_count{carried} FROM (\n        \
         SELECT {alias}.start_id AS start_id, {alias}.end_id AS end_id, \
         {alias}.hop_count AS hop_count{of_trail}, {rank} AS shortest\n        \
         FROM {trails}\n        \
         WHERE {longer}\n    \
         ) WHERE {keep}\n)",
        trails = with_ends(trails, alias, ends),
        longer = longer.join(" AND "),
    );
    Ok((Cte::new(name.clone(), CteContent::RawSql(sql), false), name))
}

/// One relationship's identity as the generator spells a `path_edges`
/// element: the schema's `edge_id` (a column, or a tuple of columns), else
/// the tuple of its stored endpoints' columns (#887; a composite end's each,
/// S8d).
pub(super) fn edge_identity_sql(edge: &RelationshipSchema, alias: &str) -> String {
    // The identity is always given (`edge_identity`): the endpoints, which
    // spell it only without one, are not read.
    spell_edge_identity(
        current_function_mapper().tuple_constructor(),
        &Some(edge_identity(edge)),
        alias,
        "",
        "",
        |c| c,
    )
}

/// The columns identifying one relationship: its `edge_id`, else its stored
/// `(from, to)` endpoints, in that order whichever way a walk follows it, so
/// every path and hop of a table spells a relationship alike (#887).
/// The number of columns of a relationship's identity ([`edge_identity`]).
pub(super) fn edge_arity(edge: &RelationshipSchema) -> usize {
    edge_identity(edge).columns().len()
}

fn edge_identity(edge: &RelationshipSchema) -> Identifier {
    edge.edge_id.clone().unwrap_or_else(|| {
        Identifier::Composite(
            edge.from_id
                .columns()
                .into_iter()
                .chain(edge.to_id.columns())
                .map(|c| c.to_string())
                .collect(),
        )
    })
}

/// `NOT has(path.path_edges, edge)`: the path does not use the relationship.
pub(super) fn path_avoids_edge(path_alias: &str, edge: &str) -> RenderExpr {
    let has = current_function_mapper().array_contains();
    RenderExpr::Raw(format!("NOT {has}({path_alias}.path_edges, {edge})"))
}

/// `NOT hasAny(a.path_edges, b.path_edges)`: two paths share no relationship.
pub(super) fn paths_disjoint(a: &str, b: &str) -> RenderExpr {
    let overlap = current_function_mapper().arrays_overlap();
    RenderExpr::Raw(format!("NOT {overlap}({a}.path_edges, {b}.path_edges)"))
}

/// What a walk over the definitions of several types or labels needs
/// (`Lowerer::build_union_path`, S7b3a).
pub(super) struct UnionWalk<'a> {
    /// The relationship's binding name (`v{N}`): the relation is
    /// `vlp_v{N}_path`.
    pub var: &'a str,
    /// The CTE of the nodes a path can visit (`Lowerer::walk_nodes`): their
    /// label, id, identity as a text and, with `node_values`, value.
    pub nodes: &'a str,
    /// The CTE of its relationships in the orientations the walk follows
    /// them (`Lowerer::rel_union_of`: the node each row leaves and enters,
    /// its identity as a text and, with `rel_values`, value); none when only
    /// the path of none can match.
    pub rels: Option<&'a str>,
    pub min: u32,
    pub max: Option<u32>,
    /// Conjuncts over the first node, a row of `nodes` aliased [`START`].
    pub start: Vec<RenderExpr>,
    /// Conjuncts over the last node, a row of `nodes` aliased [`END`] (a
    /// shortest-path search's, [`UnionWalk::search`]).
    pub end: Vec<RenderExpr>,
    /// Conjuncts every relationship of the path satisfies, a row of `rels`
    /// aliased [`REL`].
    pub rel: Vec<RenderExpr>,
    pub node_values: bool,
    pub rel_values: bool,
    /// The relation's `start_id` and `end_id` are its ends' identities as
    /// texts (the nodes' `KEY`), as a shortest-path search has them, without
    /// their labels.
    pub keyed: bool,
}

impl UnionWalk<'_> {
    /// The shortest-path search of these paths (S7b3b): over the walk's
    /// nodes and relationships, a node identified by its identity as a text
    /// (`rels` carries those of the nodes its rows leave and enter,
    /// `REL_START_KEY` / `REL_END_KEY`).
    pub(super) fn search(&self) -> Search {
        let at = |alias: &str, column: &str| format!("{alias}.{column}");
        Search {
            // A walk of several labels or types carries no element tuples
            // (they are of one type each).
            node_tuple: None,
            rel_tuple: None,
            var: self.var.to_string(),
            node_table: self.nodes.to_string(),
            id: vec![super::KEY.to_string()],
            edge_table: self.rels.map(str::to_string),
            from_id: vec![super::REL_START_KEY.to_string()],
            to_id: vec![super::REL_END_KEY.to_string()],
            min: self.min,
            max: self.max,
            start: self.start.clone(),
            end: self.end.clone(),
            rel: self.rel.clone(),
            edge_text: Some(at(REL, super::KEY)),
            node_value: self.node_values.then(|| {
                (
                    at(START, super::ELEMENT_VALUE),
                    at(END, super::ELEMENT_VALUE),
                )
            }),
            rel_value: self.rel_values.then(|| at(REL, super::ELEMENT_VALUE)),
        }
    }
}

/// The relation of paths of `w` (§4.11, S7b3a) and its CTEs: the recursive
/// `vlp_{var}_trails`, whose first rows are the paths of none at each first
/// node and whose every step extends each path by a relationship that
/// leaves its last node (the row's start label and id are the node's) to a
/// node there is, not on the path already (a trail); and `vlp_{var}_path`,
/// those of the range. Columns: [`START_LABEL`], `start_id`, [`END_LABEL`],
/// `end_id`, `hop_count`, `path_edges` and `path_nodes` (the identities of
/// its relationships and nodes as texts, as a path's identity spells them),
/// and the values `w` carries. An unbounded range is unbounded, as a walk of
/// one table's is.
pub(super) fn union_path_ctes(w: &UnionWalk<'_>) -> Result<(Vec<Cte>, String), LowerError> {
    let m = current_function_mapper();
    let (Some(spelling), Some(g)) = (m.shortest_path_search(), m.graph_values()) else {
        return unsupported(
            "a variable-length relationship over several labels or types in this SQL dialect",
        );
    };
    let depth = spelling.depth;
    let (label, id, key, value) = (
        super::LABEL_COLUMN,
        super::indexed_column(super::LABEL_ID, 0),
        super::KEY,
        super::ELEMENT_VALUE,
    );
    let trails = format!("vlp_{}_trails", w.var);
    let name = format!("vlp_{}_path", w.var);
    let nodes = w.nodes;
    let mut columns = vec!["path_edges", "path_nodes"];
    // (column, its value at the first node, its value one step on).
    let mut carried: Vec<(&str, String, String)> = vec![
        (
            "path_edges",
            (g.texts)(&[]),
            (g.concat)(&[
                "vp.path_edges".to_string(),
                (g.texts)(&[format!("{REL}.{key}")]),
            ]),
        ),
        (
            "path_nodes",
            (g.texts)(&[format!("{START}.{key}")]),
            (g.concat)(&[
                "vp.path_nodes".to_string(),
                (g.texts)(&[format!("{END}.{key}")]),
            ]),
        ),
    ];
    if w.node_values {
        columns.push(VALUE_COLUMNS[0]);
        carried.push((
            VALUE_COLUMNS[0],
            (g.list)(&[format!("{START}.{value}")]),
            (g.concat)(&[
                format!("vp.{}", VALUE_COLUMNS[0]),
                (g.list)(&[format!("{END}.{value}")]),
            ]),
        ));
    }
    if w.rel_values {
        columns.push(VALUE_COLUMNS[1]);
        carried.push((
            VALUE_COLUMNS[1],
            (g.list)(&[]),
            (g.concat)(&[
                format!("vp.{}", VALUE_COLUMNS[1]),
                (g.list)(&[format!("{REL}.{value}")]),
            ]),
        ));
    }
    let at_first: Vec<String> = carried
        .iter()
        .map(|(c, first, _)| format!("{first} AS {c}"))
        .collect();
    // Keyed, its ends' identities as texts are carried along.
    let (first_keys, step_keys, ends) = match w.keyed {
        true => (
            format!(", {START}.{key} AS start_key, {START}.{key} AS end_key"),
            format!(", vp.start_key AS start_key, {END}.{key} AS end_key"),
            "start_key AS start_id, end_key AS end_id".to_string(),
        ),
        false => (
            String::new(),
            String::new(),
            format!("{START_LABEL}, start_id, {END_LABEL}, end_id"),
        ),
    };
    let mut sql = format!(
        "{trails} AS (\n    \
         SELECT {START}.{label} AS {START_LABEL}, {START}.{id} AS start_id, \
         {START}.{label} AS {END_LABEL}, {START}.{id} AS end_id, \
         CAST(0 AS {depth}) AS hop_count, {}{first_keys}\n    \
         FROM {nodes} AS {START}",
        at_first.join(", ")
    );
    if let Some(c) = conjunction(&w.start) {
        sql.push_str(&format!("\n    WHERE {c}"));
    }
    if let (Some(rels), false) = (w.rels, w.max == Some(0)) {
        let stepped: Vec<String> = carried
            .iter()
            .map(|(c, _, step)| format!("{step} AS {c}"))
            .collect();
        let mut step = Vec::new();
        if let Some(max) = w.max {
            step.push(format!("vp.hop_count < {max}"));
        }
        step.push(format!(
            "NOT {}(vp.path_edges, {REL}.{key})",
            m.array_contains()
        ));
        step.extend(conjunction(&w.rel));
        sql.push_str(&format!(
            "\n    UNION ALL\n    \
             SELECT vp.{START_LABEL} AS {START_LABEL}, vp.start_id AS start_id, \
             {END}.{label} AS {END_LABEL}, {END}.{id} AS end_id, \
             CAST(vp.hop_count + 1 AS {depth}) AS hop_count, {}{step_keys}\n    \
             FROM {trails} AS vp\n    \
             JOIN {rels} AS {REL} ON {REL}.{start_label} = vp.{END_LABEL} \
             AND {REL}.{start_id} = vp.end_id\n    \
             JOIN {nodes} AS {END} ON {END}.{label} = {REL}.{end_label} \
             AND {END}.{id} = {REL}.{end_id}\n    \
             WHERE {}",
            stepped.join(", "),
            step.join("\n      AND "),
            start_label = super::REL_START_LABEL,
            start_id = super::indexed_column(super::BOTH_START, 0),
            end_label = super::REL_END_LABEL,
            end_id = super::indexed_column(super::BOTH_END, 0),
        ));
    }
    sql.push_str("\n)");
    let mut range = String::new();
    if w.min > 0 {
        range = format!("\n    WHERE hop_count >= {}", w.min);
    }
    let path = format!(
        "{name} AS (\n    \
         SELECT {ends}, hop_count, {}\n    \
         FROM {trails}{range}\n)",
        columns.join(", ")
    );
    Ok((
        vec![
            Cte::new(trails, CteContent::RawSql(sql), true),
            Cte::new(name.clone(), CteContent::RawSql(path), false),
        ],
        name,
    ))
}

/// The paths a shortestPath search picks among (`Lowerer::shortest_relation`):
/// of one table's walk, or of a union's (S7b3b, `keyed`).
pub(super) enum Walk<'a> {
    One(PathCall<'a>),
    Union(UnionWalk<'a>),
}

impl Walk<'_> {
    pub(super) fn min(&self) -> u32 {
        match self {
            Walk::One(c) => c.min,
            Walk::Union(w) => w.min,
        }
    }

    pub(super) fn max_mut(&mut self) -> &mut Option<u32> {
        match self {
            Walk::One(c) => &mut c.max,
            Walk::Union(w) => &mut w.max,
        }
    }

    /// Conjuncts over the first node, aliased [`START`].
    pub(super) fn start_mut(&mut self) -> &mut Vec<RenderExpr> {
        match self {
            Walk::One(c) => &mut c.start,
            Walk::Union(w) => &mut w.start,
        }
    }

    pub(super) fn search(&self, schema: &GraphSchema) -> Result<Search, LowerError> {
        match self {
            Walk::One(c) => c.search(schema),
            Walk::Union(w) => Ok(w.search()),
        }
    }

    /// The trails of the range, as the relation `vlp_{var}_path` (its CTEs),
    /// read by a pick as the relation named last (between composite ids,
    /// S8d2: `vlp_{var}_keyed`, its ends keyed as the search keys a node,
    /// [`composite_key`] of the walk's `start_<col>` / `end_<col>`),
    /// and how they spell their relationships.
    pub(super) fn trails(
        self,
        schema: &GraphSchema,
    ) -> Result<(Vec<Cte>, TrailEdges, String), LowerError> {
        match self {
            Walk::One(c) => {
                let var = c.var.to_string();
                let id = c.node.id_physical_columns();
                // The columns a recovered path reads (its nodes as the search
                // keys them: the generator's composite text is the same,
                // `composite_by_columns`, S8d2c).
                let mut carried = vec!["path_nodes"];
                for (wanted, column) in [
                    (c.node_values, VALUE_COLUMNS[0]),
                    (c.rel_values, VALUE_COLUMNS[1]),
                    (c.node_tuples, TUPLE_COLUMNS[0]),
                    (c.rel_tuples, TUPLE_COLUMNS[1]),
                ] {
                    if wanted {
                        carried.push(column);
                    }
                }
                let arity = edge_arity(c.edge);
                let built = path_cte(schema, c)?;
                let edges = match built.edges {
                    true => TrailEdges::Typed(arity),
                    false => TrailEdges::None,
                };
                let path = format!("vlp_{var}_path");
                if id.len() == 1 {
                    return Ok((vec![built.cte], edges, path));
                }
                let side = |s: &str| -> Vec<String> {
                    id.iter().map(|col| format!("{s}_{col}")).collect()
                };
                let (Some(start), Some(end)) =
                    (composite_key(&side("start")), composite_key(&side("end")))
                else {
                    return unsupported("a shortestPath between composite ids in this SQL dialect");
                };
                let keyed = format!("vlp_{var}_keyed");
                if edges != TrailEdges::None {
                    carried.insert(0, "path_edges");
                }
                let sql = format!(
                    "{keyed} AS (\n    \
                     SELECT {start} AS start_id, {end} AS end_id, hop_count, {} \
                     FROM {path}\n)",
                    carried.join(", ")
                );
                Ok((
                    vec![
                        built.cte,
                        Cte::new(keyed.clone(), CteContent::RawSql(sql), false),
                    ],
                    edges,
                    keyed,
                ))
            }
            Walk::Union(w) => {
                let var = w.var.to_string();
                Ok((
                    union_path_ctes(&w)?.0,
                    TrailEdges::Texts,
                    format!("vlp_{var}_path"),
                ))
            }
        }
    }
}

/// The shortest paths of a union (S7b3b), the relation `keyed` whose ends
/// are identities as texts (`UnionWalk::keyed`), as a walk's relation has
/// them (`union_path_ctes`): `vlp_{var}_ends`, with [`START_LABEL`],
/// `start_id`, [`END_LABEL`], `end_id` (each end the row of `nodes` of its
/// identity), `hop_count` and `columns`.
pub(super) fn union_ends_cte(
    var: &str,
    nodes: &str,
    keyed: &str,
    columns: &[&str],
) -> (Cte, String) {
    let name = format!("vlp_{var}_ends");
    let (label, id, key) = (
        super::LABEL_COLUMN,
        super::indexed_column(super::LABEL_ID, 0),
        super::KEY,
    );
    let carried: String = columns.iter().map(|c| format!(", p.{c} AS {c}")).collect();
    let sql = format!(
        "{name} AS (\n    \
         SELECT {START}.{label} AS {START_LABEL}, {START}.{id} AS start_id, \
         {END}.{label} AS {END_LABEL}, {END}.{id} AS end_id, p.hop_count AS hop_count{carried}\n    \
         FROM {keyed} AS p\n    \
         JOIN {nodes} AS {START} ON {START}.{key} = p.start_id\n    \
         JOIN {nodes} AS {END} ON {END}.{key} = p.end_id\n)"
    );
    (Cte::new(name.clone(), CteContent::RawSql(sql), false), name)
}
