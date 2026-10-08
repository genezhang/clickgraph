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
//! refused; composite endpoint components (registered by the legacy caller)
//! do not arise, as a composite identity is refused.
//!
//! The walk is a trail: no relationship twice (the generator's
//! `NOT has(path_edges, …)`), as Cypher's relationship uniqueness requires.
//! An unbounded range (`*`, `*2..`) is unbounded: the walk ends when no trail
//! extends, or the query fails at ClickHouse's recursion limit
//! (`max_recursive_cte_evaluation_depth`, the server's `max_cte_depth`). It
//! is never cut at a depth of its own (the generator's default bound for a
//! missing maximum, which drops longer paths silently).

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
use crate::sql_generator::emitters::clickhouse::variable_length_cte::spell_edge_identity;
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

/// Columns of a path relation that stand for it where an element's identity
/// would (carried through a CTE, tested for NULL). A path is not an element:
/// two paths can agree on all three.
pub(super) const PATH_COLUMNS: [&str; 3] = ["start_id", "end_id", "hop_count"];

/// What one variable-length relationship needs from the generator.
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
    /// Conjuncts over the first node, aliased [`START`].
    pub start: Vec<RenderExpr>,
    /// Conjuncts over the last node, aliased [`END`] (the ends whose
    /// `allShortestPaths` rows are repeated).
    pub end: Vec<RenderExpr>,
    /// Conjuncts every relationship of the path satisfies, aliased [`REL`].
    pub rel: Vec<RenderExpr>,
}

/// The generated relation.
pub(super) struct PathCte {
    pub cte: Cte,
    /// It has a `path_edges` column (a path that can have relationships).
    pub edges: bool,
}

/// The schema context of `call`'s edge and nodes, walked from `from_id` to
/// `to_id` (exchanged when [`PathCall::backward`]): the standard layout
/// (the nodes and the edge each in their own table, single-column
/// identities), or `Unsupported`.
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
    if call.backward {
        // The walk joins `rel.from_id` to the node it is at and moves to
        // `rel.to_id`: with the two exchanged it moves backward.
        if let EdgeAccessStrategy::SeparateTable { from_id, to_id, .. } = &mut ctx.edge {
            std::mem::swap(from_id, to_id);
        }
    }
    let own_table = |n: &NodeAccessStrategy| {
        matches!(
            n,
            NodeAccessStrategy::OwnTable {
                id_column: Identifier::Single(_),
                ..
            }
        )
    };
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
    let result = CteManager::with_context(Arc::new(schema.clone()), context)
        .generate_vlp_cte(&ctx, &[], &filters)
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

/// The relation of `shortestPath` (`all`: `allShortestPaths`) paths of
/// `call` whose WHERE does not read the path (§4.11): a breadth-first search
/// from each first node. Its columns are `start_id`, `end_id`, `hop_count`,
/// one row per path; with `min` 1 a node is no path's other end.
///
/// `vlp_{var}_bfs` holds, for each first node `start_id`, the nodes `node` it
/// reaches and their distance `depth`. ClickHouse gives each step of a
/// recursive CTE only the rows of the step before, so each step carries the
/// nodes reached so far (`new = 0`) along with those reached first at this
/// depth (`new = 1`), and a node already reached is not reached again: the
/// search visits each node once per first node, and ends when a depth adds
/// no node (one step later, when the carried rows stop). A node is reached
/// through a relationship of the type that satisfies its property map, and
/// exists in the node table.
///
/// For `shortestPath` a pair's path is any one of its shortest: one row per
/// `(start_id, end_id)`. For `allShortestPaths` every shortest path is a
/// row: the search counts them (`paths`: a node's count is the sum of the
/// counts of the nodes one level nearer with a relationship to it, once per
/// relationship), and a pair's row is repeated that many times. The paths
/// of a pair differ only in their nodes and relationships, which are not
/// values yet (S6c); the rows of the last node are only those `call.end`
/// allows.
pub(super) fn shortest_ctes(
    schema: &GraphSchema,
    call: PathCall<'_>,
    all: bool,
) -> Result<(Vec<Cte>, String), LowerError> {
    let ctx = standard_layout(schema, &call)?;
    let (
        NodeAccessStrategy::OwnTable {
            table: node_table,
            id_column: Identifier::Single(id),
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
        return unsupported("internal: the standard layout without its tables");
    };
    let Some(spelling) = current_function_mapper().shortest_path_search() else {
        return unsupported("shortestPath in this SQL dialect");
    };
    let (depth, flag, count) = (spelling.depth, spelling.flag, spelling.count);
    let var = call.var;
    let bfs = format!("vlp_{var}_bfs");
    let path = format!("vlp_{var}_path");
    let seed_where = conjunction(&call.start)
        .map(|c| format!("\n    WHERE {c}"))
        .unwrap_or_default();
    let mut step = vec!["f.new = 1".to_string()];
    if let Some(max) = call.max {
        step.push(format!("f.depth < {max}"));
    }
    step.extend(conjunction(&call.rel));
    step.push(format!(
        "(f.start_id, end_node.{id}) NOT IN (SELECT start_id, node FROM {bfs})"
    ));
    // `shortestPath` needs a node once; `allShortestPaths` the number of
    // shortest paths to it, summed over the relationships reaching it.
    let (seed_paths, step_paths, carried_paths, reach) = if all {
        (
            format!(", CAST(1 AS {count}) AS paths"),
            format!(", CAST(sum(f.paths) AS {count}) AS paths"),
            ", paths",
            format!("\n    GROUP BY f.start_id, end_node.{id}, f.depth"),
        )
    } else {
        (String::new(), String::new(), "", String::new())
    };
    let distinct = if all { "" } else { "DISTINCT " };
    let search = format!(
        "{bfs} AS (\n    \
         SELECT DISTINCT start_node.{id} AS start_id, start_node.{id} AS node, \
         CAST(0 AS {depth}) AS depth{seed_paths}, CAST(1 AS {flag}) AS new\n    \
         FROM {node_table} AS start_node{seed_where}\n    \
         UNION ALL\n    \
         SELECT {distinct}f.start_id AS start_id, end_node.{id} AS node, \
         CAST(f.depth + 1 AS {depth}) AS depth{step_paths}, CAST(1 AS {flag}) AS new\n    \
         FROM {bfs} AS f\n    \
         JOIN {edge_table} AS rel ON rel.{from_id} = f.node\n    \
         JOIN {node_table} AS end_node ON end_node.{id} = rel.{to_id}\n    \
         WHERE {step}{reach}\n    \
         UNION ALL\n    \
         SELECT start_id, node, depth{carried_paths}, CAST(0 AS {flag}) AS new\n    \
         FROM {bfs}\n    \
         WHERE start_id IN (SELECT start_id FROM {bfs} WHERE new = 1)\n)",
        step = step.join("\n      AND "),
    );
    let mut reached = format!("new = 1 AND depth >= {}", call.min);
    let mut copies = String::new();
    if all {
        if let Some(c) = conjunction(&call.end) {
            reached.push_str(&format!(
                "\n      AND node IN (SELECT {END}.{id} FROM {node_table} AS {END} WHERE {c})"
            ));
        }
        copies = format!(" ARRAY JOIN {} AS copy", (spelling.copies)("paths"));
    }
    let select = format!(
        "{path} AS (\n    \
         SELECT start_id, node AS end_id, depth AS hop_count FROM {bfs}{copies}\n    \
         WHERE {reached}\n)"
    );
    Ok((
        vec![
            Cte::new(bfs, CteContent::RawSql(search), true),
            Cte::new(path.clone(), CteContent::RawSql(select), false),
        ],
        path,
    ))
}

/// An end node of a path read by a condition of a shortestPath search:
/// joined to the paths under its own alias.
pub(super) struct PickEnd {
    pub alias: String,
    pub table: String,
    pub id: String,
    /// `start_id` or `end_id`.
    pub column: &'static str,
}

/// The relation of `shortestPath` (`all`: `allShortestPaths`) paths among the
/// paths of `var` (its CTE `vlp_{var}_path` of every path, a trail, read
/// under `alias`) that satisfy `conditions`: per `(start_id, end_id)`, one of the shortest
/// (every shortest). The conditions read the path (`alias`) and the `ends`,
/// and are evaluated before the pick, so a pair whose shortest path fails
/// them has its shortest path that satisfies them (#1312). With
/// `distinct_ends`, a node is no path's other end.
pub(super) fn pick_cte(
    var: &str,
    alias: &str,
    ends: &[PickEnd],
    conditions: &[RenderExpr],
    distinct_ends: bool,
    all: bool,
) -> (Cte, String) {
    let name = format!("vlp_{var}_shortest");
    let mut from = format!("vlp_{var}_path AS {alias}");
    for e in ends {
        from.push_str(&format!(
            "\n        JOIN {} AS {} ON {}.{} = {alias}.{}",
            e.table, e.alias, e.alias, e.id, e.column
        ));
    }
    let mut conds: Vec<String> = conjunction(conditions).into_iter().collect();
    if distinct_ends {
        conds.push(format!("{alias}.start_id <> {alias}.end_id"));
    }
    let where_ = if conds.is_empty() {
        String::new()
    } else {
        format!("\n        WHERE {}", conds.join(" AND "))
    };
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
         SELECT start_id, end_id, hop_count FROM (\n        \
         SELECT {alias}.start_id AS start_id, {alias}.end_id AS end_id, \
         {alias}.hop_count AS hop_count, {rank} AS shortest\n        \
         FROM {from}{where_}\n    \
         ) WHERE {keep}\n)"
    );
    (Cte::new(name.clone(), CteContent::RawSql(sql), false), name)
}

/// One relationship's identity as the generator spells a `path_edges`
/// element: the schema's `edge_id` (a column, or a tuple of columns), else
/// the tuple of its stored endpoints (#887).
pub(super) fn edge_identity_sql(edge: &RelationshipSchema, alias: &str) -> Option<String> {
    let (Identifier::Single(from), Identifier::Single(to)) = (&edge.from_id, &edge.to_id) else {
        return None;
    };
    Some(spell_edge_identity(
        current_function_mapper().tuple_constructor(),
        &Some(edge_identity(edge)),
        alias,
        from,
        to,
        |c| c,
    ))
}

/// The columns identifying one relationship: its `edge_id`, else its stored
/// `(from, to)` endpoints, in that order whichever way a walk follows it, so
/// every path and hop of a table spells a relationship alike (#887).
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
