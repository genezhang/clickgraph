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
//! * `start_id`, `end_id`: the identities of the path's first and last node
//!   in the stored orientation (the walk follows `from` → `to`);
//! * `hop_count`: the number of relationships;
//! * `path_edges`: the identities of its relationships, spelled as
//!   [`edge_identity_sql`] spells one (when [`PathCte::edges`]);
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
    /// Conjuncts every relationship of the path satisfies, aliased [`REL`].
    pub rel: Vec<RenderExpr>,
}

/// The generated relation.
pub(super) struct PathCte {
    pub cte: Cte,
    /// It has a `path_edges` column (a path that can have relationships).
    pub edges: bool,
}

/// Build the relation of paths of `call` (the standard layout: the nodes and
/// the edge each in their own table, single-column identities).
pub(super) fn path_cte(schema: &GraphSchema, call: PathCall<'_>) -> Result<PathCte, LowerError> {
    let start_alias = call.var.to_string();
    let end_alias = "path".to_string();
    let ctx = PatternSchemaContext::analyze(
        &start_alias,
        &end_alias,
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
    let mut ctx = ctx;
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
    let conjunction = |cs: &[RenderExpr]| {
        (!cs.is_empty()).then(|| {
            cs.iter()
                .map(|c| format!("({})", render_expr_to_sql_plain(c)))
                .collect::<Vec<_>>()
                .join(" AND ")
        })
    };
    let mut context = CteGenerationContext::new()
        .with_spec(VariableLengthSpec {
            min_hops: Some(call.min),
            max_hops: Some(call.max.unwrap_or(UNBOUNDED)),
        })
        .with_schema_owned(schema.clone())
        .with_relationship_types(Some(vec![call.rel_type.to_string()]))
        .with_edge_id(call.edge.edge_id.clone())
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

/// One relationship's identity as the generator spells a `path_edges`
/// element: the schema's `edge_id` (a column, or a tuple of columns), else
/// the tuple of its endpoints, in the walk's order (#887).
pub(super) fn edge_identity_sql(
    edge: &RelationshipSchema,
    alias: &str,
    backward: bool,
) -> Option<String> {
    let (Identifier::Single(from), Identifier::Single(to)) = (&edge.from_id, &edge.to_id) else {
        return None;
    };
    let (from, to) = if backward { (to, from) } else { (from, to) };
    Some(spell_edge_identity(
        current_function_mapper().tuple_constructor(),
        &edge.edge_id,
        alias,
        from,
        to,
        |c| c,
    ))
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
