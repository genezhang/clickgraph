//! Filter Builder Module
//!
//! This module handles extraction of filter expressions from logical plans.
//! It processes WHERE clauses, HAVING clauses, and other filter conditions
//! that need to be applied to the generated SQL queries.

use crate::graph_catalog::config::Identifier;
use crate::query_planner::logical_expr::expression_rewriter::{
    rewrite_expression_with_property_mapping, ExpressionRewriteContext,
};
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::cte_extraction::{
    extract_relationship_columns, table_to_id_column, RelationshipColumns,
};
use crate::render_plan::errors::RenderBuildError;
// P2.10 import hygiene: named imports (the exact set the compiler requires) so
// shadowing becomes visible; previously a `plan_builder_helpers::*` glob.
use crate::render_plan::plan_builder_helpers::{
    apply_property_mapping_to_expr, collect_graphrel_predicates, collect_schema_filters,
    collect_schema_filters_with_alias, extract_id_column, extract_table_name, is_node_denormalized,
};
use crate::render_plan::render_expr::{Operator, OperatorApplication, RenderExpr};

/// Result type for filter builder operations
pub type FilterBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting filter expressions from logical plans
pub trait FilterBuilder {
    /// Extract filters from WHERE clauses and other filter conditions
    fn extract_filters(&self) -> FilterBuilderResult<Option<RenderExpr>>;

    /// Extract DISTINCT flag from projection nodes
    fn extract_distinct(&self) -> bool;
}

impl FilterBuilder for LogicalPlan {
    fn extract_filters(&self) -> FilterBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => {
                // For node-only queries, extract both view_filter and schema_filter from the input ViewScan
                if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                    log::info!(
                        "🔍 GraphNode '{}' extract_filters: ViewScan table={}",
                        graph_node.alias,
                        scan.source_table
                    );

                    let mut filters = Vec::new();

                    // Extract view_filter (user's WHERE clause, injected by optimizer)
                    if let Some(ref view_filter) = scan.view_filter {
                        log::debug!(
                            "extract_filters: view_filter BEFORE rewrite: {:?}",
                            view_filter
                        );

                        // 🔧 FIX: Rewrite property names to DB column names BEFORE converting to RenderExpr
                        // This uses the same function as WITH clause processing for consistency.
                        // #600.2: build the rewrite context from the GraphNode (`self`), NOT
                        // `graph_node.input` (the bare ViewScan). `find_label_for_alias_in_plan`
                        // returns None for a bare ViewScan (it has no alias field), so the
                        // context lost the alias→label binding and the inline-map property
                        // (`a.name`) was left unmapped. Passing the GraphNode lets the label
                        // (`User`) resolve so `name` → `full_name`, mirroring the working
                        // explicit-WHERE path (`Filter::to_render_plan` passes its GraphNode child).
                        let rewrite_ctx = ExpressionRewriteContext::new(self);
                        let rewritten_filter =
                            rewrite_expression_with_property_mapping(view_filter, &rewrite_ctx);

                        log::debug!(
                            "extract_filters: view_filter AFTER rewrite: {:?}",
                            rewritten_filter
                        );

                        let expr: RenderExpr = rewritten_filter.try_into()?;
                        log::debug!("extract_filters: view_filter AFTER conversion: {:?}", expr);
                        log::info!(
                            "GraphNode '{}': Adding view_filter: {:?}",
                            graph_node.alias,
                            expr
                        );
                        filters.push(expr);
                    }

                    // Extract schema_filter (from YAML schema)
                    // Wrap in parentheses to ensure correct operator precedence when combined with user filters
                    if let Some(ref schema_filter) = scan.schema_filter {
                        if let Ok(sql) = schema_filter.to_sql(&graph_node.alias) {
                            log::info!(
                                "GraphNode '{}': Adding schema filter: {}",
                                graph_node.alias,
                                sql
                            );
                            // Always wrap schema filter in parentheses for safe combination
                            filters.push(RenderExpr::Raw(format!("({})", sql)));
                        }
                    }

                    // Combine filters with AND if multiple
                    // Use explicit AND combination - each operand will be wrapped appropriately
                    if filters.is_empty() {
                        return Ok(None);
                    } else if filters.len() == 1 {
                        // Safety: len() == 1 guarantees next() returns Some
                        return Ok(Some(
                            filters
                                .into_iter()
                                .next()
                                .expect("filters has exactly one element"),
                        ));
                    } else {
                        // When combining filters, wrap non-Raw expressions in parentheses
                        // to handle AND/OR precedence correctly
                        let combined = filters
                            .into_iter()
                            .reduce(|acc, pred| {
                                // The OperatorApplicationExp will render as "(left) AND (right)"
                                // due to the render_expr_to_sql_string logic
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, pred],
                                })
                            })
                            .expect("filters is non-empty, reduce succeeds");
                        return Ok(Some(combined));
                    }
                }
                None
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!(
                    "GraphRel node detected, collecting filters from ALL nested where_predicates"
                );

                // 🔧 VLP FILTER FIX (Feb 17, 2026): Different handling based on whether CTE is used
                //
                // VLP queries have two execution paths:
                // 1. Fixed-length (*N where N is exact): Uses chained JOINs, NO CTE
                //    → where_predicate must be extracted as outer WHERE clause
                // 2. Variable-length (*1..N, *, etc.) or shortest path: Uses CTE
                //    → where_predicate is handled inside CTE, don't duplicate
                //
                // Example: MATCH (a)-[*2]->(b) WHERE b.name = 'Diana'
                //   - Uses chained JOINs: a → r1 → r2 → b
                //   - WHERE b.name = 'Diana' must be in outer query

                // #983: a CLOSED single-hop relationship — `(a)-[:R]->(a)` (and
                // its `*1`/`*1..1` spelling, which is stripped to
                // `variable_length: None` for a single-type edge in
                // `match_clause/helpers.rs`) — matches only SELF-LOOP edges
                // (`from_id == to_id`). The self-loop constraint used to survive
                // only IMPLICITLY, through the two node-join ON clauses
                // (`t1.from = a.id AND a.id = t1.to` ⇒ `from = to` transitively).
                // But a bare `RETURN count(*)` elides both (unreferenced) node
                // joins (plan_optimizer.rs), leaving `FROM <edge> AS t1` with the
                // constraint GONE → it silently counts ALL edges, not just
                // self-loops (returned 6 where 0 is correct). Compute the explicit
                // self-loop equality on the edge alias here and inject it into
                // `all_predicates` below (NOT an early return — the normal path's
                // schema-filter / OPTIONAL null-safe collection must still run).
                // The `left == right` gate is exact and rename-safe (the planner
                // never renames one endpoint of a same-variable pattern — see
                // from_builder.rs #605/#625), so the OPEN single hop
                // `(a)-[:R]->(b)` (distinct connections) is untouched. Applies to
                // the STANDARD separate-edge-table schema AND the DENORMALIZED
                // schema (#987 facet 2): for a denorm edge the endpoint columns
                // (`from_id`/`to_id`, e.g. `Origin`/`Dest`) live on the single
                // edge=node scan, so `alias.from_id = alias.to_id` is exactly the
                // self-loop constraint there too (`t1.Origin = t1.Dest`) — the
                // denorm closed single-hop otherwise bare-counts ALL edges (silent
                // over-count: 10 flights returned where only 1 is a self-loop).
                // FK-EDGE is still EXCLUDED: its "edge" is a node table whose
                // endpoint is a FK column, so the self-loop test is `node_id ==
                // fk_col`, not `from_id == to_id` — a different constraint tracked
                // as a separate #987 facet. Longer closed VLPs (`*2..2` etc.) keep
                // their spec and route through the recursive CTE's own `start_id =
                // end_id` (#625), so the `variable_length.is_none()` guard excludes
                // them. OPTIONAL is excluded too: an OPTIONAL closed single-hop is
                // already broken on main (Code 179 duplicate anchor alias with a
                // property, anchor dropped for bare count), and a self-loop
                // equality in the outer WHERE would filter out the NULL-extended
                // anchor rows — leave it on its current (unchanged) path rather
                // than risk a new OPTIONAL-semantics violation.
                let closed_single_hop_self_loop: Option<RenderExpr> = if graph_rel
                    .variable_length
                    .is_none()
                    && graph_rel.shortest_path_mode.is_none()
                    && graph_rel.left_connection == graph_rel.right_connection
                    && !graph_rel.is_optional.unwrap_or(false)
                    && !crate::render_plan::cte_extraction::vlp_relationship_is_foreign_key_edge(
                        graph_rel,
                    ) {
                    crate::render_plan::cte_extraction::extract_relationship_columns(
                        &graph_rel.center,
                    )
                    .map(|rel_cols| {
                        // Use `Identifier::to_sql_equality` so a COMPOSITE key
                        // expands to a per-column AND chain
                        // (`t1.from_a = t1.to_a AND t1.from_b = t1.to_b`) with
                        // proper identifier quoting — a bare `format!` would emit
                        // the invalid `t1.from_a, from_b = t1.to_a, to_b` (the
                        // `Identifier` Display comma-joins). Same alias on both
                        // sides (the single edge scan).
                        let self_loop = rel_cols.from_id.to_sql_equality(
                            &graph_rel.alias,
                            &rel_cols.to_id,
                            &graph_rel.alias,
                        );
                        log::info!(
                            "🔧 #983: closed single-hop self-loop constraint — emitting {}",
                            self_loop
                        );
                        RenderExpr::Raw(self_loop)
                    })
                } else {
                    None
                };

                if graph_rel.variable_length.is_some() || graph_rel.shortest_path_mode.is_some() {
                    // Check if this uses chained JOINs (fixed-length, no CTE)
                    let uses_cte = if let Some(ref spec) = graph_rel.variable_length {
                        // Fixed-length without shortest path uses chained JOINs...
                        // EXCEPT: denormalized schemas always use recursive CTE.
                        let is_denorm_vlp = matches!(
                            graph_rel.left.as_ref(),
                            crate::query_planner::logical_plan::LogicalPlan::GraphNode(n)
                                if crate::graph_catalog::pattern_schema::node_denormalized_flag(n)
                        ) && matches!(
                            graph_rel.right.as_ref(),
                            crate::query_planner::logical_plan::LogicalPlan::GraphNode(n)
                                if crate::graph_catalog::pattern_schema::node_denormalized_flag(n)
                        );
                        let is_fixed_length = spec.exact_hop_count().is_some()
                            && graph_rel.shortest_path_mode.is_none()
                            && !is_denorm_vlp
                            // #603: directed OPTIONAL exact VLP uses recursive CTE
                            && !crate::render_plan::from_builder::optional_directed_exact_vlp_uses_cte(
                                graph_rel,
                            )
                            // #623: exact VLP adjacent to another hop uses recursive CTE
                            && !crate::render_plan::from_builder::adjacent_exact_vlp_uses_cte(
                                graph_rel,
                            )
                            // #605: closed exact VLP (a)-[*N..N]->(a) uses recursive CTE
                            && !crate::render_plan::from_builder::closed_exact_vlp_uses_cte(
                                graph_rel,
                            );
                        !is_fixed_length // CTE used if NOT fixed-length
                    } else {
                        // Shortest path always uses CTE
                        true
                    };

                    if uses_cte {
                        // CTE handles its own VLP path filters internally.
                        // For OPTIONAL VLP, anchor node filters (from the base MATCH WHERE)
                        // must still appear in the outer WHERE clause since the anchor node
                        // is in the FROM clause, not in the CTE.
                        // Fall through to collect_graphrel_predicates which correctly:
                        // - Skips VLP GraphRel's own where_predicate (already in CTE)
                        // - Recurses into children to find anchor node filters
                        if graph_rel.is_optional.unwrap_or(false) {
                            log::info!(
                                "🔧 OPTIONAL VLP: Falling through to collect child filters for outer WHERE"
                            );
                            // Fall through to collect_graphrel_predicates below
                        } else {
                            // #625: a CLOSED VLP pattern pins both endpoints to
                            // the SAME variable (`(a)-[*min..max]-(a)` /
                            // `(a)-[*min..max]->(a)`), so the path must return to
                            // its start. The recursive CTE enumerates ALL paths
                            // (its `start_id`/`end_id` are independent), and the
                            // outer query is otherwise unconstrained — without an
                            // explicit `t.start_id = t.end_id` it counts every
                            // path, not just cycles (silent wrong results: 84 vs
                            // 12 undirected, 29 vs 6 directed). Detect the closed
                            // pattern directly from the two connection aliases
                            // (direction-agnostic; the planner leaves them equal
                            // for a same-variable pattern, never renaming one) and
                            // emit the equality on the CTE's own start/end
                            // columns. Single AND composite ids are both covered:
                            // the CTE collapses a composite key into one
                            // pipe-joined `concat(...)` string column, so string
                            // equality of `start_id`/`end_id` == per-column
                            // equality. Exact-bound closed patterns take other
                            // paths (directed `(a)-[*N]->(a)` is #605, undirected
                            // exact errors); this range branch is the recursive
                            // CTE only.
                            if graph_rel.left_connection == graph_rel.right_connection {
                                use crate::query_planner::join_context::{
                                    VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
                                };
                                // #628: a closed `*0..N` on a STANDARD schema is now
                                // supported. The recursive CTE switches to
                                // EDGE-uniqueness for closed patterns
                                // (`uses_edge_uniqueness()` in variable_length_cte.rs
                                // now returns true when `min_hops == 0 &&
                                // is_closed_pattern()`), seeding an empty
                                // `path_edges` at the zero-hop base and deduping
                                // edges from hop 1 — so real cycles survive and the
                                // outer `start_id = end_id` selects them (plus the
                                // zero-length self rows). This replaces the old
                                // loud #625 `*0..N` error (node-uniqueness could not
                                // return to the start, so cycles were dropped);
                                // lower-bound ≥ 1 closed VLPs already used
                                // edge-uniqueness and are unchanged. shortestPath is
                                // unaffected (it stays node-unique and its
                                // zero-length self path is the correct shortest
                                // cycle).
                                //
                                // Two schema shapes are NOT covered and stay loud for
                                // the closed `*0..N` case (ground rule 1 — fail loud
                                // over silent-wrong):
                                //   - DENORMALIZED (guard just below, all lower
                                //     bounds): the generator has no separate node
                                //     identity to seed edge-uniqueness, so a cycle
                                //     count would be silently 0.
                                //   - FK-EDGE (guard below, `min_hops == 0` only):
                                //     the FK-edge VLP recursive arm has a pre-existing
                                //     degenerate-join bug for the FK-on-`to_id`
                                //     self-ref convention (e.g. ldbc `REPLY_OF`:
                                //     node_id=commentId, to_id=replyOfCommentId),
                                //     joining node_id to itself and producing phantom
                                //     `(n,n)` self-loops → a silent OVER-count. That
                                //     traversal bug is tracked separately (#902) and
                                //     is independent of #628 (it corrupts `*1..N`
                                //     identically). Because pre-#628 the closed
                                //     `*0..N` case failed loud for FK-edge too,
                                //     keeping it loud here is a no-op for the working
                                //     FK-on-`from_id` convention and prevents the
                                //     loud→silent regression for the FK-on-`to_id`
                                //     one. FK-edge `*1..N` closed is untouched (it
                                //     already rendered pre-#628, with the same #902
                                //     bug — not this fix's concern).
                                // #605/#625/#980 denormalized closed guard:
                                // HISTORICALLY a DENORMALIZED closed VLP with
                                // LOWER BOUND 0 and a traversable upper bound
                                // (`*0..N`, N >= 1) silently undercounted — its
                                // zero-hop recursive CTE enforced
                                // NODE-uniqueness because the zero-length base
                                // had no edge to seed edge-uniqueness, and
                                // node-uniqueness forbids a walk from returning
                                // to its start, so real cycles were dropped and
                                // the closed `start_id = end_id` count collapsed
                                // to the zero-length self rows. It failed loud
                                // (ground rule 1).
                                //
                                // #980 CORRECTION (superseded by #887 Phase 2b):
                                // this guard formerly fired for ALL lower bounds
                                // on the (once-true) premise that the denorm CTE
                                // is node-unique everywhere. That premise went
                                // STALE with #606/#710, which switched
                                // `DenormalizedCteStrategy` to EDGE-uniqueness
                                // for `effective_min_hops() >= 1` — a walk CAN
                                // then revisit nodes and return to its start, so
                                // a closed denorm `*1..`/`*2..2` counts cycles
                                // CORRECTLY and MUST render (live-verified:
                                // `*2..2` → 2, `*1..` → 5, both matching a hand
                                // oracle). Only `*0..N` with N >= 1 stayed
                                // node-unique and broken. `*0..0` was exempt
                                // too: it can only be the zero-length self path
                                // (no edge traversable), correctly 1/node.
                                //
                                // #887 Phase 2b RESOLUTION: `*0..N` now counts
                                // cycles too — `DenormalizedCteStrategy::uses_edge_uniqueness`
                                // returns true for a closed zero-hop pattern
                                // (mirror of the standard #628 fix), the zero-hop
                                // base seeds a TYPED-empty `path_edges` (a bare
                                // `[]` = Array(Nothing) FAILS ClickHouse
                                // recursive-CTE column unification against the
                                // recursive arm's Array(Tuple(...)) — #887 live
                                // proof — so the seed is a typed-empty slice of a
                                // real edge-identity tuple, `typed_empty_edges_seed`),
                                // and the recursive cycle check switches to
                                // `NOT has(vp.path_edges, edge_tuple("next"))`.
                                // The guard below is DELETED (both guards — the
                                // analyzer-side optional sibling #978 in
                                // graph_join/inference.rs and this render-side
                                // non-optional one — are lifted in the same
                                // change); a closed denorm `*0..N` now renders
                                // with edge-uniqueness and the shared
                                // `start_id = end_id` emission below. The FK-edge
                                // guard just below keeps its own `closed_min_hops
                                // == 0` gate (#902 — a separate degenerate-join
                                // defect, untouched).
                                let closed_min_hops = graph_rel
                                    .variable_length
                                    .as_ref()
                                    .map(|s| s.effective_min_hops())
                                    .unwrap_or(1);
                                // #628/#902 FK-edge closed `*0..N` guard: the
                                // zero-hop-enabled closed path must NOT render for a
                                // self-referencing FK-edge — its VLP recursive arm has
                                // a pre-existing degenerate-join bug (#902) that
                                // yields phantom `(n,n)` self-loops (silent
                                // over-count). Scoped to `min_hops == 0` (the case
                                // #628 newly enables): `*1..N` FK-edge closed already
                                // rendered before #628 and is left as-is. Routes
                                // through the canonical schema-catalog dispatch
                                // predicate (axis-dispatch rule), not an inline
                                // schema-flag read. (`closed_min_hops` is computed
                                // above for the #980 denorm guard.)
                                if closed_min_hops == 0
                                    && crate::render_plan::cte_extraction::vlp_relationship_is_foreign_key_edge(
                                        graph_rel,
                                    )
                                    && graph_rel.shortest_path_mode.is_none()
                                {
                                    return Err(RenderBuildError::UnsupportedFeature(format!(
                                        "closed variable-length path with lower bound 0 on a \
                                         self-referencing FK-edge schema \
                                         (`({a})-[*0..N]->({a})`): the FK-edge VLP recursive \
                                         join has a pre-existing degenerate-join defect (#902) \
                                         that would silently over-count via phantom self-loops. \
                                         Use a lower bound >= 1, or a standard (edge-table) \
                                         schema, to count cycles. (#628/#902)",
                                        a = graph_rel.left_connection
                                    )));
                                }
                                log::info!(
                                    "🔧 #625: closed VLP pattern ({} == {}) — emitting start_id = end_id",
                                    graph_rel.left_connection,
                                    graph_rel.right_connection
                                );
                                return Ok(Some(RenderExpr::Raw(format!(
                                    "{a}.{s} = {a}.{e}",
                                    a = VLP_CTE_FROM_ALIAS,
                                    s = VLP_START_ID_COLUMN,
                                    e = VLP_END_ID_COLUMN,
                                ))));
                            }
                            log::info!(
                                "🔧 Required VLP with CTE: Filters already in CTE, skipping outer WHERE extraction"
                            );
                            return Ok(None);
                        }
                    } else {
                        // Fixed-length VLP.
                        //
                        // Multi-type VLP (labels.len() > 1) does NOT actually use
                        // the flat r1..rN self-join — despite exact_hop_count being
                        // Some it renders as a `vlp_multi_type_*` CTE, whose outer
                        // WHERE must reference the CTE's own columns. Keep the
                        // original early return for it: falling through would
                        // property-map the predicate onto base-table aliases that
                        // aren't in the outer scope and would inject a uniqueness
                        // guard with the wrong (r{i}) aliases/columns.
                        let is_multi_type = graph_rel
                            .labels
                            .as_ref()
                            .map(|l| l.len() > 1)
                            .unwrap_or(false);
                        if is_multi_type {
                            if let Some(ref predicate) = graph_rel.where_predicate {
                                if let Ok(expr) = RenderExpr::try_from(predicate.clone()) {
                                    return Ok(Some(expr));
                                }
                            }
                        }
                        // Single-type flat-join VLP: DO NOT early-return (issue
                        // #598). The previous early return skipped BOTH the
                        // relationship-uniqueness block (~269) and the
                        // all_predicates AND-combine (~331), so a user WHERE
                        // silently dropped the uniqueness guard entirely.
                        // collect_graphrel_predicates (below) already collects a
                        // single-type fixed-length VLP GraphRel's own
                        // where_predicate, so we fall through to normal GraphRel
                        // processing: the predicate lands in all_predicates
                        // EXACTLY ONCE and gets AND-ed with the uniqueness guard.
                        // Do NOT push it again here or it would be duplicated.
                    }
                }

                // PatternResolver 2.0: pattern_combinations generates a UNION CTE
                // (pattern_union_{alias}). The outer WHERE must reference CTE columns
                // (start_id, start_type, end_id, end_type) instead of base table aliases.
                if graph_rel.pattern_combinations.is_some() {
                    let cte_alias = &graph_rel.alias;
                    let mut conjuncts: Vec<String> = Vec::new();

                    if let Some(ref predicate) = graph_rel.where_predicate {
                        let left = &graph_rel.left_connection;
                        let right = &graph_rel.right_connection;
                        let rewritten =
                            rewrite_predicate_for_pattern_cte(predicate, cte_alias, left, right)?;
                        if !rewritten.is_empty() {
                            log::info!(
                                "🔀 PatternResolver 2.0: Rewritten {} WHERE predicate(s) for CTE columns",
                                rewritten.len()
                            );
                        }
                        conjuncts.extend(rewritten);
                    }

                    // #987: a CLOSED unlabeled single-hop `(a)-[r]->(a)` (same
                    // variable on both endpoints, no node label, no rel type) fans
                    // out to a `pattern_union_{alias}` CTE (a UNION ALL over every
                    // edge type). That CTE enumerates ALL edges of every type, so
                    // without a self-loop constraint the outer query counts every
                    // edge (returned 26 where 0 self-loops is correct) — the exact
                    // silent over-count #983 fixed for the LABELED/single-type path,
                    // but that fix lives after this early return and never runs here.
                    // The CTE already projects `start_id`/`end_id` (stringified
                    // endpoint ids) and `start_type`/`end_type` (label literals), so
                    // a self-loop is `start_id = end_id AND start_type = end_type`.
                    // BOTH conjuncts are required: `start_id = end_id` alone would
                    // spuriously match a cross-type id collision (e.g. User#5 and
                    // Post#5 in a `(a)-[:AUTHORED]->` branch); the type equality
                    // pins both endpoints to the same node type, and for a closed
                    // pattern `a` is ONE node so its type and id are necessarily
                    // equal on both sides. The `left_connection == right_connection`
                    // gate is exact and rename-safe (the planner never renames one
                    // endpoint of a same-variable pattern — see #983/#625), so the
                    // OPEN unlabeled hop `(a)-[r]->(b)` (distinct connections) is
                    // untouched. OPTIONAL is excluded: an OPTIONAL `(a)-[r]->(a)`
                    // takes the `vlp_multi_type_*` path (already-bound anchor), not
                    // this one, and a self-loop equality in the outer WHERE would
                    // drop NULL-extended anchor rows — leave it on its current path.
                    //
                    // The `variable_length.is_none()` / `shortest_path_mode.is_none()`
                    // conjuncts are defensive: a fully-unlabeled relationship collapses
                    // to this single-hop `pattern_union` with `variable_length` hard-set
                    // to `None` (traversal.rs), DROPPING the original spec — so a
                    // CLOSED unlabeled VLP `(a)-[r*..]->(a)` would otherwise reach here
                    // looking like a single hop and get a self-loop count that is NOT
                    // the multi-hop cycle count. That shape is now rejected LOUDLY at
                    // the collapse site (traversal.rs, #987), so it never arrives here;
                    // these two conjuncts stay as belt-and-suspenders. shortestPath
                    // keeps its `shortest_path_mode` through the collapse and is
                    // excluded here too.
                    //
                    // GATED on `is_pattern_union_in_scope`: a `pattern_combinations`
                    // GraphRel does NOT always render a `pattern_union_{alias}` CTE
                    // as its FROM — some schema shapes (e.g. polymorphic) collapse
                    // the closed pattern to a bare `SELECT count(*)` with no FROM /
                    // no CTE at all (a separate pre-existing defect, #1024). Emitting
                    // the `r.start_id = r.end_id` conjunct there would reference
                    // columns of a CTE that isn't in scope → a loud Code 47. Only
                    // inject when from_builder has registered the CTE as the FROM
                    // (the same guard the node-property rewrite below uses), so this
                    // is strictly additive on the exact plan shape that owns those
                    // CTE columns.
                    let pattern_cte_name = format!("pattern_union_{cte_alias}");
                    if graph_rel.left_connection == graph_rel.right_connection
                        && graph_rel.variable_length.is_none()
                        && graph_rel.shortest_path_mode.is_none()
                        && !graph_rel.is_optional.unwrap_or(false)
                        && crate::server::query_context::is_pattern_union_in_scope(
                            &pattern_cte_name,
                        )
                    {
                        let self_loop = format!(
                            "{a}.start_id = {a}.end_id AND {a}.start_type = {a}.end_type",
                            a = cte_alias
                        );
                        log::info!(
                            "🔧 #987: closed unlabeled single-hop over pattern_union — emitting {}",
                            self_loop
                        );
                        conjuncts.push(self_loop);
                    }

                    if !conjuncts.is_empty() {
                        return Ok(Some(RenderExpr::Raw(conjuncts.join(" AND "))));
                    }
                    log::info!(
                        "🔀 PatternResolver 2.0: No outer WHERE predicates for pattern_combinations"
                    );
                    return Ok(None);
                }
                let all_predicates =
                    collect_graphrel_predicates(&LogicalPlan::GraphRel(graph_rel.clone()));

                // Strip labels() predicates — they're redundant for non-CTE paths
                // (label is already constrained by the MATCH pattern)
                let mut all_predicates: Vec<_> = all_predicates
                    .into_iter()
                    .filter(|p| !is_labels_predicate(p))
                    .collect();

                // #983: inject the closed single-hop self-loop equality (computed
                // above) into the normal predicate flow, so it is AND-ed with the
                // anchor WHERE, schema filters, and OPTIONAL null-safe filters
                // collected below (rather than short-circuiting past them).
                if let Some(self_loop) = closed_single_hop_self_loop {
                    all_predicates.push(self_loop);
                }

                // 🔒 Add schema-level filters from ViewScans
                // For OPTIONAL MATCH (LEFT JOIN), wrap filters with NULL-safety
                // to avoid filtering out unmatched rows
                if graph_rel.is_optional.unwrap_or(false) {
                    let filters_with_alias = collect_schema_filters_with_alias(
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                        None,
                    );
                    if !filters_with_alias.is_empty() {
                        log::info!(
                            "Adding {} NULL-safe schema filter(s) for OPTIONAL MATCH",
                            filters_with_alias.len()
                        );
                        for (filter_expr, alias, id_col) in filters_with_alias {
                            // Wrap: (original_filter OR alias.id_col IS NULL)
                            let null_safe =
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::Or,
                                    operands: vec![
                                        filter_expr,
                                        RenderExpr::Raw(format!("{}.{} IS NULL", alias, id_col)),
                                    ],
                                });
                            all_predicates.push(null_safe);
                        }
                    }
                } else {
                    let schema_filters =
                        collect_schema_filters(&LogicalPlan::GraphRel(graph_rel.clone()), None);
                    if !schema_filters.is_empty() {
                        log::info!(
                            "Adding {} schema filter(s) to WHERE clause",
                            schema_filters.len()
                        );
                        all_predicates.extend(schema_filters);
                    }
                }

                // TODO: Add relationship uniqueness filters for undirected multi-hop patterns
                // This requires fixing Issue #1 (Undirected Multi-Hop Patterns Generate Broken SQL) first.
                // See KNOWN_ISSUES.md for details.
                // Currently, undirected multi-hop patterns generate broken SQL with wrong aliases,
                // so adding uniqueness filters here would not work correctly.

                // 🚀 ADD CYCLE PREVENTION for fixed-length paths (only for 2+ hops)
                // Single hop (*1) can't have cycles - no need for cycle prevention
                if let Some(spec) = &graph_rel.variable_length {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        // Skip cycle prevention for *1 - single hop can't cycle.
                        // #603: a DIRECTED OPTIONAL exact VLP now renders as a
                        // recursive CTE (uniqueness lives inside the CTE); the
                        // flat-join r1..rN aliases this guard references don't
                        // exist there, so skip it. Undirected optional exact
                        // stays on the flat join and still needs the guard.
                        if exact_hops >= 2
                            && graph_rel.shortest_path_mode.is_none()
                            && !crate::render_plan::from_builder::optional_directed_exact_vlp_uses_cte(
                                graph_rel,
                            )
                            // #605: a CLOSED exact VLP `(a)-[*N..N]->(a)` reroutes
                            // to the recursive CTE (both endpoints are the same
                            // variable, so this flat-path cycle-prevention's
                            // `extract_table_name` on the two endpoints fails
                            // loud). Skip the flat guard; the CTE's own edge
                            // uniqueness + the #625 `start_id = end_id` closed
                            // constraint handle it.
                            && !crate::render_plan::from_builder::closed_exact_vlp_uses_cte(
                                graph_rel,
                            )
                        {
                            crate::debug_println!("DEBUG: extract_filters - Adding cycle prevention for fixed-length *{}", exact_hops);

                            // Check if this is a denormalized pattern
                            let is_denormalized = is_node_denormalized(&graph_rel.left)
                                && is_node_denormalized(&graph_rel.right);

                            let rel_cols = extract_relationship_columns(&graph_rel.center)
                                .unwrap_or(RelationshipColumns {
                                    from_id: Identifier::Single("from_node_id".to_string()),
                                    to_id: Identifier::Single("to_node_id".to_string()),
                                });

                            // For denormalized, use relationship columns directly (nodes
                            // have no separate table — extract_table_name would fail).
                            // For normal schemas, use node ID columns from node tables.
                            //
                            // #802: `start_id_cols`/`end_id_cols` are per-column
                            // vectors so the legacy FkEdge / multi-type `start !=
                            // end` node guard stays correct for COMPOSITE node
                            // keys. Previously these were single strings sourced
                            // from `ViewScan.id_column`, which silently drops
                            // every column past the first for a composite key —
                            // emitting `a.region <> b.region` for a
                            // `[region, object_id]` key (an under-constrained
                            // guard that admits distinct nodes sharing a region).
                            // That defect was masked while the FK-edge exact VLP
                            // errored Code 179 (dup FROM/JOIN alias, #802 FROM
                            // fix); un-masking it here keeps the guard honest.
                            // Single-column keys yield one-element vectors →
                            // byte-identical to the old single-string path.
                            let schema_for_ids =
                                crate::server::query_context::get_current_schema_with_fallback();
                            let composite_node_id_cols = |node_plan: &LogicalPlan| -> Option<Vec<String>> {
                                let schema = schema_for_ids.as_ref()?;
                                let label =
                                    crate::render_plan::cte_extraction::extract_node_label_from_viewscan_with_schema(
                                        node_plan, schema,
                                    )?;
                                let node_schema = schema.node_schema_opt(&label)?;
                                if node_schema.node_id.is_composite() {
                                    Some(
                                        node_schema
                                            .node_id
                                            .columns()
                                            .iter()
                                            .map(|c| c.to_string())
                                            .collect(),
                                    )
                                } else {
                                    None
                                }
                            };
                            let (start_id_cols, end_id_cols): (Vec<String>, Vec<String>) =
                                if is_denormalized {
                                    (
                                        rel_cols
                                            .from_id
                                            .columns()
                                            .iter()
                                            .map(|c| c.to_string())
                                            .collect(),
                                        rel_cols
                                            .to_id
                                            .columns()
                                            .iter()
                                            .map(|c| c.to_string())
                                            .collect(),
                                    )
                                } else {
                                    let start_table =
                                        extract_table_name(&graph_rel.left).ok_or_else(|| {
                                            RenderBuildError::MissingTableInfo(
                                                "start node in cycle prevention".to_string(),
                                            )
                                        })?;
                                    let end_table =
                                        extract_table_name(&graph_rel.right).ok_or_else(|| {
                                            RenderBuildError::MissingTableInfo(
                                                "end node in cycle prevention".to_string(),
                                            )
                                        })?;
                                    // Prefer the node schema's full composite key;
                                    // else the lossy-but-correct single column.
                                    let start = composite_node_id_cols(&graph_rel.left)
                                        .unwrap_or_else(|| {
                                            vec![extract_id_column(&graph_rel.left)
                                                .unwrap_or_else(|| table_to_id_column(&start_table))]
                                        });
                                    let end = composite_node_id_cols(&graph_rel.right)
                                        .unwrap_or_else(|| {
                                            vec![extract_id_column(&graph_rel.right)
                                                .unwrap_or_else(|| table_to_id_column(&end_table))]
                                        });
                                    (start, end)
                                };

                            // #617: single-walk undirected exact-bound chains hop
                            // over the doubled-edge CTE; the pairwise uniqueness
                            // guard must compare the ORIGINAL-orientation identity
                            // columns — comparing the (swapped) from/to columns
                            // would treat one edge's two orientations as distinct
                            // relationships and allow immediate backtracking.
                            let undirected_doubled = graph_rel.was_undirected == Some(true)
                                && crate::server::query_context::get_current_schema_with_fallback()
                                    .map(|s| {
                                        crate::query_planner::analyzer::bidirectional_union::undirected_vlp_single_walk_core(
                                            graph_rel, &s,
                                        )
                                    })
                                    .unwrap_or(false);
                            // The pairwise relationship-uniqueness guard references
                            // the r1..rN edge-table aliases of the single-type flat
                            // self-join. Two paths reach here WITHOUT those aliases
                            // and must keep the legacy start != end guard instead:
                            //  - FkEdge: node aliases m1..m{N-1}, self-referencing.
                            //  - Multi-type VLP (labels.len() > 1): renders as a
                            //    vlp_multi_type_* CTE addressed via start_id/end_id.
                            // (Denormalized exact-bound routes to the recursive CTE
                            // and never reaches here.)
                            // Route the schema-pattern decision through the
                            // schema-catalog dispatch API (detect_vlp_schema_type)
                            // rather than a raw flag, per the axis-dispatch rule.
                            let vlp_schema_type =
                                crate::render_plan::cte_extraction::detect_vlp_schema_type(
                                    graph_rel,
                                );
                            let is_multi_type = graph_rel
                                .labels
                                .as_ref()
                                .map(|l| l.len() > 1)
                                .unwrap_or(false);
                            let use_legacy_start_end_guard = vlp_schema_type
                                == crate::render_plan::cte_extraction::VlpSchemaType::FkEdge
                                || is_multi_type;
                            // #604 Site A: the relationship-uniqueness edge
                            // columns (from_id/to_id) may be COMPOSITE. Pass the
                            // full per-column vectors to the composite-aware
                            // generator instead of stringifying the Identifier
                            // (which collapsed a composite to one bogus quoted
                            // column `r1."from_bank_id, from_account_number"`).
                            // Single-column ids yield one-element vectors — the
                            // exact input the old single-column wrapper produced,
                            // so single-key schemas are byte-identical.
                            let (from_cols, to_cols): (Vec<String>, Vec<String>) =
                                if undirected_doubled {
                                    use crate::sql_generator::emitters::clickhouse::variable_length_cte as vlc;
                                    (
                                        vec![vlc::DOUBLED_EDGES_ORIG_FROM.to_string()],
                                        vec![vlc::DOUBLED_EDGES_ORIG_TO.to_string()],
                                    )
                                } else {
                                    (
                                        rel_cols
                                            .from_id
                                            .columns()
                                            .iter()
                                            .map(|c| c.to_string())
                                            .collect(),
                                        rel_cols
                                            .to_id
                                            .columns()
                                            .iter()
                                            .map(|c| c.to_string())
                                            .collect(),
                                    )
                                };
                            // `start_id_cols`/`end_id_cols` feed only the legacy
                            // start != end node guard (FkEdge / multi-type),
                            // which is now composite-aware (#802).
                            let from_col_refs: Vec<&str> =
                                from_cols.iter().map(String::as_str).collect();
                            let to_col_refs: Vec<&str> =
                                to_cols.iter().map(String::as_str).collect();
                            let start_id_refs: Vec<&str> =
                                start_id_cols.iter().map(String::as_str).collect();
                            let end_id_refs: Vec<&str> =
                                end_id_cols.iter().map(String::as_str).collect();
                            // #806: a schema-declared composite `edge_id` (e.g. a
                            // polymorphic `interactions` table keyed
                            // `[from_id, to_id, interaction_type, timestamp]`) is
                            // the true physical-edge identity. The flat pairwise
                            // uniqueness guard must compare that full column set —
                            // matching the recursive path's
                            // `build_edge_tuple_recursive` — or two PARALLEL edges
                            // (same node pair, distinct timestamp) collapse into
                            // one relationship and every trail through them is
                            // dropped (under-count). Consulted only on the
                            // Normal/Polymorphic pairwise path; the legacy
                            // FkEdge/multi-type start != end guard has no edge
                            // table and ignores it (kept None there).
                            //
                            // #617: on the doubled-edge undirected walk the from/to
                            // columns are SWAPPED in reverse-orientation rows, so we
                            // apply the same orientation correction the recursive
                            // path uses — replace the schema from/to columns with
                            // the original-orientation `__cg_orig_from/to` columns
                            // the doubled CTE projects. Any non-from/to identity
                            // column (interaction_type, timestamp) is
                            // orientation-independent and passes through unchanged.
                            let edge_id_cols_owned: Option<Vec<String>> =
                                (!use_legacy_start_end_guard)
                                    .then(|| {
                                        schema_for_ids.as_ref().and_then(|schema| {
                                            graph_rel.labels.as_ref().and_then(|labels| {
                                                labels.first().and_then(|label| {
                                                    schema
                                                        .get_rel_schema(label)
                                                        .ok()
                                                        .and_then(|rs| rs.edge_id.clone())
                                                })
                                            })
                                        })
                                    })
                                    .flatten()
                                    .map(|edge_id| {
                                        use crate::sql_generator::emitters::clickhouse::variable_length_cte as vlc;
                                        let (from_key, to_key) = (
                                            rel_cols.from_id.columns(),
                                            rel_cols.to_id.columns(),
                                        );
                                        edge_id
                                            .columns()
                                            .iter()
                                            .map(|c| {
                                                if undirected_doubled {
                                                    vlc::doubled_edge_identity_col(
                                                        c,
                                                        &from_key,
                                                        &to_key,
                                                    )
                                                    .to_string()
                                                } else {
                                                    c.to_string()
                                                }
                                            })
                                            .collect::<Vec<String>>()
                                    });
                            let edge_id_refs: Option<Vec<&str>> = edge_id_cols_owned
                                .as_ref()
                                .map(|cols| cols.iter().map(String::as_str).collect());
                            // Generate relationship-uniqueness filters
                            if let Some(cycle_filter) = crate::render_plan::cte_extraction::generate_cycle_prevention_filters_composite(
                                exact_hops,
                                &start_id_refs,
                                &to_col_refs,
                                &from_col_refs,
                                &end_id_refs,
                                &graph_rel.left_connection,
                                &graph_rel.right_connection,
                                use_legacy_start_end_guard,
                                edge_id_refs.as_deref(),
                            ) {
                                crate::debug_println!("DEBUG: extract_filters - Generated relationship-uniqueness filter");
                                all_predicates.push(cycle_filter);
                            }
                        }
                    }
                }

                // #1006: register own-table join requests for WHERE-only
                // property references on mixed-access (foreign-embedded)
                // endpoints. Must run BEFORE the denorm property mapping
                // below — the intercept in `apply_property_mapping_to_expr`
                // needs the registry populated to keep the node alias and
                // resolve against the node's own table instead of remapping
                // the alias to the edge table (`t1.name` → Code 47).
                for pred in &all_predicates {
                    crate::render_plan::plan_builder_helpers::register_own_table_property_requests(
                        pred,
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                    );
                }

                if all_predicates.is_empty() {
                    None
                } else if all_predicates.len() == 1 {
                    log::trace!("Found 1 GraphRel predicate");
                    // Safety: len() == 1 guarantees next() returns Some
                    let mut single_pred = all_predicates
                        .into_iter()
                        .next()
                        .expect("all_predicates has exactly one element");
                    // Apply property mapping for denormalized nodes
                    apply_property_mapping_to_expr(
                        &mut single_pred,
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                    );
                    Some(single_pred)
                } else {
                    // Combine with AND
                    log::trace!(
                        "Found {} GraphRel predicates, combining with AND",
                        all_predicates.len()
                    );
                    let combined = all_predicates
                        .into_iter()
                        .map(|mut pred| {
                            // Apply property mapping for denormalized nodes
                            apply_property_mapping_to_expr(
                                &mut pred,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                            );
                            pred
                        })
                        .reduce(|acc, pred| {
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![acc, pred],
                            })
                        })
                        .expect("all_predicates is non-empty, reduce succeeds");
                    Some(combined)
                }
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // For GraphJoins, extract filters from the input
                let input_filter = graph_joins.input.extract_filters()?;

                // #922: a CLOSED OPTIONAL VLP (`(a)-[*..]->(a)`) loses its
                // MANDATORY anchor WHERE (`WHERE a.name = 'Alice'`) on the way
                // here. That filter is a `LogicalPlan::Filter` wrapping the
                // anchor GraphNode. `GraphJoinInference` runs FIRST and captures
                // a clone of the pattern's `GraphRel` (Filter and all) into the
                // VLP LEFT JOIN's own `join.graph_rel`. `DuplicateScansRemoving`
                // runs AFTER and — because the anchor alias appears at BOTH VLP
                // endpoints — elides the pattern's LEFT endpoint scan in
                // `graph_joins.input` to `Empty`, taking the Filter with it (its
                // `GraphJoins` arm only recurses into `.input`, never into
                // `joins[].graph_rel`, so the captured clone is untouched). Net
                // result: `graph_joins.input` no longer carries the anchor
                // predicate and the recursion above returns None for it, while
                // the filter survives intact inside the closed VLP join's
                // `graph_rel.left`. Recover it here: for a closed optional VLP
                // join (`left_connection == right_connection`), pull the anchor
                // filter out of that `graph_rel.left` and AND it into the outer
                // WHERE (where it BELONGS — restricting which base anchors
                // appear; embedding it only in the CTE would return every anchor
                // NULL-extended). The `left == right` gate is exclusive to the
                // closed shape: an OPEN optional VLP keeps its anchor scan
                // (distinct endpoints, not elided), so `input_filter` already has
                // the predicate and its endpoints never satisfy this gate — no
                // double-add. Directed OR undirected, `*0..`/`*1..` all covered.
                //
                // Dedup by equality: two closed optional VLPs on the SAME anchor
                // (`... OPTIONAL MATCH (a)->(a) OPTIONAL MATCH (a)->(a)`) each
                // carry the same anchor filter, and both would otherwise be AND-ed
                // in (`a.name='X' AND a.name='X'`). Distinct anchors keep their
                // distinct filters. Skip anything already present in
                // `input_filter` (belt-and-suspenders — the gate already excludes
                // the open shape whose filter lives in the input).
                let mut input_filter = input_filter;
                let mut recovered: Vec<RenderExpr> = Vec::new();
                for join in &graph_joins.joins {
                    if let Some(gr) = &join.graph_rel {
                        // Mirror the inference-side `rel_is_optional` gate exactly
                        // (`optional_aliases.contains(rel) || is_optional`): a VLP
                        // that became optional purely via `optional_aliases`
                        // (without the `is_optional` field) would otherwise get the
                        // closed-constraint conjunct in the JOIN ON but not this
                        // WHERE recovery → silent-wrong. In practice a
                        // directly-written OPTIONAL MATCH sets `is_optional:
                        // Some(true)` at lowering, so they already agree; keying on
                        // both keeps the two gates provably consistent.
                        let is_optional_vlp_rel = gr.is_optional.unwrap_or(false)
                            || graph_joins.optional_aliases.contains(&gr.alias);
                        let is_closed_optional_vlp = gr.variable_length.is_some()
                            && is_optional_vlp_rel
                            && gr.left_connection == gr.right_connection;
                        if is_closed_optional_vlp {
                            if let Some(anchor_filter) = gr.left.extract_filters()? {
                                let already_in_input =
                                    input_filter.as_ref().is_some_and(|f| *f == anchor_filter);
                                if !already_in_input && !recovered.contains(&anchor_filter) {
                                    recovered.push(anchor_filter);
                                }
                            }
                        }
                    }
                }
                for anchor_filter in recovered {
                    input_filter = Some(match input_filter {
                        Some(existing) => RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![existing, anchor_filter],
                        }),
                        None => anchor_filter,
                    });
                }

                // #518: `correlation_predicates` carries relationship-
                // uniqueness guards (e.g. `NOT (t1.follower_id = t2.follower_id
                // AND t1.followed_id = t2.followed_id)`, generated by
                // `cross_branch.rs::generate_relationship_uniqueness_constraints`
                // for directed same-type multi-hop patterns like
                // `(a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)`) alongside genuine
                // cross-CTE join correlations. The WITH/CTE-specific machinery
                // (`extract_correlation_predicates` /
                // `convert_correlation_predicates_to_joins` in
                // plan_builder_utils.rs) already consumes this field for
                // chained-WITH queries, but a PLAIN (non-WITH) query's
                // GraphJoins never goes through that machinery at all, so its
                // correlation_predicates were silently dropped here — the
                // uniqueness guard was computed but never rendered into the
                // WHERE clause. (The UNDIRECTED/BidirectionalUnion path gets
                // its equivalent guard via a real `LogicalPlan::Filter` node
                // instead, which this same `extract_filters` recursion
                // already picks up via the `Filter` arm above — so only the
                // directed path was affected.)
                let uniqueness_predicates: FilterBuilderResult<Vec<RenderExpr>> = graph_joins
                    .correlation_predicates
                    .iter()
                    .map(|p| p.clone().try_into())
                    .collect();
                let uniqueness_predicates = uniqueness_predicates?;

                uniqueness_predicates
                    .into_iter()
                    .fold(input_filter, |acc, pred| {
                        Some(match acc {
                            Some(existing) => {
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![existing, pred],
                                })
                            }
                            None => pred,
                        })
                    })
            }
            LogicalPlan::Filter(filter) => {
                // Normal filter extraction - always include WHERE clause predicates
                // For VLP: Start node filters are pushed into CTE base case during CTE generation,
                // but end node filters MUST be in outer WHERE clause (after CTE join)
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // #1006: register own-table join requests BEFORE property
                // mapping (see the GraphRel arm above — same rationale for
                // standalone Filter nodes whose predicate was not embedded
                // into a GraphRel's where_predicate).
                crate::render_plan::plan_builder_helpers::register_own_table_property_requests(
                    &expr,
                    &filter.input,
                );
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);

                // Also check for schema filters from the input (e.g., GraphNode → ViewScan)
                if let Some(input_filter) = filter.input.extract_filters()? {
                    crate::debug_println!("DEBUG: extract_filters - Combining Filter predicate with input schema filter");
                    // Combine the Filter predicate with input's schema filter using AND
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![input_filter, expr],
                    }))
                } else {
                    crate::debug_println!("DEBUG: extract_filters - Returning Filter predicate only (no input filter)");
                    Some(expr)
                }
            }
            LogicalPlan::Projection(projection) => projection.input.extract_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::Union(union) => {
                // For BidirectionalUnion: both branches have the same filters
                // (where_predicate is cloned to both). Extract from first branch,
                // but only if all branches have filters. Propagate errors instead
                // of swallowing them.
                if union.inputs.is_empty() {
                    return Ok(None);
                }

                let mut filters = Vec::with_capacity(union.inputs.len());
                for input in &union.inputs {
                    filters.push(input.extract_filters()?);
                }

                let mut iter = filters.into_iter();
                let first = iter.next().unwrap();
                first.filter(|_| iter.all(|f| f.is_some()))
            }
            LogicalPlan::PageRank(_) => None,
            LogicalPlan::Unwind(u) => u.input.extract_filters()?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine filters from both sides AND the join_condition with AND
                let left_filters = cp.left.extract_filters()?;
                let right_filters = cp.right.extract_filters()?;

                // Also extract join_condition — this holds WHERE predicates that span
                // both sides of the CartesianProduct (e.g., `country IN [countryX, countryY]`
                // where country is from the right GraphRel and countryX/countryY are CTE vars)
                let jc_filter = if let Some(ref jc) = cp.join_condition {
                    match RenderExpr::try_from(jc.clone()) {
                        Ok(expr) => {
                            log::info!("🔍 CartesianProduct extract_filters: extracted join_condition as filter: {:?}", expr);
                            Some(expr)
                        }
                        Err(e) => {
                            log::warn!("🔍 CartesianProduct extract_filters: failed to convert join_condition: {:?}", e);
                            None
                        }
                    }
                } else {
                    None
                };

                // Combine all filter sources with AND
                let mut all_filters: Vec<RenderExpr> = Vec::new();
                if let Some(l) = left_filters {
                    all_filters.push(l);
                }
                if let Some(r) = right_filters {
                    all_filters.push(r);
                }
                if let Some(jc) = jc_filter {
                    all_filters.push(jc);
                }

                match all_filters.len() {
                    0 => None,
                    1 => Some(all_filters.into_iter().next().unwrap()),
                    _ => {
                        // Fold into nested ANDs
                        let combined = all_filters
                            .into_iter()
                            .reduce(|acc, f| {
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, f],
                                })
                            })
                            .unwrap();
                        Some(combined)
                    }
                }
            }
            LogicalPlan::WithClause(wc) => wc.input.extract_filters()?,
            // Write variants — recurse into preceding read pipeline for filter extraction.
            LogicalPlan::Create(c) => c.input.extract_filters()?,
            LogicalPlan::SetProperties(sp) => sp.input.extract_filters()?,
            LogicalPlan::Delete(d) => d.input.extract_filters()?,
            LogicalPlan::Remove(r) => r.input.extract_filters()?,
        };
        Ok(filters)
    }

    fn extract_distinct(&self) -> bool {
        // Extract distinct flag from Projection nodes
        let result = match &self {
            LogicalPlan::Projection(projection) => {
                crate::debug_println!(
                    "DEBUG extract_distinct: Found Projection, distinct={}",
                    projection.distinct
                );
                projection.distinct
            }
            LogicalPlan::OrderBy(order_by) => {
                crate::debug_println!("DEBUG extract_distinct: OrderBy, recursing");
                order_by.input.extract_distinct()
            }
            LogicalPlan::Skip(skip) => {
                crate::debug_println!("DEBUG extract_distinct: Skip, recursing");
                skip.input.extract_distinct()
            }
            LogicalPlan::Limit(limit) => {
                crate::debug_println!("DEBUG extract_distinct: Limit, recursing");
                limit.input.extract_distinct()
            }
            LogicalPlan::GroupBy(group_by) => {
                crate::debug_println!("DEBUG extract_distinct: GroupBy, recursing");
                group_by.input.extract_distinct()
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                crate::debug_println!("DEBUG extract_distinct: GraphJoins, recursing");
                graph_joins.input.extract_distinct()
            }
            LogicalPlan::Filter(filter) => {
                crate::debug_println!("DEBUG extract_distinct: Filter, recursing");
                filter.input.extract_distinct()
            }
            _ => {
                crate::debug_println!("DEBUG extract_distinct: Other variant, returning false");
                false
            }
        };
        crate::debug_println!("DEBUG extract_distinct: Returning {}", result);
        result
    }
}

/// Rewrite a logical predicate for use as outer WHERE on a pattern_union CTE.
/// Converts `id(alias)` → `cte.start_id`/`end_id` and `labels(alias)` → `cte.start_type`/`end_type`.
/// Splits AND predicates and rewrites each individually. Returns raw SQL fragments.
fn rewrite_predicate_for_pattern_cte(
    predicate: &LogicalExpr,
    cte_alias: &str,
    left_alias: &str,
    right_alias: &str,
) -> Result<Vec<String>, RenderBuildError> {
    let mut parts = Vec::new();
    split_and_predicates(predicate, &mut parts);
    let mut rewritten = Vec::new();

    for part in &parts {
        // #466 (round 3): conjuncts referencing NODE PROPERTIES are resolved
        // per-branch INSIDE the pattern_union CTE (each branch knows which
        // label/table the alias binds to — see
        // cte_extraction::substitute_pattern_branch_refs). Skip them here
        // ONLY if the plan's FROM/JOIN genuinely references that CTE
        // (registered by from_builder/join_builder, which run before filter
        // extraction in every render arm). Plan shapes that never reference
        // the CTE (e.g. some multi-MATCH cartesians whose FROM/JOINs stay on
        // plain tables — their built-but-unreferenced CTE is dead-eliminated
        // later) must NOT have the conjunct skipped: an unconditional skip
        // silently dropped it (applied nowhere). Non-absorbed conjuncts stay
        // in the outer WHERE as-is: correct when the alias is in scope as a
        // plain table; a loud database error (unknown identifier) when the
        // plan shape dropped the alias — never silently wrong. They must
        // also not go through the id-column rewrite below, which degraded
        // `o.name = 'Alice'` to `r.end_id = 'Alice'` (always false).
        if crate::render_plan::cte_extraction::conjunct_references_node_property(
            part,
            &[left_alias, right_alias],
        ) {
            let pattern_cte_name = format!("pattern_union_{cte_alias}");
            if crate::server::query_context::is_pattern_union_in_scope(&pattern_cte_name) {
                log::debug!(
                    "🔀 PatternResolver 2.0: node-property conjunct applied per-branch \
                     inside {pattern_cte_name}, skipping outer rewrite: {:?}",
                    part
                );
            } else {
                log::warn!(
                    "⚠️ PatternResolver 2.0: node-property conjunct NOT absorbed by a \
                     pattern_union CTE (plan shape without the CTE reference) — \
                     keeping it in the outer WHERE: {:?}",
                    part
                );
                let render_expr = RenderExpr::try_from((*part).clone()).map_err(|e| {
                    RenderBuildError::UnsupportedFeature(format!(
                        "WHERE predicate on a multi-type/undirected pattern could not \
                         be rendered for the outer WHERE: {e:?}"
                    ))
                })?;
                rewritten.push(
                    crate::render_plan::cte_extraction::render_expr_to_sql_string(
                        &render_expr,
                        &[],
                    ),
                );
            }
            continue;
        }
        // #466 round 3 (ride-along): whole-entity / subquery conjuncts cannot
        // be resolved against the CTE projection either — clean error, never
        // a silent drop.
        if crate::render_plan::cte_extraction::conjunct_has_unresolvable_entity_ref(
            part,
            &[left_alias, right_alias],
        ) {
            return Err(RenderBuildError::UnsupportedFeature(format!(
                "WHERE predicate on a multi-type/undirected pattern contains {} \
                 which cannot be resolved against the pattern UNION; rewrite the \
                 filter using node properties, id(), or labels()",
                crate::render_plan::cte_extraction::describe_unresolvable_conjunct(
                    part,
                    &[left_alias, right_alias]
                )
            )));
        }
        if let Some(sql) =
            rewrite_single_predicate_for_cte(part, cte_alias, left_alias, right_alias)
        {
            rewritten.push(sql);
        } else {
            log::warn!(
                "⚠️ PatternResolver 2.0: Could not rewrite predicate for CTE columns: {:?}",
                part
            );
        }
    }
    Ok(rewritten)
}

/// Split an AND-combined predicate into individual parts.
pub(crate) fn split_and_predicates<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::OperatorApplicationExp(op_app) = expr {
        if op_app.operator == crate::query_planner::logical_expr::Operator::And {
            for operand in &op_app.operands {
                split_and_predicates(operand, out);
            }
            return;
        }
    }
    out.push(expr);
}

/// Rewrite a single predicate expression to reference CTE columns.
/// Handles patterns:
/// - `id(alias) IN [values]` or `id(alias) = value` (ScalarFnCall)
/// - `labels(alias) = value` (ScalarFnCall)
/// - `alias.id_prop IN [values]` (PropertyAccess — optimizer-resolved id())
fn rewrite_single_predicate_for_cte(
    expr: &LogicalExpr,
    cte_alias: &str,
    left_alias: &str,
    right_alias: &str,
) -> Option<String> {
    match expr {
        LogicalExpr::OperatorApplicationExp(op_app) => {
            if op_app.operands.len() == 2 {
                let lhs = &op_app.operands[0];
                let rhs = &op_app.operands[1];

                // Case 1: ScalarFnCall — id(alias) or labels(alias)
                if let LogicalExpr::ScalarFnCall(fn_call) = lhs {
                    if let Some(alias) = extract_fn_alias(fn_call) {
                        let position = if alias == left_alias {
                            "start"
                        } else if alias == right_alias {
                            "end"
                        } else {
                            return None;
                        };

                        match fn_call.name.to_lowercase().as_str() {
                            "id" => {
                                let cte_col = format!("{}.{}_id", cte_alias, position);
                                let rhs_sql = render_rhs_to_sql(rhs, true);
                                let op_str = render_operator(&op_app.operator);
                                return Some(format!("{} {} {}", cte_col, op_str, rhs_sql));
                            }
                            // #466 round 4.5: elementId in THIS codebase is the
                            // composite string `Label:id-` (see
                            // graph_catalog::element_id::generate_node_element_id
                            // — trailing `-` is a Browser-compat sentinel). The
                            // CTE exposes exactly its ingredients per row, so
                            // rebuild it label-agnostically. Previously
                            // elementId fell through every handler and the
                            // conjunct was silently dropped.
                            "elementid" => {
                                let cte_expr = format!(
                                    "concat({cte}.{pos}_type, ':', {cte}.{pos}_id, '-')",
                                    cte = cte_alias,
                                    pos = position
                                );
                                let rhs_sql = render_rhs_to_sql(rhs, true);
                                let op_str = render_operator(&op_app.operator);
                                return Some(format!("{} {} {}", cte_expr, op_str, rhs_sql));
                            }
                            "labels" => {
                                let cte_col = format!("{}.{}_type", cte_alias, position);
                                let rhs_sql = render_rhs_to_sql(rhs, false);
                                let op_str = render_operator(&op_app.operator);
                                return Some(format!("{} {} {}", cte_col, op_str, rhs_sql));
                            }
                            _ => {}
                        }
                    }
                }

                // Case 2: PropertyAccess — optimizer-resolved id(a) becomes a.customer_id
                // Map any property on a node alias to start_id/end_id (click-to-expand uses ID)
                // For the relationship alias itself, map directly to the CTE column (r.name → r.name)
                if let LogicalExpr::PropertyAccessExp(prop) = lhs {
                    let alias = &prop.table_alias.0;
                    if alias == cte_alias {
                        // Relationship property access: the CTE exposes these as direct columns
                        let prop_name = match &prop.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(c) => {
                                c.clone()
                            }
                            crate::graph_catalog::expression_parser::PropertyValue::Expression(
                                e,
                            ) => e.clone(),
                        };
                        let cte_col = format!("{}.{}", cte_alias, prop_name);
                        let rhs_sql = render_rhs_to_sql(rhs, true);
                        let op_str = render_operator(&op_app.operator);
                        return Some(format!("{} {} {}", cte_col, op_str, rhs_sql));
                    }
                    // #466: node-alias property access is handled per-branch
                    // INSIDE the pattern_union CTE (the caller skips such
                    // conjuncts before reaching here). The old fallback mapped
                    // ANY node property to start_id/end_id, silently degrading
                    // e.g. `o.name = 'Alice'` to `r.end_id = 'Alice'`.
                    return None;
                }

                // Case 3: "Label" IN labels(alias) — reversed operand order
                if let LogicalExpr::ScalarFnCall(fn_call) = rhs {
                    if fn_call.name.eq_ignore_ascii_case("labels")
                        || fn_call.name.eq_ignore_ascii_case("label")
                    {
                        if let Some(alias) = extract_fn_alias(fn_call) {
                            let position = if alias == left_alias {
                                "start"
                            } else if alias == right_alias {
                                "end"
                            } else {
                                return None;
                            };
                            let cte_col = format!("{}.{}_type", cte_alias, position);
                            let lhs_sql = render_rhs_to_sql(lhs, false);
                            return Some(format!("{} = {}", cte_col, lhs_sql));
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract the alias argument from a ScalarFnCall like id(a) or labels(a).
fn extract_fn_alias(fn_call: &crate::query_planner::logical_expr::ScalarFnCall) -> Option<&str> {
    if fn_call.args.len() == 1 {
        match &fn_call.args[0] {
            LogicalExpr::TableAlias(alias) => Some(&alias.0),
            _ => None,
        }
    } else {
        None
    }
}

/// Check if a predicate involves labels() function (e.g., `labels(n) = "User"`).
/// These predicates are redundant for non-CTE paths where the label is already
/// constrained by the MATCH pattern.
fn is_labels_predicate(expr: &RenderExpr) -> bool {
    if let RenderExpr::OperatorApplicationExp(op_app) = expr {
        for operand in &op_app.operands {
            if let RenderExpr::ScalarFnCall(fn_call) = operand {
                if fn_call.name.eq_ignore_ascii_case("labels")
                    || fn_call.name.eq_ignore_ascii_case("label")
                {
                    return true;
                }
            }
        }
    }
    false
}

/// Render operator to SQL string.
fn render_operator(op: &crate::query_planner::logical_expr::Operator) -> &'static str {
    use crate::query_planner::logical_expr::Operator as LogicalOp;
    match op {
        LogicalOp::Equal => "=",
        LogicalOp::NotEqual => "!=",
        LogicalOp::In => "IN",
        LogicalOp::NotIn => "NOT IN",
        LogicalOp::LessThan => "<",
        LogicalOp::GreaterThan => ">",
        LogicalOp::LessThanEqual => "<=",
        LogicalOp::GreaterThanEqual => ">=",
        _ => "=",
    }
}

/// Render the RHS of a comparison to SQL. For id() comparisons, wrap values in toString().
fn render_rhs_to_sql(expr: &LogicalExpr, as_string: bool) -> String {
    use crate::query_planner::logical_expr::Literal;
    match expr {
        LogicalExpr::Literal(lit) => match lit {
            Literal::Integer(n) => {
                if as_string {
                    format!("'{}'", n)
                } else {
                    n.to_string()
                }
            }
            Literal::String(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
            Literal::Float(f) => f.to_string(),
            Literal::Boolean(b) => b.to_string(),
            Literal::Null => "NULL".to_string(),
        },
        LogicalExpr::List(items) => {
            let rendered: Vec<String> = items
                .iter()
                .map(|i| render_rhs_to_sql(i, as_string))
                .collect();
            crate::sql_generator::function_mapper::current_function_mapper()
                .array_literal(&rendered.join(", "))
        }
        _ => {
            // Fallback: try to render via RenderExpr conversion and SQL generation
            if let Ok(render_expr) = RenderExpr::try_from(expr.clone()) {
                use crate::render_plan::cte_extraction::render_expr_to_sql_string;
                render_expr_to_sql_string(&render_expr, &[])
            } else {
                "NULL".to_string()
            }
        }
    }
}
