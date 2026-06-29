//! Phase 1.2 spike: end-to-end Cypher → Databricks SQL.
//!
//! These tests are the load-bearing assertion of the whole DeltaGraph
//! refactor so far: if `current_function_mapper()` correctly reads from
//! the task-local `QueryContext` and gets propagated through every
//! rendering layer, then setting `dialect: Databricks` in the context
//! before running the existing ClickHouse pipeline produces SQL that's
//! correct for Databricks at every FunctionMapper-routed site.
//!
//! What's tested:
//! - VLP query under CH dialect emits CH spellings (`arrayConcat`,
//!   `has`, `CAST([] AS Array(...))`).
//! - The same VLP query under Databricks dialect emits Spark spellings
//!   (`concat`, `array_contains`, `CAST(array() AS ARRAY<...>)`).
//!
//! ## Phase 1.2 spike findings — what's still CH-locked
//!
//! Actual Databricks output for `MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
//! RETURN b.id` is *mostly* correct — FunctionMapper-routed sites all
//! flip cleanly. Run `print_databricks_vlp_sql` with `--nocapture` to
//! see the full output. The remaining gaps for Phase 1.3+:
//!
//! 1. **Array-literal syntax (Phase 1.3, done).** `format!("[{...}]")`
//!    sites in the rendering layer now go through
//!    `FunctionMapper::array_literal(&elems)`, so Spark sees
//!    `array(a, b)` and CH sees `[a, b]`. Asserted by
//!    `vlp_under_databricks_dialect_emits_spark_spellings` below.
//!
//! 2. **Identifier / alias quoting (Phase 1.4, done).** `AS "alias"`
//!    sites in the rendering layer now route through
//!    `FunctionMapper::quote_alias(&name)`, so Spark sees
//!    `` AS `b.id` `` and CH sees `AS "b.id"` (existing behavior).
//!    Spark parses double quotes as string literals, so backticks are
//!    mandatory there. Asserted by the spellings test below.
//!
//! 3. **Aggregate functions via function_registry (Phase 1.5, done).**
//!    `FunctionMapping` now carries an optional `databricks_name` and
//!    a `name_for(dialect)` accessor. Consumer sites in `to_sql.rs`,
//!    `function_translator.rs`, and `to_sql_query.rs` (2 sites) all
//!    route through it. The `collect` entry is the first to opt in:
//!    CH gets `groupArray`, Spark gets `collect_list`. Asserted by
//!    `collect_under_databricks_emits_collect_list` below. Other
//!    entries default to `clickhouse_name` for both dialects (most
//!    ANSI-shaped aggregates: count, sum, min, max, avg, ...). New
//!    Databricks-incompatible entries should set `databricks_name`
//!    explicitly as they get exercised by future test coverage.
//!
//! 4. **Type-cast function names (Phase 1.6, done).** Cypher's type
//!    conversion functions — `toInteger`, `toFloat`, `toString` — were
//!    hard-coded to their ClickHouse spellings (`toInt64`, `toFloat64`,
//!    `toString`) in `function_registry`. Phase 1.6 added
//!    `databricks_name: Some("bigint" | "double" | "string")` to those
//!    registry entries so Spark sees its function-call cast aliases.
//!    Asserted by `tointeger_under_databricks_emits_bigint`,
//!    `tofloat_under_databricks_emits_double`, and
//!    `tostring_under_databricks_emits_string` below (each with a CH
//!    baseline).
//!
//!    Additionally, the four ad-hoc `format!("toInt64({})", ...)`
//!    call sites in `to_sql.rs` and `to_sql_query.rs` (reduce/arrayFold
//!    init, `is_vlp_path_is_null` rewrite) now route through
//!    `FunctionMapper::cast_int64()`. The reduce/arrayFold init path is
//!    asserted by `reduce_init_under_databricks_uses_bigint_cast`;
//!    the `is_vlp_path_is_null` rewrite isn't reached by the current
//!    spike-test query plans (the rewrite fires for `CASE path IS NULL`
//!    style queries), but the routing is mechanical and consistent
//!    with the asserted sites.
//!
//! 5. **BFS shortestPath casts (Phase 1.7, done).** The lightweight
//!    `generate_bfs_shortest_path_sql` path emitted hard-coded
//!    `toUInt16(...)` for the hop counter, and the heavier weighted
//!    reconstruction (`generate_weighted_bfs_reconstruction_sql`) used
//!    a mix of `CAST(... AS Int64)`, `CAST(... AS Array(Int64))`, and
//!    `toFloat64(0)`. Three new mapper methods cover this surface:
//!    - `cast_uint16()` (CH: `toUInt16`, Spark: `int` — widening
//!      since Spark has no unsigned types. `smallint` would be the
//!      conceptual match but `max_hops` is `u32` and unbounded via
//!      `CLICKGRAPH_VLP_MAX_HOPS`, so we widen to `int` to remove
//!      any wrap risk.)
//!    - `cast_float64()` (CH: `toFloat64`, Spark: `double`)
//!    - `int64_array_cast(expr)` (CH: `CAST({expr} AS Array(Int64))`,
//!      Spark: `CAST({expr} AS ARRAY<BIGINT>)`)
//!
//!    The lighter BFS path is asserted by
//!    `shortestpath_bfs_under_databricks_uses_smallint_hop` and its
//!    CH baseline. The weighted reconstruction path needs a `weight:`
//!    keyword and isn't reached by any current spike-test schema, so
//!    it relies on mechanical consistency with the unweighted path
//!    plus the unit test for `int64_array_cast` in `databricks.rs`.
//!
//! With Phase 1.7 done, every syntactic-layer gap that the Phase 1.2
//! spike reaches has a dialect-aware abstraction in place. Adding
//! new Databricks-incompatible patterns is now a localized change at
//! the trait/registry, not a search-and-replace across the codebase.

use crate::{
    clickhouse_query_generator,
    graph_catalog::config::Identifier,
    graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    graph_catalog::schema_types::SchemaType,
    open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
    server::query_context::{with_query_context, QueryContext},
    sql_generator::SqlDialect,
};
use std::collections::HashMap;

/// Render Cypher to SQL using whatever dialect is set in the active
/// `QueryContext`. Mirrors the helper in other test files but doesn't
/// hardcode ClickHouse — the dialect comes from the task-local.
fn cypher_to_sql(cypher: &str) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");
    let graph_schema = setup_test_schema();

    let (logical_plan, mut plan_ctx) = build_logical_plan(&ast, &graph_schema, None, None, None)
        .expect("Failed to build logical plan");

    use crate::query_planner::analyzer;
    use crate::query_planner::optimizer;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    let render_plan = logical_plan
        .to_render_plan(&graph_schema)
        .expect("Failed to build render plan");

    clickhouse_query_generator::generate_sql(render_plan, 100)
}

fn setup_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    let user = NodeSchema {
        database: "test_db".to_string(),
        table_name: "users".to_string(),
        column_names: vec!["id".to_string(), "name".to_string()],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
        property_mappings: [
            (
                "id".to_string(),
                crate::graph_catalog::expression_parser::PropertyValue::Column("id".to_string()),
            ),
            (
                "name".to_string(),
                crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
        view_parameters: None,
        engine: None,
        use_final: None,
        filter: None,
        is_denormalized: false,
        from_properties: None,
        to_properties: None,
        denormalized_source_table: None,
        label_column: None,
        label_value: None,
        node_id_types: None,
        source: None,
        property_types: HashMap::new(),
        id_generation: None,
    };
    nodes.insert("User".to_string(), user);

    let follows = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "follows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_node_table: "users".to_string(),
        to_node_table: "users".to_string(),
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
        from_node_id_dtype: SchemaType::Integer,
        to_node_id_dtype: SchemaType::Integer,
        property_mappings: HashMap::new(),
        view_parameters: None,
        engine: None,
        use_final: None,
        filter: None,
        edge_id: None,
        type_column: None,
        from_label_column: None,
        to_label_column: None,
        from_node_properties: None,
        to_node_properties: None,
        from_label_values: None,
        to_label_values: None,
        is_fk_edge: false,
        constraints: None,
        edge_id_types: None,
        source: None,
        property_types: HashMap::new(),
    };
    relationships.insert("FOLLOWS::User::User".to_string(), follows);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

/// Sanity check: default dialect (no task-local set) emits ClickHouse
/// spellings — `arrayConcat`, `has`, `CAST([] AS Array(...))`. This
/// proves the baseline still works.
#[tokio::test]
async fn vlp_under_clickhouse_dialect_emits_ch_spellings() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;

    assert!(
        sql.contains("arrayConcat"),
        "expected CH spelling `arrayConcat` in VLP recursive CTE; got:\n{sql}"
    );
    assert!(
        sql.contains("has(vp.path_nodes"),
        "expected CH spelling `has(vp.path_nodes, ...)` for cycle detection; got:\n{sql}"
    );
    assert!(
        sql.contains("CAST([] AS Array(String))"),
        "expected CH empty-array cast in VLP CTE; got:\n{sql}"
    );
    // The VLP recursive case builds `[end_id]` to append onto path_nodes.
    // Under CH dialect this stays as bracket-syntax array literal.
    assert!(
        sql.contains("arrayConcat(vp.path_nodes, [") || sql.contains("[toString("),
        "expected CH bracket-style array literal in VLP path append; got:\n{sql}"
    );
    // Spark spellings must NOT appear under CH dialect.
    assert!(!sql.contains("ARRAY<STRING>"), "CH SQL leaked Spark type");
    assert!(
        !sql.contains("array_contains"),
        "CH SQL leaked Spark function name"
    );
    assert!(
        !sql.contains("array(toString("),
        "CH SQL leaked Spark array() literal"
    );

    // Phase 1.4: alias quoting. CH keeps historical double-quote form
    // for `AS` clauses. Backticks must NOT appear in the AS position
    // (they may appear elsewhere from `quote_identifier`).
    assert!(
        sql.contains("AS \"b.id\""),
        "expected CH double-quoted alias `AS \"b.id\"`; got:\n{sql}"
    );
    assert!(
        !sql.contains("AS `b.id`"),
        "CH SQL leaked Spark backtick alias quoting; got:\n{sql}"
    );
}

/// Dump the Databricks output for visual inspection. Always passes —
/// just a fast way to grep for remaining CH-locked surface during the
/// spike. To see the output: `cargo test print_databricks_vlp -- --nocapture`.
#[tokio::test]
async fn print_databricks_vlp_sql() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;
    println!("\n--- DATABRICKS VLP SQL (Phase 1.2 spike) ---\n{sql}\n--- END ---\n");
}

/// The load-bearing test: under Databricks dialect, the FunctionMapper-
/// routed sites all flip to Spark spellings. No call-site changes
/// outside the two structural-gap helpers (`array_count`,
/// `json_extract_string`) — Phase 0.1–1.1's abstractions do the work.
#[tokio::test]
async fn vlp_under_databricks_dialect_emits_spark_spellings() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;

    // arrayConcat → concat
    assert!(
        !sql.contains("arrayConcat"),
        "Databricks SQL still has CH `arrayConcat`; got:\n{sql}"
    );
    // Note: bare `concat(` substring would match either dialect — assert
    // a more specific shape Spark would use. The VLP recursive case
    // builds `concat(vp.path_nodes, [end_id])` style calls; under
    // Spark the empty-array cast uses `array()`.
    assert!(
        sql.contains("CAST(array() AS ARRAY<STRING>)"),
        "expected Spark empty-array cast; got:\n{sql}"
    );

    // has(...) → array_contains(...)
    assert!(
        !sql.contains("NOT has(vp.path_nodes"),
        "Databricks SQL still has CH `has(...)` cycle check; got:\n{sql}"
    );
    assert!(
        sql.contains("array_contains(vp.path_nodes"),
        "expected Spark `array_contains` for cycle detection; got:\n{sql}"
    );

    // No CH empty-array cast
    assert!(
        !sql.contains("CAST([] AS Array(String))"),
        "Databricks SQL leaked CH empty-array cast; got:\n{sql}"
    );

    // Phase 1.3: array literals now route through FunctionMapper.
    // The VLP recursive case builds `array(end_id)` to append onto
    // path_nodes under Databricks dialect (CH builds `[end_id]`).
    assert!(
        sql.contains("concat(vp.path_nodes, array("),
        "expected Spark `array(...)` literal in VLP path append; got:\n{sql}"
    );
    // No bracket-style array literals should leak into Databricks SQL.
    // (We allow `[` to appear in other contexts like `Array(String)`
    // type names inside CH-only output, but those shouldn't show up here.)
    assert!(
        !sql.contains(", [toString(") && !sql.contains("vp.path_nodes, ["),
        "Databricks SQL leaked CH bracket-style array literal; got:\n{sql}"
    );

    // Phase 1.4: alias quoting. Spark parses double-quoted identifiers
    // as string literals, so the `AS` clause must use backticks.
    assert!(
        sql.contains("AS `b.id`"),
        "expected Spark backtick alias `AS `b.id``; got:\n{sql}"
    );
    assert!(
        !sql.contains("AS \"b.id\""),
        "Databricks SQL leaked CH double-quoted alias; got:\n{sql}"
    );
}

/// Pattern comprehension `[(a)-[:R]->(x) | x.p]` lowers to a UNION-ALL branch
/// aggregated with a list aggregate. That aggregate must be dialect-aware:
/// CH `groupArray`, Spark `collect_list` (Spark has no `groupArray`).
#[tokio::test]
async fn pattern_comprehension_collect_list_per_dialect() {
    let cypher = "MATCH (a:User) RETURN a.id, [(a)-[:FOLLOWS]->(x:User) | x.id] AS following";

    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        ch.contains("groupArray"),
        "CH pattern comprehension should use `groupArray`; got:\n{ch}"
    );
    assert!(
        !ch.contains("collect_list"),
        "CH SQL leaked Spark `collect_list`; got:\n{ch}"
    );

    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        dbx.contains("collect_list"),
        "Databricks pattern comprehension should use `collect_list`; got:\n{dbx}"
    );
    assert!(
        !dbx.contains("groupArray"),
        "Databricks SQL leaked CH `groupArray`; got:\n{dbx}"
    );
}

/// Cypher `split(str, delim)` must map per dialect: CH `splitByChar(delim, str)`
/// (name + args swapped), Spark `split(str, delim)` (name change, Cypher arg
/// order). The arg swap is ClickHouse-only.
#[tokio::test]
async fn split_function_per_dialect() {
    let cypher = "MATCH (a:User) RETURN split(a.name, ' ') AS parts";

    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        ch.contains("splitByChar(' ', "),
        "CH should emit `splitByChar(delim, str)`; got:\n{ch}"
    );

    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        !dbx.contains("splitByChar"),
        "Databricks SQL leaked CH `splitByChar`; got:\n{dbx}"
    );
    // Spark `split(str, delim)` keeps Cypher arg order (str first).
    assert!(
        dbx.contains("split(") && dbx.contains(", ' ')"),
        "Databricks should emit `split(str, delim)` in Cypher arg order; got:\n{dbx}"
    );
}

/// UNWIND of a literal list lowers to array expansion. The base relation and
/// expansion syntax are dialect-specific: CH `FROM system.one` + `ARRAY JOIN`,
/// Spark `FROM (SELECT 1) AS _unwind` + `LATERAL VIEW explode` (Spark has
/// neither `system.one` nor `ARRAY JOIN`).
#[tokio::test]
async fn unwind_literal_per_dialect() {
    let cypher = "UNWIND [1, 2, 3, 4] AS x RETURN x, x * x AS sq ORDER BY x";

    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        ch.contains("FROM system.one") && ch.contains("ARRAY JOIN"),
        "CH UNWIND should use system.one + ARRAY JOIN; got:\n{ch}"
    );

    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        dbx.contains("LATERAL VIEW explode("),
        "Databricks UNWIND should use LATERAL VIEW explode; got:\n{dbx}"
    );
    assert!(
        dbx.contains("FROM (SELECT 1)"),
        "Databricks UNWIND-only needs a one-row subquery base; got:\n{dbx}"
    );
    assert!(
        !dbx.contains("system.one") && !dbx.contains("ARRAY JOIN"),
        "Databricks SQL leaked CH `system.one`/`ARRAY JOIN`; got:\n{dbx}"
    );
}

/// Regression for issue #401: an UNWIND-only segment wrapped into a CTE (via a
/// following WITH) must keep both its array expansion AND its one-row base
/// relation inside the CTE body. Previously the CTE body dropped both, leaving
/// the unwound variable undefined (`SELECT x ... WHERE x > 1` with no FROM/JOIN).
#[tokio::test]
async fn unwind_in_cte_per_dialect() {
    let cypher = "UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN x";

    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    // The CTE body must contain the base relation + ARRAY JOIN, not just `SELECT x`.
    assert!(
        ch.contains("FROM system.one") && ch.contains("ARRAY JOIN"),
        "CH UNWIND-in-CTE body should keep system.one + ARRAY JOIN; got:\n{ch}"
    );

    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        dbx.contains("FROM (SELECT 1)") && dbx.contains("LATERAL VIEW explode("),
        "Databricks UNWIND-in-CTE body should keep one-row base + LATERAL VIEW explode; got:\n{dbx}"
    );
    assert!(
        !dbx.contains("system.one") && !dbx.contains("ARRAY JOIN"),
        "Databricks UNWIND-in-CTE leaked CH `system.one`/`ARRAY JOIN`; got:\n{dbx}"
    );
}

/// Regression for issue #404: with MULTIPLE UNWINDs in one segment wrapped into
/// a CTE (`UNWIND .. AS x UNWIND .. AS y WITH x, y ...`), every UNWIND variable
/// must appear in the CTE projection. Previously only the outermost UNWIND alias
/// was recognized; the others were treated as graph aliases, expanded to nothing,
/// and dropped from the SELECT — so the outer query referenced an undefined column.
#[tokio::test]
async fn multi_unwind_in_cte_projects_all_vars() {
    let cypher = "UNWIND [1, 2] AS x UNWIND [3, 4] AS y WITH x, y WHERE x < y RETURN x, y";

    for dialect in [SqlDialect::ClickHouse, SqlDialect::Databricks] {
        let sql = with_query_context(
            QueryContext {
                dialect,
                ..QueryContext::default()
            },
            async { cypher_to_sql(cypher) },
        )
        .await;
        // Isolate the CTE body's projection list (between `(SELECT` and its `FROM`)
        // so we don't accidentally match the OUTER SELECT's `x_y.x AS "x"`.
        let cte_projection = sql
            .split("(SELECT")
            .nth(1)
            .and_then(|s| s.split("FROM").next())
            .unwrap_or("");
        // The bug dropped `x` from this projection, leaving only `y AS "y"`.
        assert!(
            cte_projection.contains("x AS") && cte_projection.contains("y AS"),
            "{dialect:?}: CTE projection must include both x and y; got projection:\n{cte_projection}\n--- full SQL ---\n{sql}"
        );
        // Both array expansions must be present (guards the #401 path too).
        let expansions = if dialect == SqlDialect::Databricks {
            sql.matches("LATERAL VIEW explode(").count()
        } else {
            sql.matches("ARRAY JOIN").count()
        };
        assert_eq!(
            expansions, 2,
            "{dialect:?}: expected 2 array expansions (x and y); got {expansions} in:\n{sql}"
        );
    }
}

/// Regression for issue #405: a bidirectional (undirected) variable-length path
/// combined with an UNWIND in the same WITH segment produces a *structured* CTE
/// body with an inline UNION (the two VLP direction branches). The plan-level
/// UNWIND expansion must appear in EVERY union branch — previously the union
/// branches of `Cte::to_sql` never emitted `array_join`, so `n` was referenced
/// but never expanded, yielding invalid SQL.
#[tokio::test]
async fn unwind_in_union_cte_body_per_dialect() {
    let cypher =
        "MATCH (a:User)-[:FOLLOWS*1..2]-(b:User) UNWIND [1, 2] AS n WITH b, n RETURN b.id, n";

    // ClickHouse: each of the two union branches needs its own ARRAY JOIN.
    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        ch.contains("UNION ALL") && ch.matches("ARRAY JOIN [1, 2] AS n").count() >= 2,
        "CH: each union branch must emit `ARRAY JOIN [1, 2] AS n`; got:\n{ch}"
    );

    // Databricks: each branch needs its own LATERAL VIEW explode.
    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        dbx.contains("UNION ALL")
            && dbx.matches("LATERAL VIEW explode(array(1, 2)) AS n").count() >= 2,
        "Databricks: each union branch must emit `LATERAL VIEW explode(array(1, 2)) AS n`; got:\n{dbx}"
    );
}

/// `RETURN DISTINCT expr ORDER BY expr`: Spark resolves ORDER BY against the
/// DISTINCT output, so the sort term must reference the projection's backtick
/// alias, not the underlying `table.col`. ClickHouse keeps `table.col`.
#[tokio::test]
async fn distinct_order_by_per_dialect() {
    let cypher = "MATCH (a:User) RETURN DISTINCT a.name ORDER BY a.name";

    let ch = with_query_context(
        QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        ch.contains("ORDER BY a.name"),
        "CH DISTINCT should order by `table.col`; got:\n{ch}"
    );

    let dbx = with_query_context(
        QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        },
        async { cypher_to_sql(cypher) },
    )
    .await;
    assert!(
        dbx.contains("ORDER BY `a.name`"),
        "Databricks DISTINCT should order by the backtick alias; got:\n{dbx}"
    );
    assert!(
        !dbx.contains("ORDER BY a.name "),
        "Databricks DISTINCT leaked raw `table.col` in ORDER BY; got:\n{dbx}"
    );
}

/// Aggregation path — exercises `build_outer_aggregate_select` and the
/// `extract_outer_aggregation_info` rewrite where ColumnAlias references
/// in GROUP BY / aggregate args get quoted via `quote_alias`. The plain
/// VLP test above only covers the final-SELECT `AS` path, so this test
/// guards the references-side routing from silently regressing back to
/// double-quoted identifiers.
#[tokio::test]
async fn aggregation_under_databricks_uses_backtick_alias_refs() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id AS bid, count(a) AS ct")
    })
    .await;

    // The aggregate result alias `ct` should emit `AS ` + backticks.
    // Non-aggregate alias `bid` likewise. No double-quoted aliases.
    assert!(
        !sql.contains("AS \""),
        "Databricks aggregation SQL leaked CH double-quoted alias; got:\n{sql}"
    );
    // Spot-check that at least one backtick alias appears in the AS
    // position. The exact site varies by plan shape, so use a
    // tolerant pattern.
    assert!(
        sql.contains("AS `"),
        "expected Spark backtick alias in aggregation SQL; got:\n{sql}"
    );
}

/// Phase 1.5: aggregate function_registry is dialect-aware. Under
/// Databricks dialect, Cypher `collect()` must emit Spark's
/// `collect_list(...)` (not CH's `groupArray(...)`). The `cypher_to_sql`
/// helper in this file goes through `generate_sql` → `to_sql_query.rs`,
/// which covers the aggregate-rendering path that calls
/// `mapping.name_for(dialect)`. The `to_sql.rs` and
/// `function_translator.rs` consumer sites are routed the same way
/// (verified by reading them) but aren't reached by this particular
/// test pipeline.
#[tokio::test]
async fn collect_under_databricks_emits_collect_list() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN collect(b.id) AS ids")
    })
    .await;

    assert!(
        sql.contains("collect_list("),
        "expected Spark `collect_list(...)` for Cypher `collect()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("groupArray("),
        "Databricks SQL leaked CH `groupArray`; got:\n{sql}"
    );
}

/// CH baseline for the collect path — guards against silently flipping
/// the CH side to Spark spellings.
#[tokio::test]
async fn collect_under_clickhouse_emits_group_array() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN collect(b.id) AS ids")
    })
    .await;

    assert!(
        sql.contains("groupArray("),
        "expected CH `groupArray(...)` for Cypher `collect()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("collect_list("),
        "CH SQL leaked Spark `collect_list`; got:\n{sql}"
    );
}

/// CH baseline for the aggregation path — guards against the CH side
/// silently flipping to backticks when we touch this code in the future.
#[tokio::test]
async fn aggregation_under_clickhouse_keeps_double_quoted_alias_refs() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id AS bid, count(a) AS ct")
    })
    .await;

    // CH historically emits double-quoted aliases here. Verify both
    // sites still produce the existing shape.
    assert!(
        sql.contains("AS \""),
        "expected CH double-quoted alias in aggregation SQL; got:\n{sql}"
    );
}

/// Phase 1.6: Cypher's type-conversion functions now route through the
/// function_registry's dialect-aware `name_for(...)` accessor. Under
/// Databricks dialect, `toInteger(x)` must emit Spark's function-call
/// cast alias `bigint(x)` (not CH's `toInt64(x)`).
#[tokio::test]
async fn tointeger_under_databricks_emits_bigint() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN toInteger(a.id) AS i")
    })
    .await;

    assert!(
        sql.contains("bigint("),
        "expected Spark `bigint(...)` for Cypher `toInteger()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("toInt64("),
        "Databricks SQL leaked CH `toInt64`; got:\n{sql}"
    );
}

/// CH baseline for `toInteger` — guards against the CH side silently
/// flipping to Spark spellings.
#[tokio::test]
async fn tointeger_under_clickhouse_emits_to_int64() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN toInteger(a.id) AS i")
    })
    .await;

    assert!(
        sql.contains("toInt64("),
        "expected CH `toInt64(...)` for Cypher `toInteger()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("bigint("),
        "CH SQL leaked Spark `bigint`; got:\n{sql}"
    );
}

/// Phase 1.6: `toFloat(x)` under Databricks emits Spark's `double(x)`
/// (function-call cast alias). The CH side keeps `toFloat64(x)`.
#[tokio::test]
async fn tofloat_under_databricks_emits_double() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN toFloat(a.id) AS f")
    })
    .await;

    assert!(
        sql.contains("double("),
        "expected Spark `double(...)` for Cypher `toFloat()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("toFloat64("),
        "Databricks SQL leaked CH `toFloat64`; got:\n{sql}"
    );
}

/// CH baseline for `toFloat` — guards against the CH side silently
/// flipping to Spark spellings.
#[tokio::test]
async fn tofloat_under_clickhouse_emits_to_float64() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN toFloat(a.id) AS f")
    })
    .await;

    assert!(
        sql.contains("toFloat64("),
        "expected CH `toFloat64(...)` for Cypher `toFloat()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("double("),
        "CH SQL leaked Spark `double`; got:\n{sql}"
    );
}

/// Phase 1.6: the ad-hoc `format!("toInt64({})", init)` wrapper around
/// integer-literal `reduce()` initial values now routes through
/// `FunctionMapper::cast_int64()`. Under Databricks dialect, the
/// arrayFold init should be wrapped in `bigint(0)` instead of
/// `toInt64(0)`. This exercises both consumer sites
/// (`to_sql.rs::ReduceExpr` and `to_sql_query.rs::ReduceExpr`) — they
/// route the same way and at least one is reached by this query plan.
#[tokio::test]
async fn reduce_init_under_databricks_uses_bigint_cast() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS total")
    })
    .await;

    // ClickHouse emits arrayFold(... , bigint(0)) under Spark routing.
    // The literal `0` argument to reduce() is the only int-literal init
    // here, so the bigint cast must appear and `toInt64` must not.
    assert!(
        sql.contains("bigint("),
        "expected Spark `bigint(...)` wrapping reduce init; got:\n{sql}"
    );
    assert!(
        !sql.contains("toInt64("),
        "Databricks SQL leaked CH `toInt64` reduce-init wrapper; got:\n{sql}"
    );
}

/// Phase 1.7: `shortestPath()` with bounded endpoints hits the BFS
/// optimization in `generate_bfs_shortest_path_sql`, which previously
/// hard-coded `toUInt16(...)` casts for the hop counter. Under
/// Databricks dialect, those now route through
/// `FunctionMapper::cast_uint16()` and emit Spark's `int(...)`. We
/// widen to `int` instead of the conceptually-closer `smallint`
/// because `max_hops` is a `u32` overridable via
/// `CLICKGRAPH_VLP_MAX_HOPS` with no upper bound — a signed 16-bit
/// cast could wrap.
#[tokio::test]
async fn shortestpath_bfs_under_databricks_uses_int_hop() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    // The BFS path wraps the hop counter in `cast_uint16()`. Under
    // Databricks that emits `int(...)` (Spark function-call cast).
    // Assert the call shape rather than bare `int(` since that could
    // arise from many idioms.
    assert!(
        sql.contains("int(0)") || sql.contains("int(hop)"),
        "expected Spark `int(0)`/`int(hop)` hop cast in BFS shortestPath SQL; got:\n{sql}"
    );
    assert!(
        !sql.contains("toUInt16("),
        "Databricks BFS SQL leaked CH `toUInt16`; got:\n{sql}"
    );
    assert!(
        !sql.contains("smallint("),
        "Databricks BFS SQL emitted `smallint` — should widen to `int` per Phase 1.7 docs; got:\n{sql}"
    );
}

/// CH baseline for the BFS shortestPath hop cast.
#[tokio::test]
async fn shortestpath_bfs_under_clickhouse_uses_uint16_hop() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    assert!(
        sql.contains("toUInt16("),
        "expected CH `toUInt16(...)` hop cast in BFS shortestPath SQL; got:\n{sql}"
    );
}

/// CH baseline for the reduce-init cast path.
#[tokio::test]
async fn reduce_init_under_clickhouse_uses_to_int64_cast() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS total")
    })
    .await;

    assert!(
        sql.contains("toInt64("),
        "expected CH `toInt64(...)` wrapping reduce init; got:\n{sql}"
    );
    assert!(
        !sql.contains("bigint("),
        "CH SQL leaked Spark `bigint` reduce-init wrapper; got:\n{sql}"
    );
}

/// Phase 1.6: `toString(x)` under Databricks emits Spark's `string(x)`
/// (function-call cast alias). The CH side keeps `toString(x)`.
#[tokio::test]
async fn tostring_under_databricks_emits_string() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User) RETURN toString(a.id) AS s")
    })
    .await;

    // Spark gets `string(x)`. CH would emit `toString(x)`. Be tolerant
    // of `string(` appearing elsewhere — assert it shows up in a call
    // shape (followed by `(` or part of `string(a`) and CH `toString`
    // does NOT appear.
    assert!(
        sql.contains("string("),
        "expected Spark `string(...)` for Cypher `toString()`; got:\n{sql}"
    );
    assert!(
        !sql.contains("toString("),
        "Databricks SQL leaked CH `toString`; got:\n{sql}"
    );
}

/// BFS shortestPath target branch under Databricks emits the structural
/// `min(CASE WHEN ... THEN ... END)` form for the conditional min, not
/// CH's `minIf(val, cond)`. Spark has no `minIf` — this is the load-bearing
/// rewrite for complex-13. Pairs with `count_if` for the existence guard.
#[tokio::test]
async fn shortestpath_bfs_under_databricks_uses_min_if_rewrite() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    // The target branch must use `count_if(...)` for the existence guard
    // (Spark spelling, not CH's `countIf`).
    assert!(
        sql.contains("count_if("),
        "expected Spark `count_if(...)` in BFS target branch; got:\n{sql}"
    );
    assert!(
        !sql.contains("countIf("),
        "Databricks BFS SQL leaked CH `countIf`; got:\n{sql}"
    );
    // The conditional min must collapse to `min(CASE WHEN cond THEN val END)`
    // since Spark has no `minIf`. Match the call-shape boundary so an
    // unrelated `min(` elsewhere can't satisfy the assertion alone.
    assert!(
        sql.contains("min(CASE WHEN"),
        "expected Spark `min(CASE WHEN ...)` conditional-min rewrite; got:\n{sql}"
    );
    assert!(
        !sql.contains("minIf("),
        "Databricks BFS SQL leaked CH `minIf`; got:\n{sql}"
    );
}

/// CH baseline for the BFS target branch — confirms the CH side still
/// uses native `countIf` and `minIf` byte-for-byte (no accidental cross-
/// dialect bleed from the Databricks rewrite).
#[tokio::test]
async fn shortestpath_bfs_under_clickhouse_keeps_native_min_if() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    assert!(
        sql.contains("countIf("),
        "expected CH `countIf(...)` in BFS target branch; got:\n{sql}"
    );
    assert!(
        sql.contains("minIf("),
        "expected CH `minIf(...)` in BFS target branch; got:\n{sql}"
    );
}

/// Undirected BFS shortestPath under Databricks collapses the
/// anchor/forward/reverse 3-branch recursive UNION ALL into a single
/// recursive branch joined against a bidirectional sub-UNION ALL over
/// the rel table. Spark requires exactly two children under a recursive
/// UNION ALL (anchor + recursive) — three children fail with
/// INVALID_RECURSIVE_CTE. This shape is the structural rewrite that
/// unblocks complex-13.
#[tokio::test]
async fn shortestpath_bfs_under_databricks_collapses_undirected_to_single_branch() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    // The single-branch recursive body should reference `neighbor.node_id`
    // — the alias of the bidirectional sub-UNION inside the JOIN.
    assert!(
        sql.contains("neighbor.node_id"),
        "expected Spark single-branch recursive form joining against `neighbor`; got:\n{sql}"
    );
    // And the bidirectional sub-UNION over the rel table must be present
    // as a non-recursive inner UNION ALL. Loose match on the structural
    // hint — the exact column names depend on the schema's rel keys.
    assert!(
        sql.contains("AS neighbor ON neighbor.prev = b.node_id"),
        "expected Spark bidirectional sub-UNION joined as `neighbor`; got:\n{sql}"
    );
    // The legacy 3-branch shape would have a *second* `FROM <bfs_cte> b`
    // (the reverse recursive branch) per BFS CTE. shortestPath emits two
    // BFS CTEs total (one per direction in the bidirectional outer UNION),
    // so the rewritten Spark form has exactly one `b` JOIN per BFS CTE = 2.
    // The legacy 3-branch shape would have 4 (2 BFS CTEs × 2 branches).
    let count_b_alias = sql.matches(" b\n    JOIN ").count();
    assert_eq!(
        count_b_alias, 2,
        "expected exactly one recursive branch per BFS CTE (2 total); got {count_b_alias}:\n{sql}"
    );
}

/// CH baseline for the undirected BFS shape — confirms the existing
/// 3-branch (anchor + forward + reverse) recursive UNION ALL is preserved
/// byte-for-byte. ClickHouse accepts N-ary recursive UNION ALL.
#[tokio::test]
async fn shortestpath_bfs_under_clickhouse_keeps_three_branch_undirected() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql(
            "MATCH p = shortestPath((a:User {id: 1})-[:FOLLOWS*]-(b:User {id: 2})) \
             RETURN length(p) AS hops",
        )
    })
    .await;

    // CH path uses the legacy 2 recursive branches (forward + reverse).
    // `neighbor` alias should NOT appear — that's the Spark-only rewrite.
    assert!(
        !sql.contains("neighbor.node_id"),
        "CH BFS SQL must not use Spark `neighbor` sub-UNION rewrite; got:\n{sql}"
    );
    // Confirm both recursive branches present in each BFS CTE — 2 BFS CTEs
    // × 2 branches each = 4 occurrences of `<bfs> b\n    JOIN`.
    let count_b_alias = sql.matches(" b\n    JOIN ").count();
    assert_eq!(
        count_b_alias, 4,
        "expected two recursive branches per BFS CTE on CH (4 total); got {count_b_alias}:\n{sql}"
    );
}

// ===========================================================================
// Native-function pass-through, end to end (sql_generator::passthrough).
//
// `dbx.` reaches Spark/Databricks native functions; the registry decides
// scalar vs aggregate (single prefix, no `dbxagg.`). `ch.`/`chagg.` stay
// ClickHouse-only. These exercise the full Cypher → SQL path under each
// dialect; the prefix-matching / cross-backend-rejection logic itself is
// unit-tested in `sql_generator::passthrough`.
// ===========================================================================

/// `dbx.<scalar>` under the Databricks dialect emits the bare Spark name.
#[tokio::test]
async fn dbx_scalar_passthrough_under_databricks() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN dbx.upper(u.name) AS n")
    })
    .await;
    assert!(
        sql.contains("upper(") && !sql.contains("dbx."),
        "expected bare `upper(` with the `dbx.` prefix stripped; got:\n{sql}"
    );
}

/// `dbx.<aggregate>` is recognised as an aggregate via the Spark registry
/// (no `dbxagg.` needed) and emitted bare.
#[tokio::test]
async fn dbx_aggregate_passthrough_under_databricks() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN dbx.collect_list(u.id) AS ids")
    })
    .await;
    assert!(
        sql.contains("collect_list(") && !sql.contains("dbx."),
        "expected bare `collect_list(` (registry-detected aggregate); got:\n{sql}"
    );
}

/// `dbx.percentile_approx(...)` — multi-arg aggregate, prefix stripped.
#[tokio::test]
async fn dbx_percentile_approx_under_databricks() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN dbx.percentile_approx(u.id, 0.95) AS p")
    })
    .await;
    assert!(
        sql.contains("percentile_approx("),
        "expected bare `percentile_approx(`; got:\n{sql}"
    );
}

/// Regression: `ch.`/`chagg.` pass-through still works under the ClickHouse
/// dialect, scalar and aggregate, with the prefix stripped.
#[tokio::test]
async fn ch_passthrough_still_works_under_clickhouse() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN ch.cityHash64(u.id) AS h, ch.uniq(u.id) AS c")
    })
    .await;
    assert!(
        sql.contains("cityHash64(") && sql.contains("uniq("),
        "expected bare CH `cityHash64(` (scalar) and `uniq(` (aggregate); got:\n{sql}"
    );
    assert!(
        !sql.contains("ch."),
        "CH prefix should be stripped from emitted SQL; got:\n{sql}"
    );
}

/// A foreign-backend prefix on the rendering path (which returns `String`,
/// not `Result`) is emitted **verbatim** so the query fails loudly at the
/// database on an unknown function — never silently stripped into a
/// valid-looking call. `ch.uniq` (aggregate) under Databricks.
#[tokio::test]
async fn foreign_aggregate_prefix_emitted_verbatim_under_databricks() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN ch.uniq(u.id) AS c")
    })
    .await;
    // The invalid prefixed name survives into the SQL (DB will reject it);
    // it is NOT silently stripped to a bare `uniq(`.
    assert!(
        sql.contains("ch.uniq("),
        "expected the foreign `ch.uniq(` to be emitted verbatim (loud failure); got:\n{sql}"
    );
}

/// Foreign scalar prefix `dbx.upper` under ClickHouse — emitted verbatim.
#[tokio::test]
async fn foreign_scalar_prefix_emitted_verbatim_under_clickhouse() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN dbx.upper(u.name) AS n")
    })
    .await;
    assert!(
        sql.contains("dbx.upper("),
        "expected the foreign `dbx.upper(` to be emitted verbatim; got:\n{sql}"
    );
}

/// Nested foreign pass-through as an argument must NOT be silently dropped.
/// `dbx.upper(ch.lower(u.name))` under Databricks: the outer `dbx.` strips
/// to `upper(...)`, the inner foreign `ch.lower` is emitted verbatim as the
/// argument — so the argument list is intact (no `upper()` with a lost arg).
#[tokio::test]
async fn nested_foreign_passthrough_arg_is_not_dropped() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (u:User) RETURN dbx.upper(ch.lower(u.name)) AS n")
    })
    .await;
    assert!(
        sql.contains("upper(ch.lower("),
        "expected nested arg preserved as `upper(ch.lower(`, not dropped; got:\n{sql}"
    );
}

/// `is_integer_literal` is the guard that keeps the Databricks anchor
/// cast from wrapping non-numeric IDs. Direct unit test — covers the
/// shape via the cypher_to_sql path elsewhere but locks the predicate
/// itself so refactors don't silently regress it.
#[test]
fn is_integer_literal_recognises_only_integers() {
    use crate::sql_generator::emitters::clickhouse::variable_length_cte::is_integer_literal;
    assert!(is_integer_literal("14"));
    assert!(is_integer_literal("-42"));
    assert!(is_integer_literal("0"));
    // Not integers: column refs, string literals, floats, empty, bare minus.
    assert!(!is_integer_literal("p.id"));
    assert!(!is_integer_literal("'abc'"));
    assert!(!is_integer_literal("3.14"));
    assert!(!is_integer_literal(""));
    assert!(!is_integer_literal("-"));
    assert!(!is_integer_literal("123abc"));
}
