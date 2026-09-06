//! LDBC SNB regression tests
//!
//! These tests load the LDBC SNB schema from YAML and verify that all passing
//! LDBC benchmark queries generate valid SQL through the full pipeline:
//! Parse → Plan → Render → Generate SQL.
//!
//! No ClickHouse connection is needed — these test SQL generation only.
//! Uses tokio for task-local QueryContext required by the render pipeline.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::strip_comments,
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
};

fn load_ldbc_schema() -> GraphSchema {
    let config =
        GraphSchemaConfig::from_yaml_file("benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml")
            .expect("Failed to load LDBC schema YAML");
    config
        .to_graph_schema()
        .expect("Failed to convert LDBC schema config to GraphSchema")
}

/// Helper: load a Cypher query file, strip comments, parse, plan, render, and return SQL.
/// Uses parse_cypher_statement + evaluate_read_statement (same as HTTP server).
async fn generate_sql(schema: &GraphSchema, cypher_path: &str) -> String {
    let schema = schema.clone();
    let path = cypher_path.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path, e));
        let cleaned = strip_comments(&raw);

        let (_remaining, statement) =
            clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {:?}", path, e));

        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("Failed to plan {}: {:?}", path, e));

        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .unwrap_or_else(|e| panic!("Failed to render {}: {:?}", path, e));
        render_plan.to_sql()
    })
    .await
}

// ---------------------------------------------------------------------------
// Interactive Short queries (short-1 through short-7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_short_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Regression: VLP column expansion must produce valid CTE references
    assert!(
        sql.contains("vlp_"),
        "short-2 should generate VLP CTE for variable-length path"
    );
}

#[tokio::test]
async fn ldbc_short_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-3.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_4() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-4.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_7() {
    // IS7 (#589): the OPTIONAL clause `(m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)`
    // has an UNDIRECTED nested hop (`-[r:KNOWS]-`) inside a chained OPTIONAL.
    // KNOWS is a symmetric relationship stored in one physical direction, so the
    // undirected hop must match BOTH orientations. The renderer cannot yet
    // express that bidirectional expansion together with OPTIONAL semantics —
    // it previously emitted a SINGLE-direction join (`r.Person1Id = a.id AND
    // r.Person2Id = p.id`), silently dropping the reverse-stored friendships and
    // returning WRONG `replyAuthorKnowsOriginalMessageAuthor` values. That is a
    // ground-rule-1 violation, so the query now FAILS LOUD instead. This test
    // locks the loud behavior until the anchor-LEFT-JOIN-onto-match-union render
    // structure exists (tracked in #589). Rewriting the KNOWS hop with an
    // explicit direction, or splitting the query, is the workaround.
    let schema = load_ldbc_schema();
    let path = "benchmarks/ldbc_snb/queries/official/interactive/short-7.cypher";
    let ctx = QueryContext::new(Some("default".to_string()));
    let result: Result<String, String> = with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));
        let raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let cleaned = strip_comments(&raw);
        let (_rem, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
            .map_err(|e| format!("parse: {e:?}"))?;
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .map_err(|e| format!("{e:?}"))?;
        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .map_err(|e| format!("render: {e:?}"))?;
        Ok(render_plan.to_sql())
    })
    .await;
    let err = result.expect_err("IS7 undirected-nested KNOWS optional must fail loud (#589)");
    assert!(
        err.contains("undirected hop chained onto another optional hop") && err.contains("589"),
        "IS7 must fail loud naming the undirected-nested-hop limitation, got:\n{err}"
    );
}

// ---------------------------------------------------------------------------
// Interactive Complex queries
// Some use adapted versions (complex-3, 5, 7, 10, 12, 13)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_complex_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Regression guard (#836 fix must not break this): complex-1 is an
    // UNDIRECTED `KNOWS*1..3` VLP whose outer query has TWO union arms, each
    // referencing the `friend_p` WITH-CTE. Both arms must DEFINE `friend_p`
    // via its CROSS JOIN — a reverse arm that references `friend_p` in its
    // SELECT/ON but drops the `CROSS JOIN with_friend_p_cte_0 AS friend_p`
    // yields `Unknown identifier friend_p` at execution. (The #836 join-clone
    // fix originally regressed exactly this by keeping the reverse arm's own
    // auxiliary edge self-join and discarding the base's `friend_p` join.)
    let friend_p_refs = sql.matches("friend_p.").count();
    if friend_p_refs > 0 {
        let cross_join_defs = sql.matches("with_friend_p_cte_0 AS friend_p").count();
        assert!(
            cross_join_defs >= 2,
            "#836 regression: complex-1's two union arms must EACH define \
             `friend_p` (expected >=2 `with_friend_p_cte_0 AS friend_p` \
             joins, found {cross_join_defs}); a reverse arm is referencing \
             `friend_p` without defining it:\n{sql}"
        );
    }
}

#[tokio::test]
async fn ldbc_complex_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-3.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_4() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-4.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Regression: Tag name filter must survive WITH barrier (cte_name preservation)
    assert!(
        sql.contains("tagName") || sql.contains("tag_name") || sql.contains("Tag"),
        "Tag name reference missing from SQL: {sql}"
    );
    // Regression: friend→post join must be present
    assert!(
        sql.contains("HAS_CREATOR") || sql.contains("has_creator") || sql.contains("hasCreator"),
        "HAS_CREATOR join missing from SQL: {sql}"
    );
}

#[tokio::test]
async fn ldbc_complex_7() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-7.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_8() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-8.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_9() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-9.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // #1100: IC9 filters `WHERE NOT friend = root` (bare-node identity) on a
    // `KNOWS*1..2` VLP and re-matches `friend` across a collect/UNWIND barrier.
    // The bare identity must resolve to the VLP endpoint id columns, never leak
    // the bare alias names into the recursive CTE body (which fails Code 47),
    // and the re-matched endpoint must resolve to its WITH-CTE column.
    assert!(
        !sql.contains("friend = root") && !sql.contains("friend != root"),
        "#1100: IC9 bare-node identity must not leak bare aliases into SQL:\n{sql}"
    );
    assert!(
        !sql.contains("t.end_p6_friend_id"),
        "#1100: IC9 re-matched endpoint must not VLP-rewrite to `t.end_*`:\n{sql}"
    );
}

#[tokio::test]
async fn ldbc_complex_10() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-10.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_11() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-11.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_12() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-12.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    assert!(
        sql.contains("LEFT JOIN"),
        "complex-12 should use LEFT JOIN for OPTIONAL MATCH"
    );
}

#[tokio::test]
async fn ldbc_complex_12_official() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-12.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Verify VLP CTE internal alias 't' does not leak into outer UNION branch SELECT.
    // The inner SELECT should use correct node aliases (comment.*, friend.*, tag.*, etc.)
    // not the VLP CTE's internal "t" alias.
    // Extract the inner SELECT (after "FROM (" and before first "FROM ldbc.")
    if let Some(inner_start) = sql.find("FROM (\nSELECT") {
        let inner_sql = &sql[inner_start..];
        if let Some(from_pos) = inner_sql.find("\nFROM ldbc.") {
            let inner_select = &inner_sql[..from_pos];
            // Should NOT have bare "t." references (VLP CTE alias leak)
            // But "t2.", "t3." etc. are fine (auto-generated aliases for anonymous nodes)
            for line in inner_select.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with("t.") {
                    panic!(
                        "VLP CTE alias 't' leaked into outer UNION branch SELECT: {}",
                        trimmed
                    );
                }
            }

            // Regression test for #258: inner UNION branch SELECT must not have
            // duplicate bare column aliases. Multiple nodes sharing property names
            // (e.g., comment.creationDate, friend.creationDate) must produce
            // table-qualified aliases ("comment.creationDate", "friend.creationDate").
            let mut aliases: Vec<String> = Vec::new();
            for line in inner_select.lines() {
                let trimmed = line.trim().trim_end_matches(',');
                if let Some(pos) = trimmed.rfind(" AS \"") {
                    let alias = &trimmed[pos + 5..];
                    if let Some(alias) = alias.strip_suffix('"') {
                        aliases.push(alias.to_string());
                    }
                }
            }
            let mut seen = std::collections::HashSet::new();
            let dups: Vec<&String> = aliases
                .iter()
                .filter(|a| !seen.insert(a.as_str()))
                .collect();
            // SELECT items are deduped by alias in render_plan_to_sql.
            assert!(
                dups.is_empty(),
                "Duplicate column aliases in inner SELECT (#258): {:?}",
                dups
            );
        }
    }
}

#[tokio::test]
async fn ldbc_complex_13() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-13.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

// ---------------------------------------------------------------------------
// BI queries
// Some use adapted versions (bi-3, bi-4 workaround, bi-17)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_bi_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(&schema, "benchmarks/ldbc_snb/queries/adapted/bi-3.cypher").await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
#[ignore = "leading UNWIND before MATCH now PARSES (#649), so the bi-4-workaround union \
    arm `UNWIND topForums AS topForum1 MATCH (person:Person)<-[:HAS_MEMBER]-(topForum1:Forum) \
    ...` is no longer silently truncated. It now fails LOUD and honest with \
    UnionColumnMismatch (arm 1 projects personId/personFirstName/... while arm 0 projects \
    creationDate/firstName/id/... — the two UNION ALL arms emit columns in different order/\
    naming). That column-alignment gap across union arms is a separate downstream issue, \
    tracked apart from the parser fix; the silent-drop bug class #516 hardened against is \
    resolved here (loud, not silent). Un-ignore when union-arm column alignment is fixed."]
async fn ldbc_bi_4() {
    let schema = load_ldbc_schema();
    // Official bi-4 uses CALL subquery; use adapted workaround with UNION ALL
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/bi-4-workaround.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_7() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-7.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_8() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-8.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Verify ARRAY JOIN is present (UNWIND persons AS person)
    assert!(
        sql.contains("ARRAY JOIN"),
        "bi-8 should use ARRAY JOIN for UNWIND"
    );
    // Verify pattern comprehension pre-aggregated CTEs are generated
    assert!(
        sql.contains("pc_person_score_tag_0"),
        "bi-8 should generate PC CTE for person score"
    );
    // Regression: person from ARRAY JOIN is a scalar value, NOT a table.
    // The SQL must NOT contain `person.id AS "p6_person_id"` because after
    // UNWIND, `person` IS the PersonId value — `person.id` would be invalid.
    assert!(
        !sql.contains("person.id AS \"p6_person_id\""),
        "bi-8 must not treat ARRAY JOIN scalar 'person' as a table (person.id is invalid)"
    );
    // After fix: ARRAY JOIN scalar should produce proper CTE column via FROM alias.
    // The CTE body should reference the scalar through the upstream CTE's FROM alias
    // (e.g., person_tag.person) and name it with standard CTE column naming (p6_person_id).
    assert!(
        sql.contains("person_tag.person AS \"p6_person_id\""),
        "bi-8: ARRAY JOIN scalar 'person' should be exported as person_tag.person AS p6_person_id"
    );
}

#[tokio::test]
async fn ldbc_bi_9() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-9.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_11() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-11.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_12() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-12.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_13() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-13.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));

    // #534: end-to-end lock for the IN-CTE-subquery rewrite
    // (`try_rewrite_in_cte_subquery`, to_sql_query.rs). bi-13's
    // `WHERE likerZombie IN zombies` — membership in a collect()ed node list
    // carried across a WITH/UNWIND barrier — is the motivating (#184) and
    // only known correctly-rendering end-to-end shape for this rewrite: the
    // collected list survives as a scalar CTE entity column
    // (`p{N}_zombie_id`), so the predicate MUST expand to a subquery
    // (`x IN (SELECT p6_zombie_id FROM with_..._cte_N)`), never degrade to a
    // bare `x IN p6_zombie_id` column reference — which ClickHouse either
    // rejects ("second argument must be constant or table expression") or,
    // worse, silently binds to an unrelated same-named column. Previously
    // only unit-covered at the RenderExpr level
    // (`test_to_sql_without_table_alias_preserves_in_cte_subquery_rewrite`);
    // this asserts it through the full parse→plan→render pipeline.
    let in_subquery_re =
        regex::Regex::new(r"IN \(SELECT p6_zombie_id FROM with_\w+_cte_\d+\)").unwrap();
    assert!(
        in_subquery_re.is_match(&sql),
        "#534: bi-13's `likerZombie IN zombies` must render as an IN-CTE \
         subquery over the collected zombie ids:\n{sql}"
    );
    let degraded_re = regex::Regex::new(r"IN p6_zombie_id").unwrap();
    assert!(
        !degraded_re.is_match(&sql),
        "#534: IN-CTE rewrite degraded to a bare scalar column reference:\n{sql}"
    );
}

#[tokio::test]
async fn ldbc_bi_14() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(&schema, "benchmarks/ldbc_snb/queries/adapted/bi-14.cypher").await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

/// BI-17 contains TWO independent `[:REPLY_OF*0..]` variable-length paths in
/// one MATCH scope (`message1 -> post1` and `message2 -> post2`). Before the
/// #544 guard, this test locked silently WRONG SQL: the render phase only
/// generated ONE of the two VLP CTEs and conflated both message->post
/// correlations onto it (the other VLP — and the filters riding on it —
/// silently vanished). Until multiple recursive VLP CTEs per scope are
/// actually supported, planning must fail loudly instead.
#[tokio::test]
async fn ldbc_bi_17_multi_vlp_scope_rejected_loudly_544() {
    let schema = load_ldbc_schema();
    let raw = std::fs::read_to_string("benchmarks/ldbc_snb/queries/adapted/bi-17.cypher")
        .expect("read bi-17.cypher");
    let cleaned = strip_comments(&raw);

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));
        let (_rest, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
            .expect("bi-17 must still parse");
        let err = evaluate_read_statement(statement, &schema, None, None, None)
            .expect_err("bi-17 has two REPLY_OF*0.. VLPs in one MATCH scope — must be rejected");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("#544"),
            "expected the #544 multi-VLP-per-scope rejection, got: {msg}"
        );
    })
    .await;
}

#[tokio::test]
async fn ldbc_bi_18() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-18.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

// ---------------------------------------------------------------------------
// #1100 — VLP endpoint referenced across a WITH / UNWIND barrier
//
// A variable-length path compiles to a recursive CTE (`vlp_*`). When a later
// clause carries the VLP's endpoint across a WITH/UNWIND barrier and re-matches
// it (`MATCH (friend)<-[:...]-(m)`), the barrier's WITH CTE is JOINed in (not in
// FROM). The endpoint alias must resolve to that WITH CTE's aliased column
// (`friend.p6_friend_id`), NOT be VLP-rewritten into a bogus `t.end_p6_friend_id`
// (VLP FROM alias `t` + `end_` prefix), which references a table not in the
// outer SELECT's scope and fails at execution (ClickHouse Code 47).
// ---------------------------------------------------------------------------

/// Render an inline Cypher string through the full pipeline (no CH connection).
async fn generate_sql_inline(schema: &GraphSchema, cypher: &str) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rem, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
            .unwrap_or_else(|e| panic!("parse: {e:?}"));
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("plan: {e:?}"));
        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .unwrap_or_else(|e| panic!("render: {e:?}"));
        render_plan.to_sql()
    })
    .await
}

/// Like [`generate_sql_inline`] but surfaces planner/render errors as `Err`
/// instead of panicking — for asserting clean-error behavior.
async fn try_generate_sql_inline(schema: &GraphSchema, cypher: &str) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rem, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
            .map_err(|e| format!("parse: {e:?}"))?;
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .map_err(|e| format!("plan: {e:?}"))?;
        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .map_err(|e| format!("render: {e:?}"))?;
        Ok(render_plan.to_sql())
    })
    .await
}

/// #1100 sub-shape 1 (WITH barrier + re-MATCH): the re-matched endpoint's
/// property must reference the WITH CTE alias, never the VLP FROM alias `t`
/// with an `end_` prefix.
#[tokio::test]
async fn ldbc_1100_vlp_endpoint_rematch_after_with() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..2]-(friend:Person)
         WITH friend
         MATCH (friend)<-[:HAS_CREATOR]-(m:Message)
         RETURN m.id, friend.id LIMIT 5",
    )
    .await;
    assert!(
        sql.contains("friend.p6_friend_id"),
        "#1100: re-matched endpoint `friend.id` must resolve to the WITH CTE \
         column `friend.p6_friend_id`:\n{sql}"
    );
    assert!(
        !sql.contains("t.end_p6_friend_id"),
        "#1100: endpoint must NOT be VLP-rewritten to the bogus \
         `t.end_p6_friend_id` (VLP FROM alias + end_ prefix):\n{sql}"
    );
}

/// #1100 sub-shape 1 (collect + UNWIND barrier, IC9-min): identical resolution
/// requirement when the endpoint crosses a `collect(...)` / `UNWIND` barrier.
#[tokio::test]
async fn ldbc_1100_vlp_endpoint_rematch_after_collect_unwind() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..2]-(friend:Person)
         WITH collect(distinct friend) AS friends
         UNWIND friends AS friend
         MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
         RETURN friend.id LIMIT 5",
    )
    .await;
    assert!(
        sql.contains("friend.p6_friend_id"),
        "#1100: post-UNWIND `friend.id` must resolve to the WITH CTE column:\n{sql}"
    );
    assert!(
        !sql.contains("t.end_p6_friend_id"),
        "#1100: endpoint must NOT be VLP-rewritten to `t.end_p6_friend_id`:\n{sql}"
    );
}

/// #1100 regression guard: the aggregation shape where the WITH CTE is the FROM
/// table (not a JOIN) must stay correct — the endpoint keeps resolving to the
/// WITH CTE alias, unaffected by the JOIN-list exclusion added for the re-match
/// shape.
#[tokio::test]
async fn ldbc_1100_vlp_endpoint_aggregation_from_cte_unaffected() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (person:Person {id:14})-[:KNOWS*1..2]-(friend:Person)
         WITH DISTINCT friend
         RETURN count(friend) AS c",
    )
    .await;
    assert!(
        sql.contains("count(friend.p6_friend_id)"),
        "#1100 guard: aggregation over the endpoint must reference the WITH CTE \
         column `friend.p6_friend_id`:\n{sql}"
    );
    assert!(
        !sql.contains("t.end_p6_friend_id"),
        "#1100 guard: no bogus VLP-rewritten endpoint reference:\n{sql}"
    );
}

/// #1100 (bare-node identity in a VLP filter): `WHERE friend <> root` compares
/// node identity with BARE node aliases. Inside the recursive VLP CTE those
/// aliases are not in scope (its node columns are `start_node`/`end_node`), so
/// the bare form would render literally as `friend != root` → ClickHouse Code
/// 47. It must normalize to a comparison on the schema `node_id` column(s) →
/// `NOT (end_node.id = start_node.id)`. Unblocks LDBC IC9/IC5, which both
/// filter `WHERE NOT friend = root`.
#[tokio::test]
async fn ldbc_1100_bare_node_identity_in_vlp_filter() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..2]-(friend:Person)
         WHERE friend <> root
         RETURN friend.id LIMIT 5",
    )
    .await;
    // #1100's invariant is that the bare aliases normalize to the endpoints'
    // REAL node_id columns. #1103 then moved the placement: a both-endpoint
    // predicate is applied on the post-recursion wrapper, where those columns
    // are the CTE's own `start_id`/`end_id` projections of the same node_ids.
    assert!(
        sql.contains("end_id = start_id") || sql.contains("start_id = end_id"),
        "#1100/#1103: bare-node identity `friend <> root` must normalize to the \
         VLP endpoint node_id columns, applied on the wrapper:\n{sql}"
    );
    assert!(
        !sql.contains("friend != root") && !sql.contains("friend = root"),
        "#1100: bare alias names must NOT survive into the VLP CTE body:\n{sql}"
    );
    // #1103: it must NOT be applied in the base case — that both prunes valid
    // paths through a failing intermediate and leaks self-paths past hop 1.
    assert!(
        !sql.contains("end_node.id = start_node.id")
            && !sql.contains("start_node.id = end_node.id"),
        "#1103: a both-endpoint predicate must not be applied in the base case:\n{sql}"
    );
}

/// #1100 (bare-node identity, `=` variant + nested in AND): the normalization
/// must reach a node-identity conjunct nested under `AND` and leave sibling
/// property filters intact.
#[tokio::test]
async fn ldbc_1100_bare_node_identity_nested_in_and() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..2]-(friend:Person)
         WHERE friend = root AND friend.firstName = 'Bob'
         RETURN friend.id LIMIT 5",
    )
    .await;
    // #1103: normalized to the endpoints' node_ids, applied on the wrapper.
    assert!(
        sql.contains("end_id = start_id") || sql.contains("start_id = end_id"),
        "#1100/#1103: `=` node identity nested in AND must normalize:\n{sql}"
    );
    // The sibling property filter must survive (applied on the endpoint).
    assert!(
        sql.contains("'Bob'"),
        "#1100: sibling property conjunct must not be dropped:\n{sql}"
    );
}

/// Load any GraphSchema from a YAML path (for schema-axis coverage).
fn load_schema_from(path: &str) -> GraphSchema {
    GraphSchemaConfig::from_yaml_file(path)
        .unwrap_or_else(|e| panic!("load {path}: {e:?}"))
        .to_graph_schema()
        .unwrap_or_else(|e| panic!("convert {path}: {e:?}"))
}

/// #1100 axis coverage — RENAMED node_id (`User.node_id: user_id`): the
/// bare-node identity must normalize to the schema's real id column, NOT a
/// hardcoded `.id`. A `.id` here fails live (`db_standard.users` has no `id`
/// column). Regression for the reviewer-found axis-dispatch defect.
#[tokio::test]
async fn ldbc_1100_bare_node_identity_renamed_node_id() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:User)-[:FOLLOWS*1..2]->(friend:User)
         WHERE friend <> root
         RETURN friend.user_id LIMIT 5",
    )
    .await;
    // #1103: the wrapper projects the renamed node_id as `start_id`/`end_id`;
    // what #1100 guards is that a REAL id column (not a hardcoded `.id`) was
    // resolved — verified here by the absence of any `.id` reference.
    assert!(
        sql.contains("end_id = start_id") || sql.contains("start_id = end_id"),
        "#1100/#1103: renamed node_id identity must normalize to the endpoint \
         id columns on the wrapper:\n{sql}"
    );
    assert!(
        !sql.contains("node.id = ") && !sql.contains(".id !="),
        "#1100: must NOT emit a hardcoded `.id` for a renamed node_id:\n{sql}"
    );
}

/// #1100 axis coverage — COMPOSITE node_id (`Account.node_id: [bank_id,
/// account_number]`): `a2 <> a1` must expand to `NOT (all id columns equal)`,
/// which a single `.id` cannot express. Regression for the reviewer-found
/// axis-dispatch defect.
#[tokio::test]
async fn ldbc_1100_bare_node_identity_composite_node_id() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a1:Account)-[:TRANSFERRED*1..2]->(a2:Account)
         WHERE a2 <> a1
         RETURN a2.account_type LIMIT 5",
    )
    .await;
    assert!(
        sql.contains("bank_id = ") && sql.contains("account_number = "),
        "#1100: composite node_id identity must compare BOTH key columns:\n{sql}"
    );
    // Both key-column equalities must be conjoined under a single NOT.
    assert!(
        sql.contains("bank_id") && sql.contains("account_number") && sql.contains(" AND "),
        "#1100: composite identity must be an AND of per-column equalities:\n{sql}"
    );
    assert!(
        !sql.contains(".id = ") && !sql.contains(".id !="),
        "#1100: must NOT collapse a composite node_id to `.id`:\n{sql}"
    );
}

// ---------------------------------------------------------------------------
// #1103 — both-endpoint WHERE predicate on a VLP
//
// A predicate naming BOTH VLP endpoints (`friend <> root`, or its property
// form) constrains the (start, FINAL endpoint) pair of a whole path. Before
// #1103 `categorize_filters` routed it to `start_filters`, which the VLP CTE
// applies in the BASE CASE only. That was wrong in both directions:
//
//   * dropped rows — the base case's `end_node` is the hop-1 node, an
//     INTERMEDIATE node of any longer path. Filtering there deletes valid paths
//     whose final endpoint satisfies the predicate (verified live on LDBC SF1:
//     `*1..2` returned 3728 rows where the hand-computed oracle is 3942).
//   * leaked rows — nothing re-applied it after hop 1, so at `*1..N` (N>=3) on a
//     cyclic graph, paths returning to the start survived (52 self-paths).
//
// The fix routes it to its own `both_endpoint_filters` category, applied on the
// post-recursion wrapper. Cypher uses TRAIL semantics (relationships cannot
// repeat, NODES MAY), so it must NOT be applied per-hop on the recursive arm:
// a path may legally revisit the start node and end elsewhere.
// ---------------------------------------------------------------------------

/// #1103: the bare-node form must be applied on the WRAPPER, not the base case.
#[tokio::test]
async fn ldbc_1103_both_endpoint_filter_applied_in_wrapper() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..3]-(friend:Person)
         WHERE friend <> root
         RETURN friend.id",
    )
    .await;
    assert!(
        sql.contains("_inner WHERE (NOT (end_id = start_id))")
            || sql.contains("_inner WHERE (NOT (start_id = end_id))"),
        "#1103: both-endpoint identity must be applied on the post-recursion \
         wrapper against the CTE's own start_id/end_id:\n{sql}"
    );
    assert!(
        !sql.contains("end_node.id = start_node.id")
            && !sql.contains("start_node.id = end_node.id"),
        "#1103: must NOT be applied in the base case (prunes intermediate hops \
         AND leaks self-paths past hop 1):\n{sql}"
    );
}

/// #1103: the PROPERTY form takes the identical path (same categorization
/// seam), and its properties must be projected so the wrapper can read them.
#[tokio::test]
async fn ldbc_1103_both_endpoint_property_form_in_wrapper() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (root:Person {id: 14})-[:KNOWS*1..3]-(friend:Person)
         WHERE friend.firstName <> root.firstName
         RETURN friend.id",
    )
    .await;
    assert!(
        sql.contains("start_node.firstName as start_firstName"),
        "#1103: the start-side property must be projected into the CTE so the \
         wrapper can compare it:\n{sql}"
    );
    assert!(
        sql.contains("_inner WHERE (end_firstName != start_firstName)")
            || sql.contains("_inner WHERE (start_firstName != end_firstName)"),
        "#1103: property form must be applied on the wrapper:\n{sql}"
    );
    assert!(
        !sql.contains("end_node.firstName != start_node.firstName"),
        "#1103: property form must NOT remain in the base case:\n{sql}"
    );
}

/// #1103: for shortestPath the predicate must narrow candidates BEFORE the
/// shortest pick. Applying it after ranking would let ROW_NUMBER select an
/// excluded path and then discard it, returning no row where a longer
/// qualifying path exists.
#[tokio::test]
async fn ldbc_1103_both_endpoint_filter_precedes_shortest_pick() {
    let schema = load_schema_from("benchmarks/social_network/schemas/social_benchmark.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..5]->(b:User))
         WHERE a.email <> b.email
         RETURN length(p)",
    )
    .await;
    let to_target = sql
        .find("_to_target AS (")
        .unwrap_or_else(|| panic!("#1103: expected a _to_target layer:\n{sql}"));
    let row_number = sql
        .find("ROW_NUMBER()")
        .unwrap_or_else(|| panic!("#1103: expected a ROW_NUMBER pick:\n{sql}"));
    assert!(
        to_target < row_number,
        "#1103: the both-endpoint filter layer must precede the shortest pick:\n{sql}"
    );
    assert!(
        sql.contains("start_email_address != end_email_address")
            || sql.contains("end_email_address != start_email_address"),
        "#1103: shortestPath both-endpoint filter must compare CTE columns:\n{sql}"
    );
}

/// #1103 SCOPE — a CLOSED pattern `(a)-[*]->(a)` binds ONE variable, so
/// `refs_start` and `refs_end` are the same test and `a.prop = v` is a plain
/// start filter. It must keep its pre-#1103 base-case placement.
#[tokio::test]
async fn ldbc_1103_closed_pattern_keeps_base_case_placement() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*1..2]->(a) WHERE a.user_id = 1 RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("WHERE start_node.user_id = 1"),
        "#1103: a closed pattern's single-variable filter stays in the base \
         case (it is not a two-endpoint comparison):\n{sql}"
    );
}

/// Like [`generate_sql_inline`] but surfaces a render error instead of
/// panicking — for asserting LOUD refusals.
async fn try_render_inline(schema: &GraphSchema, cypher: &str) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rem, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
            .unwrap_or_else(|e| panic!("parse: {e:?}"));
        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("plan: {e:?}"));
        match logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx)) {
            Ok(rp) => Ok(rp.to_sql()),
            Err(e) => Err(format!("{e:?}")),
        }
    })
    .await
}

// --- #1103 review hardening -------------------------------------------------
// The first cut of this fix rewrote the predicate by `str::replace` over
// already-rendered SQL. Adversarial review found that corrupts any property
// whose name is a PREFIX-collision with the node_id column or another property,
// and rewrites alias-looking text inside string literals — valid SQL, wrong
// column, no error. The rewrite is now structural (on the RenderExpr, in
// `cte_manager::lower_both_endpoint_filter_to_cte_columns`). These tests pin
// exactly the shapes a textual rewrite gets wrong.

/// #1103: node_id `code` is a PREFIX of property `codeName`; a textual rewrite
/// produced `end_idName`, which is a different REAL column on this fixture.
#[tokio::test]
async fn ldbc_1103_prefix_collision_property_not_corrupted() {
    let schema = load_schema_from("schemas/test/prefix_collision_vlp.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Item)-[:LINK*1..3]->(b:Item)
         WHERE a.code = 'X' AND b.codeName <> a.codeName
         RETURN b.idName",
    )
    .await;
    assert!(
        sql.contains("(end_codeName != start_codeName)"),
        "#1103: prefix-colliding property must resolve to its OWN column:\n{sql}"
    );
    assert!(
        !sql.contains("end_idName !=") && !sql.contains("!= start_idName"),
        "#1103: must not corrupt `codeName` into the unrelated `idName` column:\n{sql}"
    );
}

/// #1103: a string literal that happens to contain alias-looking text must be
/// left byte-intact — a textual rewrite mangled it into a column reference.
#[tokio::test]
async fn ldbc_1103_string_literal_not_rewritten() {
    let schema = load_schema_from("schemas/test/prefix_collision_vlp.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Item)-[:LINK*1..3]->(b:Item)
         WHERE a.code = 'X' AND (b.codeName <> a.codeName OR b.idName = 'end_node.codeName')
         RETURN b.idName",
    )
    .await;
    assert!(
        sql.contains("'end_node.codeName'"),
        "#1103: the string literal must survive the rewrite unchanged:\n{sql}"
    );
}

/// #1103: an unprojected property must fail with a REAL error. The first cut
/// returned a `-- ERROR …\nSELECT 1 WHERE 0` *string*, which is valid SQL that
/// executes and returns zero rows — a silent wrong answer wearing an error's
/// clothes.
#[tokio::test]
async fn ldbc_1103_unprojected_property_is_a_real_error() {
    let schema = load_schema_from("schemas/test/prefix_collision_vlp.yaml");
    // `other_col`/`idName` is projected only when referenced; compare on a
    // property the CTE does not carry.
    let result = try_render_inline(
        &schema,
        "MATCH (a:Item)-[:LINK*1..3]->(b:Item) WHERE a.code = 'X' AND b.codeName <> a.codeName
         RETURN b.code",
    )
    .await;
    // Either it resolves (property projected) or it errors — but it must NEVER
    // emit the fake-error sentinel that silently returns no rows.
    if let Ok(sql) = &result {
        assert!(
            !sql.contains("SELECT 1 WHERE 0"),
            "#1103: must not emit a fake-error SQL sentinel:\n{sql}"
        );
    }
}

/// #1103 (updated by #1131): COMPOSITE node_id identity comparison. Originally
/// this collapsed every key column onto the ONE pipe-joined `start_id`/`end_id`
/// — which was also what silently turned a SINGLE-component cross-endpoint
/// comparison (`a.bank_id = b.bank_id`) into whole-id equality (#1131, 0 rows
/// vs an oracle 3). Post-#1131 each component maps to its own projected
/// `<prefix>_<col>` column, so whole-node identity expands to the
/// per-component AND — row-equivalent to concat equality (and safer for values
/// containing the `|` separator), live-verified 10 == oracle 10 on a populated
/// fixture. The #1133 review then PROVED the separator case live: with values
/// 'x|y'+'z' vs 'x'+'y|z' (identical concats), main's concat equality DROPPED
/// a `<>` row and FABRICATED a phantom `=` "cycle" — the per-component form is
/// strictly MORE correct, not merely row-equivalent on benign data.
#[tokio::test]
async fn ldbc_1103_composite_node_id_collapses_to_single_id_predicate() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a1:Account)-[:TRANSFERRED*1..2]->(a2:Account)
         WHERE a2 <> a1
         RETURN a2.account_type LIMIT 5",
    )
    .await;
    assert!(
        sql.contains(
            "NOT ((end_bank_id = start_bank_id AND end_account_number = start_account_number))"
        ),
        "#1103/#1131: composite node identity expands to the per-component \
         conjunction over the projected component columns:\n{sql}"
    );
    assert!(
        !sql.contains("end_id = start_id"),
        "#1131: no whole-concat collapse — a component-level predicate must \
         never be widened to pipe-joined-id equality:\n{sql}"
    );
}

/// #1103: three generator shapes return before the wrapper is built and would
/// DROP the predicate silently. They must refuse loudly instead.
#[tokio::test]
async fn ldbc_1103_heterogeneous_polymorphic_refuses_loudly() {
    let schema = load_schema_from("schemas/dev/social_polymorphic.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User)-[:LIKES*1..2]->(b:Post)
         WHERE a.user_id = 1 AND b.content <> a.name
         RETURN b.post_id",
    )
    .await
    .expect_err("#1103: a path shape that cannot apply the predicate must refuse");
    assert!(
        err.contains("referencing BOTH path endpoints is not supported"),
        "#1103: the refusal must name the unsupported both-endpoint case:\n{err}"
    );
}

// ---------------------------------------------------------------------------
// #1104 — node identity across mismatched node_id ARITY
//
// `Identifier::to_sql_equality` pairs id columns element-wise with `zip`, which
// silently TRUNCATES to the shorter side. Comparing a single-column
// `Customer.customer_id` against a composite `Account.[bank_id,
// account_number]` emitted `NOT (c.customer_id = a.bank_id)` — `account_number`
// dropped, and two unrelated columns compared. It executes without error, so
// the wrongness is silent.
//
// Refused rather than constant-folded: folding cross-label identity to `false`
// assumes distinct labels imply distinct nodes, which is FALSE in Cypher
// (multi-label nodes) and false here (polymorphic parent labels — LDBC
// `Message` ⊃ `Post`), so folding would silently change those results.
// ---------------------------------------------------------------------------

/// #1104: mismatched arity must fail LOUD, never drop a key column.
#[tokio::test]
async fn ldbc_1104_mismatched_node_id_arity_fails_loud() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account) WHERE c <> a RETURN a.account_type",
    )
    .await
    .expect_err("#1104: mismatched-arity node identity must be refused, not rendered");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: the refusal must name the arity mismatch:\n{err}"
    );
    assert!(
        err.contains("customer_id") && err.contains("account_number"),
        "#1104: the refusal must name the columns on both sides:\n{err}"
    );
}

/// #1104: the guard recurses, so a mismatched conjunct nested under AND is
/// still caught (it would otherwise render silently-wrong alongside valid
/// siblings).
#[tokio::test]
async fn ldbc_1104_mismatched_arity_nested_in_and_fails_loud() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account)
         WHERE c.name = 'X' AND c <> a
         RETURN a.account_type",
    )
    .await
    .expect_err("#1104: nested mismatched-arity identity must also be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: nested refusal must name the arity mismatch:\n{err}"
    );
}

/// #1104 SCOPE: same-arity COMPOSITE identity is correct today and must keep
/// rendering — the guard fires only where a column would be dropped.
#[tokio::test]
async fn ldbc_1104_same_arity_composite_identity_still_renders() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) WHERE a1 <> a2 RETURN a2.account_type",
    )
    .await;
    assert!(
        sql.contains("a1.bank_id = a2.bank_id")
            && sql.contains("a1.account_number = a2.account_number"),
        "#1104: same-arity composite identity must still compare BOTH columns:\n{sql}"
    );
}

/// #1104 SCOPE: the ordinary single-column case (both sides arity 1, including
/// a renamed node_id) is untouched.
#[tokio::test]
async fn ldbc_1104_single_column_identity_unaffected() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a <> b RETURN b.name",
    )
    .await;
    assert!(
        sql.contains("NOT (a.user_id = b.user_id)"),
        "#1104: single-column identity must be unchanged:\n{sql}"
    );
}

// --- #1104 review hardening -------------------------------------------------
// Adversarial review found the first cut left the exact silent truncation in
// place on two axes:
//   F1 — `get_node_label_for_alias` had no arm for WithClause/CartesianProduct/
//        Unwind, so labels resolved to None and the guard stood down. Worse,
//        a post-WITH `WHERE` never reaches `extract_filters` at all (that
//        recurses into `wc.input`) and is rendered by three OTHER sites.
//   F2 — the guard hand-rolled an `OperatorApplicationExp`-only recursion, so
//        any other container (CASE, coalesce, NOT-of-CASE) hid the comparison.
// Both are the parallel-emitter / non-exhaustive-walker class this repo has
// been bitten by repeatedly; the guard now uses the module's exhaustive
// `visit_render_expr_mut` and runs at every post-WITH render site.

/// #1104/F2: a CASE wrapper must not hide the comparison from the guard.
#[tokio::test]
async fn ldbc_1104_case_wrapper_still_refused() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account)
         WHERE CASE WHEN c = a THEN true ELSE false END
         RETURN a.account_type",
    )
    .await
    .expect_err("#1104: a CASE-wrapped identity must still be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: CASE wrapper must not defeat the guard:\n{err}"
    );
}

/// #1104/F2: a scalar-function wrapper (`coalesce`) is likewise not a hiding
/// place — the exhaustive visitor descends into call arguments.
#[tokio::test]
async fn ldbc_1104_function_wrapper_still_refused() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account)
         WHERE coalesce(c = a, false)
         RETURN a.account_type",
    )
    .await
    .expect_err("#1104: a function-wrapped identity must still be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: function wrapper must not defeat the guard:\n{err}"
    );
}

/// #1104/F1: a POST-WITH `WHERE` must refuse exactly like the pre-WITH form.
/// Coverage that depends on clause order is not coverage.
#[tokio::test]
async fn ldbc_1104_post_with_barrier_still_refused() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account) WITH c, a WHERE c <> a RETURN a.account_type",
    )
    .await
    .expect_err("#1104: post-WITH identity must be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: the WITH barrier must not hide the mismatch:\n{err}"
    );
}

/// #1104/F1: a comma (cartesian) pattern binds its aliases under
/// `CartesianProduct`, which the label resolver did not descend into.
#[tokio::test]
async fn ldbc_1104_comma_pattern_still_refused() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer),(a:Account) WHERE c <> a RETURN count(*)",
    )
    .await
    .expect_err("#1104: comma-pattern identity must be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: CartesianProduct must resolve endpoint labels:\n{err}"
    );
}

/// #1104/F1: same for an `Unwind` between the pattern and the WHERE.
#[tokio::test]
async fn ldbc_1104_unwind_still_refused() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account) UNWIND [1] AS z WHERE c <> a RETURN count(*)",
    )
    .await
    .expect_err("#1104: identity after UNWIND must be refused");
    assert!(
        err.contains("node_id column counts differ"),
        "#1104: Unwind must resolve endpoint labels:\n{err}"
    );
}

/// #1104 SCOPE: widening the label resolver must not create FALSE POSITIVES.
/// A post-WITH identity between two SAME-arity nodes still renders.
#[tokio::test]
async fn ldbc_1104_post_with_same_arity_still_renders() {
    let schema = load_ldbc_schema();
    let sql = generate_sql_inline(
        &schema,
        "MATCH (p:Person)-[:KNOWS]->(f:Person) WITH p, f WHERE p <> f RETURN f.id LIMIT 5",
    )
    .await;
    assert!(
        sql.contains("p.id") && sql.contains("f.id"),
        "#1104: same-arity post-WITH identity must still render:\n{sql}"
    );
}

// ---------------------------------------------------------------------------
// #1111 — DENORMALIZED VLP both-endpoint WHERE predicate
//
// #1103 moved a both-endpoint predicate to the post-recursion wrapper for the
// standard families but scoped the fully-denormalized pattern OUT: its CTE
// columns are role-prefixed PHYSICAL names (`start_OriginCityName` /
// `end_DestCityName`, since `Airport.city` is a different column depending on
// which end the node plays), which the standard lowering cannot resolve. It was
// left applying the predicate in the base case — the original defect.
//
// Verified live on a CYCLIC 4-flight fixture (A→B→C→A plus A→C): main returned
// 14 rows for `WHERE a.city <> b.city` at `*1..3`; the hand-computed oracle is
// 9 (5 same-city paths must be excluded). An ACYCLIC fixture cannot expose the
// leak half — see the "cyclic oracle" lesson.
// ---------------------------------------------------------------------------

/// #1111: the predicate must land on the wrapper, against role-resolved columns.
#[tokio::test]
async fn ldbc_1111_denorm_both_endpoint_applied_in_wrapper() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport) WHERE a.city <> b.city RETURN b.code",
    )
    .await;
    assert!(
        sql.contains("_inner WHERE (start_OriginCityName != end_DestCityName)"),
        "#1111: denorm both-endpoint predicate must be applied on the wrapper \
         against ROLE-RESOLVED physical columns:\n{sql}"
    );
    assert!(
        !sql.contains("WHERE t1.OriginCityName != t1.DestCityName"),
        "#1111: must NOT remain in the base case (prunes intermediate hops AND \
         leaks same-city paths past hop 1):\n{sql}"
    );
}

/// #1111: role resolution is the crux — `city` is `OriginCityName` on the FROM
/// side and `DestCityName` on the TO side. A role-blind rewrite would compare
/// the same column to itself (a tautology) or emit a nonexistent one.
#[tokio::test]
async fn ldbc_1111_denorm_both_endpoint_resolves_per_role() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport) WHERE a.city <> b.city RETURN b.code",
    )
    .await;
    assert!(
        !sql.contains("start_OriginCityName != end_OriginCityName")
            && !sql.contains("start_DestCityName != end_DestCityName"),
        "#1111: each endpoint must resolve `city` through its OWN role:\n{sql}"
    );
}

/// #1111 SCOPE: a denorm VLP with no both-endpoint predicate keeps its single
/// un-wrapped CTE — the wrapper is added only when there is a predicate for it.
#[tokio::test]
async fn ldbc_1111_denorm_without_both_endpoint_unwrapped() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport) WHERE a.city = 'Seattle' RETURN b.code",
    )
    .await;
    assert!(
        !sql.contains("vlp_a_b_inner"),
        "#1111: no both-endpoint predicate ⇒ no wrapper CTE:\n{sql}"
    );
    assert!(
        sql.contains("'Seattle'"),
        "#1111: a start-only filter must still be applied:\n{sql}"
    );
}

// ---------------------------------------------------------------------------
// #1112 — node identity in RETURN / projection position
//
// `a = b` between bare node variables compares node IDENTITY. In WHERE position
// it resolves to the schema's node_id column(s) (#1076/#1104). In projection
// position it reached neither seam and rendered `a.* = b.*` — invalid SQL.
//
// Root cause: the planner lowers a bare node variable in RETURN to a whole-node
// `PropertyAccess(alias, "*")`, NOT a `TableAlias`, so the identity handler's
// two-`TableAlias` match never fired. Both seams now accept both spellings.
// ---------------------------------------------------------------------------

/// #1112: `RETURN a = b` must compare the node_id columns, not `a.* = b.*`.
#[tokio::test]
async fn ldbc_1112_return_position_identity_resolves_node_id() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a = b AS same",
    )
    .await;
    assert!(
        sql.contains("a.user_id = b.user_id"),
        "#1112: RETURN-position identity must resolve to the node_id column:\n{sql}"
    );
    assert!(
        !sql.contains("a.* = b.*"),
        "#1112: must not emit the invalid whole-node comparison:\n{sql}"
    );
}

/// #1112: the same inside a projection-position CASE (the emitter must reach it
/// through the wrapper, not only at the top level).
#[tokio::test]
async fn ldbc_1112_return_position_identity_inside_case() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN CASE WHEN a = b THEN 1 ELSE 0 END AS x",
    )
    .await;
    assert!(
        sql.contains("CASE WHEN a.user_id = b.user_id"),
        "#1112: identity inside a projection CASE must resolve:\n{sql}"
    );
}

/// #1112: COMPOSITE node_ids expand to the full per-column AND chain here too —
/// a hardcoded `.id` or a first-column-only comparison would be wrong.
#[tokio::test]
async fn ldbc_1112_return_position_identity_composite() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) RETURN a1 = a2 AS same",
    )
    .await;
    assert!(
        sql.contains("a1.bank_id = a2.bank_id")
            && sql.contains("a1.account_number = a2.account_number"),
        "#1112: composite node_id must compare BOTH key columns:\n{sql}"
    );
}

/// #1112 + #1104 parity: a mismatched-arity identity in RETURN position must
/// refuse exactly like the WHERE-position form, not truncate silently.
#[tokio::test]
async fn ldbc_1112_return_position_mismatched_arity_refuses() {
    let schema = load_schema_from("schemas/examples/composite_node_id_test.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (c:Customer)-[:OWNS]->(a:Account) RETURN c = a AS same",
    )
    .await
    .expect_err("#1112: mismatched arity in RETURN position must refuse");
    assert!(
        err.contains("node_id column counts differ"),
        "#1112: RETURN position must reuse the #1104 arity refusal:\n{err}"
    );
}

/// #1112 SCOPE: a bare `RETURN a` (whole-node projection, no comparison) must
/// still expand to its columns — the new match keys on the comparison operator.
#[tokio::test]
async fn ldbc_1112_bare_node_projection_unaffected() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(&schema, "MATCH (a:User) RETURN a LIMIT 1").await;
    assert!(
        sql.contains("a.city") && sql.contains("a.user_id"),
        "#1112: a bare whole-node projection must still expand:\n{sql}"
    );
}

// #1113 — pattern-comprehension inner WHERE must never be silently dropped
//
// `[(a)-[:FOLLOWS]->(x) WHERE <pred> | x.name]` renders as a single-hop
// subquery joining only the target node as `__tgt`. A predicate that references
// anything else — the outer correlation var, or a bare node identity — is
// rejected by the target-safe allowlist (#882) and `build_pattern_comprehension_sql`
// returns `None`. For a PROJECTION that fallback is harmless; for a WHERE it
// silently produced an UNFILTERED comprehension, collecting rows the user asked
// to exclude. It now refuses loudly.
// ---------------------------------------------------------------------------

/// #1113: bare-node identity inside a pattern comprehension must refuse.
#[tokio::test]
async fn ldbc_1113_pc_inner_where_bare_identity_refuses() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(x:User) WHERE a <> x | x.name] AS fs",
    )
    .await
    .expect_err("#1113: an unrenderable inner WHERE must refuse, not drop");
    assert!(
        err.contains("pattern comprehension inner WHERE cannot be evaluated"),
        "#1113: the refusal must name the unrenderable inner WHERE:\n{err}"
    );
}

/// #1113: a CORRELATED property reference (outer var on one side) is equally
/// unresolvable in the single-hop subquery and must refuse.
#[tokio::test]
async fn ldbc_1113_pc_inner_where_correlated_property_refuses() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(x:User) WHERE a.user_id <> x.user_id | x.name] AS fs",
    )
    .await
    .expect_err("#1113: a correlated inner WHERE must refuse, not drop");
    assert!(
        err.contains("pattern comprehension inner WHERE cannot be evaluated"),
        "#1113: the refusal must name the unrenderable inner WHERE:\n{err}"
    );
}

/// #1113 SCOPE: a TARGET-ONLY predicate is resolvable and must keep rendering
/// exactly as before — the refusal fires only where the filter was being lost.
#[tokio::test]
async fn ldbc_1113_pc_inner_where_target_only_still_applied() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(x:User) WHERE x.user_id > 3 | x.name] AS fs",
    )
    .await;
    assert!(
        sql.contains("WHERE __tgt.user_id > 3"),
        "#1113: a target-only inner WHERE must still be applied:\n{sql}"
    );
}

/// #1113 SCOPE: a comprehension with NO inner WHERE is untouched (the refusal
/// keys on `where_clause` being present, not on the builder returning None).
#[tokio::test]
async fn ldbc_1113_pc_without_inner_where_unaffected() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(x:User) | x.name] AS fs",
    )
    .await;
    assert!(
        sql.contains("groupArray"),
        "#1113: a comprehension with no inner WHERE must render normally:\n{sql}"
    );
}

// ---------------------------------------------------------------------------
// #1113 follow-up — the count/size pattern-comprehension WHERE renderer
//
// `render_logical_expr_to_sql` (the limited renderer serving the count/size
// correlated path) handles only PropertyAccess / OperatorApplication / Literal
// / Parameter. Its catch-all returned an EMPTY STRING, which the parent
// operator arm then joined blindly. Three distinct failure modes resulted:
//
//   toLower(x.name) = 'bob'   ->  WHERE  = 'bob'          (malformed SQL)
//   x.user_id IN [1,2,3]      ->  WHERE x.user_id IN      (malformed SQL)
//   CASE WHEN ... END         ->  no WHERE at all         (SILENT WRONG)
//
// Live proof of the silent case on a 5-follow fixture: `main` returned a count
// of 4 where the correct answer is 2 — the filter simply vanished.
//
// The renderer now returns `Option<String>`; `None` propagates through every
// operand via `?` instead of poisoning the parent, and the callers refuse
// loudly rather than emitting an unfiltered comprehension.
// ---------------------------------------------------------------------------

/// Follow-up: a function call in the count-form inner WHERE must refuse, not
/// emit `WHERE  = 'bob'`.
#[tokio::test]
async fn ldbc_pc_count_where_function_call_refuses() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User) WITH a, size([(a)-[:FOLLOWS]->(x:User) WHERE toLower(x.name) = 'bob' | x]) AS n
         RETURN a.name, n",
    )
    .await
    .expect_err("an unrenderable count-form inner WHERE must refuse");
    assert!(
        err.contains("cannot be rendered for the count/size form"),
        "the refusal must name the count/size form:\n{err}"
    );
}

/// Follow-up: an `IN` list must refuse rather than emit a truncated `IN `.
#[tokio::test]
async fn ldbc_pc_count_where_in_list_refuses() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User) WITH a, size([(a)-[:FOLLOWS]->(x:User) WHERE x.user_id IN [1,2,3] | x]) AS n
         RETURN a.name, n",
    )
    .await
    .expect_err("an IN-list count-form inner WHERE must refuse");
    assert!(
        err.contains("cannot be rendered for the count/size form"),
        "the refusal must name the count/size form:\n{err}"
    );
}

/// Follow-up: the SILENT case — a CASE expression produced no WHERE at all, so
/// the comprehension counted rows the user excluded (live: 4 vs the correct 2).
#[tokio::test]
async fn ldbc_pc_count_where_case_refuses_instead_of_dropping() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let err = try_render_inline(
        &schema,
        "MATCH (a:User)
         WITH a, size([(a)-[:FOLLOWS]->(x:User) WHERE CASE WHEN x.user_id > 3 THEN true ELSE false END | x]) AS n
         RETURN a.name, n",
    )
    .await
    .expect_err("a CASE inner WHERE must refuse, not silently drop the filter");
    assert!(
        err.contains("cannot be rendered for the count/size form"),
        "the refusal must name the count/size form:\n{err}"
    );
}

/// Follow-up SCOPE: every shape the renderer DOES handle must keep rendering —
/// plain comparisons, AND-chains, NOT, STARTS WITH, and IS NOT NULL. The
/// `Option` conversion must not narrow existing coverage.
#[tokio::test]
async fn ldbc_pc_count_where_supported_shapes_still_render() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    for (pred, expect) in [
        ("x.user_id > 3", "__n0e.user_id > 3"),
        ("x.user_id > 3 AND x.city = 'NYC'", "__n0e.user_id > 3"),
        ("NOT (x.user_id = 5)", "NOT __n0e.user_id = 5"),
        ("x.name STARTS WITH 'A'", "startsWith(__n0e.full_name, 'A')"),
        ("x.user_id IS NOT NULL", "__n0e.user_id IS NOT NULL"),
    ] {
        let sql = generate_sql_inline(
            &schema,
            &format!(
                "MATCH (a:User) WITH a, size([(a)-[:FOLLOWS]->(x:User) WHERE {pred} | x]) AS n \
                 RETURN a.name, n"
            ),
        )
        .await;
        assert!(
            sql.contains(expect),
            "count-form inner WHERE `{pred}` must still render `{expect}`:\n{sql}"
        );
    }
}

// --- #1118: node schema `filter:` column must be projected into the VLP CTE ---
//
// A node-level YAML `filter:` (`Country.filter: "type = 'Country'"`) is combined
// into the end-filter slot, but #607 defers the end predicate to the OUTER
// WRAPPER, which can only see columns the recursive CTE projects. The filter
// column was never added to the CTE's property list (the pruner only keeps
// properties the query TEXT mentions, and a schema filter is invisible to it),
// so the wrapper emitted a bare `end_node.type` — out of scope there.
//
// Live before the fix (LDBC SF1, `MATCH (a:Place)-[:IS_PART_OF*1..2]->(c:Country)`):
//   Code 47: Unknown expression or function identifier `end_node.type`
// After: 1343 rows, matching a hand-computed edge-distinct oracle.
//
// This is reachable in the SHIPPED LDBC schema — City/Country/Continent/
// University/Company are all `filter:`-discriminated views over one table.

/// The end-node schema filter must resolve against a PROJECTED CTE column
/// (`end_type`), never a bare `end_node.<col>` the wrapper cannot see.
#[tokio::test]
async fn ldbc_1118_end_schema_filter_column_projected_into_vlp_cte() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    for cypher in [
        "MATCH (a:Place)-[:IS_PART_OF*1..2]->(c:Country) RETURN count(*)",
        "MATCH (a:Place)-[:IS_PART_OF*0..2]->(c:Country) RETURN count(*)",
        "MATCH (a:Place)-[:IS_PART_OF*1..3]->(c:Country) RETURN c.name",
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            sql.contains("as end_type") || sql.contains("AS end_type"),
            "#1118: the schema-filter column must be PROJECTED by the CTE \
             for `{cypher}`:\n{sql}"
        );
        // The wrapper predicate must reference the projected column, not the
        // out-of-scope node alias.
        assert!(
            !sql.contains("end_node.type = 'Country'"),
            "#1118: the wrapper must not reference the unprojected \
             `end_node.type` (ClickHouse Code 47) for `{cypher}`:\n{sql}"
        );
        assert!(
            sql.contains("end_type = 'Country'"),
            "#1118: the end schema filter must survive, rewritten onto the \
             projected column, for `{cypher}`:\n{sql}"
        );
    }
}

/// shortestPath uses its own `_to_target` wrapper — same defect, same fix.
#[tokio::test]
async fn ldbc_1118_shortest_path_end_schema_filter_projected() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH p=shortestPath((a:City)-[:IS_PART_OF*1..3]->(c:Country)) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("as end_type") || sql.contains("AS end_type"),
        "#1118: shortestPath must project the end schema-filter column:\n{sql}"
    );
    // The WRAPPER must read the projected column. `end_node.type` still appears
    // in the CTE's own projection (`end_node.type as end_type`) — that is the
    // fix working, not the defect; assert on the wrapper predicate itself.
    assert!(
        sql.contains("end_type = 'Country'"),
        "#1118: the shortestPath wrapper must filter on the projected \
         `end_type`, not an out-of-scope `end_node.type`:\n{sql}"
    );
}

/// The START endpoint's schema filter goes in the BASE ARM, where `start_node`
/// IS in scope — that path already worked and must stay byte-compatible.
///
/// This is why #1118 projects a column for the END endpoint ONLY. The first cut
/// pushed one for both; review showed the start-side column was then silently
/// deleted again by `prune_vlp_columns` (`render_plan/plan_optimizer.rs`) in
/// every shape but the undirected mixed VLP, where it survived as dead weight.
#[tokio::test]
async fn ldbc_1118_start_schema_filter_stays_in_base_arm() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Country)-[:IS_PART_OF*1..2]->(c:Place) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("start_node.type = 'Country'"),
        "#1118: the START schema filter is applied in the base arm, where \
         `start_node` is in scope — unchanged:\n{sql}"
    );
}

/// A single-hop VLP has no wrapper: the base case IS the endpoint, so the end
/// filter stays on `end_node` there. Guards against "fix everything" overreach.
#[tokio::test]
async fn ldbc_1118_single_hop_end_filter_stays_on_end_node() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Place)-[:IS_PART_OF*1..1]->(c:Country) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("'Country'"),
        "#1118: the single-hop end schema filter must still be applied:\n{sql}"
    );
}

/// #1120 (upgraded from the #1118 scope-boundary lock): a CLOSED pattern
/// `(a:Country)-[:R*2..3]->(a)` DROPPED its schema filter — the dedup pass
/// replaces the repeated endpoint's subtree with `Empty`, and the surviving
/// side's ViewScan is rebuilt without `schema_filter`, so both plan-walks in
/// `extract_schema_filter_from_node` returned None. Silent-wrong: 5 vs an
/// oracle 3 on a cyclic fixture where the filter discriminates (invisible on
/// LDBC's acyclic Place graph, where 0 == 0 by coincidence).
///
/// Fixed by re-resolving the filter from the SCHEMA by label into the base
/// arm's START slot. Same variable ⇒ same node ⇒ once suffices: the closure
/// predicate `start_id = end_id` equates the endpoints, and intermediate hops
/// are unlabeled and stay unfiltered.
#[tokio::test]
async fn ldbc_1120_closed_pattern_schema_filter_in_base_arm() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Country)-[:IS_PART_OF*2..3]->(a) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("t.start_id = t.end_id"),
        "#1120: the closed-pattern closure predicate must remain:\n{sql}"
    );
    // #1125 upstream fix: the type-inference ViewScan rebuilds now CARRY
    // `schema_filter`, so the filter reaches this shape through the normal
    // plan-walk — as an END filter on the wrapper (`end_type = 'Country'`),
    // which under the closure predicate `start_id = end_id` is equivalent to
    // filtering the start. Row-verified on the cyclic fixture (4/5/12 == the
    // same oracles the base-arm placement produced). The #1120 label-fallback
    // stays as a dead-code safety net (it requires BOTH plan-walks to return
    // None, which the #1125 rebuild fix now prevents for labeled patterns).
    assert!(
        sql.contains("end_type = 'Country'") || sql.contains("start_node.type = 'Country'"),
        "#1120/#1125: the schema filter must be applied (either placement is \
         row-equivalent under start_id = end_id):\n{sql}"
    );
    // Never the #1118 defect class: no out-of-scope bare alias in the wrapper.
    assert!(
        !sql.contains("WHERE ((end_node.type = 'Country'))"),
        "#1118 class: the wrapper must not reference the unprojected \
         `end_node.type`:\n{sql}"
    );
}

/// #1120 boundary: an UNFILTERED label's closed pattern must be byte-compatible
/// with the pre-#1120 render (no fallback fires when the schema has no filter).
#[tokio::test]
async fn ldbc_1120_closed_pattern_unfiltered_label_unchanged() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    for cypher in [
        "MATCH (a:Place)-[:IS_PART_OF*2..3]->(a) RETURN count(*)",
        "MATCH (a:Person)-[:KNOWS*2..2]->(a) RETURN count(*)",
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            !sql.contains("'Country'") && !sql.contains("schema filter"),
            "#1120: no fallback filter may appear for an unfiltered label \
             (`{cypher}`):\n{sql}"
        );
    }
}

/// #1120: the `JoinStrategy::Traditional` gate is WIDER than "standard schema"
/// — a POLYMORPHIC edge over standard node tables also classifies Traditional,
/// so the fallback fires there too. Intended and verified correct live during
/// the #1124 review (closed *2..3/*2..2/*0..2 matched hand oracles 4/3/6 where
/// main returned 7/4/10 on a cyclic fixture). This pins that silently
/// load-bearing coverage: the filter must land in the base arm AND compose
/// with the polymorphic type discriminators.
#[tokio::test]
async fn ldbc_1120_polymorphic_closed_pattern_filter_in_base_arm() {
    let schema = load_schema_from("schemas/test/polymorphic_filtered_closed.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*2..3]->(a) RETURN count(*)",
    )
    .await;
    // Post-#1125 the filter arrives via the plan-walk as a projected end-wrapper
    // filter (row-equivalent under `start_id = end_id`; see the standard test).
    assert!(
        sql.contains("end_account_status = 'active'")
            || sql.contains("start_node.account_status = 'active'"),
        "#1120/#1125: the polymorphic closed pattern must get the schema \
         filter (either row-equivalent placement):\n{sql}"
    );
    assert!(
        sql.contains("rel.interaction_type = 'FOLLOWS'"),
        "#1120: the polymorphic type discriminator must still compose:\n{sql}"
    );
}

/// #1120 boundary: an OPEN pattern with a filtered label is untouched by the
/// fallback (its filters resolve through the normal plan-walk, per #1118).
#[tokio::test]
async fn ldbc_1120_open_pattern_untouched() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Country)-[:IS_PART_OF*2..3]->(b:Place) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("start_node.type = 'Country'"),
        "#1120: the open pattern's start filter comes from the normal \
         plan-walk and must remain in the base arm:\n{sql}"
    );
}

/// A DENORMALIZED / MIXED-access endpoint keeps its end filter in the base and
/// recursive arms (`end_filter_in_base_recursive_case`), where the raw
/// `end_node` alias IS in scope — so #1118 must NOT project a column there: it
/// would be dead weight no predicate references. Review of the first cut caught
/// exactly that, on the UNDIRECTED mixed shape where `prune_vlp_columns` did not
/// strip it and that family's SQL changed for no reason.
///
/// That family's dropped schema filter is a SEPARATE, pre-existing defect
/// (#1119, silent-wrong: 6 vs an oracle 3) — this locks the scope boundary, not
/// the still-wrong behavior.
///
/// The fixture DECLARES a `filter:`, so the gate is load-bearing: delete it and
/// this test fails. (An unfiltered fixture would pass either way.)
#[tokio::test]
async fn ldbc_1118_embedded_endpoint_not_projected() {
    let schema = load_schema_from("schemas/test/foreign_selfloop_filtered.yaml");
    for cypher in [
        "MATCH (a:Person)-[:REPORTS_TO*1..2]->(b:Person) RETURN count(*)",
        // The undirected shape is the one where the pruner does NOT strip an
        // unreferenced projected column, so a leak here is visible.
        "MATCH (a:Person)-[:REPORTS_TO*1..2]-(b:Person) RETURN count(*)",
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            !sql.contains("end_active") && !sql.contains("start_active"),
            "#1118: a denormalized/mixed endpoint must NOT get a projected \
             schema-filter column (dead weight; the filter stays in the base \
             arm) for `{cypher}`:\n{sql}"
        );
        assert!(
            sql.contains("start_own"),
            "#1118: the mixed-access arm must otherwise be unchanged:\n{sql}"
        );
    }
}

// --- #1125: flat single-hop closed pattern must keep the schema filter --------
//
// `(a:Country)-[:R]->(a)` and `*1..1` render via the #994 fold (edge join with
// both endpoint equalities; the node scan elided when unreferenced). The node's
// schema `filter:` vanished because the type-inference ViewScan REBUILDS (the
// closed endpoint always goes through label inference) dropped `schema_filter`.
// Live before the fix: 2 self-loops counted where the oracle is 1 (a City
// self-loop leaked through a :Country pattern). The rebuilds now carry the
// filter (via `NodeSchema::carryable_schema_filter` — denorm returns None,
// #1119), which also lets the #1120 CTE shapes resolve it through the normal
// plan-walk instead of the label fallback.

/// The flat closed single-hop must apply the filter — via a node join when one
/// exists, or by whatever placement — the string must reach the SQL.
#[tokio::test]
async fn ldbc_1125_flat_closed_single_hop_keeps_schema_filter() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    for cypher in [
        "MATCH (a:Country)-[:IS_PART_OF]->(a) RETURN count(*)",
        "MATCH (a:Country)-[:IS_PART_OF*1..1]->(a) RETURN count(*)",
        "MATCH (a:Country)-[:IS_PART_OF]->(a) RETURN a.name",
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            sql.contains("'Country'"),
            "#1125: the schema filter must survive the flat closed single-hop \
             for `{cypher}`:\n{sql}"
        );
    }
}

/// #1125 boundary: unfiltered labels' closed single-hop stays the bare #994
/// fold (no node join reintroduced, no phantom filter).
#[tokio::test]
async fn ldbc_1125_unfiltered_closed_single_hop_unchanged() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(&schema, "MATCH (a:Person)-[:KNOWS]->(a) RETURN count(*)").await;
    assert!(
        !sql.contains("'Country'") && !sql.contains("type ="),
        "#1125: an unfiltered label's closed single-hop is unchanged:\n{sql}"
    );
}

/// #1125 boundary: a MULTI-label inferred node must NOT get the first label's
/// filter as a GLOBAL restriction (it stands for several labels; restricting
/// to one would drop rows — caught live when `(a:Person)-[r]->(b)` grew a
/// spurious `WHERE b.type = 'City'`).
///
/// NOTE (#1127): PER-ARM filters inside the multi-type CTE — each UNION arm
/// applying its own label's filter to its own node join — are the eventual
/// goal, NOT forbidden by this test. Today the arms are unfiltered, which
/// over-returns when an edge row violates the schema's declared subtype; that
/// gap is tracked as #1127. This test only pins that the wrong GLOBAL
/// restriction stays fixed.
#[tokio::test]
async fn ldbc_1125_multi_label_inferred_node_gets_no_filter() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(&schema, "MATCH (a:Person)-[r]->(b) RETURN count(*)").await;
    assert!(
        !sql.contains("WHERE (b.type = 'City')") && !sql.contains("WHERE (b.type = 'Country')"),
        "#1125: a multi-label inferred node must not be restricted by one \
         label's schema filter:\n{sql}"
    );
}

// --- #1122: schema-filter column colliding with a differently-mapped property -
//
// `filter: "kind = 'A'"` (physical column `kind`) + `property_mappings:
// kind: decoy` (cypher `kind` -> physical `decoy`). Two coupled defects:
//   1. The #1118 projection SKIPPED the collision (ambiguous `end_kind`),
//      leaving the filter unprojected.
//   2. `rewrite_end_filter_for_cte` ALSO matched the cypher-alias spelling
//      (`end_node.kind`), rewriting the unprojected filter onto `end_kind` —
//      the DECLARED property's projected column, holding `decoy`'s value.
// Net: valid SQL, wrong rows, no error — live pk 3 where the answer is pk 2,
// with the COUNTS agreeing by coincidence (1 either way).
//
// Fix: project the collision under a mangled `__sf_<col>` alias bound to the
// REAL column, and drop the alias-spelling replace (filters reach the rewrite
// already property-mapped, so it had no covered use — an uncovered reliance
// now fails LOUD instead of silently against the wrong column).

/// The wrapper predicate must bind to the REAL filter column via the mangled
/// projection — never to the declared property's (differently-mapped) column.
#[tokio::test]
async fn ldbc_1122_colliding_schema_filter_binds_real_column() {
    let schema = load_schema_from("schemas/test/decoy_schema_filter.yaml");
    for cypher in [
        "MATCH (a:P)-[:L*1..3]->(b:P) RETURN b.pk",
        "MATCH (a:P)-[:L*1..3]->(b:P) RETURN count(*)",
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            sql.contains("end_node.kind as end___sf_kind"),
            "#1122: the colliding filter column must be projected under the \
             mangled alias, bound to the REAL `kind` column, for `{cypher}`:\n{sql}"
        );
        assert!(
            sql.contains("end___sf_kind = 'A'"),
            "#1122: the wrapper predicate must reference the mangled \
             projection for `{cypher}`:\n{sql}"
        );
        // The mis-bind: predicate on the DECLARED property's projection.
        assert!(
            !sql.contains("(end_kind = 'A'"),
            "#1122: the predicate must NOT bind to `end_kind` (that column \
             holds `decoy`'s value) for `{cypher}`:\n{sql}"
        );
    }
}

/// #1122 boundary: the normal declared-property path (filter column IS the
/// declared property's physical column) keeps its unmangled alias.
#[tokio::test]
async fn ldbc_1122_non_colliding_filter_unmangled() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Place)-[:IS_PART_OF*1..2]->(c:Country) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("end_type = 'Country'") && !sql.contains("__sf_"),
        "#1122: the non-colliding LDBC shape keeps its plain alias:\n{sql}"
    );
}

/// #1122 boundary: a user WHERE spelled with the cypher alias still works —
/// it reaches the rewrite already property-mapped to the physical column, so
/// removing the alias-spelling replace loses nothing.
#[tokio::test]
async fn ldbc_1122_user_where_still_property_mapped() {
    let schema = load_schema_from("benchmarks/social_network/schemas/social_benchmark.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = 'Bob' RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("end_name = 'Bob'"),
        "#1122: a user WHERE on a renamed property must still rewrite onto \
         the projected column:\n{sql}"
    );
}

/// #1128 review (HIGH): prefix-pair collision. `kind` is a strict prefix of
/// `kind_extra` (which maps to `decoy2`); the shortest-first plain replace
/// corrupted `end_node.kind_extra` into `end_kind_extra` — the decoy
/// projection — with correctness dependent on the YAML predicate ORDER.
/// Live before the rework: pk 3 where the answer is pk 2 in one order,
/// correct in the other. Now longest-first + word-boundary + literal-aware.
#[tokio::test]
async fn ldbc_1122_prefix_pair_filter_binds_real_columns() {
    let schema = load_schema_from("schemas/test/decoy_schema_filter.yaml");
    let sql = generate_sql_inline(&schema, "MATCH (a:Q)-[:L2*1..3]->(b:Q) RETURN b.pk").await;
    assert!(
        sql.contains("end___sf_kind_extra = 'X'"),
        "#1128: the longer colliding column must bind its mangled projection \
         (never the decoy `end_kind_extra`):\n{sql}"
    );
    assert!(
        sql.contains("end_kind = 'A'"),
        "#1128: the shorter (undeclared, non-colliding) column keeps its \
         plain projection:\n{sql}"
    );
    assert!(
        !sql.contains("end_kind_extra = 'X'") || sql.contains("end___sf_kind_extra = 'X'"),
        "#1128: `end_node.kind_extra` must never be corrupted into the decoy \
         `end_kind_extra` projection:\n{sql}"
    );
}

/// #1128 review: a string LITERAL that happens to contain `end_node.<col>`
/// must not be edited by the rewrite (`rewrite_outside_string_literals`).
#[tokio::test]
async fn ldbc_1122_literal_value_not_corrupted() {
    let schema = load_schema_from("benchmarks/social_network/schemas/social_benchmark.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) \
         WHERE b.name = 'end_node.full_name' RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("end_name = 'end_node.full_name'"),
        "#1128: the literal VALUE must survive the rewrite verbatim:\n{sql}"
    );
}

// --- #1123: filter on a COMPOSITE node_id component --------------------------
//
// A composite id's `end_id` is a pipe-joined concat with no per-component
// target, so the id rewrite could not handle `end_node.bank_id` and BOTH
// channels — a schema `filter: "bank_id = 7"` and a user `WHERE b.bank_id`
// — left the reference unrewritten in the wrapper -> Code 47.
// Fix: composite id components FALL THROUGH the id-skips (the #1118 block and
// the regular VLP property builder) and are projected as ordinary
// `<prefix>_<col>` columns; the boundary-aware rewrite binds the wrapper
// predicate to them. Live: Code 47 -> 2 == oracle on a populated fixture.
// Single-column ids keep the skip: they're already handled by the id rewrite.

/// Both channels — schema filter and user WHERE — on a composite component
/// must project the component and bind the wrapper predicate to it.
#[tokio::test]
async fn ldbc_1123_composite_component_filter_projected() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[r*1..2]->(b:Account) WHERE b.bank_id = 'x' RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("end_node.bank_id as end_bank_id"),
        "#1123: the composite id component must be projected:\n{sql}"
    );
    assert!(
        sql.contains("end_bank_id = 'x'"),
        "#1123: the wrapper predicate must bind to the projected component:\n{sql}"
    );
    assert!(
        !sql.contains("WHERE (end_node.bank_id"),
        "#1123: no bare out-of-scope `end_node.bank_id` in the wrapper:\n{sql}"
    );
}

/// #1123 boundary: a SINGLE-column id keeps the skip — its filter rewrites
/// onto `end_id`, with no extra projected column.
#[tokio::test]
async fn ldbc_1123_single_id_filter_still_uses_end_id() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.user_id = 5 RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("end_id = 5"),
        "#1123: a single-column id filter must still rewrite onto `end_id`:\n{sql}"
    );
    assert!(
        !sql.contains("as end_user_id"),
        "#1123: no redundant projected column for a single-column id:\n{sql}"
    );
}

/// #1130 review (CRITICAL): the recursive arm emitted composite id components
/// INTERLEAVED per column (`vp.start_<c>, end_node.<c>` pairs) while the base
/// arm groups them (all `start_*`, then all `end_*`). UNION ALL binds by
/// POSITION, so every hop>=2 row swapped `start_account_number` with
/// `end_bank_id` — silently scrambling RETURN-position component reads
/// (pre-existing on main) and the #1123 wrapper end-filter (loud on main,
/// WRONG on the unfixed branch: 2 rows where the oracle is 4, and a filter on
/// a nonexistent bank returned phantom rows). Structural lock: the ordered
/// `as start_*`/`as end_*` sequences must be IDENTICAL across arms.
#[tokio::test]
async fn ldbc_1123_composite_component_arm_order_aligned() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[r*1..2]->(b:Account) \
         RETURN a.bank_id, a.account_number, b.bank_id, b.account_number",
    )
    .await;
    let union_pos = sql
        .to_uppercase()
        .find("UNION ALL")
        .unwrap_or_else(|| panic!("expected a recursive UNION ALL:\n{sql}"));
    let (base_arm, recursive_arm) = sql.split_at(union_pos);
    let component_aliases = |arm: &str| -> Vec<String> {
        arm.split(" as ")
            .skip(1)
            .filter_map(|tail| tail.split([',', '\n', ' ']).next())
            .filter(|tok| {
                (tok.starts_with("start_") || tok.starts_with("end_"))
                    && *tok != "start_id"
                    && *tok != "end_id"
            })
            .map(|s| s.to_string())
            .collect()
    };
    let base_order = component_aliases(base_arm);
    let rec_order = component_aliases(recursive_arm);
    assert!(
        !base_order.is_empty(),
        "expected composite component columns in the base arm:\n{sql}"
    );
    assert_eq!(
        base_order, rec_order,
        "#1130: base and recursive arms must emit component columns in the \
         SAME positional order (UNION ALL binds by position):\n{sql}"
    );
}

// --- #1131: cross-endpoint COMPONENT comparison must not widen to whole-id ---
//
// `WHERE a.bank_id = b.bank_id` on a composite-id VLP was lowered by the #1103
// path to `start_id = end_id` — equality of the FULL pipe-joined id, i.e. a
// cycle test — silently excluding same-bank different-account paths (live: 0
// vs an oracle 3). Post-#1130 every component is projected in both arms
// (grouped order), so a component reference maps to its own
// `<prefix>_<col>` column. Single-column ids keep the id collapse.

/// A single-component cross-endpoint comparison binds the component columns.
#[tokio::test]
async fn ldbc_1131_component_comparison_not_widened_to_whole_id() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]->(b:Account) \
         WHERE a.bank_id = b.bank_id RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("start_bank_id = end_bank_id"),
        "#1131: the component comparison must bind the projected component \
         columns:\n{sql}"
    );
    assert!(
        !sql.contains("start_id = end_id"),
        "#1131: never widened to whole-pipe-joined-id equality (a cycle \
         test):\n{sql}"
    );
}

/// #1131 boundary: a SINGLE-column id keeps the id collapse.
#[tokio::test]
async fn ldbc_1131_single_id_comparison_still_collapses() {
    let schema = load_schema_from("benchmarks/ldbc_snb/schemas/ldbc_snb.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Place)-[:IS_PART_OF*1..2]->(c:Place) WHERE a.id = c.id RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("start_id = end_id"),
        "#1131: a single-column id comparison still collapses to the id \
         projection:\n{sql}"
    );
}

// --- #1132: zero-hop (*0..N) composite VLP base arm ---------------------------
//
// `generate_zero_hop_base_case` interpolated the RAW comma-separated composite
// column string: `start_node.bank_id, account_number as start_id` — two
// columns with the alias on the second; the pipe-joined concat never built —
// Code 44 on every composite `*0..N` (loud, both channels). The zero-hop arm
// also never projected the per-component columns the non-zero arms carry
// (#1130 grouped order), so UNION ALL would misbind by position once the id
// emission was fixed. Fix: route through `emit_id_expr` + emit the grouped
// component columns + skip them in the props loop (Code 44 double-projection).
// Live: count 15 == oracle AND the full 4-column 15-row matrix row-for-row
// identical to a hand enumeration.

/// Zero-hop composite: pipe-joined id, grouped component columns, no
/// double-projection.
#[tokio::test]
async fn ldbc_1132_zero_hop_composite_id_emission() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*0..2]->(b:Account) \
         RETURN a.bank_id, a.account_number, b.bank_id, b.account_number",
    )
    .await;
    assert!(
        sql.contains(
            "concat(toString(start_node.bank_id), '|', toString(start_node.account_number)) as start_id"
        ),
        "#1132: the zero-hop seed id must be the composite-aware concat:\n{sql}"
    );
    assert!(
        !sql.contains(", account_number as start_id"),
        "#1132: never the raw comma-joined column string:\n{sql}"
    );
    // Grouped component columns present exactly once each in the zero-hop arm.
    let zero_arm = sql.split("UNION ALL").next().unwrap_or("");
    for col in [
        "as start_bank_id",
        "as start_account_number",
        "as end_bank_id",
        "as end_account_number",
    ] {
        assert_eq!(
            zero_arm.matches(col).count(),
            1,
            "#1132: `{col}` must appear exactly once in the zero-hop arm:\n{sql}"
        );
    }
}

/// #1132 boundary: single-column-id zero-hop is byte-compatible (the id
/// emission path is identical for `Identifier::Single`).
#[tokio::test]
async fn ldbc_1132_zero_hop_single_id_unchanged() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*0..2]->(b:User) RETURN count(*)",
    )
    .await;
    assert!(
        sql.contains("start_node.user_id as start_id"),
        "#1132: single-column zero-hop id emission unchanged:\n{sql}"
    );
}

/// #1132 review (HIGH): the undirected two-CTE split gave BOTH directional
/// arms `min_hops: 0`, so the direction-independent zero-hop identity rows
/// were seeded TWICE and the outer UNION ALL kept both copies — every
/// zero-hop row silently doubled (30 vs an oracle 25 on the composite
/// fixture), newly reachable once #1132 made composite `*0..N` render at all.
/// The seed now lives in the FORWARD arm only; the swapped Incoming arm
/// starts at 1 hop.
#[tokio::test]
async fn ldbc_1132_undirected_split_seeds_zero_hop_once() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*0..2]-(b:Account) RETURN count(*)",
    )
    .await;
    assert_eq!(
        sql.matches("0 as hop_count").count(),
        1,
        "#1132: the two-CTE undirected split must seed the zero-hop identity \
         row in exactly ONE arm:\n{sql}"
    );
}
// --- #1136: ORDER BY / GROUP BY on a composite id COMPONENT ------------------
//
// The late SELECT/ORDER BY rewriter's id heuristic (`ends_with("_id")`)
// collapsed a COMPONENT reference (`a.bank_id`) onto the whole pipe-joined
// `t.start_id` — wrong ordering silently (concat lexicographic), Code 215
// under aggregation, Code 47 in union forms. Components are projected as
// `<prefix>_<col>` in every arm since #1130; a task-local registration
// (`register_vlp_composite_id_components`) now lets the rewriter bind them.

/// ORDER BY and the aggregate GROUP BY form must bind the component column.
#[tokio::test]
async fn ldbc_1136_order_by_component_binds_component_column() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]->(b:Account) \
         RETURN a.bank_id ORDER BY a.bank_id",
    )
    .await;
    assert!(
        sql.contains("ORDER BY t.start_bank_id"),
        "#1136: ORDER BY must bind the projected component column:\n{sql}"
    );
    assert!(
        !sql.contains("ORDER BY t.start_id"),
        "#1136: never the whole pipe-joined id (concat-lexicographic \
         ordering):\n{sql}"
    );
    let agg = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]->(b:Account) \
         RETURN a.bank_id, count(*) ORDER BY a.bank_id",
    )
    .await;
    assert!(
        agg.contains("GROUP BY t.start_bank_id") && agg.contains("ORDER BY t.start_bank_id"),
        "#1136: the aggregate form (was Code 215) groups and orders by the \
         component:\n{agg}"
    );
}

/// #1136 boundary: a single-column id keeps the whole-id collapse.
#[tokio::test]
async fn ldbc_1136_single_id_order_by_unchanged() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) \
         RETURN a.user_id ORDER BY a.user_id",
    )
    .await;
    assert!(
        sql.contains("ORDER BY t.start_id"),
        "#1136: single-column id ORDER BY still collapses to `t.start_id`:\n{sql}"
    );
}

/// #1138: the undirected two-arm union's synthesized ORDER BY key
/// (`__order_col_*`) bound the START role in BOTH arms while the projected
/// value role-swaps — the reversed arm sorted by the wrong endpoint (silently
/// mis-sorted rows, live-proven). Each arm now re-resolves the key through
/// its own select item carrying the SAME output alias.
#[tokio::test]
async fn ldbc_1138_undirected_union_order_key_role_swaps() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.bank_id ORDER BY a.bank_id",
    )
    .await;
    assert_eq!(
        sql.matches("t.start_bank_id AS \"__order_col_0\"").count(),
        1,
        "#1138: the forward arm binds the start role once:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_bank_id AS \"__order_col_0\"").count(),
        1,
        "#1138: the reversed arm must bind ITS role (end), not repeat the \
         start role:\n{sql}"
    );
}

/// #1138 boundary: with BOTH endpoints ordered, each arm binds each key to
/// its own role — start key start-roled in arm 1 / end-roled in arm 2, and
/// vice versa for the second key.
#[tokio::test]
async fn ldbc_1138_two_key_order_role_maps_per_arm() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.bank_id, b.bank_id ORDER BY a.bank_id, b.bank_id",
    )
    .await;
    // Key 0 is `a.bank_id`: start-roled in the forward arm, END-roled in the
    // reversed arm (on main both arms bound start — the defect).
    assert_eq!(
        sql.matches("t.end_bank_id AS \"__order_col_0\"").count(),
        1,
        "#1138: key 0 must be END-roled in the reversed arm:\n{sql}"
    );
    // Key 1 is `b.bank_id`: end-roled forward, START-roled reversed.
    assert_eq!(
        sql.matches("t.start_bank_id AS \"__order_col_1\"").count(),
        1,
        "#1138: key 1 must be START-roled in the reversed arm:\n{sql}"
    );
}

// --- #1140: unprojected / expression ORDER BY keys on the undirected union ---
//
// #1139 fixed the role binding for keys that are PROJECTED (resolvable through
// an output alias). A key with no projection to chain through fell to the
// legacy path and bound the START role in BOTH arms — silently mis-sorted
// rows on the reversed arm. Each arm's own CTE records its role assignment
// (`vlp_a_b` = a-start/b-end, `vlp_b_a` the reverse), so an arm that swaps
// roles relative to the primary direction flips the key's role prefixes.

/// #1140 shape A: the ORDER BY key is not projected (`RETURN a.bank_id
/// ORDER BY a.balance`).
#[tokio::test]
async fn ldbc_1140_unprojected_order_key_role_swaps_per_arm() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.bank_id ORDER BY a.balance",
    )
    .await;
    assert_eq!(
        sql.matches("t.start_balance AS \"__order_col_0\"").count(),
        1,
        "#1140: the forward arm keeps the start role:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_balance AS \"__order_col_0\"").count(),
        1,
        "#1140: the reversed arm must order by ITS endpoint (end role), not \
         repeat the start role:\n{sql}"
    );
}

/// #1140 shape B: an EXPRESSION key (`ORDER BY toUpper(a.bank_id)`). The swap
/// must reach INSIDE the function wrapper, which is why the rewriter descends
/// structurally instead of cloning non-column nodes.
#[tokio::test]
async fn ldbc_1140_expression_order_key_role_swaps_inside_wrapper() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.bank_id ORDER BY toUpper(a.bank_id)",
    )
    .await;
    assert_eq!(
        sql.matches("upperUTF8(t.start_bank_id) AS \"__order_col_0\"")
            .count(),
        1,
        "#1140: forward arm:\n{sql}"
    );
    assert_eq!(
        sql.matches("upperUTF8(t.end_bank_id) AS \"__order_col_0\"")
            .count(),
        1,
        "#1140: the reversed arm must swap the role INSIDE the function \
         wrapper:\n{sql}"
    );
}

// --- #1146: denorm VLP projection must be ROLE-SYMMETRIC ---------------------
//
// An undirected VLP renders TWO CTEs whose role assignment is SWAPPED
// (`vlp_o_d` binds o=start/d=end, `vlp_d_o` the reverse), but the denorm
// property collection filtered the from-side by the START alias's requirements
// and the to-side by the END alias's. In the reversed arm those roles no longer
// line up with the aliases, so each arm pruned exactly the column the OTHER
// arm's role needed — leaving `vlp_d_o` with no `state` column at all, and
// every downstream reference dangling (Code 47) no matter how correctly the
// rewriter resolved the role.
//
// A denormalized property is now projected role-symmetrically: if EITHER
// endpoint needs it, both sides carry it. This only ever ADDS columns.

/// #1146: an ORDER BY key on the OTHER endpoint is projected into both arms.
#[tokio::test]
async fn ldbc_1146_other_endpoint_key_is_projected_role_symmetrically() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city ORDER BY d.state",
    )
    .await;
    // Both role spellings of `state` must exist — before the fix the reversed
    // arm carried neither, so the key had nothing to bind.
    assert!(
        sql.contains("start_OriginState") && sql.contains("end_DestState"),
        "#1146: both role columns for the ORDER BY key must be projected:\n{sql}"
    );
}

/// #1146: an AGGREGATE ARGUMENT on the other endpoint gets the same treatment —
/// the gap was never ORDER BY-specific.
#[tokio::test]
async fn ldbc_1146_aggregate_argument_on_other_endpoint_is_projected() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city, min(d.state)",
    )
    .await;
    // Assert PER ARM: the two CTEs are separate, and before the fix EACH was
    // missing the column the OTHER arm's role needed. A whole-file `contains`
    // would pass on main, where the pair exists but split across the two arms.
    let arms: Vec<&str> = sql.split("vlp_d_o AS (").collect();
    assert_eq!(arms.len(), 2, "expected two VLP CTEs:\n{sql}");
    assert!(
        arms[0].contains("end_DestState") && arms[0].contains("start_OriginState"),
        "#1146: the forward CTE must carry BOTH roles' columns for the \
         aggregate argument:\n{sql}"
    );
    assert!(
        arms[1].contains("end_DestState") && arms[1].contains("start_OriginState"),
        "#1146: the reversed CTE must carry them too — before the fix it \
         carried neither, so the outer reference dangled:\n{sql}"
    );
}

// --- #1141: role-ambiguous denorm property in a VLP ORDER BY -----------------
//
// A denormalized node whose from/to maps DISAGREE on a property's physical
// column (`Airport.city` -> `OriginCityName` from-side, `DestCityName`
// to-side) reaches the emitter in its raw Cypher form: the #471 guard
// deliberately declines to pick one mapping up front. The VLP CTE names each
// projected property column after the PHYSICAL column it resolved per role,
// so prefixing the raw Cypher name emitted `t.start_city` — a column no arm
// projects (ClickHouse Code 47, loud). Resolved per endpoint through
// `denorm_role_properties`, which also gives each undirected arm its OWN
// role (the #1138 class of defect, here for a shape #1139 could not reach
// because the raw spelling never matched an output alias).

/// #1141: the ORDER BY helper column binds the PHYSICAL, role-correct column
/// in each arm — not the raw Cypher property name.
#[tokio::test]
async fn ldbc_1141_denorm_vlp_order_by_binds_physical_role_column() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city ORDER BY o.city",
    )
    .await;
    assert!(
        !sql.contains("start_city") && !sql.contains("end_city"),
        "#1141: the raw Cypher property name must not be prefixed onto a CTE \
         column (no arm projects `start_city`/`end_city`):\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_OriginCityName AS \"__order_col_0\"")
            .count(),
        1,
        "#1141: the forward arm orders by its own (from-role) column:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_DestCityName AS \"__order_col_0\"")
            .count(),
        1,
        "#1141: the reversed arm must order by ITS role's column, not repeat \
         the forward arm's:\n{sql}"
    );
}

/// #1141: the node_id property is role-ambiguous too (`code` ->
/// `Origin`/`Dest`) and was equally broken — both arms emitted the dangling
/// `t.start_code`. It resolves through the same path, and the reversed arm
/// gets its OWN role.
#[tokio::test]
async fn ldbc_1141_denorm_id_property_order_by_binds_role_column() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.code ORDER BY o.code",
    )
    .await;
    assert!(
        !sql.contains("start_code") && !sql.contains("end_code"),
        "#1141: no arm projects `start_code`/`end_code`:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_Origin AS \"__order_col_0\"").count(),
        1,
        "#1141: forward arm orders by its from-role id column:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_Dest AS \"__order_col_0\"").count(),
        1,
        "#1141: reversed arm orders by its to-role id column:\n{sql}"
    );
}

/// #1141 boundary: the MULTI-TYPE union CTE (`vlp_multi_type_*`) is built by a
/// different generator that names property columns after the CYPHER property
/// (`start_ip`), so this resolution must NOT touch it — doing so re-dangles a
/// working shape (caught by the zeek corpus golden during development).
#[tokio::test]
async fn ldbc_1141_multi_type_union_cte_keeps_cypher_spelling() {
    let schema = load_schema_from("schemas/examples/zeek_merged.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (ip:IP)-[:DNS_REQUESTED|CONNECTED_TO]->(target) \
         WHERE ip.ip = '192.168.1.10' RETURN target LIMIT 20",
    )
    .await;
    // Assert the PRECONDITION rather than branching on it: if planning ever
    // stops routing this shape through the multi-type generator, this test
    // must fail loudly rather than silently pass while testing nothing.
    assert!(
        sql.contains("vlp_multi_type_"),
        "#1141: precondition — this shape must render through the multi-type \
         union generator:\n{sql}"
    );
    assert!(
        !sql.contains("start_id.orig_h"),
        "#1141: the multi-type union CTE must keep its Cypher-name \
         spelling (`start_ip`):\n{sql}"
    );
}

/// #1141 review (CRITICAL): an UNPROJECTED role-ambiguous key. There is no
/// select item to chain through (#1139) so it falls to #1140's role swap —
/// which must re-resolve the DENORM COLUMN for the new role, not merely flip
/// the `start_`/`end_` prefix. Flipping alone yields `end_OriginState`: the
/// end role paired with the FROM-role column, which once #1141 makes such
/// names resolvable would bind a column holding the OTHER endpoint's value
/// (silently mis-sorted rows — a loud->silent regression).
#[tokio::test]
async fn ldbc_1141_unprojected_role_ambiguous_key_reresolves_column() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city ORDER BY o.state",
    )
    .await;
    assert_eq!(
        sql.matches("t.start_OriginState AS \"__order_col_0\"")
            .count(),
        1,
        "#1141: forward arm keeps its from-role column:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_DestState AS \"__order_col_0\"").count(),
        1,
        "#1141: the reversed arm must order by the TO-role column \
         (`end_DestState`), not the from-role column under an `end_` prefix:\n{sql}"
    );
    assert!(
        !sql.contains("end_OriginState") && !sql.contains("start_DestState"),
        "#1141: no arm may pair a role prefix with the OTHER role's \
         column:\n{sql}"
    );
}

/// #1141/#1140 re-review (CRITICAL): the role swap must be bound to the ARM's
/// OWN projected columns. Resolving it through a schema-wide scan let ANY
/// denormalized label in the catalog rewrite an unrelated — here entirely
/// NON-denormalized — VLP's ORDER BY key into a real column holding the other
/// endpoint's value (loud -> silently mis-sorted; ground rule 1).
///
/// The fixture is the plain composite `Account` graph plus one unrelated
/// denormalized `Leg` label on a different table AND database.
#[tokio::test]
async fn ldbc_1141_unrelated_denorm_label_must_not_rewrite_key() {
    let schema = load_schema_from("schemas/test/denorm_decoy_unrelated_label.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.bank_id ORDER BY a.account_type",
    )
    .await;
    // Scoped to the ORDER BY KEY: `holder_name` legitimately appears as a
    // projected CTE column (the schema declares it); what must never happen is
    // the unrelated label's column becoming this query's sort key.
    assert!(
        !sql.contains("holder_name AS \"__order_col_"),
        "#1141 re-review: an unrelated denormalized label must not reach \
         across into this query's ORDER BY key:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_account_type AS \"__order_col_0\"")
            .count(),
        1,
        "#1141 re-review: forward arm:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_account_type AS \"__order_col_0\"")
            .count(),
        1,
        "#1141 re-review: the reversed arm flips only the ROLE — this node has \
         no role-specific maps, so the column name is unchanged:\n{sql}"
    );
}

/// #1141/#1140 re-review (HIGH): SQL generation must be deterministic. The
/// previous resolution iterated a `HashMap`, so when two Cypher properties
/// share a from-side physical column but differ on the to-side, the answer
/// varied run to run (observed 6/6 split over 12 runs). Reading the arm's
/// ordered `CteColumnMetadata` makes it a lookup, not a guess.
#[tokio::test]
async fn ldbc_1141_role_swap_resolution_is_deterministic() {
    let schema = load_schema_from("schemas/test/denorm_shared_from_column.yaml");
    let mut seen = std::collections::HashSet::new();
    for _ in 0..8 {
        let sql = generate_sql_inline(
            &schema,
            "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.state ORDER BY o.city",
        )
        .await;
        let key = sql
            .split_whitespace()
            .find(|w| w.starts_with("t.end_"))
            .unwrap_or("<none>")
            .to_string();
        seen.insert(key);
    }
    assert_eq!(
        seen.len(),
        1,
        "#1141 re-review: role-swap resolution must be deterministic, saw {seen:?}"
    );
}

/// #1141/#1140 final review (CRITICAL): when the arm publishes no target-role
/// entry for the key's property, the swap must ABSTAIN — leave the expression
/// exactly as the global rewrite produced it — never emit a bare prefix flip.
///
/// That flip is the spelling round 1 proved wrong (`end_` + the FROM-role
/// column). It is reachable on the shipped denorm schema through any
/// EXPRESSION key, because the pruner only projects the opposite-role column
/// for BARE keys (which #1141 resolves in `rewrite_expr_for_vlp`). Emitting it
/// caused a Code 47 that main does NOT have.
///
/// Abstaining reproduces main's spelling for this shape exactly: still
/// imperfect (both arms sort by the forward endpoint — tracked separately),
/// but never a regression and never a NEW silent wrong.
#[tokio::test]
async fn ldbc_1141_expression_key_never_pairs_a_role_with_the_other_column() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) \
         RETURN o.city ORDER BY toUpper(o.state)",
    )
    .await;
    assert!(
        !sql.contains("end_OriginState"),
        "#1141: must never emit the FROM-role column under an `end_` prefix — \
         no arm projects it:\n{sql}"
    );
    // Still ABSTAINS, byte-identical to main. #1146 unions the two aliases'
    // REQUIRED SETS, but an EXPRESSION key is recorded differently by the
    // requirements analyzer than a bare one: main projects `end_DestState` for
    // `ORDER BY o.state` and NOT for `ORDER BY toUpper(o.state)`, so the
    // opposite-role column still isn't there for the swap to bind and
    // abstaining remains correct. That upstream asymmetry is pre-existing and
    // out of scope here.
    assert_eq!(
        sql.matches("upperUTF8(t.start_OriginState) AS \"__order_col_0\"")
            .count(),
        2,
        "#1141: with no opposite-role column to bind, BOTH arms keep the \
         global rewrite — abstain, never a half-swapped column:\n{sql}"
    );
}

/// #1141/#1140 final review (HIGH): two Cypher properties may share one
/// from-side physical column (`city` and `town` both -> `origin_city`) while
/// differing on the to-side (`dest_city` vs `dest_code`). The swap must
/// resolve the key's OWN property, not whichever entry matches the column
/// name first.
#[tokio::test]
async fn ldbc_1141_shared_from_column_resolves_the_keys_own_property() {
    let schema = load_schema_from("schemas/test/denorm_shared_from_column.yaml");

    // #1146 UPDATE: this shape is UNEXECUTABLE on main and here alike — when two
    // properties share a from-side column the CTE emits that column twice
    // (ClickHouse Code 44, verified live on BOTH). Before #1146 the two
    // properties disambiguated themselves BY LUCK, because pruning happened to
    // keep only one of them; with role-symmetric projection both survive and the
    // shared column is genuinely ambiguous, so the swap correctly ABSTAINS
    // rather than guessing.
    //
    // So this test now pins the safe property — never pairing a role prefix with
    // the other role's column — rather than an exact spelling that only held
    // while pruning was doing accidental disambiguation. Properly resolving a
    // shared from-side column needs the ORDER BY key's own Cypher property
    // threaded down to the swap; no shipped schema has this shape (verified by
    // scanning every schema under schemas/ and benchmarks/).
    for q in [
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.state ORDER BY o.town",
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.state ORDER BY o.city",
    ] {
        let sql = generate_sql_inline(&schema, q).await;
        assert!(
            !sql.contains("end_origin_") && !sql.contains("start_dest_"),
            "#1146: no arm may pair a role prefix with the OTHER role's \
             column:\n{sql}"
        );
    }
}

// --- #1143: denorm undirected VLP aggregate binds the reversed arm's column --
//
// The aggregate/GROUP BY union path text-substituted `t.start_` <-> `t.end_`
// on a copy of the FIRST arm's rendered SELECT (`swap_vlp_start_end`), which
// keeps the first arm's PHYSICAL column under the flipped prefix. On a
// denormalized schema whose per-role property maps disagree that produced
// `t.end_OriginCityName` — a column no arm projects (Code 47). It survived
// because on every other schema pattern both roles map to the SAME physical
// column, so the bare prefix flip is accidentally correct.
//
// The reversed branch already carried the right answer in its own `select`
// (resolved per-arm upstream); the emitter now uses it, as the non-VLP
// branches already did.

/// #1143: the reversed arm projects ITS role's column, not the forward arm's
/// column under an `end_` prefix.
#[tokio::test]
async fn ldbc_1143_denorm_aggregate_uses_the_arms_own_column() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city, count(*)",
    )
    .await;
    assert!(
        !sql.contains("end_OriginCityName"),
        "#1143: must not pair the `end_` role with the FROM-role column — no \
         arm projects it:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.end_DestCityName AS \"o.city\"").count(),
        1,
        "#1143: the reversed arm projects its own to-role column:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_OriginCityName AS \"o.city\"").count(),
        1,
        "#1143: the forward arm is unchanged:\n{sql}"
    );
}

/// #1143: both endpoints grouped — each arm resolves each key to its own role.
#[tokio::test]
async fn ldbc_1143_denorm_aggregate_two_keys_role_map_per_arm() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city, d.city, count(*)",
    )
    .await;
    assert!(
        !sql.contains("end_OriginCityName") && !sql.contains("start_DestCityName"),
        "#1143: no arm may pair a role prefix with the OTHER role's \
         column:\n{sql}"
    );
}

/// #1143 review (CRITICAL): an AGGREGATE-ARGUMENT column needs the same
/// per-arm role resolution as a GROUP BY key.
///
/// `build_union_inner_select` exports aggregate args through the same inner
/// SELECT slot the grouping keys use, but the #844 override machinery
/// originally resolved only `group_by_exprs`. The old text substitution
/// covered both for free (it rewrote the whole rendered string), so routing a
/// swapped arm through the per-arm path without extending the resolution left
/// `count(DISTINCT b.x)` reading arm 0's endpoint on the reversed arm — the
/// right row COUNT with wrong VALUES, and a REGRESSION on composite where main
/// was correct.
///
/// Asserts the helper column, which is where the defect lives: my first cut's
/// tests only checked the grouping-key projection and passed with the bug.
#[tokio::test]
async fn ldbc_1143_aggregate_argument_column_role_swaps_per_arm() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) \
         RETURN o.city, count(DISTINCT d.city)",
    )
    .await;
    // The aggregate's argument is `d.city`: to-role in the forward arm,
    // from-role in the reversed one. UNION ALL binds by POSITION and the outer
    // aggregate reads one alias, so the helper keeps the SAME output alias in
    // both arms while its EXPRESSION must differ per arm.
    assert_eq!(
        sql.matches("t.end_DestCityName AS \"t.end_DestCityName\"")
            .count(),
        1,
        "#1143: forward arm exports the aggregate arg at its own role:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_OriginCityName AS \"t.end_DestCityName\"")
            .count(),
        1,
        "#1143: under the SHARED alias the reversed arm must bind ITS role's \
         column — repeating the forward arm's expression here is the \
         silent-wrong defect (right row count, wrong values):\n{sql}"
    );
}

/// #1143 review: the NON-denormalized composite schema also renders the
/// two-arm undirected split (unlike standard/polymorphic, which use a single
/// `undir_edges_` CTE), so it exercises this path too — and main was CORRECT
/// there. Pins that the aggregate argument still role-swaps.
#[tokio::test]
async fn ldbc_1143_composite_aggregate_argument_still_role_swaps() {
    let schema = load_schema_from("schemas/test/composite_node_ids.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Account)-[:TRANSFERRED*1..2]-(b:Account) \
         RETURN a.account_number, count(DISTINCT b.account_number)",
    )
    .await;
    assert_eq!(
        sql.matches("t.end_account_number AS \"t.end_account_number\"")
            .count(),
        1,
        "#1143: forward arm:\n{sql}"
    );
    assert_eq!(
        sql.matches("t.start_account_number AS \"t.end_account_number\"")
            .count(),
        1,
        "#1143: under the shared alias the reversed arm must bind the \
         start-role column — main did this correctly via the text swap, so \
         losing it is a REGRESSION:\n{sql}"
    );
}

/// #1143 review round 2: UNION ALL requires every arm to project the same
/// NUMBER of columns. A role-swapped arm's own select carries its own
/// aggregate (whose argument resolves to THIS arm's role column), and
/// `agg_arg_cols` is derived from whatever the merged list holds — so merging
/// it alongside the outer aggregate exported BOTH arms' argument columns from
/// one arm: 3 against the other's 2, ClickHouse Code 53. Main emitted Code 47
/// for this shape, so that would be trading one loud error for another.
///
/// The branch's aggregate is dropped (its per-arm role is carried by
/// `key_branch_overrides`, which rewrites the EXPRESSION under the outer
/// argument's alias), and the trim is scoped to swapped VLP arms so the
/// long-standing coupled / denorm from-to callers keep contributing their own
/// items verbatim.
#[tokio::test]
async fn ldbc_1143_swapped_arm_matches_the_outer_column_contract() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) \
         RETURN o.city, count(DISTINCT o.state)",
    )
    .await;
    // Both arms of the inner union must project the same number of columns.
    let inner = sql
        .split("FROM (")
        .nth(1)
        .expect("inner union present")
        .split(") AS __union")
        .next()
        .expect("inner union closed");
    let widths: Vec<usize> = inner
        .split("UNION ALL")
        .map(|arm| arm.matches(" AS \"").count())
        .collect();
    assert!(
        widths.len() >= 2 && widths.iter().all(|w| *w == widths[0]),
        "#1143: every arm must project the same column count, saw {widths:?}:\n{sql}"
    );
}

/// #1143 boundary: an AGGREGATE-ARGS-ONLY shape has no non-aggregate item to
/// carry, and its arms contribute different helper-column counts — routing it
/// through the per-arm select would turn the existing loud Code 47 into an
/// equally loud Code 53. It deliberately stays on the old path (still broken,
/// tracked on #1143) rather than swapping one error for another.
#[tokio::test]
async fn ldbc_1143_aggregate_args_only_shape_stays_on_the_legacy_path() {
    let schema = load_schema_from("schemas/test/denormalized_flights.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) \
         RETURN count(*), count(DISTINCT o.city)",
    )
    .await;
    // Unchanged from main: the prefix-flipped spelling, still loud at execution.
    assert!(
        sql.contains("end_OriginCityName"),
        "#1143 boundary: this shape is intentionally NOT rerouted; if it now \
         resolves, re-scope the gate and update this test:\n{sql}"
    );
}

// --- #1135: `*0..0` is the zero-hop identity, not a 1-hop chain --------------
//
// `exact_hop_count()` returned `Some(0)` and every consumer treats `Some(n)`
// as an n-edge flat r1..rN chain — which has no zero-length form, so `*0..0`
// (and the `*0` spelling) silently rendered as ONE hop: live, the 1-hop
// pairs where the answer is the identity rows (a = b, one per node). Fixed
// at the single source of truth: `exact_hop_count()` returns None for
// (0, 0), routing through `is_range()` to the recursive CTE, whose zero-hop
// seed with `max_hops == Some(0)` (no recursive arm) is exactly this shape.
//
// Two corpus goldens had FROZEN the wrong 1-hop SQL — their own source
// Python tests assert `a.name == b.name` (identity) and row_count 1, so the
// goldens contradicted the tests that generated them (the #933-class stale
// golden trap). Regenerated.

/// `*0..0` renders the zero-hop seed with no edge join and no recursion.
#[tokio::test]
async fn ldbc_1135_zero_zero_hop_is_identity() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:User)-[:FOLLOWS*0..0]->(b:User) RETURN a.user_id, b.user_id",
    )
    .await;
    assert!(
        sql.contains("0 as hop_count"),
        "#1135: *0..0 must render the zero-hop identity seed:\n{sql}"
    );
    assert!(
        !sql.contains("user_follows") && !sql.contains("JOIN"),
        "#1135: no edge join — a zero-hop path traverses no edge:\n{sql}"
    );
}

/// #1135 boundary: `*1..1` and `*2..2` keep the flat chained-join path.
#[tokio::test]
async fn ldbc_1135_positive_exact_bounds_stay_flat() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    for (cypher, expect_joins) in [
        (
            "MATCH (a:User)-[:FOLLOWS*1..1]->(b:User) RETURN a.user_id, b.user_id",
            1,
        ),
        (
            "MATCH (a:User)-[:FOLLOWS*2..2]->(b:User) RETURN a.user_id, b.user_id",
            2,
        ),
    ] {
        let sql = generate_sql_inline(&schema, cypher).await;
        assert!(
            !sql.contains("WITH RECURSIVE"),
            "#1135: positive exact bounds keep the flat join path \
             (`{cypher}`):\n{sql}"
        );
        assert_eq!(
            sql.matches("user_follows").count(),
            expect_joins,
            "#1135: expected {expect_joins} edge join(s) for `{cypher}`:\n{sql}"
        );
    }
}

// --- #1152: whole-node RETURN on a denormalized undirected VLP ---------------
//
// `RETURN o` (as opposed to `RETURN o.city`) reaches a DIFFERENT expansion site
// than the per-property path: `SelectBuilder`'s `n.*` wildcard branch, which
// resolves each property against the BASE TABLE and, on a denormalized schema,
// through the alias->edge-alias remap -> `t1.origin_city`. The physical column
// is already role-correct for the arm; only the alias is wrong (`t1` is bound
// only INSIDE the CTE body), so the outer scope saw Code 47.
//
// A first attempt (PR #1153, closed) suppressed that remap upstream in
// `SelectBuilder`. It had to gate on VLP-endpoint-ness alone, which ALSO caught
// the flat chained render (where the remap is load-bearing -> new loud
// regression, #1155) and the mixed-access arm (where it produced silently wrong
// rows, #1154). The discriminator does not exist at that layer.
//
// This resolves one layer down instead, in the emitter, where each arm's own
// `CteColumnMetadata` says exactly which column carries which
// `(cypher_alias, cypher_property)` pair — the same query-bound truth the
// per-arm ORDER BY machinery of #1140/#1143 already consumes. Everything
// unresolvable ABSTAINS, reproducing today's output byte-for-byte.

/// #1152: each undirected arm binds ITS OWN role's projected CTE columns, and
/// the edge-table alias never escapes the CTE body.
#[tokio::test]
async fn ldbc_1152_whole_node_denorm_undirected_vlp_binds_cte_columns() {
    let schema = load_schema_from("schemas/test/flights_denorm_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o",
    )
    .await;

    // Only the OUTER select matters: the CTE bodies legitimately reference the
    // edge alias, so a whole-file assertion would be vacuous.
    let outer = sql
        .rsplit_once(") AS __union")
        .map(|(head, _)| {
            let idx = head.rfind("SELECT `").unwrap_or(0);
            head[idx..].to_string()
        })
        .unwrap_or_else(|| panic!("expected a two-arm union:\n{sql}"));

    // Match ANY numbered edge alias (`t1.`, `t3.`, ...): the anon-alias counter
    // is query-scoped (#1088), so pinning one spelling would let a mere
    // renumbering pass.
    let leaked: Vec<&str> = outer
        .split_whitespace()
        .filter(|tok| {
            tok.strip_prefix('t')
                .and_then(|rest| rest.split_once('.'))
                .is_some_and(|(n, _)| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
        })
        .collect();
    assert!(
        leaked.is_empty(),
        "#1152: an edge-table alias escaped into the outer SELECT (bound only \
         inside the CTE body -> Code 47): {leaked:?}\n{outer}"
    );

    // PER ARM, not whole-file: main emitted both physical spellings too (off
    // the edge alias), so a bare `contains` pair would pass on main.
    let arms: Vec<&str> = outer.split("UNION ALL").collect();
    assert_eq!(arms.len(), 2, "#1152: expected two arms:\n{outer}");
    assert!(
        arms[0].contains("t.start_origin_city")
            && arms[0].contains("t.start_origin_code")
            && arms[0].contains("t.start_origin_state"),
        "#1152: the forward arm must read its FROM-role columns:\n{outer}"
    );
    assert!(
        arms[1].contains("t.end_dest_city")
            && arms[1].contains("t.end_dest_code")
            && arms[1].contains("t.end_dest_state"),
        "#1152: the reversed arm must read its TO-role columns — binding the \
         forward arm's spelling here would return the WRONG airport's data \
         while still executing:\n{outer}"
    );
}

/// #1152: the flat CHAINED render (#1155) has NO VLP CTE, so the rebind is
/// inert there by construction. This is the shape PR #1153's upstream gate
/// regressed (22 rows -> Code 47); pin that it keeps the edge-alias spelling.
#[tokio::test]
async fn ldbc_1152_flat_chained_render_keeps_edge_alias() {
    let schema = load_schema_from("schemas/test/flights_denorm_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport)-[:FLIGHT]->(e:Airport) RETURN o",
    )
    .await;
    assert!(
        !sql.contains("vlp_"),
        "#1152/#1155: this shape renders FLAT — if it ever grows a VLP CTE, \
         this test's premise (and the rebind's inertness here) must be \
         re-derived:\n{sql}"
    );
    // The edge alias is correct here: it IS the FROM. Removing it (as #1153
    // did) makes the reference dangle. Assert STRUCTURALLY: whatever numbered
    // alias the FROM binds, the projection must use that same alias — the
    // anon-alias counter is query-scoped (#1088) and shared across
    // concurrently-running tests, so a hardcoded `t1` is order-dependent.
    let from_alias = sql
        .split("flights_denorm AS ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .unwrap_or_else(|| panic!("#1152: expected an aliased base-table FROM:\n{sql}"));
    assert!(
        sql.contains(&format!("{from_alias}.origin_city")),
        "#1152: the flat render must keep resolving through the edge alias \
         (`{from_alias}`), which is bound by its own FROM:\n{sql}"
    );
}

/// #1152: the mixed-access arm (#1154) must ABSTAIN — it stays exactly as loud
/// as main. Making it resolvable without fixing #1154's NULL projection would
/// turn a loud error into silently wrong rows (PR #1153 did exactly that).
#[tokio::test]
async fn ldbc_1152_mixed_access_arm_abstains() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO*1..2]-(b:Person) RETURN a, b",
    )
    .await;
    // `mgr_id` is NOT a column any arm projects, so this stays Code 47 at
    // execution — the pre-existing, ground-rule-1-safe behavior.
    assert!(
        sql.contains("t.mgr_id"),
        "#1152: the mixed-access arm must abstain and reproduce main's \
         spelling; resolving it here surfaces #1154's NULL projection as \
         silently wrong rows:\n{sql}"
    );
    assert!(
        sql.contains("NULL AS \"b.pid\"") || sql.contains("NULL AS `b.pid`"),
        "#1152: #1154's NULL projection is expected to still be present \
         (tracked separately) — if it is gone, this test's premise changed:\n{sql}"
    );
}

/// #1152: a standard (non-denormalized) undirected VLP endpoint already
/// resolved to its CTE columns; the rebind must leave it exactly as it was.
#[tokio::test]
async fn ldbc_1152_standard_undirected_vlp_whole_node_unchanged() {
    let schema = load_schema_from("schemas/dev/social_standard.yaml");
    let sql =
        generate_sql_inline(&schema, "MATCH (a:User)-[:FOLLOWS*1..2]-(b:User) RETURN a").await;
    assert!(
        sql.contains("t.start_name") && sql.contains("t.start_city"),
        "#1152: a standard VLP endpoint must keep reading its CTE columns:\n{sql}"
    );
}

/// #1152: a DIRECTED denorm VLP renders a single arm and already resolved
/// correctly.
#[tokio::test]
async fn ldbc_1152_directed_denorm_vlp_whole_node_unchanged() {
    let schema = load_schema_from("schemas/test/flights_denorm_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]->(d:Airport) RETURN o",
    )
    .await;
    assert!(
        sql.contains("t.start_origin_city"),
        "#1152: the directed shape must keep its FROM-role CTE columns:\n{sql}"
    );
    assert!(
        !sql.contains(") AS __union"),
        "#1152: the directed shape renders ONE outer arm, not a union:\n{sql}"
    );
}

/// #1152 review H1: the rebind may only name a column the CTE BODY actually
/// emits, not merely one its `CteColumnMetadata` advertises.
///
/// On the composite-denorm path the two disagree: the metadata claims
/// `start_code`/`start_state` while the body emits only
/// `start_origin_code`/`start_origin_state`. Binding straight off the metadata
/// fabricated a name no arm defines. That is loud here only by luck — on a
/// schema where a logical property maps to a physical column literally named
/// `code`, the fabricated `start_code` EXISTS and carries the wrong value, so
/// loud would become silently wrong. The body/metadata intersection makes the
/// item abstain instead.
///
/// This also pins the `[]`-no-candidates ABSTAIN arm that the whole safety
/// argument rests on (review M1): a mutation replacing it with a guess must be
/// caught here.
#[tokio::test]
async fn ldbc_1152_rebind_never_names_a_column_the_body_lacks() {
    let schema = load_schema_from("schemas/dev/flights_denorm_mixed_sources.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:AirportComposite)-[:COMPOSITE_FLIGHT*1..2]-(b:AirportComposite) RETURN a",
    )
    .await;

    let outer = sql
        .rsplit_once(") AS __union")
        .map(|(head, _)| {
            let idx = head.rfind("SELECT `").unwrap_or(0);
            head[idx..].to_string()
        })
        .unwrap_or_else(|| panic!("expected a two-arm union:\n{sql}"));

    // Collect every column the outer SELECT reads off the VLP CTE alias, and
    // require each to be a name some CTE body actually exports. Asserting the
    // GENERAL property (rather than the two names that happen to be wrong
    // today) is what makes this survive changes to the metadata builder.
    let exported: std::collections::HashSet<String> = sql
        .split(" as ")
        .skip(1)
        .chain(sql.split(" AS ").skip(1))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(|n| n.trim_matches(|c| c == '"' || c == ',').to_string())
        .collect();

    for tok in outer.split(|c: char| c.is_whitespace() || c == ',') {
        let Some(col) = tok.strip_prefix("t.") else {
            continue;
        };
        let col = col.trim_matches('"');
        if col.is_empty() {
            continue;
        }
        assert!(
            exported.contains(col),
            "#1152 H1: outer SELECT reads `t.{col}`, which no CTE body \
             exports — the rebind must ABSTAIN rather than name a column \
             that only exists in `CteColumnMetadata`:\n{sql}"
        );
    }

    // Concretely: these two were fabricated before the intersection.
    assert!(
        !outer.contains("t.start_code") && !outer.contains("t.end_code"),
        "#1152 H1: `start_code`/`end_code` are metadata-only names on this \
         schema; the body emits `start_origin_code`/`end_dest_code`:\n{outer}"
    );
}

/// #1152 review M1: pin the `[]`-no-candidates ABSTAIN arm directly.
///
/// `RETURN o.city, o` projects the same property twice; the duplicate carries a
/// disambiguated output alias (`o.city_2`) that no `CteColumnMetadata` entry
/// matches, so the lookup finds ZERO candidates and must abstain. Replacing
/// that arm with any same-alias fallback binds `t.end_id` there — a CITY column
/// holding an airport CODE, executing happily and silently wrong.
///
/// The other tests all pass under that mutation (it only affects this
/// duplicate-projection shape), so without this the guard the whole safety
/// argument rests on would be untested.
#[tokio::test]
async fn ldbc_1152_no_candidate_lookup_abstains_rather_than_guessing() {
    let schema = load_schema_from("schemas/test/flights_denorm_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT*1..2]-(d:Airport) RETURN o.city, o",
    )
    .await;

    let dup = sql
        .lines()
        .find(|l| l.contains("\"o.city_2\""))
        .unwrap_or_else(|| panic!("#1152 M1: expected a disambiguated duplicate:\n{sql}"));

    assert!(
        !dup.contains("end_id") && !dup.contains("start_id"),
        "#1152 M1: the unmatched duplicate must ABSTAIN, not fall back to the \
         arm's id column — that binds an airport CODE under a CITY alias and \
         executes silently wrong:\n{dup}"
    );
    // Abstaining means keeping the pre-existing edge-alias spelling, which
    // stays loud exactly as on main (ground rule 1).
    assert!(
        dup.contains("dest_city") || dup.contains("origin_city"),
        "#1152 M1: abstaining reproduces main's spelling for this item:\n{dup}"
    );
}

// ===========================================================================
// #1154 — mixed-access whole-node endpoint resolution
//
// On a MIXED-access edge (`classify_edge_table_pattern` → `Mixed`: one endpoint
// `EmbeddedInEdge`, the other `OwnTable`) a whole-node `RETURN a` / `RETURN a, b`
// sourced its property set from the plan-walk, which returns the EDGE's embedded
// map. That map is incomplete for both roles at once, so main:
//   * lost the embedded endpoint's non-embedded properties (`a.name`), and
//   * projected NOTHING for the own-table endpoint — the column vanished from
//     the result with no error (undirected then NULL-padded it into
//     right-count/wrong-value rows).
// ===========================================================================

/// #1154 behavior: a directed mixed-access single hop must project BOTH
/// endpoints in full. On main this rendered a single column (`a.pid`) for two
/// whole nodes.
#[tokio::test]
async fn ldbc_1154_mixed_access_whole_node_projects_both_endpoints() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a, b",
    )
    .await;
    for want in ["\"a.name\"", "\"a.pid\"", "\"b.name\"", "\"b.pid\""] {
        assert!(
            sql.contains(want),
            "#1154: whole-node RETURN must project {want}; main dropped it:\n{sql}"
        );
    }
    // The own-table endpoint's columns bind the NODE alias, not the edge alias.
    assert!(
        sql.contains("b.name") && sql.contains("b.pid"),
        "#1154: own-table endpoint must resolve against its own table:\n{sql}"
    );
}

/// #1154 role symmetry: the mirrored schema (`foreign_selfloop_end` embeds the
/// TO role instead of the FROM role) must be repaired identically. A fix that
/// keyed on one role would leave this one truncated.
#[tokio::test]
async fn ldbc_1154_mixed_access_whole_node_is_role_symmetric() {
    let schema = load_schema_from("schemas/test/foreign_selfloop_end.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a, b",
    )
    .await;
    for want in ["\"a.name\"", "\"a.pid\"", "\"b.name\"", "\"b.pid\""] {
        assert!(
            sql.contains(want),
            "#1154: mirrored schema must project {want} too:\n{sql}"
        );
    }
}

/// #1154 undirected: each UNION arm must resolve BOTH endpoints. On main each
/// arm projected one endpoint and let `normalize_branch` pad the other with a
/// literal NULL — right row count, every value wrong, no error.
#[tokio::test]
async fn ldbc_1154_undirected_arms_resolve_both_endpoints_no_null_padding() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]-(b:Person) RETURN a, b",
    )
    .await;
    assert!(
        !sql.contains("NULL AS"),
        "#1154: no arm may stand in a NULL literal for an unresolved endpoint \
         — that is indistinguishable from a legitimately absent value:\n{sql}"
    );
    // Both arms present, and each names both endpoints.
    assert_eq!(
        sql.matches("UNION ALL").count(),
        1,
        "#1154: expected the two-arm undirected split:\n{sql}"
    );
    for want in ["\"a.name\"", "\"b.name\""] {
        assert!(
            sql.contains(want),
            "#1154: undirected arms must project {want}:\n{sql}"
        );
    }
}

/// #1154 abstain: a same-table denormalized node (`flights_denorm`'s Airport
/// declares `property_mappings: {}` — every property lives on the edge under a
/// role-specific name) already renders correctly and must be left BYTE-identical.
/// This is the gate that keeps the fix inert for every non-mixed schema.
#[tokio::test]
async fn ldbc_1154_same_table_denorm_whole_node_unchanged() {
    let schema = load_schema_from("schemas/test/flights_denorm_test.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (o:Airport)-[:FLIGHT]->(d:Airport) RETURN o, d",
    )
    .await;
    // Role-specific edge columns, NOT own-table columns.
    for want in ["origin_city", "origin_code", "dest_city", "dest_code"] {
        assert!(
            sql.contains(want),
            "#1154: same-table denorm must keep its per-role edge columns \
             ({want} missing) — the fix must not fire here:\n{sql}"
        );
    }
}

/// #1154 abstain: a fully-denormalized schema must be left alone. Zeek's Domain
/// declares `property_mappings: {}` in YAML but the catalog SYNTHESIZES an id
/// entry (`{query → query}`), so the emptiness filter does NOT stop it — the
/// endpoint-join gate does, because a fully-denormalized pattern never binds an
/// endpoint's own table.
///
/// This distinction is load-bearing and was found by measurement: an earlier
/// cut gated on "declares a property beyond its id" and, verified by mutation,
/// that gate turned out to be masked by the join gate on all 2400 swept shapes
/// (dead code), so it was removed rather than shipped unexercised. Without the
/// join gate this query rebinds `b.name` → `b.query` against an unjoined table:
/// a renamed output column AND a Code 47.
#[tokio::test]
async fn ldbc_1154_fully_denormalized_schema_abstains() {
    let schema = load_schema_from("schemas/test/zeek_merged_collision.yaml");
    let sql =
        generate_sql_inline(&schema, "MATCH (a:IP)-[:REQUESTED]->(b:Domain) RETURN a, b").await;
    assert!(
        sql.contains("\"b.name\""),
        "#1154: the node's declared Cypher property name must survive — \
         rebinding to the synthesized id entry renames the output column:\n{sql}"
    );
    assert!(
        !sql.contains("\"b.query\""),
        "#1154: an id-only property map must NOT drive the own-table rebind:\n{sql}"
    );
}

/// #1154 abstain: when the pattern collapses to a bare edge scan
/// (`SingleTableScan`) no endpoint node table is joined, so own-table columns
/// would dangle. `RETURN *` reaches that collapse on a mixed-access edge because
/// `plan_references_alias` has no `Star` arm — a PRE-EXISTING analyzer defect
/// (main also discards an explicitly written `b.name` next to `RETURN *`).
/// Until that is fixed upstream this shape must keep main's exact output rather
/// than trade a silent truncation for a Code 47.
#[tokio::test]
async fn ldbc_1154_collapsed_pattern_abstains_rather_than_dangling() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN *",
    )
    .await;
    assert!(
        !sql.contains("a.name"),
        "#1154: with the pattern collapsed to a bare edge scan there is no \
         own-table join to bind to — the fix must abstain here:\n{sql}"
    );
    assert!(
        sql.contains("mgr_id"),
        "#1154: abstaining must reproduce main's spelling:\n{sql}"
    );
}

/// #1154 review (CRITICAL): the endpoint-join gate is evaluated against the
/// OUTERMOST plan, but a Cypher `UNION` renders each arm separately. A first cut
/// disjoined across branches (`Union => inputs.iter().any(..)`), so a COLLAPSED
/// arm — `RETURN *`, where `SingleTableScan` fires and nothing binds an endpoint
/// table — passed the gate on its SIBLING's evidence and emitted exactly the
/// dangling own-table columns the gate exists to prevent (Code 47 where main
/// returned rows).
///
/// The fix requires EVERY branch that owns the relationship to bind an endpoint.
/// Here the collapsed arm does not, so its own expansion abstains. What remains
/// is a genuine Cypher arity mismatch, surfaced LOUDLY: main hid it by
/// truncating BOTH arms to one column so they happened to line up.
///
/// Single-arm `RETURN *` cannot catch this — the leak needs a sibling to leak
/// from, which is why the earlier sweep (single-clause queries only) missed it.
#[tokio::test]
async fn ldbc_1154_union_arm_gate_does_not_leak_across_branches() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let err = try_generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN * \
         UNION MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a, b",
    )
    .await
    .expect_err("#1154: expected a loud arity error, not a dangling projection");
    assert!(
        err.contains("UnionColumnMismatch"),
        "#1154: the collapsed arm must abstain, leaving a clean arity error — \
         a rendered SQL with b.name and no b join means the gate leaked \
         across branches:\n{err}"
    );
    // The collapsed arm keeps main's single column; the healthy arm keeps its
    // full set. Both halves of that are the point: neither arm borrowed the
    // other's evidence.
    assert!(
        err.contains("first_columns: \"a.pid\"") && err.contains("a.name, a.pid, b.name, b.pid"),
        "#1154: expected the collapsed arm truncated and the healthy arm \
         complete:\n{err}"
    );
}

/// #1154 review: the undirected two-arm split reuses the SAME rel alias in both
/// arms with swapped connections. It is a legitimate multi-owner case, so the
/// branch-scoped gate must resolve it rather than abstain on ambiguity — an
/// "exactly one owning branch" rule silently re-broke this (back to NULL
/// padding) while every single-clause test still passed.
#[tokio::test]
async fn ldbc_1154_undirected_split_shares_rel_alias_across_arms() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]-(b:Person) RETURN a, b",
    )
    .await;
    assert!(
        !sql.contains("NULL AS"),
        "#1154: both arms share rel alias `t1`; requiring a UNIQUE owning \
         branch makes the gate abstain and restores the NULL padding:\n{sql}"
    );
    for want in ["\"a.name\"", "\"b.name\""] {
        assert!(
            sql.contains(want),
            "#1154: undirected arms must still resolve {want}:\n{sql}"
        );
    }
}

/// #1154 review (HIGH): two independent `MATCH` clauses plan as a
/// `CartesianProduct`, whose sides are also rendered separately. The gate
/// judges each side on ITS OWN joins.
///
/// Here both sides DO bind their own-table endpoint (`b`, `d`), so both expand
/// — while `a` and `c` stay embedded-only because nothing binds a table for
/// them. The mixed result is not a "partial set": it is per-alias resolution
/// doing exactly what the evidence supports, and it matches the per-property
/// oracle on this build row-for-row (verified live). Main projected only
/// `a.pid`/`c.pid`, dropping `b` and `d` entirely.
///
/// HONEST SCOPE: unlike its UNION sibling, this test is a BEHAVIOR pin, not a
/// gate pin. Mutating the cartesian arm back to the leaky cross-side
/// disjunction leaves this output byte-identical — on every available fixture
/// the two sides agree about whether an endpoint is bound, so the leak has no
/// observable effect. No schema in the repo produces a mixed-verdict cartesian.
/// The scoping is still correct by construction (a side's joins say nothing
/// about its sibling); it is simply not observable here, and this comment
/// exists so nobody later reads a passing test as proof that it is.
#[tokio::test]
async fn ldbc_1154_cartesian_sides_are_scoped_independently() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) \
         MATCH (c:Person)-[:REPORTS_TO]->(d:Person) RETURN *",
    )
    .await;
    // Each own-table endpoint resolves against ITS OWN side's join.
    assert!(
        sql.contains("b.name") && sql.contains("d.name"),
        "#1154: each cartesian side binds its own endpoint table, so both \
         must expand:\n{sql}"
    );
    // Symmetry is the tell that neither side borrowed the other's evidence:
    // an asymmetric result would mean one side passed on foreign joins.
    assert_eq!(
        sql.matches("\"b.").count(),
        sql.matches("\"d.").count(),
        "#1154: the two sides are structurally identical, so their projections \
         must be too — asymmetry means the gate crossed sides:\n{sql}"
    );
}

/// #1154 follow-up review (MEDIUM): `pattern_binds_an_endpoint_table` must stop
/// at a `Union` (or a branch borrows a sibling's joins), so a wrapper above the
/// root — `ORDER BY`, `LIMIT`, `SKIP` — would hide the `Union` beneath it, the
/// branch lookup would find nothing, and the gate would abstain.
///
/// That silently disabled the whole fix for the undirected shape the moment a
/// sort or limit was added: back to `NULL AS "b.pid"` and wrong values, with
/// every unsorted test still green. `union_branches_all_bind` now descends
/// through those wrappers.
#[tokio::test]
async fn ldbc_1154_wrapper_above_union_does_not_disable_the_fix() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");
    for suffix in [
        "",
        " ORDER BY a.pid",
        " LIMIT 3",
        " SKIP 1",
        " ORDER BY a.pid LIMIT 2",
    ] {
        let sql = generate_sql_inline(
            &schema,
            &format!("MATCH (a:Person)-[:REPORTS_TO]-(b:Person) RETURN a, b{suffix}"),
        )
        .await;
        assert!(
            !sql.contains("NULL AS"),
            "#1154: `{suffix}` above the undirected UNION must not hide it from \
             the branch lookup — a NULL literal here means the fix silently \
             stopped applying:\n{sql}"
        );
        assert!(
            sql.contains("\"a.name\"") && sql.contains("\"b.name\""),
            "#1154: both endpoints must still resolve with `{suffix}`:\n{sql}"
        );
    }
}

/// #1154 follow-up review (HIGH): across a WITH barrier the planner binds a
/// second pattern's own-table endpoint through the EDGE-side id column —
/// `people AS d ON d.mgr_id = t2.emp_id`, and `people` has no `mgr_id` (#1160).
///
/// Main hides that by dropping the endpoint from the projection entirely.
/// Projecting it correctly makes the broken join reachable, turning a silent
/// truncation into a Code 47 — trading one wrong answer for another. The
/// corruption happens in a render-stage rewrite AFTER this gate runs (at
/// analyzer level the join is still the correct `d.pid = t2.emp_id`), so it
/// cannot be detected by inspecting the join; the gate abstains across a
/// barrier instead and reproduces main until #1160 lands.
///
/// A TRIVIAL `WITH a, b` is eliminated by the analyzer and never reaches here,
/// so the abstain costs nothing on that shape — pinned below so the scope of
/// this concession stays visible.
#[tokio::test]
async fn ldbc_1154_with_barrier_second_match_abstains_pending_1160() {
    let schema = load_schema_from("schemas/test/foreign_selfloop.yaml");

    // Real barrier + a second pattern: abstain, reproducing main.
    let sql = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WITH count(*) AS n \
         MATCH (c:Person)-[:REPORTS_TO]->(d:Person) RETURN c, d, n",
    )
    .await;
    assert!(
        !sql.contains("d.name"),
        "#1154/#1160: across a WITH barrier the endpoint's join is spelled with \
         the edge-side id column, so projecting own-table columns yields a \
         Code 47 — abstain until #1160 fixes the join:\n{sql}"
    );

    // Trivial WITH is eliminated upstream, so the fix still applies there.
    let trivial = generate_sql_inline(
        &schema,
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WITH a, b RETURN a, b",
    )
    .await;
    assert!(
        trivial.contains("\"a.name\"") && trivial.contains("\"b.name\""),
        "#1154: a trivial WITH is eliminated by the analyzer and must NOT be \
         caught by the barrier abstain:\n{trivial}"
    );
}
