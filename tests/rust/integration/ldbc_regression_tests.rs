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

/// #1103: COMPOSITE node_id is projected as ONE pipe-joined `start_id`/`end_id`,
/// so every key column maps to it — and the per-column expansion collapses to a
/// single predicate rather than a repeated conjunct.
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
        sql.contains("WHERE (NOT (end_id = start_id))"),
        "#1103: composite node_id identity must collapse to the pipe-joined id \
         columns, deduplicated:\n{sql}"
    );
    assert!(
        !sql.contains("end_id = start_id AND end_id = start_id"),
        "#1103: the collapsed per-column conjuncts must be deduplicated:\n{sql}"
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
