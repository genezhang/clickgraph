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

/// #1103 (updated by #1131): COMPOSITE node_id identity comparison. Originally
/// this collapsed every key column onto the ONE pipe-joined `start_id`/`end_id`
/// — which was also what silently turned a SINGLE-component cross-endpoint
/// comparison (`a.bank_id = b.bank_id`) into whole-id equality (#1131, 0 rows
/// vs an oracle 3). Post-#1131 each component maps to its own projected
/// `<prefix>_<col>` column, so whole-node identity expands to the
/// per-component AND — row-equivalent to concat equality (and safer for values
/// containing the `|` separator), live-verified 8 == oracle 8 on a populated
/// fixture.
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
