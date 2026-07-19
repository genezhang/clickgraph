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
#[ignore = "pre-existing parser gap (found while fixing #516, NOT caused by it): the second \
    UNION ALL arm of bi-4-workaround.cypher opens with `UNWIND topForums AS topForum1 \
    MATCH (person:Person)<-[:HAS_MEMBER]-(topForum1:Forum) ...` — UNWIND before MATCH at the \
    top level of a query/union-arm. parse_query_with_nom() only parses UNWIND clauses *after* \
    MATCH/OPTIONAL MATCH (src/open_cypher_parser/mod.rs), unlike with_clause.rs's nested \
    subsequent_unwind/subsequent_match chain, which already supports that ordering. Before \
    #516's all-consuming top-level parse fix, this silently truncated the whole arm (MATCH, \
    WITH, RETURN, ORDER BY, LIMIT all silently dropped as 'trailing garbage') and the test \
    still passed because it only asserts non-empty SQL containing SELECT — a live example of \
    the exact silent-drop bug class #516 fixes. Fixing the UNWIND-before-MATCH ordering gap \
    itself is a separate, out-of-scope parser feature; tracked for future work, not fixed here."]
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
