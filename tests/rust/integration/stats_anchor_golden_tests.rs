//! S1 stats-informed anchor selection — with-stats golden set.
//!
//! `docs/design/STATS_PLANNING.md`: the main golden net (sql_golden_tests.rs,
//! corpus_sweep.rs) is deliberately STATS-LESS — no test there ever attaches a
//! `TableStatsSnapshot`, so those outputs are byte-identical to the stats-less
//! engine regardless of this feature. THIS file is the with-stats counterpart:
//! it locks the plan the engine produces when a FIXED, programmatically
//! injected stats fixture is present (no live ClickHouse needed), so a change
//! in stats-driven anchor ranking shows up as a reviewable golden diff.
//!
//! Goldens live in `tests/rust/integration/golden/sql_ir/stats_standard/`.
//! Regenerate after an intended change with:
//!
//! ```text
//! UPDATE_GOLDEN=1 cargo test -p clickgraph --test integration stats_anchor -- --nocapture
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use clickgraph::{
    graph_catalog::{
        config::GraphSchemaConfig, graph_schema::GraphSchema, table_stats::TableStatsSnapshot,
    },
    open_cypher_parser::{parse_cypher_statement, strip_comments},
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
    server::query_context::{
        set_current_schema, set_current_table_stats, with_query_context, QueryContext,
    },
};

/// FIXED stats fixture for the social benchmark schema: posts much smaller
/// than users, follows the largest. Chosen so stats-ranked anchors DIFFER from
/// the alphabetical tie-break in the corpus below.
fn fixture_snapshot() -> Arc<TableStatsSnapshot> {
    let counts: HashMap<String, u64> = [
        ("social.users_bench", 1_000_000u64),
        ("social.posts_bench", 100),
        ("social.user_follows_bench", 5_000_000),
        ("social.authored_bench", 900),
        ("social.post_likes_bench", 20_000),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    Arc::new(TableStatsSnapshot::from_counts(counts))
}

/// Corpus: shapes where `select_anchor` has more than one candidate FROM
/// marker, so the stats ranking is actually exercised.
///
/// `stats_cartesian_smaller_table_anchor`: `(a:User), (b:Post)` — two FROM
/// markers; alphabetical picks `a` (users, 1M in the fixture); stats must flip
/// the anchor to `b` (posts, 100).
const STATS_CORPUS: &[(&str, &str)] = &[
    (
        "stats_cartesian_smaller_table_anchor",
        "MATCH (a:User), (b:Post) RETURN a.name, b.title",
    ),
    (
        "stats_cartesian_three_way",
        "MATCH (a:User), (b:Post), (c:User) RETURN a.name, b.title, c.name",
    ),
];

fn load_schema() -> GraphSchema {
    let yaml = "benchmarks/social_network/schemas/social_benchmark.yaml";
    GraphSchemaConfig::from_yaml_file(yaml)
        .unwrap_or_else(|e| panic!("load schema {yaml}: {e:?}"))
        .to_graph_schema()
        .unwrap_or_else(|e| panic!("convert {yaml} to GraphSchema: {e:?}"))
}

/// Render through the production path (mirrors sql_golden_tests::render),
/// optionally with the stats fixture attached to the task-local context —
/// exactly how the server attaches it in stats-enabled mode.
async fn render_with_stats(
    schema: &GraphSchema,
    cypher: &str,
    stats: Option<Arc<TableStatsSnapshot>>,
) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    with_query_context(QueryContext::default(), async move {
        set_current_schema(Arc::new(schema.clone()));
        if let Some(s) = stats {
            set_current_table_stats(s);
        }
        let cleaned = strip_comments(&cypher);
        let (_rest, statement) =
            parse_cypher_statement(&cleaned).unwrap_or_else(|e| panic!("parse: {e:?}"));
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

fn golden_path(name: &str) -> String {
    format!(
        "{}/tests/rust/integration/golden/sql_ir/stats_standard/{}__clickhouse.sql",
        env!("CARGO_MANIFEST_DIR"),
        name
    )
}

/// The with-stats golden set: locks the plan produced under the fixed fixture.
#[tokio::test]
async fn stats_anchor_golden_snapshots() {
    let update = std::env::var("UPDATE_GOLDEN").as_deref() == Ok("1");
    let schema = load_schema();
    let mut mismatches: Vec<String> = Vec::new();

    for (name, cypher) in STATS_CORPUS {
        let sql = crate::sql_golden_tests::normalize(
            &render_with_stats(&schema, cypher, Some(fixture_snapshot())).await,
        );
        assert!(
            sql.contains("SELECT"),
            "stats_standard/{name} produced SQL without SELECT:\n{sql}"
        );
        let path = golden_path(name);
        if update {
            if let Some(dir) = std::path::Path::new(&path).parent() {
                std::fs::create_dir_all(dir).expect("create golden dir");
            }
            std::fs::write(&path, &sql).expect("write golden");
        } else {
            match std::fs::read_to_string(&path) {
                Ok(expected) if expected == sql => {}
                Ok(expected) => mismatches.push(format!(
                    "--- stats_standard/{name} MISMATCH ---\nEXPECTED:\n{expected}\nACTUAL:\n{sql}\n"
                )),
                Err(_) => mismatches.push(format!(
                    "--- stats_standard/{name} MISSING golden (run UPDATE_GOLDEN=1) ---"
                )),
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} stats golden mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// Stats flip the anchor to the smaller table; without stats the SQL is
/// byte-identical to the stats-less engine (the FROM stays on the
/// alphabetical pick). This is the "flag off == today" proof at the SQL
/// level, plus the "flag on == smaller table first" behavior check.
#[tokio::test]
async fn stats_flip_anchor_and_off_is_byte_identical() {
    let schema = load_schema();
    let cypher = "MATCH (a:User), (b:Post) RETURN a.name, b.title";

    // Without stats: rendered twice, identical (determinism), and FROM is the
    // alphabetical anchor `a` (users_bench).
    let off1 = render_with_stats(&schema, cypher, None).await;
    let off2 = render_with_stats(&schema, cypher, None).await;
    assert_eq!(off1, off2, "stats-less render must be deterministic");
    assert!(
        off1.contains("FROM social.users_bench AS a"),
        "stats-less anchor must stay the alphabetical pick, got:\n{off1}"
    );

    // With the fixture (posts=100 << users=1M): anchor flips to `b`.
    let on = render_with_stats(&schema, cypher, Some(fixture_snapshot())).await;
    assert!(
        on.contains("FROM social.posts_bench AS b"),
        "with-stats anchor must be the smaller table, got:\n{on}"
    );
    assert_ne!(off1, on, "fixture is expected to change the anchor");

    // And an EMPTY snapshot (stats enabled but no counts known) must degrade
    // to exactly the stats-less bytes.
    let empty = render_with_stats(
        &schema,
        cypher,
        Some(Arc::new(TableStatsSnapshot::from_counts(HashMap::new()))),
    )
    .await;
    assert_eq!(
        off1, empty,
        "unknown-count degradation must be byte-identical to stats-less"
    );
}
