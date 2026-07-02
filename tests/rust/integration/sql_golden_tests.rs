//! Golden SQL snapshots — Phase 0 of the dialect-neutral SQL rendering refactor.
//!
//! These lock the *current* `RenderPlan -> SQL` output for BOTH dialects
//! (ClickHouse + Databricks) over a representative query corpus. They are the
//! regression net for the refactor: every later phase must keep the ClickHouse
//! goldens **byte-identical**, and any intended Databricks change shows up as a
//! reviewable golden diff rather than silently.
//!
//! Goldens live in `tests/rust/integration/golden/sql_ir/{name}__{dialect}.sql`.
//! Regenerate after an *intended* change with:
//!
//! ```text
//! UPDATE_GOLDEN=1 cargo test -p clickgraph --test integration sql_golden -- --nocapture
//! ```
//!
//! No ClickHouse connection is needed — this is SQL generation only.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::{parse_cypher_statement, strip_comments},
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
    sql_generator::SqlDialect,
};

/// Representative corpus exercising the RenderPlan -> SQL surface. Chosen to
/// render cleanly on BOTH dialects (no UNWIND/arrayJoin or array_count, which
/// hit not-yet-implemented Spark structural gaps). Add cases as coverage grows.
const CORPUS: &[(&str, &str)] = &[
    ("simple_match", "MATCH (u:User) RETURN u.name"),
    ("project_multi", "MATCH (u:User) RETURN u.user_id, u.name, u.country"),
    ("distinct", "MATCH (u:User) RETURN DISTINCT u.country"),
    (
        "where_comparison",
        "MATCH (u:User) WHERE u.country = 'US' RETURN u.name",
    ),
    (
        "where_and",
        "MATCH (u:User) WHERE u.country = 'US' AND u.is_active = true RETURN u.name",
    ),
    (
        "where_contains",
        "MATCH (u:User) WHERE u.name CONTAINS 'a' RETURN u.user_id",
    ),
    (
        "where_in_list",
        "MATCH (u:User) WHERE u.country IN ['US', 'UK'] RETURN u.name",
    ),
    ("in_empty", "MATCH (u:User) WHERE u.country IN [] RETURN u.name"),
    (
        "order_skip_limit",
        "MATCH (u:User) RETURN u.name ORDER BY u.name DESC SKIP 5 LIMIT 10",
    ),
    // SKIP without LIMIT: CH needs a huge upper bound; Spark uses bare OFFSET.
    (
        "skip_only",
        "MATCH (u:User) RETURN u.name ORDER BY u.name SKIP 3",
    ),
    // SKIP/LIMIT inside a WITH -> drives the CTE-body LIMIT emission path.
    (
        "with_skip_limit",
        "MATCH (u:User) WITH u.name AS n ORDER BY n SKIP 2 LIMIT 5 RETURN n",
    ),
    ("aggregate_count", "MATCH (u:User) RETURN count(u)"),
    (
        "aggregate_group_collect",
        "MATCH (u:User) RETURN u.country, collect(u.name) AS names",
    ),
    (
        "string_fns",
        "MATCH (u:User) RETURN toUpper(u.name) AS up, toLower(u.country) AS lo",
    ),
    (
        "case_expr",
        "MATCH (u:User) RETURN CASE WHEN u.is_active = true THEN 'active' ELSE 'inactive' END AS status",
    ),
    (
        "single_hop",
        "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name",
    ),
    (
        "cross_node_hop",
        "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    (
        "with_having",
        "MATCH (u:User) WITH u.country AS c, count(u) AS n WHERE n > 5 RETURN c, n",
    ),
    (
        "vlp_recursive",
        "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.user_id",
    ),
    ("whole_entity", "MATCH (u:User) RETURN u"),
    // List slicing -> arraySlice (CH) / slice (Spark). Both the 3-arg bounded
    // form and the 2-arg open-ended form (Spark needs a computed length).
    // Cypher ranges are HALF-OPEN: [1..3] yields indices 1,2 (2 elements), so
    // the rendered length is `to - from`, not `to - from + 1`.
    (
        "list_slice_bounded",
        "MATCH (u:User) RETURN [10, 20, 30, 40][1..3] AS s",
    ),
    (
        "list_slice_open",
        "MATCH (u:User) RETURN [10, 20, 30, 40][1..] AS s",
    ),
    (
        "list_slice_to",
        "MATCH (u:User) RETURN [10, 20, 30, 40][..2] AS s",
    ),
    // from > to is valid Cypher and must yield []. The length must floor at 0:
    // a negative length is silently wrong on CH arraySlice and errors on Spark slice.
    (
        "list_slice_empty",
        "MATCH (u:User) RETURN [10, 20, 30, 40][3..1] AS s",
    ),
    // tail() -> CH arraySlice(list, 2) / Spark slice(list, 2, greatest(size-1, 0))
    ("list_tail", "MATCH (u:User) RETURN tail([10, 20, 30]) AS t"),
    // Interval arithmetic on an epoch-millis column -> CH
    // `toUnixTimestamp64Milli(fromUnixTimestamp64Milli(x) + toIntervalDay(n))`
    // vs Spark `unix_millis(timestamp_millis(x) + make_dt_interval(n,0,0,0))`.
    // Day-time, year-month, and same-family multi-unit are the validated cases
    // (verified live on Databricks SQL). Mixing year-month with day-time in one
    // duration() is unsupported on Spark and intentionally not in the corpus.
    (
        "interval_add_days",
        "MATCH (u:User) RETURN u.registration_date + duration({days: 7}) AS d",
    ),
    (
        "interval_sub_month",
        "MATCH (u:User) RETURN u.registration_date - duration({months: 1}) AS d",
    ),
    (
        "interval_multi_same_family",
        "MATCH (u:User) RETURN u.registration_date + duration({days: 5, hours: 2}) AS d",
    ),
    // Heterogeneous end type (User|Post) routes through multi_type_vlp_joins,
    // locking the generator output for both dialects (incl. dialect-aware
    // array/string casts: CH `toString(..)`/`['x']` vs Spark `string(..)`/
    // `array('x')`). NOTE: this query enumerates real paths, so it takes the
    // concrete-branch path and does NOT reach `generate_empty_cte_sql` (the
    // empty-placeholder CAST sites migrated in this slice) — those remain
    // covered by the `cast_as`/`sql_type_name` unit tests; an integration
    // golden for the no-path empty branch is a deferred follow-up.
    // Multi-type *variable-length* path. Beyond locking the generator output,
    // this guards a UNION column-count regression: branches span differing hop
    // counts (1-hop and 2-hop), and ALL branches must project the SAME columns.
    // In particular the 1-hop branch must NOT additionally emit the Browser
    // dedup `r_from_id`/`r_to_id` pair that multi-hop branches lack, or
    // ClickHouse rejects the UNION ("different number of columns", Code:53) —
    // the failure seen on LDBC interactive complex-12
    // (`[:HAS_TYPE|IS_SUBCLASS_OF*0..]`). Those columns are only emitted for a
    // pure fixed single hop (`*1..1`), where every branch is a uniform 1-hop.
    (
        "vlp_multi_type",
        "MATCH (a:User)-[:FOLLOWS|AUTHORED*1..2]->(b) RETURN b",
    ),
    // type(r) on a multi-type edge reads the CTE's `path_relationships` array.
    // Regression guard for the array-index fix: the FIRST relationship is index 0
    // (Cypher 0-based) -> renders 1-based as CH `path_relationships[1]` and
    // Databricks `element_at(path_relationships, 1)`. The previous `[2]` was out of
    // bounds on a 1-element array (CH silently returned ""; Databricks errored).
    (
        "multi_type_rel_type_fn",
        "MATCH (a:User)-[r:FOLLOWS|AUTHORED]->(b) RETURN type(r) AS t",
    ),
    // Negative list index: Cypher `[-1]` = last element. Both CH arrayElement and
    // Spark element_at already treat -1 as last, so it must render UNCHANGED (not
    // offset by +1). The old +1 shifted -1 -> 0, and CH `arr[0]` silently returned
    // the type default (0) instead of the last element. Guards CH `[-1]` /
    // Databricks `element_at(..., -1)`; a non-negative index (index0 -> [1]) is the
    // control.
    (
        "list_index_negative",
        "MATCH (u:User) RETURN [10, 20, 30][-1] AS last, [10, 20, 30][0] AS first",
    ),
    // Dialect function-name mappings (regression for the Databricks overrides):
    // replace -> CH replaceAll / Spark replace; head/last -> CH arrayElement /
    // Spark element_at; stdev -> CH stddevSamp / Spark stddev_samp. Previously all
    // emitted the CH name on Databricks (unmapped function -> execution error).
    (
        "fn_replace",
        "MATCH (u:User) RETURN replace(u.name, 'a', 'X') AS r",
    ),
    (
        "fn_head_last",
        "MATCH (u:User) WITH collect(u.name) AS ns RETURN head(ns) AS h, last(ns) AS l",
    ),
    (
        "fn_stdev",
        "MATCH (u:User) RETURN stdev(u.user_id) AS s",
    ),
    // toBoolean -> CH toBool / Spark boolean. Both accept string ('true'/'false')
    // and numeric args; the old if(arg,1,0) form emitted invalid SQL for string
    // inputs (CH: "Illegal type String ... of function if").
    (
        "fn_toboolean",
        "MATCH (u:User) RETURN toBoolean('true') AS r",
    ),
    // trim -> bare trim(arg) on both dialects. The old arg_transform emitted
    // `trim(BOTH arg)` (missing `' ' FROM`), invalid SQL that 500'd on CH.
    (
        "fn_trim",
        "MATCH (u:User) RETURN trim(u.name) AS r",
    ),
    // date() -> CH toDate / Spark to_date. Spark has no toDate; the entry
    // previously fell back to the CH name on Databricks (UNRESOLVED_ROUTINE).
    (
        "fn_date",
        "MATCH (u:User) RETURN date('2020-01-15') AS d",
    ),
    // Dialect string-fn mappings: ltrim/rtrim -> CH trimLeft/trimRight but Spark
    // has no trimLeft/trimRight (uses ltrim/rtrim). Previously emitted the CH name
    // on Databricks (UNRESOLVED_ROUTINE `trimLeft`).
    (
        "fn_ltrim_rtrim",
        "MATCH (u:User) RETURN ltrim(u.name) AS l, rtrim(u.name) AS r",
    ),
    // `=~` regex match -> CH match() / Spark rlike(). Spark has no match(); the
    // previous hardcoded match() at every RegexMatch render site errored on
    // Databricks (UNRESOLVED_ROUTINE `match`).
    (
        "fn_regex_match",
        "MATCH (u:User) WHERE u.name =~ '.*a.*' RETURN u.user_id",
    ),
    (
        "optional_match",
        "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
    ),
    (
        // NB: the engine currently renders this via the multi-label entity-union
        // path (a `__multi_label_union` CTE), not a projection-list UNION — the
        // golden locks that current behavior.
        "union",
        "MATCH (u:User) RETURN u.name AS x UNION MATCH (p:Post) RETURN p.title AS x",
    ),
    (
        "group_two_keys",
        "MATCH (u:User) RETURN u.country, u.city, count(u) AS n",
    ),
    // NOTE: Path D coverage (EXISTS / pattern-predicate, e.g.
    // `WHERE (u)-[:AUTHORED]->(:Post)`) is intentionally absent — that path
    // currently hits `unimplemented!` in render_expr for anonymous pattern
    // nodes in expression context. Add it to this corpus once it renders
    // (it is the path Phase 2 of the IR refactor unifies). Likewise composite
    // node-ID, denormalized, multi-label, and UNWIND/arrayJoin shapes need
    // additional schemas / not-yet-implemented Spark structural support.
];

fn load_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_file("benchmarks/social_network/schemas/social_benchmark.yaml")
        .expect("load social_benchmark.yaml")
        .to_graph_schema()
        .expect("convert to GraphSchema")
}

async fn render(schema: &GraphSchema, cypher: &str, dialect: SqlDialect) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext {
        dialect,
        ..QueryContext::default()
    };
    with_query_context(ctx, async move {
        set_current_schema(Arc::new(schema.clone()));
        let cleaned = strip_comments(&cypher);
        let (_rest, statement) =
            parse_cypher_statement(&cleaned).unwrap_or_else(|e| panic!("parse: {e:?}"));
        let (logical_plan, _plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("plan: {e:?}"));
        let render_plan = logical_plan_to_render_plan(logical_plan, &schema)
            .unwrap_or_else(|e| panic!("render: {e:?}"));
        render_plan.to_sql()
    })
    .await
}

/// Anonymize the two process-global counters whose values vary with test
/// ordering/concurrency: `ALIAS_COUNTER` (anonymous rel aliases `t{n}`) and
/// `CTE_COUNTER` (`cte{n}`). Each is remapped by first appearance, so goldens
/// are deterministic while structure (which alias joins where) is preserved.
///
/// CAUTION: this is text-blind — it rewrites any `t<digits>`/`cte<digits>`
/// token, including ones inside string literals or a schema column literally
/// named `t5`. Today the corpus contains no such token (verified), but if you
/// add a query whose SQL contains a non-counter `t<n>`/`cte<n>`, tighten this
/// (e.g. scope to the alias-defining position) so a real regression in that
/// token can't be silently normalized away.
fn normalize(sql: &str) -> String {
    fn remap(input: &str, pattern: &str, prefix: &str) -> String {
        let re = regex::Regex::new(pattern).unwrap();
        let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut next = 0usize;
        for m in re.find_iter(input) {
            seen.entry(m.as_str().to_string()).or_insert_with(|| {
                let p = format!("{prefix}{next}");
                next += 1;
                p
            });
        }
        re.replace_all(input, |c: &regex::Captures| seen[&c[0].to_string()].clone())
            .into_owned()
    }
    let s = remap(sql, r"\bt\d+\b", "t");
    remap(&s, r"\bcte\d+\b", "cte")
}

fn golden_path(name: &str, dialect: &str) -> String {
    format!(
        "{}/tests/rust/integration/golden/sql_ir/{}__{}.sql",
        env!("CARGO_MANIFEST_DIR"),
        name,
        dialect
    )
}

#[tokio::test]
async fn sql_golden_snapshots() {
    let schema = load_schema();
    let update = std::env::var("UPDATE_GOLDEN").as_deref() == Ok("1");
    let mut mismatches: Vec<String> = Vec::new();

    for (name, cypher) in CORPUS {
        for (dialect, dname) in [
            (SqlDialect::ClickHouse, "clickhouse"),
            (SqlDialect::Databricks, "databricks"),
        ] {
            let sql = normalize(&render(&schema, cypher, dialect).await);
            // Guard against a vacuous pass (e.g. a future to_sql() returning "").
            assert!(
                sql.contains("SELECT"),
                "{name}__{dname} produced SQL without SELECT:\n{sql}"
            );
            let path = golden_path(name, dname);

            if update {
                if let Some(dir) = std::path::Path::new(&path).parent() {
                    std::fs::create_dir_all(dir).expect("create golden dir");
                }
                std::fs::write(&path, &sql).expect("write golden");
            } else {
                match std::fs::read_to_string(&path) {
                    Ok(expected) if expected == sql => {}
                    Ok(expected) => mismatches.push(format!(
                        "--- {name}__{dname} MISMATCH ---\nEXPECTED:\n{expected}\nACTUAL:\n{sql}\n"
                    )),
                    Err(_) => mismatches.push(format!(
                        "--- {name}__{dname} MISSING golden (run UPDATE_GOLDEN=1) ---"
                    )),
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} golden mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
