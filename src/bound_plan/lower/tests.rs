//! Lowering tests: the SQL each lowered construct produces, and
//! `Unsupported` for what S4a does not lower. Row-level correctness is
//! checked against Neo4j by the oracle (`scripts/oracle/run_corpus.py` with
//! `CLICKGRAPH_BOUND_PLAN=on`).

use std::collections::HashMap;

use crate::graph_catalog::config::GraphSchemaConfig;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::translate::{translate_bound_plan, ReadOptions};

fn social() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(include_str!(
        "../../../schemas/test/social_integration.yaml"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

/// Table options: a schema filter, view parameters, FINAL.
fn options_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_options
graph_schema:
  nodes:
    - label: A
      database: db
      table: a
      node_id: id
      filter: "kind = 'x'"
      view_parameters: [tenant]
      property_mappings: { id: id, name: a_name }
    - label: B
      database: db
      table: b
      node_id: id
      use_final: true
      property_mappings: { id: id }
  edges:
    - type: R
      database: db
      table: r
      from_id: a_id
      to_id: b_id
      from_node: A
      to_node: B
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

fn sql(q: &str) -> String {
    translate_bound_plan(q, &social(), &ReadOptions::default())
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn has(q: &str, parts: &[&str]) {
    let got = squash(&sql(q));
    for p in parts {
        assert!(got.contains(p), "{q}\nmissing `{p}` in\n{got}");
    }
}

fn not_lowered(q: &str, why: &str) {
    match translate_bound_plan(q, &social(), &ReadOptions::default()) {
        Err(e) if e.contains(why) => {}
        other => panic!("{q}: expected not lowered ({why}), got {other:?}"),
    }
}

#[test]
fn a_hop_joins_node_edge_node_in_path_order() {
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.age > 30 RETURN a.name, b.name",
        &[
            r#"SELECT v0.full_name AS "a.name", v2.full_name AS "b.name""#,
            "FROM test_integration.users_test AS v0 \
             JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id \
             JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id",
            "WHERE v0.age > 30",
        ],
    );
    // A left-pointing hop is tied in the stored orientation.
    has(
        "MATCH (b:User)<-[:FOLLOWS]-(a:User) RETURN count(*)",
        &[
            "JOIN test_integration.user_follows_test AS v1 ON v1.followed_id = v0.user_id",
            "JOIN test_integration.users_test AS v2 ON v1.follower_id = v2.user_id",
        ],
    );
}

#[test]
fn a_closed_pattern_ties_one_scan_twice() {
    has(
        "MATCH (a)-[r]->(a) RETURN count(*)",
        &["ON v1.follower_id = v0.user_id AND v1.followed_id = v0.user_id"],
    );
}

#[test]
fn comma_parts_without_a_tie_are_a_cross_join() {
    has(
        "MATCH (a:User), (p:Post) RETURN count(*)",
        &["CROSS JOIN test_integration.posts_test AS v1"],
    );
}

#[test]
fn relationships_of_one_match_are_distinct() {
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN count(*)",
        &["WHERE v1.follow_id <> v3.follow_id"],
    );
    // Across MATCH clauses there is no uniqueness.
    let two = squash(&sql(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*)",
    ));
    assert!(!two.contains("<>"), "{two}");
}

#[test]
fn a_variable_from_an_earlier_clause_is_the_same_scan() {
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b) MATCH (c)-[r]->(d) RETURN count(*)",
        &[
            "JOIN test_integration.users_test AS v3 ON v1.follower_id = v3.user_id",
            "JOIN test_integration.users_test AS v4 ON v1.followed_id = v4.user_id",
        ],
    );
    let got = sql("MATCH (a:User)-[r:FOLLOWS]->(b) MATCH (c)-[r]->(d) RETURN count(*)");
    assert_eq!(got.matches("user_follows_test").count(), 1, "{got}");
}

#[test]
fn what_cannot_match_returns_no_rows() {
    // Impossible pattern, unknown label, a bound node written with another label.
    for q in [
        "MATCH (p:Post)-[:FOLLOWS]->(x) RETURN count(*)",
        "MATCH (n:NoSuchLabel) RETURN count(*)",
        "MATCH (a:User) MATCH (a:Post) RETURN count(*)",
        "MATCH (a:User)-[r:FOLLOWS]->(b) MATCH ()-[r:LIKED]->() RETURN count(*)",
    ] {
        let got = squash(&sql(q));
        assert!(got.ends_with("WHERE false"), "{q}\n{got}");
        assert!(!got.contains("FROM"), "{q}\n{got}");
    }
}

#[test]
fn inline_properties_and_unmapped_properties() {
    has(
        "MATCH (a:User {name: 'Alice Johnson'})-[:FOLLOWS {follow_date: '2024-01-01'}]->(b) RETURN b.city",
        &["v0.full_name = 'Alice Johnson'", "v1.follow_date = '2024-01-01'"],
    );
    // The graph has no such property: NULL, as Neo4j returns.
    has(
        "MATCH (a:User) RETURN a.nickname",
        &[r#"NULL AS "a.nickname""#],
    );
}

#[test]
fn projection_aggregation_order_and_paging() {
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.city AS c, count(b) AS n ORDER BY n DESC, c SKIP 1 LIMIT 2",
        &[
            r#"SELECT v0.city AS "c", count(v2.user_id) AS "n""#,
            "GROUP BY v0.city",
            // Neo4j's null order (the printer's, as for the legacy path).
            "ORDER BY count(v2.user_id) DESC NULLS FIRST, v0.city ASC",
            "LIMIT 1, 2",
        ],
    );
    has(
        "MATCH (a:User) RETURN DISTINCT a.city",
        &[r#"SELECT DISTINCT v0.city AS "a.city""#],
    );
    has("RETURN 1 + 1 AS two", &[r#"SELECT 1 + 1 AS "two""#]);
}

#[test]
fn labels_and_types_are_static() {
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b) RETURN type(r) AS t, labels(a) AS l, a:User AS u",
        &[
            r#"'FOLLOWS' AS "t""#,
            r#"['User'] AS "l""#,
            r#"CAST(true AS Nullable(Bool)) AS "u""#,
        ],
    );
}

#[test]
fn table_options_come_from_the_schema() {
    let opts = ReadOptions {
        view_parameter_values: Some(HashMap::from([("tenant".to_string(), "t'1".to_string())])),
        ..Default::default()
    };
    let got = squash(
        &translate_bound_plan("MATCH (a:A) RETURN a.name", &options_schema(), &opts).unwrap(),
    );
    assert!(got.contains("FROM db.a(tenant = 't''1') AS v0"), "{got}");
    assert!(got.contains("WHERE ((v0.kind = 'x'))"), "{got}");
    // FINAL on the FROM table prints; on a joined table it is not lowered yet.
    let got = squash(
        &translate_bound_plan("MATCH (b:B) RETURN count(*)", &options_schema(), &opts).unwrap(),
    );
    assert!(got.contains("FROM db.b AS v0 FINAL"), "{got}");
    let err = translate_bound_plan(
        "MATCH (a:A)-[:R]->(b:B) RETURN count(*)",
        &options_schema(),
        &opts,
    )
    .unwrap_err();
    assert!(err.contains("FINAL"), "{err}");
}

#[test]
fn what_s4a_does_not_lower() {
    not_lowered("MATCH (a:User) WITH a RETURN a.name", "WITH");
    not_lowered(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b) RETURN b.name",
        "OPTIONAL",
    );
    not_lowered(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b) RETURN count(*)",
        "variable-length",
    );
    not_lowered(
        "MATCH (a:User)-[:FOLLOWS]-(b) RETURN count(*)",
        "undirected",
    );
    not_lowered("MATCH (a:User) RETURN a", "whole node");
    not_lowered("MATCH (a:User) RETURN id(a)", "id()");
    not_lowered("MATCH (n) RETURN count(*)", "several possible labels");
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS]->(b) RETURN count(*)",
        "path variable",
    );
    not_lowered(
        "MATCH (a:User) RETURN [x IN [1, 2] | x * 2] AS l",
        "comprehension",
    );
    not_lowered("UNWIND [1, 2] AS x RETURN x", "UNWIND");
}

/// §4.1: the bound-plan path depends on neither the analyzer nor the render
/// composition code, so it cannot inherit their name-keyed repairs.
#[test]
fn the_bound_plan_does_not_use_the_legacy_composition() {
    const FORBIDDEN: &[&str] = &[
        "query_planner::analyzer",
        "plan_ctx",
        "render_plan::plan_builder",
        "with_to_cte",
        "join_builder",
        "from_builder",
        "filter_builder",
        "filter_pipeline",
        "plan_optimizer",
        "cte_extraction",
        "variable_scope",
        "query_context",
    ];
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bound_plan");
    let mut stack = vec![dir];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.file_name().is_some_and(|n| n == "tests.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            for line in text.lines().filter(|l| !l.trim_start().starts_with("//")) {
                for f in FORBIDDEN {
                    assert!(!line.contains(f), "{}: uses `{f}`: {line}", path.display());
                }
            }
        }
    }
}
