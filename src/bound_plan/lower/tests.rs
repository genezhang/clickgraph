//! Lowering tests: the SQL each lowered construct produces, and
//! `Unsupported` for what is not lowered yet. Row-level correctness is
//! checked against Neo4j by the oracle (`scripts/oracle/run_corpus.py` with
//! `CLICKGRAPH_BOUND_PLAN=on`).

use std::collections::HashMap;

use crate::bound_plan::lower::{lower_statement, LowerOptions, ResultColumn, ResultKind};
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
        .sql
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

/// As `has`, in Neo4j-compat mode, where an undeclared property is NULL.
fn has_compat(q: &str, parts: &[&str]) {
    let compat = LowerOptions {
        neo4j_compat: true,
        ..LowerOptions::default()
    };
    let got = lowered(q, &social(), &compat);
    for p in parts {
        assert!(got.contains(p), "{q}\nmissing `{p}` in\n{got}");
    }
}

fn not_lowered(q: &str, why: &str) {
    match translate_bound_plan(q, &social(), &ReadOptions::default()) {
        Err(e) if e.contains(why) => {}
        other => panic!(
            "{q}: expected not lowered ({why}), got {:?}",
            other.map(|t| t.sql)
        ),
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
    }
    // Scans that do match stay in FROM (the SELECT may read them); with no
    // scan at all there is no FROM.
    has(
        "MATCH (a:User), (b:Nope) RETURN a.name AS n",
        &["FROM test_integration.users_test AS v0", "WHERE false"],
    );
    let none = squash(&sql(
        "MATCH (b:Nope) RETURN count(*) AS c, collect(b.name) AS l",
    ));
    assert!(!none.contains("FROM"), "{none}");
    assert!(none.contains(r#"THEN [] ELSE [] END AS "l""#), "{none}");
}

#[test]
fn aggregates_of_no_values() {
    // Neo4j: [], 0, 0, NULL. ClickHouse gives NULL for groupArray/sum of a
    // NULL literal (`Nullable(Nothing)`), so these are folded.
    // Still an aggregate, so the query returns one row. (An undeclared
    // property is NULL in Neo4j-compat mode.)
    has_compat(
        "MATCH (a:User) RETURN collect(a.nickname) AS l, sum(a.nickname) AS s, count(a.nickname) AS c, max(a.nickname) AS m",
        &[
            r#"CASE WHEN count(*) >= 0 THEN [] ELSE [] END AS "l""#,
            r#"CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "s""#,
            r#"CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "c""#,
            r#"CASE WHEN count(*) >= 0 THEN NULL ELSE NULL END AS "m""#,
        ],
    );
    has_compat(
        "MATCH (a:User) RETURN collect(DISTINCT a.nickname) AS l",
        &[r#"CASE WHEN count(*) >= 0 THEN [] ELSE [] END AS "l""#],
    );
}

#[test]
fn constant_sort_and_grouping_keys() {
    // `ORDER BY 1` would be a column position in ClickHouse.
    let got = squash(&sql(
        "MATCH (a:User) RETURN a.name AS n, 1 AS one ORDER BY one, n",
    ));
    assert!(got.ends_with("ORDER BY v0.full_name ASC"), "{got}");
    // `GROUP BY 1` likewise; one constant group keeps "no row on no input".
    let got = squash(&sql("MATCH (a:User) RETURN count(*) AS c, 1 AS k"));
    assert!(!got.contains("GROUP BY"), "{got}");
    assert!(got.contains("HAVING count(*) > 0"), "{got}");
    let got = squash(&sql("RETURN 42 AS x ORDER BY x"));
    assert!(!got.contains("ORDER BY"), "{got}");
}

#[test]
fn identity_comparisons_respect_labels_and_types() {
    has(
        "MATCH (a:User), (p:Post) WHERE a = p RETURN count(*)",
        &["WHERE false"],
    );
    has(
        "MATCH (a:User), (p:Post) WHERE a <> p RETURN count(*)",
        &["WHERE true"],
    );
    has(
        "MATCH (a:User), (b:User) WHERE a <> b RETURN count(*)",
        &["WHERE v0.user_id <> v1.user_id"],
    );
    has(
        "MATCH ()-[r1:FOLLOWS]->(), ()-[r2:LIKED]->() WHERE r1 = r2 RETURN count(*)",
        &["false"],
    );
    has(
        "MATCH (a:User) RETURN count(a) AS c, count(DISTINCT a) AS d",
        &[
            r#"count(v0.user_id) AS "c""#,
            r#"count(DISTINCT v0.user_id) AS "d""#,
        ],
    );
}

#[test]
fn a_node_used_as_a_value_is_not_lowered() {
    for q in [
        "MATCH (a:User) RETURN [a] AS l",
        "MATCH (a:User) RETURN {n: a} AS m",
        "MATCH (a:User) RETURN collect(a) AS l",
        "MATCH (a:User) RETURN collect(CASE WHEN a.age > 30 THEN a END) AS l",
        "MATCH (a:User), (p:Post) RETURN count(DISTINCT CASE WHEN a.age > 30 THEN a ELSE p END) AS c",
    ] {
        not_lowered(q, "node or relationship");
    }
}

#[test]
fn the_tenant_selects_the_parameterized_view() {
    let schema =
        GraphSchemaConfig::from_yaml_str(include_str!("../../../schemas/test/multi_tenant.yaml"))
            .unwrap()
            .to_graph_schema()
            .unwrap();
    let tenant = |tenant_id: Option<&str>, explicit: Option<&str>| {
        let opts = ReadOptions {
            tenant_id: tenant_id.map(str::to_string),
            view_parameter_values: explicit
                .map(|v| HashMap::from([("tenant_id".to_string(), v.to_string())])),
            ..Default::default()
        };
        squash(
            &translate_bound_plan("MATCH (u:User) RETURN u.name", &schema, &opts)
                .unwrap()
                .sql,
        )
    };
    assert!(tenant(Some("acme"), None).contains("users_by_tenant(tenant_id = 'acme')"));
    // As in the legacy planner, an explicit view parameter wins.
    assert!(tenant(Some("acme"), Some("globex")).contains("(tenant_id = 'globex')"));
}

#[test]
fn edge_constraints_are_not_lowered_yet() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_constraints
graph_schema:
  nodes:
    - { label: F, database: db, table: f, node_id: id, property_mappings: { id: id, ts: ts } }
  edges:
    - type: R
      database: db
      table: r
      from_id: a
      to_id: b
      from_node: F
      to_node: F
      constraints: "from.ts <= to.ts"
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let err = translate_bound_plan(
        "MATCH (x:F)-[:R]->(y:F) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("constraints"), "{err}");
}

#[test]
fn inline_properties_and_unmapped_properties() {
    has(
        "MATCH (a:User {name: 'Alice Johnson'})-[:FOLLOWS {follow_date: '2024-01-01'}]->(b) RETURN b.city",
        &["v0.full_name = 'Alice Johnson'", "v1.follow_date = '2024-01-01'"],
    );
    // An undeclared property reads the same-named column, as on the legacy
    // path (a wide table needs no mapping per column; a missing column is a
    // ClickHouse error).
    has(
        "MATCH (a:User) RETURN a.nickname",
        &[r#"v0.nickname AS "a.nickname""#],
    );
}

/// Lower `q` over `schema` with `options` (no query context needed).
fn lowered(q: &str, schema: &GraphSchema, options: &LowerOptions) -> String {
    let (_, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(q).unwrap();
    let bound = crate::bound_plan::bind_statement(&stmt, schema).unwrap();
    let plan = lower_statement(&bound, schema, options).unwrap();
    crate::server::query_context::with_query_context_sync(
        crate::server::query_context::QueryContext::new(None),
        || {
            crate::server::query_context::set_current_schema(std::sync::Arc::new(schema.clone()));
            squash(
                &crate::clickhouse_query_generator::to_sql_query::render_plan_to_sql_plain(
                    plan.plan,
                ),
            )
        },
    )
}

#[test]
fn undeclared_properties_are_null_when_known_absent() {
    // Discovered columns are every property: anything else is NULL.
    let config = GraphSchemaConfig::from_yaml_str(
        r#"
graph_schema:
  nodes:
    - label: U
      database: db
      table: u
      node_id: id
      auto_discover_columns: true
      exclude_columns: [secret]
      property_mappings: {}
  edges: []
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "u",
        ["id", "name", "secret"]
            .iter()
            .map(|c| {
                crate::graph_catalog::column_info::ColumnInfo::new(
                    c.to_string(),
                    "String".to_string(),
                )
            })
            .collect(),
    );
    let discovered = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH (u:U) RETURN u.name, u.secret, u.nickname",
        &discovered,
        &LowerOptions::default(),
    );
    for part in [
        r#"v0.name AS "u.name""#,
        r#"NULL AS "u.secret""#,
        r#"NULL AS "u.nickname""#,
    ] {
        assert!(got.contains(part), "missing `{part}` in {got}");
    }

    // Neo4j-compat mode: NULL on every element.
    let got = lowered(
        "MATCH (a:User) RETURN a.nickname",
        &social(),
        &LowerOptions {
            neo4j_compat: true,
            ..LowerOptions::default()
        },
    );
    assert!(got.contains(r#"NULL AS "a.nickname""#), "{got}");
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
        &translate_bound_plan("MATCH (a:A) RETURN a.name", &options_schema(), &opts)
            .unwrap()
            .sql,
    );
    assert!(got.contains("FROM db.a(tenant = 't''1') AS v0"), "{got}");
    assert!(got.contains("WHERE ((v0.kind = 'x'))"), "{got}");
    // FINAL on the FROM table prints; on a joined table it is not lowered yet.
    let got = squash(
        &translate_bound_plan("MATCH (b:B) RETURN count(*)", &options_schema(), &opts)
            .unwrap()
            .sql,
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
fn what_is_not_lowered_yet() {
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
    not_lowered("MATCH (a:User) RETURN collect(a) AS l", "as a value");
    not_lowered("MATCH (a:User) WHERE id(a) = 1 RETURN a.name", "id()");
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

#[test]
fn sum_and_collect_of_always_null_expressions() {
    // ClickHouse: NULL for an argument typed Nullable(Nothing); Neo4j: 0, [].
    // (An undeclared property is NULL in Neo4j-compat mode.)
    has_compat(
        "MATCH (a:User) RETURN sum(a.nope + 1) AS s, collect(toUpper(a.nope)) AS l, sum(a.age) AS t",
        &[
            r#"coalesce(sum(NULL + 1), 0) AS "s""#,
            r#"coalesce(groupArray(upperUTF8(NULL)), []) AS "l""#,
            r#"coalesce(sum(v0.age), 0) AS "t""#,
        ],
    );
    // count of an element that matches nothing is still one row: 0.
    has(
        "MATCH (x:Nope) RETURN count(x) AS c, count(DISTINCT x) AS d",
        &[
            r#"CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "c""#,
            r#"CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "d""#,
        ],
    );
}

#[test]
fn relationships_of_one_type_in_different_tables_are_never_equal() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_two_tables
graph_schema:
  nodes:
    - { label: U, database: db, table: u, node_id: id, property_mappings: { id: id } }
    - { label: P, database: db, table: p, node_id: id, property_mappings: { id: id } }
  edges:
    - { type: K, database: db, table: ku, from_id: a, to_id: b, edge_id: id, from_node: U, to_node: U, property_mappings: {} }
    - { type: K, database: db, table: kp, from_id: a, to_id: b, edge_id: id, from_node: U, to_node: P, property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let q = |w: &str| {
        squash(
            &translate_bound_plan(
                &format!(
                    "MATCH (a:U)-[r1:K]->(b:U), (c:U)-[r2:K]->(p:P) WHERE {w} RETURN count(*)"
                ),
                &schema,
                &ReadOptions::default(),
            )
            .unwrap()
            .sql,
        )
    };
    assert!(q("r1 = r2").contains("WHERE false"), "{}", q("r1 = r2"));
    assert!(q("r1 <> r2").contains("WHERE true"), "{}", q("r1 <> r2"));
}

// ------------------------------------------------------------------ S4b WITH

#[test]
fn a_with_is_a_cte_of_its_output_scope() {
    // A carried node exports its identity and the properties read later.
    has(
        "MATCH (a:User) WITH a RETURN a.name",
        &[
            "WITH with_w1 AS ( SELECT v0.user_id AS \"v1__user_id\", v0.full_name AS \"p2_v1_name\" \
             FROM test_integration.users_test AS v0 )",
            "SELECT w1.p2_v1_name AS \"a.name\" FROM with_w1 AS w1",
        ],
    );
    // A value exports its value under its binding's name.
    has(
        "MATCH (a:User) WITH a.name AS n RETURN n",
        &[
            "v0.full_name AS \"v1\"",
            "SELECT w1.v1 AS \"n\" FROM with_w1 AS w1",
        ],
    );
    // A property read only after a second pass-through is exported by both.
    has(
        "MATCH (a:User) WITH a AS b WITH b AS c RETURN c.name",
        &[
            "v0.full_name AS \"p2_v1_name\"",
            "w1.p2_v1_name AS \"p2_v2_name\"",
            "SELECT w2.p2_v2_name AS \"c.name\" FROM with_w2 AS w2",
        ],
    );
}

#[test]
fn a_carried_element_is_tied_by_its_exported_columns() {
    has(
        "MATCH (a:User) WITH a MATCH (a)-[:FOLLOWS]->(b:User) RETURN b.name",
        &[
            "FROM with_w1 AS w1 JOIN test_integration.user_follows_test AS v2 \
             ON v2.follower_id = w1.v1__user_id",
        ],
    );
    // A relationship also exports its endpoints, so it can be matched again.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) WITH r MATCH (x)<-[r]-(y) RETURN x.name",
        &[
            "v1.follow_id AS \"v3__follow_id\", v1.follower_id AS \"v3__follower_id\", \
             v1.followed_id AS \"v3__followed_id\"",
            "JOIN test_integration.users_test AS v4 ON w1.v3__followed_id = v4.user_id",
            "JOIN test_integration.users_test AS v5 ON w1.v3__follower_id = v5.user_id",
        ],
    );
    // ... and takes part in the uniqueness of the MATCH that uses it.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) WITH r MATCH (x)-[r]->(y)-[s:FOLLOWS]->(z) \
         RETURN count(*)",
        &["WHERE w1.v3__follow_id <> v6.follow_id"],
    );
    // Two elements of one CTE tie in WHERE.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) WITH a, r MATCH (a)-[r]->(c) RETURN count(*)",
        &["FROM with_w1 AS w1 JOIN test_integration.users_test AS v5 ON w1.v4__followed_id = v5.user_id \
           WHERE w1.v4__follower_id = w1.v3__user_id"],
    );
    // A CTE with no tie to the next pattern is a cross join: its rows count.
    has(
        "MATCH (a:User) WITH count(*) AS c MATCH (b:User) RETURN c, b.name",
        &["FROM with_w1 AS w1 CROSS JOIN test_integration.users_test AS v2"],
    );
}

#[test]
fn with_aggregation_groups_by_identity() {
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, count(b) AS c WHERE c > 1 RETURN a.name, c",
        &[
            "count(v2.user_id) AS \"v4\"",
            "GROUP BY v0.user_id, v0.full_name HAVING count(v2.user_id) > 1",
        ],
    );
}

/// A WITH's modifiers run in the fixed order ORDER BY, SKIP, LIMIT, WHERE:
/// the WHERE filters the rows the LIMIT kept (#1311; Neo4j: 0 here).
#[test]
fn with_where_filters_the_rows_its_limit_kept() {
    has(
        "MATCH (u:User) WITH u ORDER BY u.age LIMIT 5 WHERE u.age > 30 RETURN count(*)",
        &[
            "AS \"__keep\"",
            "ORDER BY v0.age ASC LIMIT 5)",
            "FROM with_w1 AS w1 WHERE w1.__keep",
        ],
    );
    // Without a SKIP / LIMIT it is an ordinary WHERE (HAVING if aggregating).
    has(
        "MATCH (a:User) WITH a WHERE a.age > 25 RETURN count(*)",
        &["FROM test_integration.users_test AS v0 WHERE v0.age > 25 )"],
    );
}

#[test]
fn rows_keep_their_order_until_a_match_or_an_aggregation() {
    // The order travels as exported key columns: the second WITH's LIMIT and
    // the RETURN read the rows in it.
    has(
        "MATCH (a:User) WITH a.name AS n ORDER BY n WITH n LIMIT 2 RETURN n",
        &[
            "v0.full_name AS \"__o0\"",
            "FROM with_w1 AS w1 ORDER BY w1.__o0 ASC LIMIT 2)",
            "FROM with_w2 AS w2 ORDER BY w2.__o0 ASC",
        ],
    );
    // A MATCH leaves the rows in no order.
    let joined = squash(&sql(
        "MATCH (a:User) WITH a ORDER BY a.name MATCH (a)-[:FOLLOWS]->(b:User) RETURN b.name",
    ));
    assert!(!joined.contains("ORDER BY"), "{joined}");
    // An order-sensitive aggregate over ordered rows is not lowered yet.
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name RETURN collect(a.name) AS names",
        "collect() over ordered rows",
    );
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name WITH DISTINCT a.country AS c LIMIT 2 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT or aggregation",
    );
}

#[test]
fn free_standing_order_skip_limit() {
    // `MATCH … ORDER BY … LIMIT …` (GQL style): the scope is exported as is.
    has(
        "MATCH (a:User) ORDER BY a.name DESC SKIP 1 LIMIT 2 RETURN a.name",
        &[
            "WITH with_w1 AS ( SELECT v0.user_id AS \"v0__user_id\", v0.full_name AS \"p2_v0_name\", \
             v0.full_name AS \"__o0\" FROM test_integration.users_test AS v0 \
             ORDER BY v0.full_name DESC NULLS FIRST LIMIT 1, 18446744073709551615)",
            "FROM with_w1 AS w1 ORDER BY w1.__o0 DESC NULLS FIRST LIMIT 2)",
            "SELECT w2.p2_v0_name AS \"a.name\" FROM with_w2 AS w2 ORDER BY w2.__o0 DESC NULLS FIRST",
        ],
    );
}

#[test]
fn constants_need_no_column() {
    has(
        "WITH 1 AS x RETURN x",
        &[
            "WITH with_w1 AS ( SELECT 1 AS \"__row\" )",
            "SELECT 1 AS \"x\" FROM with_w1 AS w1",
        ],
    );
    // An aggregate of a NULL carried by a WITH still folds to Cypher's value.
    has(
        "MATCH (a:User) WITH a, NULL AS x RETURN collect(x) AS l",
        &["CASE WHEN count(*) >= 0 THEN [] ELSE [] END AS \"l\""],
    );
}

/// Review of S4b, each checked on ClickHouse and Neo4j 5.26.
#[test]
fn a_group_key_that_matches_nothing_still_groups() {
    // Neo4j: no row (no group). The key exports no column, so the WITH must
    // not become a global aggregate (one row, c = 0).
    has(
        "MATCH (a:Nope) WITH a, count(*) AS c RETURN c",
        &["WHERE false HAVING count(*) > 0 )"],
    );
}

#[test]
fn an_order_the_sql_cannot_keep_is_not_relied_on() {
    // `collect` reads its input in order, whatever the projection's own
    // ORDER BY does to the groups.
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name WITH a.country AS c, collect(a.name) AS names \
         ORDER BY c RETURN c, names",
        "collect() over ordered rows",
    );
    // DISTINCT and aggregation keep their input's first-seen order in Neo4j;
    // the SQL does not, so a later LIMIT or collect cannot rely on it.
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name DESC WITH DISTINCT a.country AS c \
         WITH c LIMIT 3 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT or aggregation",
    );
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name DESC WITH DISTINCT a \
         WITH collect(a.name) AS l RETURN l",
        "collect() over ordered rows",
    );
    not_lowered(
        "MATCH (a:User) ORDER BY a.name WITH DISTINCT a.country AS c LIMIT 2 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT or aggregation",
    );
    // An unordered final RETURN does not rely on it.
    sql("MATCH (a:User) WITH a ORDER BY a.name WITH DISTINCT a.country AS c RETURN c");
    // A MATCH ends the order, so nothing is lost.
    sql(
        "MATCH (a:User) WITH a ORDER BY a.name MATCH (a)-[:FOLLOWS]->(b:User) \
         RETURN collect(b.name) AS l",
    );
}

/// Spark's `length` is string-only: `size()` of a list carried by a WITH
/// must print Spark `size`. The lowered plan has no variable registry, so
/// the printer reads the list columns off the plan.
#[test]
fn databricks_size_of_a_carried_list() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "MATCH (a:User) WITH a.country AS c, collect(a.name) AS l WITH c, l AS m \
             RETURN c, size(m) AS k, size(c) AS n",
            &social(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql
    });
    let got = squash(&got);
    assert!(got.contains("size(w2.v4) AS `k`"), "{got}");
    assert!(got.contains("length(w2.v3) AS `n`"), "{got}");
}

// ------------------------------------------------------------------ S4c

/// The SQL and the result shape of `q` over `schema`.
fn shaped(q: &str, schema: &GraphSchema) -> (String, Vec<ResultColumn>) {
    let t = translate_bound_plan(q, schema, &ReadOptions::default())
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    (squash(&t.sql), t.shape)
}

fn column(name: &str, kind: ResultKind) -> (String, ResultKind) {
    (name.to_string(), kind)
}

/// Each item's name and kind.
fn kinds(shape: &[ResultColumn]) -> Vec<(String, ResultKind)> {
    shape
        .iter()
        .map(|c| (c.name.clone(), c.kind.clone()))
        .collect()
}

fn user() -> ResultKind {
    ResultKind::Node {
        label: "User".to_string(),
    }
}

/// Composite ids, and a relationship identified by an `edge_id` that is not
/// one of its properties.
fn shapes_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_shapes
graph_schema:
  nodes:
    - { label: T, database: db, table: t, node_id: [tenant, id], property_mappings: { tenant: tenant, id: id, name: t_name } }
    - { label: U, database: db, table: u, node_id: id, property_mappings: { id: id } }
  edges:
    - { type: C, database: db, table: c, from_id: [ft, fa], to_id: [tt, ta], from_node: T, to_node: T, property_mappings: {} }
    - { type: K, database: db, table: k, from_id: a, to_id: b, edge_id: kid, from_node: U, to_node: U, property_mappings: { w: weight } }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

/// A returned node or relationship is its columns, named `<item>.<…>` as on
/// the legacy pipeline: a relationship's stored endpoint columns (whatever
/// the pattern's direction), then every property by name.
#[test]
fn a_returned_element_is_its_columns_under_its_name() {
    let (sql, shape) = shaped(
        "MATCH (a:User)<-[r:FOLLOWS]-(b:User) RETURN r, a AS x, a.name",
        &social(),
    );
    assert!(
        sql.starts_with(
            r#"SELECT v1.follower_id AS "r.from_id", v1.followed_id AS "r.to_id", v1.follow_date AS "r.follow_date", v0.age AS "x.age", v0.city AS "x.city", v0.country AS "x.country", v0.email_address AS "x.email", v0.is_active AS "x.is_active", v0.full_name AS "x.name", v0.registration_date AS "x.registration_date", v0.user_id AS "x.user_id", v0.full_name AS "a.name" FROM"#
        ),
        "{sql}"
    );
    assert_eq!(
        kinds(&shape),
        vec![
            column(
                "r",
                ResultKind::Rel {
                    rel_type: "FOLLOWS".to_string(),
                    from_label: "User".to_string(),
                    to_label: "User".to_string(),
                }
            ),
            column("x", user()),
            column("a.name", ResultKind::Value),
        ]
    );
    // `v.*` is the node's columns too.
    let (star, shape) = shaped("MATCH (a:User) RETURN a.*", &social());
    let (whole, _) = shaped("MATCH (a:User) RETURN a", &social());
    assert_eq!(star, whole);
    assert_eq!(kinds(&shape), vec![column("a", user())]);
    // Composite endpoint columns are numbered.
    let (sql, _) = shaped("MATCH (:T)-[c:C]->(:T) RETURN c", &shapes_schema());
    assert!(
        sql.contains(
            r#"v1.ft AS "c.from_id_1", v1.fa AS "c.from_id_2", v1.tt AS "c.to_id_1", v1.ta AS "c.to_id_2" FROM"#
        ),
        "{sql}"
    );
}

/// A node returned after a WITH: the CTE exports every property, not just
/// those read by name.
#[test]
fn a_carried_element_returned_whole_exports_every_property() {
    let (sql, shape) = shaped(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) WITH a AS u, r WHERE u.age > 30 RETURN u, r",
        &social(),
    );
    for col in [
        r#"v0.registration_date AS "p2_v3_registration_date""#,
        r#"v1.follow_date AS "p2_v4_follow_date""#,
        r#"w1.p2_v3_registration_date AS "u.registration_date""#,
        r#"w1.v4__follower_id AS "r.from_id""#,
        r#"w1.p2_v4_follow_date AS "r.follow_date""#,
    ] {
        assert!(sql.contains(col), "missing {col} in {sql}");
    }
    assert_eq!(kinds(&shape)[0], column("u", user()));
}

/// Grouping by a returned element groups by its identity, and DISTINCT is
/// by its identity too, also when the identity is not one of its returned
/// columns: rows equal in every returned column can be different
/// relationships.
#[test]
fn grouping_and_distinct_over_a_returned_element() {
    let (sql, _) = shaped(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b, count(*) AS c",
        &social(),
    );
    assert!(
        sql.contains(
            "GROUP BY v2.age, v2.city, v2.country, v2.email_address, v2.is_active, \
             v2.full_name, v2.registration_date, v2.user_id"
        ) && !sql.contains("v2.user_id, v2.user_id"),
        "{sql}"
    );
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN k, count(*) AS c",
        &shapes_schema(),
    );
    assert!(
        sql.contains("GROUP BY v1.a, v1.b, v1.weight, v1.kid"),
        "{sql}"
    );
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN DISTINCT k, k.w AS w",
        &shapes_schema(),
    );
    assert!(
        sql.starts_with(r#"SELECT v1.a AS "k.from_id""#)
            && sql.ends_with("GROUP BY v1.a, v1.b, v1.weight, v1.weight, v1.kid"),
        "{sql}"
    );
    let err = translate_bound_plan(
        "MATCH (:U)-[k:K]->(:U) RETURN DISTINCT k, count(*) AS c",
        &shapes_schema(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("edge_id is not returned"), "{err}");
    // A node's identity is among its properties (`node_id` names
    // properties): a plain DISTINCT.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN DISTINCT b",
        &[r#"SELECT DISTINCT v2.age AS "b.age""#],
    );
}

/// `id(n)` as a RETURN item is the node's key; Bolt encodes it from the
/// shape. Anywhere else the key is not the id.
#[test]
fn id_of_a_returned_node_is_its_key() {
    let (sql, shape) = shaped(
        "MATCH (a:User) WITH a RETURN id(a), id(a) AS i, a.name",
        &social(),
    );
    assert!(
        sql.contains(r#"SELECT w1.v1__user_id AS "id(a)", w1.v1__user_id AS "i""#),
        "{sql}"
    );
    let id = ResultKind::NodeId {
        label: "User".to_string(),
    };
    assert_eq!(
        kinds(&shape),
        vec![
            column("id(a)", id.clone()),
            column("i", id),
            column("a.name", ResultKind::Value)
        ]
    );
    not_lowered(
        "MATCH (a:User) RETURN id(a) AS i ORDER BY i",
        "ORDER BY an id()",
    );
    not_lowered("MATCH (a:User) RETURN id(a) + 1 AS i", "id()");
    not_lowered("MATCH (a:User) WITH id(a) AS i RETURN i", "id()");
    not_lowered("MATCH (a:User)-[r:FOLLOWS]->(b) RETURN id(r)", "id()");
    not_lowered("MATCH (a:User) RETURN size(a.*) AS n", "`v.*`");
    let err = translate_bound_plan(
        "MATCH (t:T) RETURN id(t)",
        &shapes_schema(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("composite identity"), "{err}");
}

/// A returned element that matches nothing: no rows, one NULL column.
#[test]
fn a_returned_element_that_matches_nothing() {
    let (sql, shape) = shaped("MATCH (x:Nope) RETURN x, count(*) AS c", &social());
    assert!(
        sql.contains(r#"SELECT NULL AS "x", count(*) AS "c""#)
            && sql.contains("HAVING count(*) > 0"),
        "{sql}"
    );
    assert_eq!(kinds(&shape)[0], column("x", ResultKind::Value));
}

/// Result columns are named uniquely when lowering (the legacy names, `_2`
/// on a clash), and the shape lists each item's own columns, so a reader
/// never takes another item's column (`n.name_2`, `n.age + 1`) for a
/// property.
#[test]
fn each_item_knows_its_own_columns() {
    let (sql, shape) = shaped("MATCH (n:User) RETURN n.name, n, n.age + 1", &social());
    assert!(
        sql.contains(r#"v0.full_name AS "n.name", v0.age AS "n.age""#)
            && sql.contains(r#"v0.full_name AS "n.name_2""#)
            && sql.contains(r#"v0.age + 1 AS "n.age + 1""#),
        "{sql}"
    );
    let cols = |i: usize| -> Vec<(String, String)> { shape[i].columns.clone() };
    assert_eq!(cols(0), [("n.name".to_string(), "n.name".to_string())]);
    let node = cols(1);
    assert_eq!(node.len(), 8, "{node:?}");
    assert!(node.contains(&("name".to_string(), "n.name_2".to_string())));
    assert!(node.contains(&("age".to_string(), "n.age".to_string())));
    assert_eq!(
        cols(2),
        [("n.age + 1".to_string(), "n.age + 1".to_string())]
    );
}
