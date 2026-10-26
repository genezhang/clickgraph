//! Lowering tests: the SQL each lowered construct produces, and
//! `Unsupported` for what is not lowered yet. Row-level correctness is
//! checked against Neo4j by the oracle (`scripts/oracle/run_corpus.py`).

use std::collections::HashMap;

use crate::bound_plan::lower::{
    lower_statement, GraphType, IdLookup, LowerOptions, ResultColumn, ResultKind,
};
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

/// A relationship's `edge_id` part `col` (SQL) as its element id spells it
/// on ClickHouse (#1334).
fn edge_text(col: &str) -> String {
    format!(
        r"CASE WHEN {col} IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString({col}), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END"
    )
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

/// S8a: FK edges, whose table is one of their ends' node tables, of one
/// relationship per row: a self-referencing one, and one whose columns name
/// its end's id columns in another order.
fn fk_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_fk
graph_schema:
  nodes:
    - label: Obj
      database: db
      table: objs
      node_id: object_id
      property_mappings: { object_id: object_id, name: name }
    - label: Comment
      database: db
      table: comments
      node_id: comment_id
      property_mappings: { comment_id: comment_id }
    - label: Doc
      database: db
      table: docs
      node_id: doc_id
      view_parameters: [tenant]
      property_mappings: { doc_id: doc_id }
    - label: Ver
      database: db
      table: vers
      node_id: ver_id
      use_final: true
      property_mappings: { ver_id: ver_id }
    - label: Forum
      database: db
      table: forums
      node_id: [region, forum_id]
      property_mappings: { region: region, forum_id: forum_id }
  edges:
    - type: PARENT
      database: db
      table: objs
      from_id: parent_id
      to_id: object_id
      from_node: Obj
      to_node: Obj
      property_mappings: {}
    - type: REVISES
      database: db
      table: docs
      from_id: doc_id
      to_id: prev_id
      from_node: Doc
      to_node: Doc
      property_mappings: {}
    - type: ABOUT
      database: db
      table: vers
      from_id: ver_id
      to_id: comment_ref
      from_node: Ver
      to_node: Comment
      property_mappings: {}
    - type: IN_FORUM
      database: db
      table: comments
      from_id: comment_id
      to_id: [forum_id, region]
      from_node: Comment
      to_node: Forum
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

#[test]
fn an_fk_edge_is_read_as_a_table_of_relationships() {
    // Its rows are relationships: the table is read for the relationship and
    // again for the end it holds, as an edge table and its end would be (a
    // row whose reference is NULL or names no node joins no node).
    has(
        "MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.title",
        &["FROM test_integration.users_test AS v0 \
           JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id \
           JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id"],
    );
    // A self-referencing one relates each row's node to the node its
    // reference names (#632; `from_id: parent_id` is the child's reference):
    // `(c)-[:PARENT]->(p)` is a child and its parent.
    let schema = fk_schema();
    let hop = lowered(
        "MATCH (c:Obj)-[:PARENT]->(p:Obj) RETURN c.name, p.name",
        &schema,
        &LowerOptions::default(),
    );
    let part = "FROM db.objs AS v0 JOIN db.objs AS v1 ON v1.object_id = v0.object_id \
                JOIN db.objs AS v2 ON v1.parent_id = v2.object_id";
    assert!(hop.contains(part), "missing `{part}` in\n{hop}");
    // A walk over one is the generator's walk of an edge table, joining each
    // relationship's row and the node it enters, deduplicated on
    // relationships from a zero lower bound (not its FK-edge walk, #902).
    let walk = lowered(
        "MATCH (a:Obj)-[:PARENT*0..2]->(b:Obj) RETURN a.name, b.name",
        &schema,
        &LowerOptions::default(),
    );
    for part in [
        "FROM vlp_v1_path vp JOIN db.objs AS rel ON vp.end_id = rel.object_id \
         JOIN db.objs AS end_node ON rel.parent_id = end_node.object_id",
        "NOT has(vp.path_edges, tuple(rel.object_id, rel.parent_id))",
    ] {
        assert!(walk.contains(part), "missing `{part}` in\n{walk}");
    }
    // So is a walk of several types through one.
    has(
        "MATCH (a:User)-[*1..2]->(b:Post) RETURN count(*)",
        &["test_integration.posts_test"],
    );
}

#[test]
fn an_fk_edge_is_read_as_its_node_table_is() {
    // Its rows are its node's: an edge that declares no view parameters is
    // read with its node's (legacy reads it so).
    let options = LowerOptions {
        view_parameter_values: Some(HashMap::from([("tenant".to_string(), "7".to_string())])),
        ..LowerOptions::default()
    };
    let got = lowered(
        "MATCH (a:Doc)-[:REVISES]->(b:Doc) RETURN count(*)",
        &fk_schema(),
        &options,
    );
    assert_eq!(got.matches("db.docs(tenant = '7')").count(), 3, "{got}");
    // And with its node's FINAL: a joined read is of the merged rows
    // (S11k), never without FINAL (stale row versions).
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Ver)-[:ABOUT]->(b:Comment) RETURN count(*)",
            &fk_schema(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    assert_eq!(got.matches("FINAL").count(), 2, "{got}");
}

#[test]
fn an_end_paired_with_its_id_columns_in_another_order_is_refused() {
    // `to_id: [forum_id, region]` against the id `[region, forum_id]` would
    // tie `forum_id = region` (#672/#1010; legacy refuses it too).
    let schema = fk_schema();
    for q in [
        "MATCH (c:Comment)-[:IN_FORUM]->(f:Forum) RETURN count(*)",
        "MATCH (f:Forum)<-[:IN_FORUM]-(c) RETURN count(*)",
        "MATCH (c:Comment)-[r]->(f) RETURN count(r)",
    ] {
        let err = translate_bound_plan(q, &schema, &ReadOptions::default()).unwrap_err();
        assert!(err.contains("another order (#672/#1010)"), "{q}: {err}");
    }
}

/// S8b: labels embedded in edge tables (one, and one over two tables), an
/// own-table label at an embedding edge's end, and a "foreign" denormalized
/// label (its own table; edges carry its id).
fn embedded_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_embedded
graph_schema:
  nodes:
    - label: Airport
      database: db
      table: fl
      node_id: code
      property_mappings: {}
      from_node_properties: { code: org, city: org_city }
      to_node_properties: { code: dst, city: dst_city }
    - label: Airline
      database: db
      table: airlines
      node_id: code
      property_mappings: { code: code, name: name }
    - label: IP
      database: db
      table: dns
      node_id: ip
      property_mappings: {}
      from_node_properties: { ip: src }
    - label: IP
      database: db
      table: conn
      node_id: ip
      property_mappings: {}
      from_node_properties: { ip: src }
      to_node_properties: { ip: dst }
    - label: Domain
      database: db
      table: dns
      node_id: name
      property_mappings: {}
      to_node_properties: { name: q }
    - label: Person
      database: db
      table: people
      node_id: pid
      property_mappings: { pid: pid, name: name }
      from_node_properties: { pid: mgr_id }
  edges:
    - type: FLIGHT
      database: db
      table: fl
      from_id: org
      to_id: dst
      from_node: Airport
      to_node: Airport
      edge_id: fid
      property_mappings: { num: num }
    - type: DIVERTS
      database: db
      table: fl
      from_id: org
      to_id: alt
      from_node: Airport
      to_node: Airport
      edge_id: fid
      property_mappings: {}
    - type: SERVES
      database: db
      table: fl
      from_id: carrier
      to_id: dst
      from_node: Airline
      to_node: Airport
      edge_id: fid
      property_mappings: {}
    - type: REQ
      database: db
      table: dns
      from_id: src
      to_id: q
      from_node: IP
      to_node: Domain
      edge_id: uid
      property_mappings: {}
    - type: CONN
      database: db
      table: conn
      from_id: src
      to_id: dst
      from_node: IP
      to_node: IP
      edge_id: uid
      property_mappings: {}
    - type: REPORTS_TO
      database: db
      table: reports
      from_id: mgr_id
      to_id: emp_id
      from_node: Person
      to_node: Person
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

fn embedded(q: &str) -> String {
    lowered(q, &embedded_schema(), &LowerOptions::default())
}

#[test]
fn an_embedded_label_is_a_relation_of_its_roles() {
    // Its nodes: the distinct ids its roles hold, each property the value
    // its sources hold (each source's column of it kept apart, its type
    // checked), no node for a NULL id.
    let got = embedded("MATCH (a:Airport) RETURN a.city");
    for part in [
        "WITH __cg_nodes_Airport__cg_rows AS ( SELECT e.org AS \"code\", \
         e.org_city AS \"city__cg0\", NULL AS \"city__cg1\" FROM db.fl AS e UNION ALL \
         SELECT e.dst AS \"code\", NULL AS \"city__cg0\", e.dst_city AS \"city__cg1\" \
         FROM db.fl AS e )",
        "FROM __cg_nodes_Airport__cg_rows AS e WHERE NOT isNull(e.code) GROUP BY e.code",
        "FROM __cg_nodes_Airport AS v0",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // A label embedded in two tables: every role of each.
    let got = embedded("MATCH (i:IP) RETURN count(i)");
    for part in [
        "FROM db.dns AS e",
        "e.src AS \"ip\" FROM db.conn AS e",
        "e.dst AS \"ip\" FROM db.conn",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // An undeclared property has no column to read.
    let err = translate_bound_plan(
        "MATCH (a:Airport) RETURN a.terminal",
        &embedded_schema(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.contains("undeclared property of a node embedded"),
        "{err}"
    );
}

#[test]
fn an_embedded_end_is_read_from_its_relationships_row() {
    // The end's id and properties are the row's: no relation, a NULL end no
    // relationship.
    let got = embedded("MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.city, b.code, f.num");
    assert!(!got.contains("__cg_nodes_Airport"), "{got}");
    for part in [
        "SELECT v1.org_city AS \"a.city\", v1.dst AS \"b.code\", v1.num AS \"f.num\" FROM db.fl AS v1",
        "v1.org IS NOT NULL AND v1.dst IS NOT NULL",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // A node two relationships share is read from the first, the second
    // tied to it.
    let got = embedded("MATCH (a:Airport)-[:FLIGHT]->(b)-[:FLIGHT]->(c) RETURN count(*)");
    assert!(got.contains("JOIN db.fl AS v3 ON v3.org = v1.dst"), "{got}");
    // Not when read as a whole node, when no role of the row holds it, nor
    // at a walk's end: the relation is joined.
    for q in [
        "MATCH (a:Airport)-[f:FLIGHT]->(b) RETURN a",
        "MATCH (a:Airport)-[:DIVERTS]->(b) RETURN b.city",
        "MATCH (a:Airport)-[:FLIGHT*1..2]->(b) RETURN b.city",
    ] {
        assert!(embedded(q).contains("__cg_nodes_Airport AS"), "{q}");
    }
    // An own-table end of an embedding edge is joined as usual.
    let got = embedded("MATCH (n:Airline)-[:SERVES]->(a) RETURN n.name, a.city");
    assert!(
        got.contains("JOIN db.airlines AS v0") || got.contains("FROM db.airlines AS v0"),
        "{got}"
    );
}

#[test]
fn a_foreign_denormalized_label_is_its_own_table() {
    // Edges carry its id; its nodes are its table's rows.
    let got = embedded("MATCH (p:Person)-[:REPORTS_TO]->(m) RETURN p.name, m.name");
    for part in [
        "FROM db.people AS v0",
        "JOIN db.reports AS v1 ON v1.mgr_id = v0.pid",
        "JOIN db.people AS v2 ON v1.emp_id = v2.pid",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

#[test]
fn relationships_of_different_types_on_one_row_are_not_compared() {
    // A row is a FLIGHT and a DIVERTS: the two are different relationships
    // whatever their ids (uniqueness compares one definition's).
    let got = embedded("MATCH (a:Airport)-[f:FLIGHT]->(b), (a)-[d:DIVERTS]->(c) RETURN count(*)");
    assert!(!got.contains("<>"), "{got}");
    let got = embedded("MATCH (a:Airport)-[f:FLIGHT]->(b), (a)-[g:FLIGHT]->(c) RETURN count(*)");
    assert!(got.contains("v1.fid <> v3.fid"), "{got}");
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
    // In a list (`[a]`, `collect(a)`) it is a tuple (S7e2a); elsewhere not.
    for q in [
        "MATCH (a:User) RETURN {n: a} AS m",
        "MATCH (a:User) RETURN collect(CASE WHEN a.age > 30 THEN a END) AS l",
        "MATCH (a:User), (p:Post) RETURN count(DISTINCT CASE WHEN a.age > 30 THEN a ELSE p END) AS c",
        "MATCH (a:User), (b:User) RETURN coalesce(a, b) AS n",
    ] {
        not_lowered(q, "node");
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
    // FINAL on the FROM table prints; a joined table is read as the
    // relation of its merged rows (S11k).
    let got = squash(
        &translate_bound_plan("MATCH (b:B) RETURN count(*)", &options_schema(), &opts)
            .unwrap()
            .sql,
    );
    assert!(got.contains("FROM db.b AS v0 FINAL"), "{got}");
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:A)-[:R]->(b:B) RETURN count(*)",
            &options_schema(),
            &opts,
        )
        .unwrap()
        .sql,
    );
    assert!(got.contains("JOIN db.b AS v2 FINAL ON"), "{got}");
}

#[test]
fn what_is_not_lowered_yet() {
    not_lowered(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN labels(b) AS l",
        "labels() of a node an OPTIONAL MATCH may leave NULL",
    );
    // A walk of several types carries no element tuples (S7e2b).
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS|LIKED*1..2]->(b) RETURN [x IN nodes(p) | x.name] AS n",
        "nodes() of a path other than",
    );
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN head(nodes(p)) AS n",
        "head() of nodes or relationships of a list",
    );
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN r[0] AS n",
        "an element or slice of a list of nodes or relationships",
    );
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) > 1 RETURN b.name",
        "id() of a relationship other than",
    );
    not_lowered("MATCH (n) RETURN n.*", "`v.*` of an element of several");
    not_lowered(
        "MATCH (:User)-[r:FOLLOWS|LIKED]->() RETURN r.*",
        "`v.*` of an element of several",
    );
    // `id(n)` of a node of several labels as a value other than a RETURN
    // item (I5b): carried by a WITH, collected, aggregated.
    for q in [
        "MATCH (n) WITH id(n) AS i RETURN i",
        "MATCH (n) RETURN collect(id(n)) AS i",
        "MATCH (n) RETURN count(DISTINCT id(n)) AS i",
    ] {
        not_lowered(
            q,
            "id() of a node of several labels other than as a RETURN item",
        );
    }
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
    // `collect` lists ordered rows in their order (S7e1): numbered first.
    has(
        "MATCH (a:User) WITH a ORDER BY a.name RETURN collect(a.name) AS names",
        &[
            r#"row_number() OVER (ORDER BY w1.__o0 ASC) AS "__cg_row" FROM with_w1 AS w1 )"#,
            "SELECT arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w2.__cg_row, \
             w2.p2_v1_name), NOT isNull(w2.p2_v1_name)))) AS \"names\" FROM with_w2 AS w2",
        ],
    );
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name WITH DISTINCT a.country AS c LIMIT 2 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
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
    // ORDER BY does to the groups: by the rows' number, numbered before the
    // WITH's own CTE (S7e1).
    has(
        "MATCH (a:User) WITH a ORDER BY a.name WITH a.country AS c, collect(a.name) AS names \
         ORDER BY c RETURN c, names",
        &[
            r#"row_number() OVER (ORDER BY w1.__o0 ASC) AS "__cg_row" FROM with_w1 AS w1 )"#,
            "with_w3 AS ( SELECT w2.p2_v1_country AS \"v2\", arrayMap(t -> t.2, arraySort(t -> \
             t.1, groupArrayIf((w2.__cg_row, w2.p2_v1_name), NOT isNull(w2.p2_v1_name)))) AS \
             \"v3\", w2.p2_v1_country AS \"__o0\" FROM with_w2 AS w2 GROUP BY w2.p2_v1_country )",
            "FROM with_w3 AS w3 ORDER BY w3.__o0 ASC",
        ],
    );
    // DISTINCT and aggregation keep their input's first-seen order in Neo4j;
    // the SQL does not, so a later LIMIT or collect cannot rely on it.
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name DESC WITH DISTINCT a.country AS c \
         WITH c LIMIT 3 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
    );
    not_lowered(
        "MATCH (a:User) WITH a ORDER BY a.name DESC WITH DISTINCT a \
         WITH collect(a.name) AS l RETURN l",
        "collect() over rows in an order the SQL does not keep",
    );
    not_lowered(
        "MATCH (a:User) ORDER BY a.name WITH DISTINCT a.country AS c LIMIT 2 RETURN c",
        "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
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

/// #1395: ClickHouse evaluates a CTE once per read. A CTE whose rows or
/// values depend on the order an evaluation produces rows in, read twice
/// (an OPTIONAL MATCH's drive and the rows it joins back), is lowered stable:
/// a `collect()` lists its values sorted, a SKIP / LIMIT breaks its ties by
/// every column (a node by its identity).
#[test]
fn an_order_dependent_cte_read_twice_is_lowered_stable() {
    // The drive joins back on the collected list.
    has(
        "MATCH (a:User) WITH a.country AS g, collect(a.user_id) AS c \
         OPTIONAL MATCH (b:User) WHERE b.user_id = 5 AND size(c) > 0 RETURN g, count(b) AS m",
        &[r#"arraySort(coalesce(groupArray(v0.user_id), [])) AS "v2""#],
    );
    // The rows a LIMIT takes, read by the drive and joined back; the
    // final SELECT's list, read once, keeps its order.
    has(
        "MATCH (a:User) WITH a LIMIT 3 \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(x:User)-[:FOLLOWS]->(b:User) \
         RETURN a.name AS n, collect(b.name) AS bs",
        &[
            r#"ORDER BY "v1__user_id" ASC LIMIT 3"#,
            r#"coalesce(groupArray(o3.p2_v5_name), []) AS "bs""#,
        ],
    );
    // Ties of the sort keys broken.
    has(
        "MATCH (a:User) WITH a ORDER BY a.country LIMIT 3 \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(x:User)-[:FOLLOWS]->(b:User) RETURN a.name, count(b)",
        &[r#"ORDER BY v0.country ASC, "v1__user_id" ASC LIMIT 3"#],
    );
    // A value by its DISTINCT key.
    has(
        "MATCH (a:User) WITH a.country AS g, count(*) AS k ORDER BY k LIMIT 2 \
         OPTIONAL MATCH (b:User) WHERE b.country = g AND b.user_id > k RETURN g, count(b)",
        &[
            r#"ORDER BY count(*) ASC, tuple(multiIf(replaceRegexpOne(dynamicType(CAST("v1", 'Dynamic'))"#,
        ],
    );
    // Read once: as listed, no ties broken.
    let once = sql("MATCH (a:User) WITH a.country AS g, collect(a.user_id) AS c RETURN g, c");
    assert!(!once.contains("arraySort"), "{once}");
    let once =
        sql("MATCH (a:User) WITH a LIMIT 3 MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name");
    assert!(!once.contains("ORDER BY"), "{once}");
}

/// #1395: a drive right after an aggregating WITH whose aggregated value it
/// reads joins back on the WITH's grouping keys (carried when it does not
/// read them), not on the value: a float sum added in another order on the
/// second read would join no match, a list of maps none at all.
#[test]
fn a_drive_after_an_aggregation_joins_on_its_keys() {
    has(
        "MATCH (a:User) WITH a.country AS g, sum(a.age * 1.5) AS s \
         OPTIONAL MATCH (b:User) WHERE b.user_id = 5 AND s > 0 RETURN g, count(b) AS m",
        &[
            r#"SELECT DISTINCT w1.v1 AS "v1", w1.v2 AS "v2" FROM with_w1 AS w1"#,
            "ON w1.v1 = o3.v1 OR (w1.v1 IS NULL AND o3.v1 IS NULL)",
        ],
    );
    has(
        "MATCH (a:User) WITH a.country AS g, avg(a.age) AS s \
         WHERE EXISTS { MATCH (b:User) WHERE b.user_id = 5 AND s > 0 } RETURN g",
        &["ON isNotDistinctFrom(w1.v1, s3.v1) AND true"],
    );
    let got = sql(
        "MATCH (a:User) WITH a.country AS g, collect({n: a.name}) AS ms \
         OPTIONAL MATCH (b:User) WHERE b.user_id = 5 AND size(ms) > 0 RETURN g, count(b) AS m",
    );
    assert!(!got.contains("w1.v2 = o3.v2"), "{got}");
    // A map key is no join key: joined on the aggregated value, as before.
    has(
        "MATCH (a:User) WITH {c: a.country} AS k, count(*) AS c \
         OPTIONAL MATCH (x:User) WHERE x.user_id = c RETURN k, c, count(x) AS m",
        &["ON w1.v2 = o3.v2"],
    );
}

/// #1395 on Databricks: a list of values of any type (a map is not
/// orderable, a node's STRUCT casts to no VARIANT) sorted by their type and
/// JSON text.
#[test]
fn databricks_sorts_a_list_read_twice_by_its_keys() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "MATCH (a:User) WITH a.country AS g, collect(a.user_id) AS c \
             OPTIONAL MATCH (b:User) WHERE b.user_id = 5 AND size(c) > 0 RETURN g, count(b) AS m",
            &social(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql
    });
    let got = squash(&got);
    assert!(
        got.contains(
            "array_sort(coalesce(collect_list(v0.user_id), array()), (__cg_sl, __cg_sr) -> \
             CASE WHEN concat(typeof(__cg_sl), ':', to_json(named_struct('v', __cg_sl), \
             map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX'"
        ),
        "{got}"
    );
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
    // A relationship with an `edge_id` returns its element id after its
    // properties (#1334).
    assert!(
        sql.starts_with(
            &r#"SELECT v1.follower_id AS "r.from_id", v1.followed_id AS "r.to_id", v1.follow_date AS "r.follow_date", concat('FOLLOWS:', concat(EDGE), ':', toString(v1.follower_id), '->', toString(v1.followed_id), '-') AS "r.elementId", v0.age AS "x.age", v0.city AS "x.city", v0.country AS "x.country", v0.email_address AS "x.email", v0.is_active AS "x.is_active", v0.full_name AS "x.name", v0.registration_date AS "x.registration_date", v0.user_id AS "x.user_id", v0.full_name AS "a.name" FROM"#
                .replace("EDGE", &edge_text("v1.follow_id"))
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
    // Grouped by its columns (its element id among them) and its identity.
    assert!(
        sql.contains("GROUP BY v1.a, v1.b, v1.weight, concat('K:'")
            && sql.ends_with("'-'), v1.kid"),
        "{sql}"
    );
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN DISTINCT k, k.w AS w",
        &shapes_schema(),
    );
    // Its element id is returned, one to one with its `edge_id` (#1334): a
    // plain DISTINCT.
    assert!(
        sql.starts_with(
            r#"SELECT DISTINCT v1.a AS "k.from_id", v1.b AS "k.to_id", v1.weight AS "k.w", concat('K:'"#
        ) && sql.contains(r#"AS "k.elementId", v1.weight AS "w" FROM"#),
        "{sql}"
    );
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN DISTINCT k, count(*) AS c",
        &shapes_schema(),
    );
    assert!(
        sql.starts_with(r#"SELECT DISTINCT v1.a AS "k.from_id""#) && sql.ends_with("'-'), v1.kid"),
        "{sql}"
    );
    // A node's identity is among its properties (`node_id` names
    // properties): a plain DISTINCT.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN DISTINCT b",
        &[r#"SELECT DISTINCT v2.age AS "b.age""#],
    );
}

/// `id(n)` as a RETURN item is the node's key; Bolt encodes it from the
/// shape. It sorts by the key (S11e). Anywhere else (but compared with
/// integers) the key is not the id.
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
    has(
        "MATCH (a:User) RETURN id(a) AS i ORDER BY i",
        &["ORDER BY v0.user_id ASC"],
    );
    // As a value it is the key (S11l).
    has(
        "MATCH (a:User) RETURN id(a) + 1 AS i",
        &[r#"v0.user_id + 1 AS "i""#],
    );
    not_lowered("MATCH (a:User) RETURN size(a.*) AS n", "`v.*`");
    // A composite id is one text, as its element id spells it (I5a).
    let t = translate_bound_plan(
        "MATCH (t:T) RETURN id(t)",
        &shapes_schema(),
        &ReadOptions::default(),
    )
    .unwrap();
    assert!(
        t.sql.contains("concat(CASE WHEN v0.tenant IS NULL THEN"),
        "{}",
        t.sql
    );
    assert_eq!(
        t.shape[0].kind,
        ResultKind::NodeId {
            label: "T".to_string()
        }
    );
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

/// §4.9: an OPTIONAL MATCH is the input LEFT JOIN its matches, with the
/// clause's WHERE inside the matches, so it decides which matches there are
/// and never removes an input row.
#[test]
fn an_optional_match_is_the_input_left_join_its_matches() {
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WHERE b.age > 30 \
         RETURN a.name, b.name",
        &[
            // The shared node is read from the relationship's endpoint column.
            r#"WITH optional_o1 AS ( SELECT v1.follower_id AS "v0__user_id", v2.user_id AS "v2__user_id","#,
            "FROM test_integration.user_follows_test AS v1 \
             JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id \
             WHERE v2.age > 30 )",
            r#"SELECT v0.full_name AS "a.name", o1.p2_v2_name AS "b.name" FROM test_integration.users_test AS v0 LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id"#,
        ],
    );
    let multi_hop = squash(&sql(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) \
         RETURN a.name, c.name",
    ));
    // One unit: the second hop is an inner join inside the matches (#1235),
    // never a second LEFT JOIN of the input.
    assert_eq!(multi_hop.matches("LEFT JOIN").count(), 1, "{multi_hop}");
    assert!(
        multi_hop.contains(
            "JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v2.user_id"
        ),
        "{multi_hop}"
    );
}

/// A conjunct of the input's WHERE that reads only a shared node's columns
/// (an operator tree, no function call) also restricts the matches, which
/// then read the node's table; any other conjunct does not.
#[test]
fn the_input_where_restricts_an_anchored_optional() {
    let q = squash(&sql("MATCH (a:User) WHERE a.age > 30 AND rand() < 2 \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"));
    let (matches, outer) = q.split_once(") SELECT").expect("a CTE");
    assert!(
        matches.contains("FROM test_integration.users_test AS v0 JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id")
            && matches.contains("WHERE v0.age > 30"),
        "{q}"
    );
    assert!(!matches.contains("randCanonical"), "{q}");
    assert!(
        outer.contains("WHERE (v0.age > 30 AND randCanonical() < 2)"),
        "{q}"
    );
    // Read in the clause: the shared node's table.
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WHERE b.age > a.age \
         RETURN b.name",
        &["FROM test_integration.users_test AS v0 JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id"],
    );
}

/// A shared relationship, or a variable only the WHERE reads, is read from
/// the drive `D`: the input's distinct columns. A value only the WHERE reads
/// joins NULL-safely (NULL decides the WHERE too).
#[test]
fn a_relationship_or_a_where_only_variable_drives_the_optional() {
    has(
        "MATCH (a:User), (x:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
         WHERE b.age > x.age RETURN b.name",
        &[
            "optional_d2 AS ( SELECT DISTINCT w1.p2_v1_age AS \"p2_v1_age\", \
             w1.v0__user_id AS \"v0__user_id\", w1.v1__user_id AS \"v1__user_id\" FROM with_w1 AS w1 )",
            "FROM optional_d2 AS d2 JOIN test_integration.user_follows_test AS v2 ON v2.follower_id = d2.v0__user_id",
            "WHERE v3.age > d2.p2_v1_age )",
            // `x` is a node of a MATCH: never NULL, a plain key.
            "LEFT JOIN optional_o3 AS o3 ON w1.v0__user_id = o3.v0__user_id AND w1.v1__user_id = o3.v1__user_id",
        ],
    );
    has(
        "MATCH (a:User) WITH a, a.age AS k OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
         WHERE k IS NULL RETURN b.name",
        &[
            // The WITH's CTE is the input as it is.
            "optional_d2 AS ( SELECT DISTINCT w1.v1__user_id AS \"v1__user_id\", w1.v2 AS \"v2\" FROM with_w1 AS w1 )",
            "WHERE d2.v2 IS NULL )",
            "ON w1.v1__user_id = o3.v1__user_id AND (w1.v2 = o3.v2 OR (w1.v2 IS NULL AND o3.v2 IS NULL))",
        ],
    );
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) OPTIONAL MATCH (a)-[r]->(c:User) RETURN c.name",
        &[
            "FROM optional_d2 AS d2 JOIN test_integration.users_test AS v3 ON d2.v1__followed_id = v3.user_id \
             WHERE d2.v1__follower_id = d2.v0__user_id )",
            "ON w1.v0__user_id = o3.v0__user_id AND w1.v1__follow_id = o3.v1__follow_id",
        ],
    );
}

/// No input relation: one empty record, LEFT JOIN every match.
#[test]
fn a_leading_optional_match_reads_one_empty_record() {
    has(
        "OPTIONAL MATCH (a:User) RETURN count(a)",
        &[
            r#"with_w2 AS ( SELECT 1 AS "__row" )"#,
            r#"SELECT count(o1.v0__user_id) AS "count(a)" FROM with_w2 AS w2 LEFT JOIN optional_o1 AS o1 ON 1 = 1"#,
        ],
    );
}

/// A pattern that cannot match leaves every input row with NULLs: no join.
#[test]
fn an_optional_that_cannot_match_is_null() {
    let q = squash(&sql(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:LIKED]->(b:User) RETURN a.name, b.name, count(b), b:User",
    ));
    assert!(!q.contains("JOIN") && !q.contains("WHERE"), "{q}");
    assert!(
        q.contains(r#"NULL AS "b.name", CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS "count(b)", NULL AS "b:User""#),
        "{q}"
    );
    // A later OPTIONAL MATCH from that NULL node matches nothing either.
    let q = squash(&sql(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:LIKED]->(b:User) \
         OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User) RETURN a.name, c.name",
    ));
    assert!(!q.contains("JOIN"), "{q}");
}

/// What is constant for a matched element is NULL for an unmatched one.
#[test]
fn an_unmatched_element_is_null_in_constant_folds() {
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[r:LIKED]->(p) RETURN p:Post, type(r), a = p",
        &[
            r#"CASE WHEN o1.v2__post_id IS NULL THEN NULL ELSE true END AS "p:Post""#,
            r#"CASE WHEN o1.v1__like_id IS NULL THEN NULL ELSE 'LIKED' END AS "type(r)""#,
            r#"CASE WHEN o1.v2__post_id IS NULL THEN NULL ELSE false END AS "a = p""#,
        ],
    );
}

/// A later MATCH of a variable an OPTIONAL MATCH left NULL matches nothing:
/// a node on its own is `IS NOT NULL`, and a tie to the optional matches is
/// a WHERE (in the LEFT JOIN's ON it would keep the row).
#[test]
fn a_null_optional_variable_matches_nothing_later() {
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) MATCH (b) RETURN b.name",
        &["LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id WHERE o1.v2__user_id IS NOT NULL"],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[r:FOLLOWS]->(b:User) MATCH (a)-[r]->(b) RETURN count(*)",
        &["LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id \
           WHERE (o1.v1__follower_id = v0.user_id AND o1.v1__followed_id = o1.v2__user_id"],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WITH b MATCH (b) RETURN count(*)",
        &["FROM with_w2 AS w2 WHERE w2.v3__user_id IS NOT NULL"],
    );
}

/// Review of S5: a variable only the WHERE reads that matches nothing is
/// NULL in the matches, not a reason the clause has none (`b IS NULL`
/// holds).
#[test]
fn a_null_variable_only_the_where_reads_is_null_in_the_matches() {
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:LIKED]->(b:User) \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(c:User) WHERE b IS NULL RETURN c.name",
        &[
            "WHERE NULL IS NULL )",
            "LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id",
        ],
    );
}

/// Review of S5: with more than one relationship, matches in the whole
/// graph can be far more than the result (every two-hop path), so unless
/// the input's WHERE restricts a shared node, the drive restricts them.
#[test]
fn an_unrestricted_multi_hop_optional_is_driven_by_the_input() {
    has(
        "MATCH (x:User) WITH x OPTIONAL MATCH (x)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) \
         RETURN count(c)",
        &["FROM optional_d2 AS d2 JOIN test_integration.user_follows_test AS v2 ON v2.follower_id = d2.v1__user_id"],
    );
    for anchored in [
        "MATCH (x:User) WHERE x.age > 3 \
         OPTIONAL MATCH (x)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN count(c)",
        "MATCH (x:User) WITH x OPTIONAL MATCH (x)-[:FOLLOWS]->(b:User) RETURN count(b)",
    ] {
        let q = sql(anchored);
        assert!(!q.contains("optional_d"), "{q}");
    }
}

/// Review of S5: a relationship identified by its endpoints (no `edge_id`)
/// can have parallel edges with different properties, so the drive joins on
/// the properties it holds too.
#[test]
fn a_driven_relationship_without_edge_id_joins_on_its_properties() {
    let schema = GraphSchemaConfig::from_yaml_str(include_str!(
        "../../../benchmarks/social_network/schemas/social_benchmark.yaml"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = lowered(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) OPTIONAL MATCH (b)-[r2:FOLLOWS]->(c:User) \
         WHERE r2.follow_date > r.follow_date RETURN count(c)",
        &schema,
        &LowerOptions::default(),
    );
    assert!(
        got.contains(
            "ON w1.v2__user_id = o3.v2__user_id AND (w1.p2_v1_follow_date = o3.p2_v1_follow_date \
             OR (w1.p2_v1_follow_date IS NULL AND o3.p2_v1_follow_date IS NULL)) \
             AND w1.v1__follower_id = o3.v1__follower_id AND w1.v1__followed_id = o3.v1__followed_id"
        ),
        "{got}"
    );
}

fn benchmark() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(include_str!(
        "../../../benchmarks/social_network/schemas/social_benchmark.yaml"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

#[test]
fn a_variable_length_relationship_is_a_relation_of_paths() {
    // The path relation comes first (the probe side of ClickHouse's hash
    // joins), tied to its endpoints; the start node's own conjuncts are also
    // evaluated inside the walk.
    has(
        "MATCH (a:User {user_id: 1})-[:FOLLOWS*1..2]->(b:User) RETURN b.name",
        &[
            "WITH RECURSIVE vlp_v1_path AS (",
            "FROM test_integration.users_test AS start_node \
             JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id",
            "WHERE (start_node.user_id = 1) UNION ALL",
            "WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, rel.follow_id)",
            "FROM vlp_v1_path AS v1 \
             JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id \
             JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id \
             WHERE v0.user_id = 1",
        ],
    );
    // A missing maximum is no bound of our own: the walk ends when no trail
    // extends (or at ClickHouse's recursion limit, loudly).
    has(
        "MATCH (a:User)-[:FOLLOWS*2..]->(b:User) RETURN count(*)",
        &["vp.hop_count < 2147483647", "WHERE hop_count >= 2"],
    );
}

#[test]
fn the_walk_starts_at_the_restricted_end() {
    // Only `b` is restricted: the walk starts there and follows the
    // relationships backward, so `start_id` is `b`.
    has(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User {user_id: 5}) RETURN a.name",
        &[
            "JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.followed_id",
            "WHERE (start_node.user_id = 5)",
            "JOIN test_integration.users_test AS v0 ON v1.end_id = v0.user_id",
            "JOIN test_integration.users_test AS v2 ON v1.start_id = v2.user_id",
        ],
    );
    // Both ends restricted: from the left one.
    has(
        "MATCH (a:User {user_id: 1})-[:FOLLOWS*1..2]->(b:User {user_id: 5}) RETURN count(*)",
        &[
            "ON start_node.user_id = rel.follower_id",
            "WHERE (start_node.user_id = 1)",
        ],
    );
    // `<-[*]-` from a restricted left end: backward too.
    has(
        "MATCH (a:User {user_id: 1})<-[:FOLLOWS*1..2]-(b:User) RETURN count(*)",
        &[
            "ON start_node.user_id = rel.followed_id",
            "WHERE (start_node.user_id = 1)",
            "JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id",
        ],
    );
}

#[test]
fn a_path_starts_at_the_rows_so_far() {
    // After a hop: the walk starts at the values of its first node there.
    has(
        "MATCH (a:User {user_id: 1})-[r:FOLLOWS]->(x:User)-[:FOLLOWS*1..]->(b:User) RETURN count(*)",
        &[
            "WHERE (start_node.user_id IN (SELECT DISTINCT v2.user_id AS \"id\" \
             FROM test_integration.users_test AS v0 \
             JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id \
             JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id \
             WHERE v0.user_id = 1))",
            // the hop and the path are distinct relationships
            "NOT has(v3.path_edges, v1.follow_id)",
        ],
    );
    // A node carried by a WITH: the values of its CTE column.
    has(
        "MATCH (a:User {user_id: 1}) WITH a MATCH (a)<-[:FOLLOWS*0..2]-(b:User) RETURN count(*)",
        &[
            "WHERE (start_node.user_id IN (SELECT DISTINCT w1.v1__user_id AS \"id\" FROM with_w1 AS w1))",
            "FROM vlp_v2_path AS v2 JOIN with_w1 AS w1 ON v2.start_id = w1.v1__user_id",
        ],
    );
}

#[test]
fn relationships_of_one_match_are_unique_with_paths() {
    // Two paths of one table share no relationship; the property map holds
    // of every relationship of its path, inside the walk.
    has(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User), \
         (a)-[:FOLLOWS*1..2 {follow_date: '2023-01-01'}]->(c:User) RETURN count(*)",
        &[
            "AND (rel.follow_date = '2023-01-01') UNION ALL",
            "AND NOT has(vp.path_edges, rel.follow_id) AND (rel.follow_date = '2023-01-01') )",
            "WHERE NOT hasAny(v1.path_edges, v3.path_edges)",
        ],
    );
    // Uniqueness is per MATCH clause.
    let got = sql("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) MATCH (b)-[:FOLLOWS*1..2]->(c:User) RETURN count(*)");
    assert!(!got.contains("hasAny"), "{got}");
    // Without an `edge_id` a relationship is its stored endpoint pair,
    // whichever way a walk follows it: a backward walk, a hop and a forward
    // walk spell it alike.
    let got = lowered(
        "MATCH (a:User {user_id: 1})<-[:FOLLOWS*1..2]-(b:User)<-[r:FOLLOWS]-(c:User) RETURN count(*)",
        &benchmark(),
        &LowerOptions::default(),
    );
    for part in [
        "ON start_node.user_id = rel.followed_id",
        "NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))",
        "NOT has(v1.path_edges, tuple(v3.follower_id, v3.followed_id))",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let got = lowered(
        "MATCH (a:User {user_id: 1})<-[:FOLLOWS*1..2]-(b:User)-[:FOLLOWS*1..2]->(c:User) RETURN count(*)",
        &benchmark(),
        &LowerOptions::default(),
    );
    for part in [
        "ON start_node.user_id = rel.followed_id",
        "WHERE (start_node.user_id = 1)",
        "NOT hasAny(v1.path_edges, v3.path_edges)",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

#[test]
fn a_path_from_a_label_the_edge_does_not_join_has_no_relationship() {
    // Posts follow no one: `*0..2` from a post is the post itself only.
    has(
        "MATCH (a:Post)-[:FOLLOWS*0..2]->(b) RETURN count(*)",
        &[
            "0 as hop_count",
            "FROM test_integration.posts_test AS start_node",
        ],
    );
    let got = sql("MATCH (a:Post)-[:FOLLOWS*0..2]->(b) RETURN count(*)");
    assert!(!got.contains("user_follows_test"), "{got}");
    // A relationship is needed: nothing matches.
    has(
        "MATCH (a:Post)-[:FOLLOWS*1..2]->(b:Post) RETURN count(*)",
        &["WHERE false"],
    );
}

#[test]
fn the_walk_starts_where_the_rows_so_far_are_tied_to_it() {
    // `b` is only cross-joined to `a`: the walk starts at `a`, which has
    // conjuncts of its own.
    has(
        "MATCH (a:User {user_id: 1}) MATCH (b:User)-[:FOLLOWS*1..2]->(a) RETURN count(*)",
        &[
            "ON start_node.user_id = rel.followed_id",
            "WHERE (start_node.user_id = 1)",
        ],
    );
    // `a` carried by a WITH restricts; `b`, cross-joined, does not. The
    // semi-join reads only the relations tied to `a`.
    has(
        "MATCH (a:User) WHERE a.user_id = 1 WITH a MATCH (b:User)-[:FOLLOWS*1..2]->(a) RETURN count(*)",
        &[
            "ON start_node.user_id = rel.followed_id",
            "WHERE (start_node.user_id IN (SELECT DISTINCT w1.v1__user_id AS \"id\" FROM with_w1 AS w1))",
        ],
    );
    // Relations tied to each other but not to `a` are not in its semi-join.
    has(
        "MATCH (a:User) WHERE a.user_id = 1 WITH a \
         MATCH (x:User)-[:FOLLOWS]->(y:User), (b:User)-[:FOLLOWS*1..2]->(a) RETURN count(*)",
        &["WHERE (start_node.user_id IN (SELECT DISTINCT w1.v1__user_id AS \"id\" FROM with_w1 AS w1))"],
    );
    // An end carried from a CTE (here the OPTIONAL's drive) wins over one
    // with conjuncts of its own.
    has(
        "MATCH (a:User) WHERE a.user_id < 5 \
         OPTIONAL MATCH (a)-[:FOLLOWS*1..2]->(b:User) WHERE b.user_id < 50000 RETURN count(*)",
        &[
            "ON start_node.user_id = rel.follower_id",
            "start_node.user_id IN (SELECT DISTINCT d2.v0__user_id",
        ],
    );
    // An end with its own conjuncts wins over one the rows so far hold.
    has(
        "MATCH (x:User)-[:FOLLOWS]->(y:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.user_id = 1 RETURN count(*)",
        &["ON start_node.user_id = rel.followed_id", "WHERE (start_node.user_id = 1)"],
    );
}

#[test]
fn an_optional_path_walks_from_the_drive() {
    // The input restricts `a`, not `x`, where the path starts: the drive
    // holds `x`, and the walk starts at its values.
    has(
        "MATCH (a:User)-[:FOLLOWS]->(x:User) WHERE a.user_id = 1 \
         OPTIONAL MATCH (x)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS]->(a) RETURN count(b) AS n",
        &["optional_d", "start_node.user_id IN (SELECT DISTINCT d"],
    );
}

#[test]
fn length_of_a_path() {
    has(
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS*0..2]->(c:User) RETURN length(p) AS l",
        &[r#"v3.hop_count + 1 AS "l""#],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN length(p) AS l",
        &[r#"1 AS "l""#],
    );
    // NULL when the OPTIONAL MATCH has no match; its anonymous elements are
    // carried for it.
    has(
        "MATCH (a:User) OPTIONAL MATCH p = (a)-[:FOLLOWS*1..2]->(c:User) RETURN length(p) AS l",
        &[
            r#"CASE WHEN (o3.v2__user_id IS NULL OR o3.v1__start_id IS NULL) THEN NULL ELSE o3.v1__hop_count END AS "l""#,
        ],
    );
}

#[test]
fn optional_match_has_path_variables() {
    let (_, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(
        "MATCH (a:User) OPTIONAL MATCH p = (a)-->(b) RETURN length(p)",
    )
    .unwrap();
    assert!(format!("{stmt:?}").contains(r#"Some("p")"#));
}

#[test]
fn an_optional_path_that_cannot_match_leaves_no_relation() {
    // `c` matches nothing, so the OPTIONAL MATCH has no match; the path's
    // relation (and the drive it reads) are dropped with it.
    let got = sql("MATCH (a:User)-[:FOLLOWS]->(x:User) \
         OPTIONAL MATCH (x)-[:FOLLOWS*1..2]->(b:User), (c:Nope) RETURN count(b) AS n");
    assert!(!got.contains("vlp_") && !got.contains("optional_"), "{got}");
}

// ------------------------------------------------------------------ S6b

#[test]
fn a_shortest_path_is_a_breadth_first_search() {
    // From each first node, each node once at its distance; a node already
    // reached is not reached again, and the reached ones are carried along.
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]->(b:User)) \
         RETURN b.name AS n, length(p) AS l",
        &[
            "WITH RECURSIVE vlp_v1_bfs AS ( SELECT DISTINCT start_node.user_id AS start_id, \
             start_node.user_id AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt8) AS new \
             FROM test_integration.users_test AS start_node \
             WHERE start_node.user_id IS NOT NULL AND (start_node.user_id = 1) \
             UNION ALL",
            "FROM vlp_v1_bfs AS f \
             JOIN test_integration.user_follows_test AS rel ON rel.follower_id = f.node \
             JOIN test_integration.users_test AS end_node ON end_node.user_id = rel.followed_id \
             WHERE f.new = 1 \
             AND (f.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v1_bfs) \
             UNION ALL SELECT start_id, node, depth, CAST(0 AS UInt8) AS new FROM vlp_v1_bfs \
             WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1) )",
            "vlp_v1_path AS ( SELECT start_id, node AS end_id, depth AS hop_count \
             FROM vlp_v1_bfs WHERE new = 1 AND depth >= 1 )",
            r#"v1.hop_count AS "l" FROM vlp_v1_path AS v1"#,
        ],
    );
    // A maximum stops the search; the path of none is a pair of one node.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*0..3]->(b:User)) RETURN count(*) AS c",
        &[
            "WHERE f.new = 1 AND f.depth < 3 AND",
            "WHERE new = 1 AND depth >= 0",
        ],
    );
    // Walked from the restricted end, against the relationships.
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})<-[:FOLLOWS*]-(b:User)) RETURN b.name AS n",
        &[
            "WHERE start_node.user_id IS NOT NULL AND (start_node.user_id = 1)",
            "ON rel.followed_id = f.node",
            "ON end_node.user_id = rel.follower_id",
            "JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id",
            "JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id",
        ],
    );
}

#[test]
fn all_shortest_paths_are_counted_by_the_search() {
    // Each node's number of shortest paths is the sum over the relationships
    // reaching it from one level nearer; a pair's row is repeated that many
    // times, for the ends the last node allows.
    has(
        "MATCH p = allShortestPaths((a:User {user_id: 1})-[:FOLLOWS*]->(b:User {user_id: 2})) \
         RETURN count(*) AS c",
        &[
            "CAST(0 AS UInt32) AS depth, CAST(1 AS UInt256) AS paths, CAST(1 AS UInt8) AS new",
            "SELECT f.start_id AS start_id, end_node.user_id AS node, \
             CAST(f.depth + 1 AS UInt32) AS depth, CAST(sum(f.paths) AS UInt256) AS paths, \
             CAST(1 AS UInt8) AS new",
            "GROUP BY f.start_id, end_node.user_id, f.depth UNION ALL \
             SELECT start_id, node, depth, paths, CAST(0 AS UInt8) AS new",
            "vlp_v1_path AS ( SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs \
             ARRAY JOIN range(accurateCast(paths, 'UInt64')) AS copy \
             WHERE new = 1 AND depth >= 1 AND node IN (SELECT end_node.user_id \
             FROM test_integration.users_test AS end_node WHERE (end_node.user_id = 2)) )",
        ],
    );
}

#[test]
fn a_shortest_path_condition_holds_before_the_pick() {
    // #1312: the shortest path that satisfies the WHERE, not the shortest
    // path if it does. The conjunct reading the path goes into the search;
    // the one reading only the ends stays outside. A pair whose distance
    // satisfies it has its shortest path; the trails are searched only from
    // the first nodes of the others, and only for those pairs.
    let got = squash(&sql(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) \
         WHERE a <> b AND length(p) > 1 RETURN length(p) AS l, count(*) AS c",
    ));
    let failing = "SELECT v1.start_id AS start_id, v1.end_id AS end_id FROM vlp_v1_near AS v1 \
                   WHERE NOT coalesce((v1.hop_count > 1), false)";
    for part in [
        "WITH RECURSIVE vlp_v1_bfs AS (".to_string(),
        "vlp_v1_near AS ( SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs \
         WHERE new = 1 AND depth >= 1 )"
            .to_string(),
        format!("WHERE (start_node.user_id IN (SELECT start_id FROM ({failing})))"),
        format!(
            "vlp_v1_shortest AS ( SELECT start_id, end_id, hop_count FROM ( \
             SELECT v1.start_id AS start_id, v1.end_id AS end_id, v1.hop_count AS hop_count \
             FROM vlp_v1_near AS v1 WHERE (v1.hop_count > 1) ) UNION ALL \
             SELECT start_id, end_id, hop_count FROM ( \
             SELECT v1.start_id AS start_id, v1.end_id AS end_id, v1.hop_count AS hop_count, \
             ROW_NUMBER() OVER (PARTITION BY v1.start_id, v1.end_id ORDER BY v1.hop_count) AS shortest \
             FROM vlp_v1_path AS v1 WHERE (v1.hop_count > 1) \
             AND (v1.start_id, v1.end_id) IN ({failing}) AND v1.start_id <> v1.end_id \
             ) WHERE shortest = 1 )"
        ),
        "FROM vlp_v1_shortest AS v1".to_string(),
    ] {
        assert!(got.contains(&part), "missing `{part}` in\n{got}");
    }
    // Every shortest one, counted; a condition reading an end joins it.
    has(
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) \
         WHERE a <> b AND (length(p) > 2 OR a.user_id = 1) RETURN count(*) AS c",
        &[
            "vlp_v1_near AS ( SELECT start_id, node AS end_id, depth AS hop_count, paths",
            "SELECT v1.start_id AS start_id, v1.end_id AS end_id, v1.hop_count AS hop_count, \
             v1.paths AS paths FROM vlp_v1_near AS v1 \
             JOIN test_integration.users_test AS v0 ON v0.user_id = v1.start_id \
             WHERE ((v1.hop_count > 2 OR v0.user_id = 1)) \
             ) ARRAY JOIN range(accurateCast(paths, 'UInt64')) AS copy UNION ALL",
            "MIN(v1.hop_count) OVER (PARTITION BY v1.start_id, v1.end_id) AS shortest \
             FROM vlp_v1_path AS v1 \
             JOIN test_integration.users_test AS v0 ON v0.user_id = v1.start_id",
            ") WHERE hop_count = shortest )",
        ],
    );
    // From 0, a pair of one node has the path of none, or a closed trail.
    let got = squash(&sql(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*0..]->(b:User)) \
         WHERE length(p) > 0 RETURN count(*) AS c",
    ));
    assert!(got.contains("WHERE new = 1 AND depth >= 0 )"), "{got}");
    assert!(
        got.contains("NOT coalesce((v1.hop_count > 0), false)) ) WHERE shortest = 1"),
        "{got}"
    );
    // A lower bound above 1 is a condition on the length.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*2..]->(b:User)) RETURN count(*) AS c",
        &[
            "FROM vlp_v1_near AS v1 WHERE (v1.hop_count >= 2) )",
            "WHERE new = 1 AND depth >= 1 )",
        ],
    );
}

#[test]
fn a_bound_on_the_length_from_above_bounds_the_search() {
    // No trail is longer than the bound, and no pair needs one.
    let got = squash(&sql(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]->(b:User {user_id: 2})) \
         WHERE length(p) < 10 RETURN length(p) AS l",
    ));
    assert!(got.contains("AND f.depth < 9 AND"), "{got}");
    assert!(
        !got.contains("vlp_v1_near") && !got.contains("ROW_NUMBER"),
        "{got}"
    );
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*..5]->(b:User)) WHERE 3 >= length(p) \
         RETURN count(*) AS c",
        &["AND f.depth < 3 AND"],
    );
    // `=` bounds it and stays a condition.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE length(p) = 3 \
         RETURN count(*) AS c",
        &["AND f.depth < 3 AND", "WHERE (v1.hop_count = 3)"],
    );
    // Below the range: nothing.
    let got = sql(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE length(p) < 1 \
         RETURN count(*) AS c",
    );
    assert!(!got.contains("vlp_"), "{got}");
}

#[test]
fn a_shortest_path_search_stops_at_its_ends() {
    // Once a first node has reached every value the last node can have, it
    // goes no further (a near end of a deep graph).
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]->(b:User {user_id: 3})) \
         RETURN length(p) AS l",
        &[
            "AND f.start_id NOT IN (SELECT start_id FROM vlp_v1_bfs GROUP BY start_id \
             HAVING countIf(node IN (SELECT end_node.user_id FROM test_integration.users_test \
             AS end_node WHERE (end_node.user_id = 3))) >= (SELECT count(DISTINCT end_node.user_id) \
             FROM (SELECT end_node.user_id FROM test_integration.users_test AS end_node \
             WHERE (end_node.user_id = 3)) AS end_node))",
            "WHERE new = 1 AND depth >= 1 AND node IN (SELECT end_node.user_id",
        ],
    );
    // `*0..0`: the first nodes only.
    let got =
        sql("MATCH p = shortestPath((a:User)-[:FOLLOWS*0..0]->(b:User)) RETURN count(*) AS c");
    assert!(!got.contains("UNION ALL"), "{got}");
}

#[test]
fn a_shortest_path_starts_at_an_end_pinned_by_its_identity() {
    // `b` is one node; `a`'s conjunct holds of many.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User {user_id: 5})) \
         WHERE a.city = 'Paris' RETURN count(*) AS c",
        &[
            "WHERE start_node.user_id IS NOT NULL AND (start_node.user_id = 5)",
            "ON rel.followed_id = f.node",
            "JOIN test_integration.users_test AS v2 ON v1.start_id = v2.user_id",
        ],
    );
}

#[test]
fn a_shortest_path_is_not_unique_against_the_other_relationships() {
    // Neo4j searches a shortest path on its own: the clause's other
    // relationships may be on it.
    let got = sql(
        "MATCH (a:User)-[:FOLLOWS]->(x:User), p = shortestPath((a)-[:FOLLOWS*]->(b:User)) \
         RETURN count(*) AS c",
    );
    assert!(
        !got.contains("path_edges") && got.contains("vlp_v3_bfs"),
        "{got}"
    );
    let got = sql(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(x:User), p = shortestPath((a)-[:FOLLOWS*]->(b:User)) \
         RETURN count(*) AS c",
    );
    assert!(!got.contains("hasAny"), "{got}");
}

#[test]
fn an_optional_shortest_path() {
    has(
        "MATCH (a:User) WHERE a.user_id < 5 \
         OPTIONAL MATCH p = shortestPath((a)-[:FOLLOWS*]->(b:User {user_id: 3})) \
         WHERE length(p) > 1 RETURN a.name AS n, length(p) AS l",
        // Inside the OPTIONAL MATCH `length(p)` is NULL when `b` is: `b` is
        // joined to the paths.
        &["FROM vlp_v1_path AS v1 \
             JOIN test_integration.users_test AS v2 ON v2.user_id = v1.start_id \
             WHERE (CASE WHEN (v2.user_id IS NULL OR v1.start_id IS NULL) THEN NULL \
             ELSE v1.hop_count END > 1) AND (v1.start_id, v1.end_id) IN"],
    );
}

/// S11i: a shortest path over one anonymous relationship is the path of
/// exactly one, `*1..1`.
#[test]
fn a_shortest_path_over_one_relationship_is_one_of_one_hop() {
    for mode in ["shortestPath", "allShortestPaths"] {
        let one = |spec: &str| {
            sql(&format!(
                "MATCH p = {mode}((a:User)-[:FOLLOWS{spec}]->(b:User)) WHERE a.user_id = 1 \
                 RETURN b.user_id AS b, length(p) AS l"
            ))
        };
        assert_eq!(one(""), one("*1..1"), "{mode}");
        // Named, it is that list too, as Neo4j binds it (`size(r)` is 1).
        let named = |spec: &str| {
            sql(&format!(
                "MATCH p = {mode}((a:User)-[r:FOLLOWS{spec}]->(b:User)) WHERE a.user_id = 1 \
                 RETURN size(r) AS n"
            ))
        };
        assert_eq!(named(""), named("*1..1"), "{mode}");
    }
}

#[test]
fn what_shortest_paths_are_not_lowered() {
    not_lowered(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(a)) RETURN count(*)",
        "shortestPath from a node to itself",
    );
    // The pick would be per row of `x`, not per pair of ends.
    not_lowered(
        "MATCH (x:User), p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) \
         WHERE length(p) > x.user_id RETURN count(*)",
        "reads a variable other than the path and its ends",
    );
    not_lowered(
        "MATCH (a:User) WITH a \
         MATCH p = shortestPath((a)-[:FOLLOWS*]->(b:User)) WHERE length(p) > a.user_id \
         RETURN count(*)",
        "reads a variable other than the path and its ends",
    );
    not_lowered(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)), \
         q = shortestPath((b)-[:FOLLOWS*]->(c:User)) WHERE length(p) < length(q) \
         RETURN count(*)",
        "reads a variable other than the path and its ends",
    );
    // A value carried by a WITH is a column of its CTE, not of the paths.
    not_lowered(
        "MATCH (c:User {user_id: 3}) WITH c.age AS k \
         MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE length(p) * 10 > k \
         RETURN count(*)",
        "reads a variable other than the path and its ends",
    );
}

#[test]
fn a_shortest_path_follows_relationships_its_property_map_allows() {
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*1.. {follow_date: '2024-01-01'}]->(b:User)) \
         RETURN count(*) AS c",
        &["WHERE f.new = 1 AND (rel.follow_date = '2024-01-01') AND (f.start_id, end_node.user_id)"],
    );
    // Walked back by the relationships it allows.
    for f in ["shortestPath", "allShortestPaths"] {
        has(
            &format!(
                "MATCH p = {f}((a:User {{user_id: 1}})-[:FOLLOWS*1.. {{follow_date: '2024-01-01'}}]->(b:User)) \
                 RETURN p"
            ),
            &["WHERE w.frontier = 1 AND w.depth > 0 AND (rel.follow_date = '2024-01-01') )"],
        );
    }
}

#[test]
fn a_shortest_path_condition_may_bind_its_own_names() {
    // `s` and `x` are the reduce's own, not variables the pick would need.
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]->(b:User)) \
         WHERE reduce(s = 0, x IN range(1, length(p)) | s + x) > 5 RETURN count(*) AS c",
        &["vlp_v1_shortest AS"],
    );
}

/// A path is the list of its nodes and relationships in turn, each in
/// Neo4j's JSON form with ClickGraph's element ids (`value.rs`).
#[test]
fn a_path_is_the_list_of_its_elements() {
    let (sql, shape) = shaped(
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN p, nodes(p) AS ns, relationships(p) AS rs",
        &social(),
    );
    let node = "map('elementId', CAST(concat('User:', toString(v0.user_id), '-'), 'Dynamic'), \
                'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> \
                __v IS NOT NULL, map('age', CAST(v0.age, 'Dynamic'), ";
    // Its `edge_id`'s text, escaped (#1334).
    let rel = "map('elementId', CAST(concat('FOLLOWS:', concat(EDGE), ':', toString(v1.follower_id), '->', \
               toString(v1.followed_id), '-'), 'Dynamic'), 'startNodeElementId', \
               CAST(concat('User:', toString(v1.follower_id), '-'), 'Dynamic'), \
               'endNodeElementId', CAST(concat('User:', toString(v1.followed_id), '-'), \
               'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', \
               CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', \
               CAST(v1.follow_date, 'Dynamic'))), 'Dynamic'))";
    let rel = rel.replace("EDGE", &edge_text("v1.follow_id"));
    for part in [squash(node), squash(&rel)] {
        assert!(sql.contains(&part), "missing `{part}` in\n{sql}");
    }
    assert!(sql.starts_with("SELECT [map('elementId'"), "{sql}");
    assert_eq!(
        kinds(&shape),
        vec![
            column("p", ResultKind::Graph(GraphType::Path)),
            column(
                "ns",
                ResultKind::Graph(GraphType::List(Box::new(GraphType::Node)))
            ),
            column(
                "rs",
                ResultKind::Graph(GraphType::List(Box::new(GraphType::Relationship)))
            ),
        ]
    );
    // `nodes(p)`: every node, in order.
    let ns = sql.split(" AS \"p\", ").nth(1).unwrap_or_default();
    let (first, second) = (
        ns.find("toString(v0.user_id)").unwrap_or(usize::MAX),
        ns.find("toString(v2.user_id)").unwrap_or(usize::MAX),
    );
    assert!(
        first < second && second < ns.find(" AS \"ns\"").unwrap_or(0),
        "{ns}"
    );
    // A node of another label, and a path of one node.
    has(
        "MATCH p = (u:User)-[:LIKED]->(x:Post) RETURN p",
        &[
            "concat('Post:', toString(v2.post_id), '-')",
            "CAST(['Post'], 'Dynamic')",
        ],
    );
    has(
        "MATCH p = (a:User) RETURN relationships(p) AS rs",
        &["CAST([], 'Array(Map(String, Dynamic))') AS \"rs\""],
    );
}

/// A variable-length relationship's nodes and relationships are carried
/// through its search, only when a value reads them; `size()` and
/// `length()` count without them.
#[test]
fn a_variable_length_path_carries_its_values() {
    has(
        "MATCH p = (a:User {user_id: 1})-[:FOLLOWS*1..2]->(b:User) RETURN p",
        &[
            "[map('elementId', CAST(concat('User:', toString(start_node.user_id), '-')",
            "as path_node_values",
            "arrayConcat(vp.path_node_values, [map('elementId', CAST(concat('User:', toString(end_node.user_id)",
            "arrayConcat(vp.path_rel_values, [map('elementId', CAST(concat('FOLLOWS:', concat(CASE WHEN rel.follow_id IS NULL",
            "arrayFlatten(arrayMap((__r, __n) -> [__r, __n], v1.path_rel_values, arraySlice(v1.path_node_values, 2))))",
        ],
    );
    // Only what is read is carried.
    let nodes = squash(&sql(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN nodes(p) AS ns",
    ));
    assert!(
        nodes.contains("path_node_values") && !nodes.contains("path_rel_values"),
        "{nodes}"
    );
    let rels = squash(&sql("MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN r"));
    assert!(
        rels.contains("v1.path_rel_values AS \"r\"") && !rels.contains("path_node_values"),
        "{rels}"
    );
    let counted = squash(&sql("MATCH p = (a:User)-[r:FOLLOWS*1..2]->(b:User) \
         WHERE size(nodes(p)) > 2 RETURN length(p), size(relationships(p)), size(r)"));
    assert!(!counted.contains("_values"), "{counted}");
    assert!(
        counted.contains("v1.hop_count + 1 > 2")
            && counted.contains("v1.hop_count AS \"size(relationships(p))\"")
            && counted.contains("v1.hop_count AS \"size(r)\""),
        "{counted}"
    );
    // A path of none: its start node, and no relationship.
    has(
        "MATCH p = (a:User)-[:FOLLOWS*0..2]->(b:User) RETURN p",
        &[
            "[map('elementId', CAST(concat('User:', toString(start_node.user_id), '-')",
            "CAST([], 'Array(Map(String, Dynamic))') as path_rel_values",
        ],
    );
}

/// The walk's order is the path's, reversed when the walk starts at the
/// pattern's right end or goes against the pattern's direction (a closed
/// path written `<-`).
#[test]
fn a_path_walked_against_its_order_is_reversed() {
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User {user_id: 3}) RETURN nodes(p) AS ns",
        &["arraySlice(arrayReverse(v1.path_node_values), 2)"],
    );
    has(
        "MATCH p = (a:User)<-[:FOLLOWS*1..2]-(a) RETURN nodes(p) AS ns",
        &["arraySlice(arrayReverse(v1.path_node_values), 2)"],
    );
    let forward = squash(&sql(
        "MATCH p = (a:User {user_id: 1})<-[:FOLLOWS*1..2]-(b:User) RETURN nodes(p) AS ns",
    ));
    assert!(!forward.contains("arrayReverse"), "{forward}");
    let closed = squash(&sql(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(a) RETURN nodes(p) AS ns",
    ));
    assert!(!closed.contains("arrayReverse"), "{closed}");
}

/// DISTINCT and grouping by a value go by its elements' identities (a list
/// of relationships by its relationships only: empty lists are equal), the
/// value is `any()` of the group.
#[test]
fn distinct_and_grouping_by_a_value_use_its_identities() {
    has(
        "MATCH (a:User)-[r:FOLLOWS*0..2]->(b:User) RETURN DISTINCT r",
        &[
            "SELECT any(v1.path_rel_values) AS \"r\"",
            "GROUP BY v1.path_edges",
        ],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN nodes(p) AS ns, count(*) AS c",
        &[
            "any(arrayConcat([map(",
            "GROUP BY arrayConcat([concat('User', ':', toString(v0.user_id))], \
             arrayMap(__x -> concat('User:', toString(__x)), arraySlice(v1.path_nodes, 2)))",
        ],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN DISTINCT p",
        &["GROUP BY [concat('User', ':', toString(v0.user_id)), \
           concat('FOLLOWS:User:User:', toString(v1.follow_id))]"],
    );
    // A WITH groups a carried list by its relationships; its other columns
    // are any() of the group.
    has(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) WITH r, count(*) AS c RETURN r, c",
        &[
            "any(v1.start_id) AS \"v3__start_id\"",
            "v1.path_edges AS \"v3__path_edges\"",
            "any(v1.path_rel_values) AS \"v3__path_rel_values\"",
            "GROUP BY v1.path_edges",
        ],
    );
    // No relationship at all: one group.
    has(
        "MATCH (a:User)-[r:FOLLOWS*0..0]->(b:User) RETURN DISTINCT r",
        &["any(v1.path_rel_values) AS \"r\"", "HAVING count(*) > 0"],
    );
}

/// A WITH carries a path by its elements and rebuilds its value after; a
/// computed list is carried as its value and keys.
#[test]
fn a_with_carries_paths_and_lists() {
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH p RETURN p, length(p) AS l",
        &[
            "v1.path_node_values AS \"v1__path_node_values\"",
            "v1.path_rel_values AS \"v1__path_rel_values\"",
            "arraySlice(w2.v1__path_node_values, 2)",
            "w2.v1__hop_count AS \"l\"",
        ],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) \
         WITH nodes(p) AS ns RETURN ns, size(ns) AS n",
        &[
            // As a list's elements (S7e2b): the walk's node tuples.
            "arrayConcat([tuple(v0.user_id, ",
            "arraySlice(v1.path_node_tuples, 2)) AS \"v4\"",
            "length(w2.v4) AS \"n\"",
        ],
    );
    let (_, shape) = shaped(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH relationships(p) AS rs RETURN rs",
        &social(),
    );
    assert_eq!(
        kinds(&shape),
        vec![column(
            "rs",
            ResultKind::Graph(GraphType::List(Box::new(GraphType::Relationship)))
        )]
    );
}

/// An OPTIONAL MATCH's path is NULL where the clause did not match.
#[test]
fn an_optional_path_value_is_null_when_unmatched() {
    has(
        "MATCH (a:User) OPTIONAL MATCH p = (a)-[:FOLLOWS*1..2]->(b:User) RETURN p",
        &[
            ".v1__start_id IS NULL), NULL, CAST(arrayConcat([map(",
            "if((o",
        ],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[r:FOLLOWS*1..2]->(b:User) RETURN size(r) AS n",
        &["CASE WHEN o3.v1__start_id IS NULL THEN NULL ELSE o3.v1__hop_count END"],
    );
}

#[test]
fn what_path_values_are_not_lowered() {
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN collect(p) AS ps",
        "a path other than",
    );
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN p ORDER BY p",
        "a path other than",
    );
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH nodes(p) AS ns RETURN ns[0] AS n",
        "an element or slice of a list of nodes or relationships",
    );
    not_lowered(
        // (A walk of one type carries its nodes as tuples, S7e2b, which the
        // OPTIONAL MATCH reads; one of several types carries values.)
        "MATCH p = (a:User)-[:FOLLOWS|LIKED*1..2]->(b) WITH nodes(p) AS ns MATCH (x:User) \
         OPTIONAL MATCH (x)-[:FOLLOWS]->(y:User) WHERE size(ns) > 1 RETURN y.name",
        "a path's list read by an OPTIONAL MATCH",
    );
}

/// A list passed through and not read as a value lowers on Databricks.
#[test]
fn databricks_list_passed_through_lowers() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) WITH r, b RETURN b.name, size(r) AS n",
            &social(),
            &ReadOptions::default(),
        )
    });
    assert!(got.is_ok(), "{got:?}");
}

/// An OPTIONAL MATCH whose WHERE reads a `-[r*]->` list joins its matches
/// on the list's identity (its first node and relationships), and its drive
/// holds no value.
#[test]
fn a_list_read_by_an_optional_where_joins_on_its_identity() {
    let got = squash(&sql(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User) \
         WHERE size(r) > 1 RETURN r, c.name",
    ));
    assert!(
        got.contains("w2.v1__start_id = o4.v1__start_id AND w2.v1__path_edges = o4.v1__path_edges"),
        "{got}"
    );
    let drive = got.split("optional_d3 AS (").nth(1).unwrap_or_default();
    let drive = drive.split("), ").next().unwrap_or_default();
    assert!(!drive.contains("_values"), "{drive}");
}

/// Review findings (#1333): a path's identity is one list of its elements
/// (two variable-length parts can split a path in several ways); an
/// unmatched OPTIONAL path is one NULL; a type without properties has an
/// empty property map; a passed-through path builds no values.
#[test]
fn a_path_is_grouped_by_one_list_of_its_elements() {
    let got = squash(&sql(
        "MATCH p = (a:User)-[:FOLLOWS*0..1]->(b:User)-[:FOLLOWS*0..1]->(c:User) RETURN DISTINCT p",
    ));
    let group_by = got.split("GROUP BY ").nth(1).unwrap_or_default();
    assert!(
        group_by.starts_with("arrayConcat([concat('User', ':'"),
        "{got}"
    );
    for part in [
        "arrayMap(__x -> concat('FOLLOWS:User:User:', toString(__x)), v1.path_edges)",
        "arrayMap(__x -> concat('FOLLOWS:User:User:', toString(__x)), v3.path_edges)",
    ] {
        assert!(group_by.contains(part), "{part} in {group_by}");
    }
    // One key: the GROUP BY ends where its one expression does.
    let mut depth = 0;
    let end = group_by
        .char_indices()
        .find(|(_, c)| {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            depth == 0 && *c == ')'
        })
        .map(|(i, _)| i + 1)
        .unwrap_or(0);
    assert!(end > 0 && group_by[end..].trim().is_empty(), "{group_by}");
    // A path's nodes follow from its first node and relationships, and how
    // its parts split them does not: not in the key.
    assert!(!group_by.contains("path_nodes"), "{group_by}");
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH p, count(*) AS c RETURN p, c",
        &[
            "GROUP BY arrayConcat([concat('User', ':', toString(v0.user_id))], \
           arrayMap(__x -> concat('FOLLOWS:User:User:', toString(__x)), v1.path_edges))",
        ],
    );
    let with = squash(&sql(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User) \
         WITH DISTINCT p RETURN count(*) AS n",
    ));
    assert!(
        with.contains("__k0\"") && with.contains("any(v1.path_edges)"),
        "{with}"
    );
}

#[test]
fn an_unmatched_optional_path_is_one_null() {
    has(
        "MATCH (u:User) OPTIONAL MATCH p = (u)-[:FOLLOWS]->(v:User {user_id: 99}) RETURN DISTINCT p",
        &[
            "GROUP BY (o1.v2__user_id IS NULL OR o1.v1__follow_id IS NULL), \
             if((o1.v2__user_id IS NULL OR o1.v1__follow_id IS NULL), [], [concat('User', ':'",
        ],
    );
    has(
        "MATCH (u:User) OPTIONAL MATCH (u)-[r:FOLLOWS*1..2]->(v:User {user_id: 99}) \
         WITH DISTINCT r RETURN r",
        &[
            "AS \"v3__unmatched\"",
            "GROUP BY o3.v1__path_edges, o3.v1__start_id IS NULL",
        ],
    );
}

#[test]
fn an_element_without_properties_has_an_empty_property_map() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: no_props
graph_schema:
  nodes:
    - { label: N, database: db, table: n, node_id: id, property_mappings: {} }
  edges:
    - { type: E, database: db, table: e, from_id: a, to_id: b, from_node: N, to_node: N, property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = translate_bound_plan(
        "MATCH p = (x:N)-[:E*1..2]->(y:N) RETURN p",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    assert!(
        got.contains(
            "mapFilter((__k, __v) -> __v IS NOT NULL, \
             CAST(CAST(map(), 'Map(String, String)'), 'Map(String, Dynamic)'))"
        ),
        "{got}"
    );
}

#[test]
fn a_passed_through_path_builds_no_values() {
    for q in [
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH p RETURN length(p) AS l",
        "MATCH p = (a:User)-[r:FOLLOWS*1..2]->(b:User) WITH r, p RETURN size(r) AS n, count(*) AS c",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH DISTINCT p RETURN count(*) AS n",
    ] {
        let got = sql(q);
        assert!(!got.contains("_values"), "{q}\n{got}");
    }
    // Read after the WITH: carried.
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH p AS q RETURN nodes(q) AS ns",
        &["as path_node_values"],
    );
}

// ------------------------------------------------------------------ S6d

#[test]
fn a_shortest_path_is_walked_back_through_its_parents() {
    // The search keeps a parent of each node: the least node one level
    // nearer with a relationship to it.
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]->(b:User {user_id: 2})) \
         RETURN p",
        &[
            "CAST(0 AS UInt32) AS depth, start_node.user_id AS parent, CAST(1 AS UInt8) AS new",
            "SELECT f.start_id AS start_id, end_node.user_id AS node, \
             CAST(f.depth + 1 AS UInt32) AS depth, min(f.node) AS parent, CAST(1 AS UInt8) AS new",
            "GROUP BY f.start_id, end_node.user_id, f.depth UNION ALL \
             SELECT start_id, node, depth, parent, CAST(0 AS UInt8) AS new",
            // The search's rows are the walk's levels; it starts at the
            // paths' last nodes.
            "vlp_v1_walk AS ( SELECT start_id, node, depth, node AS end_id, depth AS hop_count, \
             arraySlice([node], 1, 0) AS path_nodes, CAST([], 'Array(String)') AS path_edges, \
             CAST([], 'Array(Map(String, Dynamic))') AS path_node_values, \
             CAST([], 'Array(Map(String, Dynamic))') AS path_rel_values, parent, \
             CAST(1 AS UInt8) AS level, CAST(depth >= 1 AND node IN (SELECT end_node.user_id \
             FROM test_integration.users_test AS end_node WHERE (end_node.user_id = 2)) AS UInt8) \
             AS frontier FROM vlp_v1_bfs WHERE new = 1 UNION ALL",
            // Each step goes back to the node's parent by one relationship.
            "SELECT w.start_id AS start_id, lv.parent AS node, CAST(w.depth - 1 AS UInt32) AS depth, \
             w.end_id AS end_id, w.hop_count AS hop_count, \
             arrayConcat([end_node.user_id], w.path_nodes) AS path_nodes, \
             arrayConcat([toString(rel.follow_id)], w.path_edges) AS path_edges, \
             arrayConcat([map('elementId', CAST(concat('User:', toString(end_node.user_id), '-')",
            "w.parent AS parent, CAST(0 AS UInt8) AS level, CAST(1 AS UInt8) AS frontier, \
             ROW_NUMBER() OVER (PARTITION BY w.start_id, w.end_id ORDER BY toString(rel.follow_id)) \
             AS pick FROM vlp_v1_walk AS w \
             JOIN (SELECT start_id, node, parent FROM vlp_v1_walk WHERE level = 1) AS lv \
             ON lv.start_id = w.start_id AND lv.node = w.node \
             JOIN (SELECT * FROM test_integration.user_follows_test WHERE followed_id IN \
             (SELECT node FROM vlp_v1_walk WHERE frontier = 1 AND depth > 0)) AS rel \
             ON rel.follower_id = lv.parent AND rel.followed_id = w.node \
             JOIN test_integration.users_test AS end_node ON end_node.user_id = w.node \
             WHERE w.frontier = 1 AND w.depth > 0 ) WHERE pick = 1 UNION ALL",
            // The levels are carried while a path has steps left.
            "level, CAST(0 AS UInt8) AS frontier FROM vlp_v1_walk WHERE level = 1 AND start_id IN \
             (SELECT start_id FROM vlp_v1_walk WHERE frontier = 1 AND depth > 1) )",
            // The paths, with their first node.
            "vlp_v1_path AS ( SELECT w.start_id AS start_id, w.end_id AS end_id, \
             w.hop_count AS hop_count, arrayConcat([w.start_id], w.path_nodes) AS path_nodes, \
             w.path_edges AS path_edges, arrayConcat([map('elementId', \
             CAST(concat('User:', toString(start_node.user_id), '-')",
            "w.path_rel_values AS path_rel_values FROM vlp_v1_walk AS w \
             JOIN test_integration.users_test AS start_node ON start_node.user_id = w.start_id \
             WHERE w.frontier = 1 AND w.depth = 0 )",
            "FROM vlp_v1_path AS v1",
        ],
    );
}

#[test]
fn all_shortest_paths_are_walked_through_every_parent() {
    // Every parent, by every relationship from it: a row per path, so the
    // search counts nothing and repeats no row.
    let q = "MATCH p = allShortestPaths((a:User {user_id: 1})-[:FOLLOWS*]->(b:User {user_id: 2})) \
             RETURN nodes(p) AS ns";
    has(
        q,
        &[
            "CAST(0 AS UInt32) AS depth, [start_node.user_id] AS parents, CAST(1 AS UInt8) AS new",
            "CAST(f.depth + 1 AS UInt32) AS depth, groupUniqArray(f.node) AS parents, \
             CAST(1 AS UInt8) AS new",
            "SELECT w.start_id AS start_id, lv.parent AS node, CAST(w.depth - 1 AS UInt32) AS depth",
            "w.parents AS parents, CAST(0 AS UInt8) AS level, CAST(1 AS UInt8) AS frontier \
             FROM vlp_v1_walk AS w \
             JOIN (SELECT start_id, node, parent FROM vlp_v1_walk ARRAY JOIN parents AS parent \
             WHERE level = 1) AS lv ON lv.start_id = w.start_id AND lv.node = w.node \
             JOIN (SELECT * FROM test_integration.user_follows_test WHERE followed_id IN \
             (SELECT node FROM vlp_v1_walk WHERE frontier = 1 AND depth > 0)) AS rel \
             ON rel.follower_id = lv.parent AND rel.followed_id = w.node \
             JOIN test_integration.users_test AS end_node ON end_node.user_id = w.node \
             WHERE w.frontier = 1 AND w.depth > 0 ) UNION ALL",
        ],
    );
    let got = sql(q);
    for absent in ["UInt256", "copy", "min(f.node)", "pick", "path_rel_values"] {
        assert!(!got.contains(absent), "`{absent}` in\n{got}");
    }
}

#[test]
fn a_shortest_path_is_recovered_only_when_read() {
    // Its identity is read: grouping or DISTINCT by the path, or by its
    // relationships.
    for q in [
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) WITH DISTINCT p \
         RETURN count(*) AS c",
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) WITH p, count(*) AS c \
         RETURN c",
        "MATCH p = allShortestPaths((a:User)-[r:FOLLOWS*]->(b:User)) WITH DISTINCT r \
         RETURN count(*) AS c",
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) WITH p WITH DISTINCT p \
         RETURN count(*) AS c",
    ] {
        let got = sql(q);
        assert!(got.contains("vlp_v1_walk"), "{q}\n{got}");
        assert!(!got.contains("_values"), "{q}\n{got}");
    }
    // Its value is read after a WITH.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WITH p RETURN p",
        &[
            "vlp_v1_walk",
            "AS path_rel_values",
            "v1.path_node_values AS \"v1__path_node_values\"",
        ],
    );
    // Passed through, its length read, or grouped by its length: the search
    // alone, and no `path_nodes` exported (it has none).
    for q in [
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WITH p RETURN length(p) AS l",
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) WITH a, p \
         RETURN a.name AS n, length(p) AS l",
        "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) \
         RETURN DISTINCT length(p) AS l",
    ] {
        let got = sql(q);
        assert!(!got.contains("vlp_v1_walk"), "{q}\n{got}");
        assert!(!got.contains("path_nodes"), "{q}\n{got}");
    }
}

#[test]
fn a_shortest_path_condition_walks_the_pairs_it_holds_of() {
    // The pairs whose distance satisfies the conditions are walked; the
    // trails of the others carry their values, their relationships as texts.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) \
         WHERE a <> b AND length(p) > 1 RETURN p",
        &[
            "CAST((start_id, node) IN (SELECT v1.start_id, v1.end_id FROM vlp_v1_near AS v1 \
             WHERE (v1.hop_count > 1)) AS UInt8) AS frontier",
            "vlp_v1_walked AS ( SELECT w.start_id AS start_id",
            "vlp_v1_shortest AS ( SELECT start_id, end_id, hop_count, path_nodes, path_edges, \
             path_node_values, path_rel_values FROM vlp_v1_walked UNION ALL \
             SELECT start_id, end_id, hop_count, path_nodes, path_edges, path_node_values, \
             path_rel_values FROM ( SELECT v1.start_id AS start_id, v1.end_id AS end_id, \
             v1.hop_count AS hop_count, v1.path_nodes AS path_nodes, \
             arrayMap(__x -> concat('', toString(__x)), v1.path_edges) AS path_edges, \
             v1.path_node_values AS path_node_values, v1.path_rel_values AS path_rel_values, \
             ROW_NUMBER() OVER",
            "FROM vlp_v1_shortest AS v1",
        ],
    );
    // A range of `*0..0` has no relationship.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*0..0]->(b:User)) WHERE length(p) % 2 = 0 \
         RETURN p",
        &["CAST([], 'Array(String)') AS path_edges, v1.path_node_values AS path_node_values"],
    );
}

#[test]
fn a_shortest_path_value_follows_the_pattern() {
    // Walked from its right end along the relationships: the reverse of the
    // pattern's order.
    has(
        "MATCH p = shortestPath((a:User)<-[:FOLLOWS*]-(b:User {user_id: 1})) RETURN nodes(p) AS ns",
        &[
            "ON rel.follower_id = lv.parent AND rel.followed_id = w.node",
            "arrayReverse(v1.path_node_values)",
        ],
    );
    // Walked from its left end against them: the pattern's order.
    let q =
        "MATCH p = shortestPath((a:User {user_id: 1})<-[:FOLLOWS*]-(b:User)) RETURN nodes(p) AS ns";
    has(
        q,
        &["ON rel.followed_id = lv.parent AND rel.follower_id = w.node"],
    );
    assert!(!sql(q).contains("arrayReverse"));
}

#[test]
fn an_optional_shortest_path_value_is_null_when_unmatched() {
    has(
        "MATCH (a:User) OPTIONAL MATCH p = shortestPath((a)-[:FOLLOWS*]->(b:User {user_id: 3})) \
         RETURN p",
        &["vlp_v1_walk", "IS NULL), NULL, CAST(arrayConcat([map("],
    );
}

#[test]
fn an_undirected_relationship_reads_its_table_in_both_directions() {
    // §4.6 `Alternatives`: each row once as stored and once reversed (a
    // self-loop only as stored); the stored columns keep their names, so the
    // relationship's identity and properties are its own either way.
    has(
        "MATCH (a:User)-[r:FOLLOWS]-(b:User) RETURN a.name, b.name, r.follow_date",
        &[
            r#"WITH v1_both AS ( SELECT e.follow_date AS "follow_date", e.follow_id AS "follow_id", e.followed_id AS "followed_id", e.follower_id AS "follower_id", e.follower_id AS "__cg_start_0", e.followed_id AS "__cg_end_0" FROM test_integration.user_follows_test AS e UNION ALL SELECT e.follow_date AS "follow_date", e.follow_id AS "follow_id", e.followed_id AS "followed_id", e.follower_id AS "follower_id", e.followed_id AS "__cg_start_0", e.follower_id AS "__cg_end_0" FROM test_integration.user_follows_test AS e WHERE e.follower_id <> e.followed_id )"#,
            "FROM test_integration.users_test AS v0 \
             JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id \
             JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id",
            r#"v1.follow_date AS "r.follow_date""#,
        ],
    );
    // Two undirected relationships of a clause differ by their stored
    // identity, whichever way each is read.
    has(
        "MATCH (a:User)-[r:FOLLOWS]-(b:User)-[s:FOLLOWS]-(c:User) RETURN count(*)",
        &["v1.follow_id <> v3.follow_id"],
    );
}

#[test]
fn an_undirected_relationship_the_schema_has_one_way_is_read_that_way() {
    let q = "MATCH (p:Post)-[:LIKED]-(u:User) RETURN count(*)";
    has(
        q,
        &["FROM test_integration.posts_test AS v0 \
           JOIN test_integration.post_likes_test AS v1 ON v1.post_id = v0.post_id \
           JOIN test_integration.users_test AS v2 ON v1.user_id = v2.user_id"],
    );
    assert!(!sql(q).contains("_both"));
}

#[test]
fn an_undirected_relationship_carries_the_columns_it_is_read_by() {
    // Named, not `*` (ClickHouse leaves ALIAS / MATERIALIZED columns out of
    // it): the mapped columns, and an undeclared property's same-named one.
    has(
        "MATCH (a:User)-[r:FOLLOWS]-(b:User) RETURN r.nope",
        &[
            r#"e.follower_id AS "follower_id", e.nope AS "nope", e.follower_id AS "__cg_start_0""#,
            r#"v1.nope AS "r.nope""#,
        ],
    );
}

/// S7b3: a variable-length relationship whose type has several schemas is a
/// walk of their union, between nodes of any label: `(:N)-[:T*2]-(:Z)` can
/// cross from a T between Ns to one from a Z. The walk follows only the
/// definitions a path between its ends can use, and ties each end's label.
#[test]
fn a_variable_length_relationship_of_a_type_with_several_schemas_walks_each() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: several
graph_schema:
  nodes:
    - { label: N, database: db, table: n, node_id: id, property_mappings: { id: id } }
    - { label: Z, database: db, table: z, node_id: id, property_mappings: { id: id } }
  edges:
    - { type: T, database: db, table: e, from_id: f, to_id: t, from_node: N, to_node: N, property_mappings: {} }
    - { type: T, database: db, table: em, from_id: f, to_id: t, from_node: Z, to_node: N, property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = |q: &str| {
        squash(
            &translate_bound_plan(q, &schema, &ReadOptions::default())
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .sql,
        )
    };
    // Undirected: each definition both ways (a T between Ns read reversed
    // leaves its self-loops out), between nodes of both labels.
    let both = got("MATCH (x:N)-[:T*1..2]-(y:Z) RETURN count(*)");
    for part in [
        "vlp_v1_nodes AS ( SELECT 'N' AS \"__cg_label\", e.id AS \"__cg_id_0\", \
         concat('N', ':', toString(e.id)) AS \"__cg_key\" FROM db.n AS e UNION ALL \
         SELECT 'Z' AS \"__cg_label\"",
        "concat('T:N:N:', toString(tuple(e.f, e.t))) AS \"__cg_key\" FROM db.e AS e UNION ALL",
        "FROM db.e AS e WHERE e.f <> e.t",
        "concat('T:Z:N:', toString(tuple(e.f, e.t))) AS \"__cg_key\" FROM db.em AS e UNION ALL",
        "JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id \
         JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label \
         AND end_node.__cg_id_0 = rel.__cg_end_0 \
         WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, rel.__cg_key)",
        "WHERE hop_count >= 1",
        "v1.end_label = 'Z'",
    ] {
        assert!(both.contains(part), "missing `{part}` in\n{both}");
    }
    // Directed from an N: a T from a Z never follows one, so it is not read.
    let directed = got("MATCH (a:N)-[:T*1..2]->(b:N) RETURN count(*)");
    assert!(directed.contains("FROM db.e AS e"), "{directed}");
    assert!(!directed.contains("db.em"), "{directed}");
    assert!(!directed.contains("'Z'"), "{directed}");
    // The path of none from a Z is a Z.
    let zero = got("MATCH (a:Z)-[:T*0..2]-(b:Z) RETURN count(*)");
    assert!(zero.contains("CAST(0 AS UInt32) AS hop_count"), "{zero}");
    assert!(!zero.contains("WHERE hop_count >="), "{zero}");
}

#[test]
fn an_undirected_relationship_reads_its_table_options_in_both_directions() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: both_options
graph_schema:
  nodes:
    - label: A
      database: db
      table: a
      node_id: id
      property_mappings: { id: id }
  edges:
    - type: S
      database: db
      table: s
      from_id: x
      to_id: y
      from_node: A
      to_node: A
      filter: "kind = 'x'"
      view_parameters: [tenant]
      use_final: true
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let opts = ReadOptions {
        view_parameter_values: Some(HashMap::from([("tenant".to_string(), "t1".to_string())])),
        ..Default::default()
    };
    let got = squash(
        &translate_bound_plan("MATCH (a:A)-[:S]-(b:A) RETURN count(*)", &schema, &opts)
            .unwrap()
            .sql,
    );
    for part in [
        "FROM db.s(tenant = 't1') AS e FINAL WHERE ((e.kind = 'x')) UNION ALL",
        "FROM db.s(tenant = 't1') AS e FINAL WHERE (((e.kind = 'x')) AND e.x <> e.y) )",
        "JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.id",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

#[test]
fn an_undirected_variable_length_relationship_walks_both_directions() {
    has(
        "MATCH (a:User)-[:FOLLOWS*1..2]-(b:User) RETURN count(*)",
        &[
            "v1_both AS ( SELECT e.follow_date",
            "JOIN v1_both AS rel ON start_node.user_id = rel.__cg_start_0 \
             JOIN test_integration.users_test AS end_node ON rel.__cg_end_0 = end_node.user_id",
            // Trail uniqueness by the stored identity.
            "NOT has(vp.path_edges, rel.follow_id)",
        ],
    );
    // Walked from its restricted end: from the right one, the path is the
    // reverse of the walk; from the left one, it is the walk.
    let right = "MATCH p = (a:User)-[:FOLLOWS*1..2]-(b:User {user_id: 1}) RETURN nodes(p) AS ns";
    has(right, &["arrayReverse(v1.path_node_values)"]);
    let left = "MATCH p = (a:User {user_id: 1})-[:FOLLOWS*1..2]-(b:User) RETURN nodes(p) AS ns";
    assert!(!sql(left).contains("arrayReverse"));
}

#[test]
fn an_undirected_shortest_path_searches_and_walks_both_directions() {
    has(
        "MATCH p = shortestPath((a:User {user_id: 1})-[:FOLLOWS*]-(b:User {user_id: 2})) RETURN p",
        &[
            "JOIN v1_both AS rel ON rel.__cg_start_0 = f.node",
            "JOIN (SELECT * FROM v1_both WHERE __cg_end_0 IN",
            "ON rel.__cg_start_0 = lv.parent AND rel.__cg_end_0 = w.node",
        ],
    );
}

#[test]
fn an_undirected_relationship_returned_whole_carries_its_mapped_columns() {
    let got = sql("MATCH ()-[r:FOLLOWS]-() RETURN r LIMIT 25");
    assert!(!got.contains("e.*"), "{got}");
    assert!(
        squash(&got).contains(r#"e.follow_date AS "follow_date""#),
        "{got}"
    );
}

// ------------------------------------------------------------------ S7b1

#[test]
fn a_node_of_several_labels_is_one_relation_of_its_labels_tables() {
    // §4.6 `Alternatives`: one arm per label, each row with its label and
    // id; a property another label declares is NULL in an arm without it.
    has(
        "MATCH (n) WHERE n.user_id = 1 RETURN n.name, labels(n)",
        &[
            r#"WITH v0_labels AS ( SELECT 'Post' AS "__cg_label", e.post_id AS "__cg_id_0", NULL AS "p2_v0_name", NULL AS "p2_v0_user_id" FROM test_integration.posts_test AS e UNION ALL SELECT 'User' AS "__cg_label", e.user_id AS "__cg_id_0", e.full_name AS "p2_v0_name", e.user_id AS "p2_v0_user_id" FROM test_integration.users_test AS e )"#,
            r#"SELECT v0.p2_v0_name AS "n.name", [v0.__cg_label] AS "labels(n)" FROM v0_labels AS v0 WHERE v0.p2_v0_user_id = 1"#,
        ],
    );
    // A label test reads the label column.
    has(
        "MATCH (n) WHERE n:User RETURN n.name",
        &["FROM v0_labels AS v0 WHERE v0.__cg_label = 'User'"],
    );
}

#[test]
fn a_property_no_label_declares_follows_the_undeclared_property_rule() {
    // The same-named column in every arm; NULL in Neo4j-compat mode.
    has(
        "MATCH (n) RETURN n.nickname",
        &[
            r#"e.post_id AS "__cg_id_0", e.nickname AS "p2_v0_nickname__cg0", NULL AS "p2_v0_nickname__cg1" FROM test_integration.posts_test"#,
            r#"e.user_id AS "__cg_id_0", NULL AS "p2_v0_nickname__cg0", e.nickname AS "p2_v0_nickname__cg1" FROM test_integration.users_test"#,
        ],
    );
    has_compat(
        "MATCH (n) RETURN n.nickname",
        &[r#"e.post_id AS "__cg_id_0", NULL AS "p2_v0_nickname""#],
    );
    // Values of two tables: one column each, read as one value of one type
    // or an error (review findings: with no common type ClickHouse makes a
    // `Variant`, whose NULLs `count` counts; a common type converts `Bool`
    // to `UInt8`, `Float32` to `Float64`, `Date` to `DateTime`).
    has(
        "MATCH (n) RETURN count(n.nickname)",
        &[
            "count(CAST(coalesce(v0.p2_v0_nickname__cg0, v0.p2_v0_nickname__cg1), \
             if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_nickname__cg0), ",
            "toTypeName(coalesce(v0.p2_v0_nickname__cg0, v0.p2_v0_nickname__cg1)) NOT LIKE 'Variant(%', \
             toTypeName(coalesce(v0.p2_v0_nickname__cg0, v0.p2_v0_nickname__cg1)), \
             'ClickGraph_property_has_different_types_on_different_labels_or_types')))",
        ],
    );
    // A property one label has is that label's column type: unguarded.
    has(
        "MATCH (n) RETURN n.name",
        &[r#"SELECT v0.p2_v0_name AS "n.name""#],
    );
}

#[test]
fn nodes_of_several_labels_are_equal_by_label_and_id() {
    // A post and a user with the same id are two nodes.
    has(
        "MATCH (a), (b) WHERE a = b RETURN count(*)",
        &["WHERE (v0.__cg_label = v1.__cg_label AND v0.__cg_id_0 = v1.__cg_id_0)"],
    );
    has(
        "MATCH (a:User), (b) WHERE a = b RETURN count(*)",
        &["WHERE ('User' = v1.__cg_label AND v0.user_id = v1.__cg_id_0)"],
    );
    // Counted by both, NULL (not counted) where an OPTIONAL MATCH left it so.
    has(
        "MATCH (n) RETURN count(DISTINCT n)",
        &["count(DISTINCT CASE WHEN v0.__cg_label IS NULL THEN NULL ELSE tuple(v0.__cg_label, v0.__cg_id_0) END)"],
    );
}

#[test]
fn a_relationship_holds_its_label_at_a_node_of_several_labels() {
    // Carried by a WITH, the node is its label and id: the relationship's
    // end is a User here.
    has(
        "MATCH (n) WITH n MATCH (n)-[:FOLLOWS]->(m) RETURN m.name",
        &[
            r#"with_w2 AS ( SELECT v0.__cg_label AS "v1____cg_label", v0.__cg_id_0 AS "v1____cg_id_0" FROM v0_labels AS v0 )"#,
            "FROM with_w2 AS w2 JOIN test_integration.user_follows_test AS v2 ON v2.follower_id = w2.v1____cg_id_0",
            "WHERE w2.v1____cg_label = 'User'",
        ],
    );
    // A written label is one of its labels.
    has(
        "MATCH (n) WITH n MATCH (n:User) RETURN count(*)",
        &["FROM with_w2 AS w2 WHERE w2.v1____cg_label = 'User'"],
    );
}

#[test]
fn a_node_of_several_labels_returned_whole_is_a_node_value() {
    // Its label differs by row: Bolt, the graph output and embedded read it
    // from the value, as for a node of a path.
    let (sql, shape) = shaped("MATCH (n) RETURN n", &social());
    assert!(
        sql.contains(
            "map('elementId', CAST(concat(v0.__cg_label, ':', toString(v0.__cg_id_0), '-'), \
             'Dynamic'), 'labels', CAST([v0.__cg_label], 'Dynamic'), 'properties'"
        ),
        "{sql}"
    );
    assert_eq!(
        kinds(&shape),
        vec![column("n", ResultKind::Graph(GraphType::Node))]
    );
    // DISTINCT by its identity (ClickHouse groups no `Dynamic`).
    let (sql, _) = shaped("MATCH (n) RETURN DISTINCT n", &social());
    assert!(
        sql.contains("GROUP BY v0.__cg_label, v0.__cg_id_0"),
        "{sql}"
    );
    // ... and by its properties, which an ORDER BY can read (review finding:
    // Code 215 without them).
    let (sql, _) = shaped("MATCH (n) RETURN DISTINCT n ORDER BY n.title", &social());
    let key = "v0.p2_v0_title";
    let (group, order) = sql.split_once(" ORDER BY ").unwrap();
    assert!(
        group
            .split(" GROUP BY ")
            .nth(1)
            .is_some_and(|g| g.contains(key))
            && order.starts_with(key),
        "{sql}"
    );
}

/// Labels with table options, and one with a composite id.
fn labels_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_labels
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
      property_mappings: { id: id, name: b_name }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

#[test]
fn a_node_of_several_labels_reads_each_table_with_its_options() {
    let options = ReadOptions {
        view_parameter_values: Some(HashMap::from([("tenant".to_string(), "t1".to_string())])),
        ..ReadOptions::default()
    };
    let sql = squash(
        &translate_bound_plan("MATCH (n) RETURN n.name", &labels_schema(), &options)
            .unwrap()
            .sql,
    );
    for part in [
        r#"SELECT 'A' AS "__cg_label", e.id AS "__cg_id_0", e.a_name AS "p2_v0_name__cg0", NULL AS "p2_v0_name__cg1" FROM db.a(tenant = 't1') AS e WHERE ((e.kind = 'x'))"#,
        r#"SELECT 'B' AS "__cg_label", e.id AS "__cg_id_0", NULL AS "p2_v0_name__cg0", e.b_name AS "p2_v0_name__cg1" FROM db.b AS e FINAL"#,
    ] {
        assert!(sql.contains(part), "missing `{part}` in\n{sql}");
    }
}

#[test]
fn a_node_whose_labels_ids_differ_in_arity_is_not_lowered() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_arity
graph_schema:
  nodes:
    - label: A
      database: db
      table: a
      node_id: id
      property_mappings: { id: id }
    - label: B
      database: db
      table: b
      node_id: [x, y]
      property_mappings: { x: x, y: y }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    match translate_bound_plan(
        "MATCH (n) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    ) {
        Err(e) if e.contains("different arities (S8)") => {}
        other => panic!("expected not lowered, got {:?}", other.map(|t| t.sql)),
    }
}

/// Relationship types of several definitions: `LIKES` to two labels (one
/// with an `edge_id`), `MENTIONS` both ways between two labels, with table
/// options.
fn rels_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_rels
graph_schema:
  nodes:
    - label: Person
      database: db
      table: people
      node_id: id
      property_mappings: { id: id, name: name }
    - label: Post
      database: db
      table: posts
      node_id: id
      property_mappings: { id: id, title: title }
    - label: Company
      database: db
      table: companies
      node_id: id
      property_mappings: { id: id, name: name }
  edges:
    - type: KNOWS
      database: db
      table: knows
      from_id: a
      to_id: b
      from_node: Person
      to_node: Person
      property_mappings: { since: since, w: w }
    - type: LIKES
      database: db
      table: likes
      edge_id: eid
      from_id: pid
      to_id: post
      from_node: Person
      to_node: Post
      view_parameters: [tenant]
      property_mappings: { since: since }
    - type: LIKES
      database: db
      table: likes_co
      from_id: pid
      to_id: cid
      from_node: Person
      to_node: Company
      use_final: true
      property_mappings: { since: liked_on }
    - type: MENTIONS
      database: db
      table: pm
      from_id: post
      to_id: person
      from_node: Post
      to_node: Person
      filter: "visible = 1"
      property_mappings: {}
    - type: MENTIONS
      database: db
      table: mp
      from_id: person
      to_id: post
      from_node: Person
      to_node: Post
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

fn rels_sql(q: &str) -> String {
    let options = ReadOptions {
        view_parameter_values: Some(HashMap::from([("tenant".to_string(), "t1".to_string())])),
        ..ReadOptions::default()
    };
    squash(
        &translate_bound_plan(q, &rels_schema(), &options)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .sql,
    )
}

fn rels_has(q: &str, parts: &[&str]) {
    let got = rels_sql(q);
    for p in parts {
        assert!(got.contains(p), "{q}\nmissing `{p}` in\n{got}");
    }
}

#[test]
fn a_relationship_of_several_types_is_one_relation_of_their_definitions() {
    // One arm per definition, each its table with its options; the identity
    // of the narrower definition is NULL-padded; the rows carry their type
    // and the labels and ids of the nodes they leave and enter.
    rels_has(
        "MATCH (p:Person)-[r:KNOWS|LIKES]->(x) RETURN type(r) AS t, x.id AS i",
        &[
            r#"SELECT 'KNOWS' AS "__cg_type", 'Person' AS "__cg_from_label", 'Person' AS "__cg_to_label", e.a AS "__cg_rid_0", e.b AS "__cg_rid_1", e.a AS "__cg_from_0", e.b AS "__cg_to_0", e.a AS "__cg_start_0", e.b AS "__cg_end_0", 'Person' AS "__cg_start_label", 'Person' AS "__cg_end_label", concat('KNOWS:Person:Person:', toString(tuple(e.a, e.b))) AS "__cg_key" FROM db.knows AS e"#,
            r#"SELECT 'LIKES' AS "__cg_type", 'Person' AS "__cg_from_label", 'Company' AS "__cg_to_label", e.pid AS "__cg_rid_0", e.cid AS "__cg_rid_1", e.pid AS "__cg_from_0", e.cid AS "__cg_to_0", e.pid AS "__cg_start_0", e.cid AS "__cg_end_0", 'Person' AS "__cg_start_label", 'Company' AS "__cg_end_label", concat('LIKES:Person:Company:', toString(tuple(e.pid, e.cid))) AS "__cg_key" FROM db.likes_co AS e FINAL"#,
            r#"SELECT 'LIKES' AS "__cg_type", 'Person' AS "__cg_from_label", 'Post' AS "__cg_to_label", e.eid AS "__cg_rid_0", NULL AS "__cg_rid_1", e.pid AS "__cg_from_0", e.post AS "__cg_to_0", e.pid AS "__cg_start_0", e.post AS "__cg_end_0", 'Person' AS "__cg_start_label", 'Post' AS "__cg_end_label", concat('LIKES:Person:Post:', toString(e.eid)) AS "__cg_key" FROM db.likes(tenant = 't1') AS e"#,
            r#"SELECT v1.__cg_type AS "t""#,
            // A node of several labels holds the label the row enters.
            "FROM db.people AS v0 JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0",
        ],
    );
}

#[test]
fn an_undirected_relationship_of_two_definitions_reads_each_its_way() {
    // `pm` as stored, `mp` reversed: every row leaves the post. A definition
    // between nodes of one label read both ways reads a self-loop once.
    rels_has(
        "MATCH (p:Post)-[r:MENTIONS]-(q:Person) RETURN count(*)",
        &[
            r#"e.post AS "__cg_start_0", e.person AS "__cg_end_0", 'Post' AS "__cg_start_label", 'Person' AS "__cg_end_label", concat('MENTIONS:Post:Person:', toString(tuple(e.post, e.person))) AS "__cg_key" FROM db.pm AS e WHERE ((e.visible = 1))"#,
            r#"e.post AS "__cg_start_0", e.person AS "__cg_end_0", 'Post' AS "__cg_start_label", 'Person' AS "__cg_end_label", concat('MENTIONS:Person:Post:', toString(tuple(e.person, e.post))) AS "__cg_key" FROM db.mp AS e"#,
        ],
    );
    let sql = rels_sql("MATCH (a:Person)-[r:KNOWS|MENTIONS]-(b) RETURN count(*)");
    assert!(
        sql.contains(r#"'Person' AS "__cg_end_label", concat('KNOWS:Person:Person:', toString(tuple(e.a, e.b))) AS "__cg_key" FROM db.knows AS e WHERE e.a <> e.b"#),
        "{sql}"
    );
    assert_eq!(sql.matches("WHERE e.a <> e.b").count(), 1, "{sql}");
}

#[test]
fn a_property_of_several_types_reads_each_definitions_mapping() {
    // Each definition's mapping; NULL where another type declares it; read
    // through the one-type guard where several definitions have a value.
    rels_has(
        "MATCH (p:Person)-[r:KNOWS|LIKES]->(x) RETURN r.since AS s, r.w AS w",
        &[
            // `since` of three definitions: a column each, the two
            // orientations of one sharing it.
            r#"e.since AS "p2_v1_since__cg0", NULL AS "p2_v1_since__cg1", NULL AS "p2_v1_since__cg2", e.w AS "p2_v1_w" FROM db.knows"#,
            r#"NULL AS "p2_v1_since__cg0", e.liked_on AS "p2_v1_since__cg1", NULL AS "p2_v1_since__cg2", NULL AS "p2_v1_w" FROM db.likes_co"#,
            "CAST(coalesce(v1.p2_v1_since__cg0, v1.p2_v1_since__cg1, v1.p2_v1_since__cg2), if(",
            r#"v1.p2_v1_w AS "w""#,
        ],
    );
}

#[test]
fn relationships_of_several_types_are_equal_by_definition_and_identity() {
    // Its definition, then its identity (NULL-safely: past a definition's
    // own arity it is NULL); and relationships of one MATCH differ.
    rels_has(
        "MATCH (a:Person)-[r:KNOWS|LIKES]->(b), (c:Person)-[s:LIKES]->(d:Post) WHERE r = s RETURN count(*)",
        &[
            "(v1.__cg_type <> 'LIKES' OR v1.__cg_from_label <> 'Person' OR v1.__cg_to_label <> 'Post' OR v1.__cg_rid_0 <> v4.eid)",
            "(v1.__cg_type = 'LIKES' AND v1.__cg_from_label = 'Person' AND v1.__cg_to_label = 'Post' AND (v1.__cg_rid_0 = v4.eid OR (v1.__cg_rid_0 IS NULL AND v4.eid IS NULL)))",
        ],
    );
    // Of no definition in common: never the same.
    let sql = rels_sql(
        "MATCH (a:Person)-[r:KNOWS|LIKES]->(b), (c:Post)-[s:MENTIONS]->(d) RETURN count(*)",
    );
    assert!(!sql.contains("<> 'MENTIONS'"), "{sql}");
}

#[test]
fn a_relationship_of_several_types_returned_whole_is_a_relationship_value() {
    let (sql, shape) = shaped(
        "MATCH (a:Person)-[r:KNOWS|LIKES]->(b) RETURN DISTINCT r",
        &rels_schema(),
    );
    for part in [
        // The edge id of a definition that declares one (#1334).
        r#"map('elementId', CAST(concat(v1.__cg_type, ':', CASE WHEN (v1.__cg_type = 'LIKES' AND v1.__cg_from_label = 'Person' AND v1.__cg_to_label = 'Post') THEN concat(concat(CASE WHEN v1.__cg_rid_0 IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString(v1.__cg_rid_0), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':') ELSE '' END, toString(v1.__cg_from_0), '->', toString(v1.__cg_to_0), '-'), 'Dynamic')"#,
        "'startNodeElementId', CAST(concat(v1.__cg_from_label, ':', toString(v1.__cg_from_0), '-'), 'Dynamic')",
        "'type', CAST(v1.__cg_type, 'Dynamic')",
        // Grouped by its definition, identity and properties.
        "GROUP BY v1.__cg_type, v1.__cg_from_label, v1.__cg_to_label, v1.__cg_rid_0, v1.__cg_rid_1,",
    ] {
        assert!(sql.contains(part), "missing `{part}` in\n{sql}");
    }
    assert_eq!(
        kinds(&shape),
        vec![column("r", ResultKind::Graph(GraphType::Relationship))]
    );
}

#[test]
fn a_carried_relationship_of_several_types_ties_its_stored_ends() {
    // Its stored ends' labels and ids: a node of one label holds the row's
    // label, one of several equals it.
    rels_has(
        "MATCH (a)-[r]->(b) WITH r MATCH (x:Person)-[r]->(y) RETURN count(*)",
        &[
            r#"v1.__cg_from_0 AS "v3____cg_from_0", v1.__cg_to_0 AS "v3____cg_to_0""#,
            "JOIN db.people AS v4 ON w4.v3____cg_from_0 = v4.id",
            "JOIN v5_labels AS v5 ON w4.v3____cg_to_label = v5.__cg_label AND w4.v3____cg_to_0 = v5.__cg_id_0",
            "WHERE w4.v3____cg_from_label = 'Person'",
        ],
    );
    // A written type is one of its types.
    rels_has(
        "MATCH (a)-[r]->(b) WITH r MATCH ()-[r:KNOWS|MENTIONS]->() RETURN count(*)",
        &["(w4.v3____cg_type = 'KNOWS' OR w4.v3____cg_type = 'MENTIONS')"],
    );
}

#[test]
fn a_relationship_bound_before_matched_undirected_is_read_both_ways() {
    // The rows so far, each in two orientations: the left end is the stored
    // `from` as stored, the `to` reversed; a self-loop is read once.
    has(
        "MATCH ()-[r:FOLLOWS]->() WITH r MATCH (a)-[r]-(b) RETURN count(*)",
        &[
            r#"v3_turns2 AS ( SELECT 0 AS "__cg_turn" UNION ALL SELECT 1 AS "__cg_turn" )"#,
            "FROM with_w1 AS w1 JOIN v3_turns2 AS v3_t2 ON 1 = 1",
            "JOIN test_integration.users_test AS v4 ON CASE WHEN v3_t2.__cg_turn = 1 THEN w1.v3__followed_id ELSE w1.v3__follower_id END = v4.user_id",
            "JOIN test_integration.users_test AS v5 ON CASE WHEN v3_t2.__cg_turn = 1 THEN w1.v3__follower_id ELSE w1.v3__followed_id END = v5.user_id",
            "WHERE (w1.v3__follower_id <> w1.v3__followed_id OR NOT v3_t2.__cg_turn = 1)",
        ],
    );
    // Ends joined before the orientation are tied in WHERE.
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) MATCH (b)-[r]-(a) RETURN count(*)",
        &[
            "JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id JOIN v1_turns1 AS v1_t1 ON 1 = 1",
            "CASE WHEN v1_t1.__cg_turn = 1 THEN v1.followed_id ELSE v1.follower_id END = v2.user_id",
        ],
    );
}
#[test]
fn a_relationship_read_both_ways_beside_a_path_joins_after_its_orientation() {
    // The path's relation goes first (`path_first`), and each tie follows
    // the relations it reads, inside a `CASE` too (review finding: the tie
    // preceded the orientation join, Code 47).
    let sql = rels_sql(
        "MATCH (a:Person)-[r:KNOWS|LIKES]->(b) WITH r MATCH (x)-[r]-(y)-[:KNOWS*1..2]-(z) RETURN count(*)",
    );
    let select = sql
        .rsplit_once(r#"SELECT count(*) AS "count(*)""#)
        .unwrap()
        .1;
    let turns = select
        .find("JOIN v3_turns5 AS v3_t5")
        .expect("the orientation join");
    let first_read = select.find("v3_t5.__cg_turn").expect("a tie through it");
    assert!(turns < first_read, "{select}");
}

/// #1360: union-walk end keys CAST edge parts to the node column type.
/// Day(Date) nodes; E1 ends DateTime, E2 ends Date (fed by discovery, as in
/// production). The BOTH_START/END key columns must wrap the DateTime side
/// in CAST(... AS Date); node-side keys stay bare.
#[test]
fn union_walk_casts_mixed_type_end_keys_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_mixed_keys
graph_schema:
  nodes:
    - label: Day
      database: db
      table: days
      node_id: d
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: E1
      database: db
      table: e1
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      auto_discover_columns: true
      property_mappings: {}
    - type: E2
      database: db
      table: e2
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "days",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new("d".to_string(), "Date".to_string()),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "nm".to_string(),
                "String".to_string(),
            ),
        ],
    );
    columns.insert(
        "db",
        "e1",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "d1".to_string(),
                "DateTime".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "d2".to_string(),
                "DateTime".to_string(),
            ),
        ],
    );
    columns.insert(
        "db",
        "e2",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "d1".to_string(),
                "Date".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "d2".to_string(),
                "Date".to_string(),
            ),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:Day)-[:E1|E2*1..]->(b:Day)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // #1360: a walk key is compared as TEXT, so both sides are cast to the
    // part's least common type. Day is Date; E1 ends are DateTime and E2 ends
    // are Date, so E1's ends widen to the common DateTime — on BOTH sides.
    //
    // The previous version cast only the edge, down to the node's Date. That
    // widened the Date to midnight and manufactured matches that `=` refuses:
    // `toDate('2020-01-01') = toDateTime('2020-01-01 12:00:00')` is 0, while
    // `toDate(toDateTime(...)) = toDate(...)` is 1.
    assert!(
        got.contains("CAST(e.d1 AS DateTime64(3))"),
        "the DateTime end widens to the common type:\n{got}"
    );
    assert!(
        got.contains("CAST(e.d2 AS DateTime64(3))"),
        "the DateTime end widens to the common type:\n{got}"
    );
    // The node side is cast too, which is what makes the two texts agree.
    assert!(
        got.contains("CAST(e.d AS DateTime64(3))"),
        "the node side casts to the same common type:\n{got}"
    );
    // Never down to the narrower type — that is the bug this fixes.
    assert!(
        !got.contains("CAST(e.d1 AS Date)") && !got.contains("CAST(e.d AS Date)"),
        "nothing is cast down to the node's narrower type:\n{got}"
    );

    // E2's ends are Date against a Date node: the two already agree, so no cast
    // is emitted at all (this retires the blanket `CAST(... AS String)` the
    // first version added to every discovered String/String key).
    let e2_only = lowered(
        "MATCH p = shortestPath((a:Day)-[:E2*1..]->(b:Day)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    assert!(
        !e2_only.contains("CAST(e.d"),
        "Date against Date needs no cast:\n{e2_only}"
    );

    // #1360: Bool node against UInt8 ends. These now DO cast, to the enum's
    // canonical integer: `toString(Bool)` is 'true' and `toString(UInt8)` is
    // '1', so the two could never match before.
    let bool_config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_mixed_keys_bool
graph_schema:
  nodes:
    - label: Flag
      database: db
      table: flags
      node_id: b
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: BE1
      database: db
      table: be1
      from_id: u1
      to_id: u2
      from_node: Flag
      to_node: Flag
      auto_discover_columns: true
      property_mappings: {}
    - type: BE2
      database: db
      table: be2
      from_id: u1
      to_id: u2
      from_node: Flag
      to_node: Flag
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut bool_columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    bool_columns.insert(
        "db",
        "flags",
        vec![crate::graph_catalog::column_info::ColumnInfo::new(
            "b".to_string(),
            "Bool".to_string(),
        )],
    );
    for t in ["be1", "be2"] {
        bool_columns.insert(
            "db",
            t,
            vec![
                crate::graph_catalog::column_info::ColumnInfo::new(
                    "u1".to_string(),
                    "UInt8".to_string(),
                ),
                crate::graph_catalog::column_info::ColumnInfo::new(
                    "u2".to_string(),
                    "UInt8".to_string(),
                ),
            ],
        );
    }
    let bool_schema = bool_config
        .to_graph_schema_with_columns(&bool_columns)
        .unwrap();
    let bool_got = lowered(
        "MATCH p = shortestPath((a:Flag)-[:BE1|BE2*1..]->(b:Flag)) RETURN count(*)",
        &bool_schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    assert!(
        bool_got.contains("__cg_start_key"),
        "union keys are built for the two-type walk:\n{bool_got}"
    );
    // Bool/UInt8 now casts on both sides, to the enum's canonical integer.
    assert!(
        bool_got.contains("CAST(e.b AS Int64)"),
        "the Bool node casts to the common integer:\n{bool_got}"
    );
    assert!(
        bool_got.contains("CAST(e.u1 AS Int64)") && bool_got.contains("CAST(e.u2 AS Int64)"),
        "the UInt8 ends cast to the same common integer:\n{bool_got}"
    );
}

/// #1360: a Nullable key column must not be cast to a non-nullable type —
/// that is `Code: 349 Cannot convert NULL value to non-Nullable type`, the
/// regression the first version introduced.
#[test]
fn union_walk_casts_nullable_key_parts_to_nullable_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_nullable_keys
graph_schema:
  nodes:
    - label: Day
      database: db
      table: days
      node_id: d
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: E1
      database: db
      table: e1
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      auto_discover_columns: true
      property_mappings: {}
    - type: E2
      database: db
      table: e2
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "days",
        vec![crate::graph_catalog::column_info::ColumnInfo::new(
            "d".to_string(),
            "Date".to_string(),
        )],
    );
    // Two types, so the walk goes by keys: a single-type walk joins the
    // columns directly and ClickHouse's own type promotion already agrees
    // with `=`, so there is nothing to fix there.
    // A dangling edge: its end can be NULL.
    for t in ["e1", "e2"] {
        columns.insert(
            "db",
            t,
            vec![
                crate::graph_catalog::column_info::ColumnInfo::new(
                    "d1".to_string(),
                    "Nullable(DateTime)".to_string(),
                ),
                crate::graph_catalog::column_info::ColumnInfo::new(
                    "d2".to_string(),
                    "Nullable(DateTime)".to_string(),
                ),
            ],
        );
    }
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:Day)-[:E1|E2*1..]->(b:Day)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    assert!(
        got.contains("CAST(e.d1 AS Nullable(DateTime64(3)))"),
        "a Nullable end keeps the wrapper:\n{got}"
    );
    assert!(
        got.contains("CAST(e.d AS Nullable(DateTime64(3)))"),
        "both sides share the Nullable target:\n{got}"
    );
    assert!(
        !got.contains("CAST(e.d1 AS DateTime64(3))"),
        "never cast Nullable into non-Nullable (Code 349):\n{got}"
    );
}

/// #1360: a composite id is described per PART, not by one dtype. `[k String,
/// v Date]` against mixed edge parts must cast each part on its own terms.
#[test]
fn union_walk_casts_composite_id_parts_independently_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE1
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE2
      database: db
      table: ce2
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "pairs",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "k".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new("v".to_string(), "Date".to_string()),
        ],
    );
    // Two arms, so the walk goes by keys. Part 0 is String against String on
    // one end and FixedString against String on the other; part 1 is Date
    // against DateTime on one end and Date against Date on the other.
    columns.insert(
        "db",
        "ce1",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv".to_string(),
                "DateTime".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk2".to_string(),
                "FixedString(8)".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv2".to_string(),
                "Date".to_string(),
            ),
        ],
    );
    columns.insert(
        "db",
        "ce2",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv".to_string(),
                "Date".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk2".to_string(),
                "FixedString(8)".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv2".to_string(),
                "Date".to_string(),
            ),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:Pair)-[:CE1|CE2*1..]->(b:Pair)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // Part 0 folds to String because one end is a FixedString. The other ends are
    // already String, so their cast is a no-op -- but it is still emitted,
    // because the node key is one CTE used for both the start and the end
    // side, and every side has to spell it identically.
    assert!(
        got.contains("CAST(e.k AS String)")
            && got.contains("CAST(e.kk AS String)")
            && got.contains("CAST(e.kk2 AS String)"),
        "part 0 agrees on String everywhere:\n{got}"
    );
    // Part 1 is cast: the shared node key and BOTH arms' ends, because the
    // node CTE is shared and one arm widened it (Date against DateTime).
    assert!(
        got.contains("CAST(e.v AS DateTime64(3))"),
        "the node side casts to the folded target:\n{got}"
    );
    assert!(
        got.contains("CAST(e.vv AS DateTime64(3))"),
        "the DateTime end casts:\n{got}"
    );
    // ...including the Date against Date end of the other arm, which must
    // widen too or it could no longer join the widened node key.
    assert!(
        got.contains("CAST(e.vv2 AS DateTime64(3))"),
        "every arm agrees with the shared node key:\n{got}"
    );
    // Nothing is cast down to the narrower Date.
    assert!(
        !got.contains("CAST(e.vv AS Date)")
            && !got.contains("CAST(e.v AS Date)")
            && !got.contains("CAST(e.vv2 AS Date)"),
        "never narrow to the node's own type:\n{got}"
    );
    // And the shortestPath search ties its ends back to their own tables
    // (`v0`/`v2`), so *those* keys have to spell the same folded targets the
    // CTE does -- or the whole walk finds the path and the tie-back drops it.
    // E2E caught this on the union composite walk: the endpoints tied on the
    // raw tuple while every inside key was cast, and the count fell to 0.
    assert!(
        got.contains("CAST(v0.k AS String)") && got.contains("CAST(v0.v AS DateTime64(3))"),
        "the first tie-back spells the folded parts:\n{got}"
    );
    assert!(
        got.contains("CAST(v2.k AS String)") && got.contains("CAST(v2.v AS DateTime64(3))"),
        "the last tie-back spells the folded parts:\n{got}"
    );
}

/// #1360 item 6 (S8d2a): the single-definition composite shortestPath holds
/// the same rule the union walk does — every BFS key one type per part,
/// folded across the node and both of the edge's ends. Before, its keys were
/// raw texts built straight on the tables (`toString(tuple(start_node.k,
/// start_node.v))`), so a DateTime end compared '2020-01-01 00:00:00' against
/// a Date node's '2020-01-01' and the search matched nothing.
#[test]
fn single_definition_bfs_casts_composite_id_parts_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "pairs",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "k".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new("v".to_string(), "Date".to_string()),
        ],
    );
    columns.insert(
        "db",
        "ce1",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv".to_string(),
                "DateTime".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk2".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv2".to_string(),
                "Date".to_string(),
            ),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:Pair)-[:CE*1..]->(b:Pair)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // Part 0 is String on both sides: no cast (the parts already agree).
    assert!(
        !got.contains("CAST(start_node.k AS") && !got.contains("CAST(rel.kk AS"),
        "an agreeing part is not cast:\n{got}"
    );
    // Part 1 folds the node's Date with the edge's DateTime and the other
    // end's Date to one target, and every key spells it -- the seed, the
    // relationship's both ends, and the reached join.
    assert!(
        got.contains("toString(tuple(start_node.k, CAST(start_node.v AS DateTime64(3))))"),
        "the seed spells the folded part:\n{got}"
    );
    assert!(
        got.contains(
            "JOIN db.ce1 AS rel ON \
                      toString(tuple(rel.kk, CAST(rel.vv AS DateTime64(3)))) = f.node"
        ),
        "the step joins the relationship's casted from key:\n{got}"
    );
    assert!(
        got.contains(
            "JOIN db.pairs AS end_node ON \
             toString(tuple(end_node.k, CAST(end_node.v AS DateTime64(3)))) = \
             toString(tuple(rel.kk2, CAST(rel.vv2 AS DateTime64(3))))"
        ),
        "the reached join casts both its sides:\n{got}"
    );
    // Never narrowed to the node's own Date.
    assert!(
        !got.contains("CAST(start_node.v AS Date)") && !got.contains("CAST(rel.vv AS Date)"),
        "never narrow to the node's own type:\n{got}"
    );
}

/// #1360 item 6 (S8d2a), the tie-back: with a condition over the path, the
/// pick joins the ends to their tables as the search spells a node -- the
/// same folded targets the BFS's keys were pre-spelled with, read from the
/// search itself rather than a walk's variable record (which an endpoint of
/// a single-definition walk never had, so this formerly spelled the raw
/// tuple and dropped the found path).
#[test]
fn single_definition_composite_tie_spells_the_folded_parts_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    columns.insert(
        "db",
        "pairs",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "k".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new("v".to_string(), "Date".to_string()),
        ],
    );
    columns.insert(
        "db",
        "ce1",
        vec![
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv".to_string(),
                "DateTime".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "kk2".to_string(),
                "String".to_string(),
            ),
            crate::graph_catalog::column_info::ColumnInfo::new(
                "vv2".to_string(),
                "Date".to_string(),
            ),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:Pair)-[:CE*1..]->(b:Pair)) \
         WHERE a <> b AND length(p) > 1 RETURN length(p) AS l",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // The ends tie as the search keys a node: per part, folded. (The join is
    // spelled the relation's way around: `v1` joins its ends by their keys.)
    assert!(
        got.contains(
            "JOIN vlp_v1_shortest AS v1 ON v1.start_id = \
             toString(tuple(v0.k, CAST(v0.v AS DateTime64(3))))"
        ),
        "the first tie-back spells the folded parts:\n{got}"
    );
    assert!(
        got.contains(
            "JOIN db.pairs AS v2 ON v1.end_id = \
             toString(tuple(v2.k, CAST(v2.v AS DateTime64(3))))"
        ),
        "the last tie-back spells the folded parts:\n{got}"
    );
    // Never the raw tuple, which the searched keys could never match.
    assert!(
        !got.contains("= toString(tuple(v0.k, v0.v))")
            && !got.contains("= toString(tuple(v2.k, v2.v))"),
        "the tie-back never spells the raw tuple:\n{got}"
    );
}

/// The fold is recorded per walk at its start (`build_union_path`), from its
/// own arms, and dropped when it returns — so a walk spells its seed with its
/// own fold, and a later walk over the same label never reads an earlier
/// walk's. Nothing pinned the seed spell; this does, with two walks (each a
/// union of two types) over the same label whose folds disagree on both
/// parts: the first's part 1 widens to `DateTime64(3)` (part 0 needs none),
/// the second's part 0 to `String` (part 1 needs none).
///
/// The walks share the `e` row alias, so an assertion binds to its walk by
/// reading that walk's own segment of the flattened query.
#[test]
fn two_walks_sharing_a_label_spell_their_own_folds_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE1
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE2
      database: db
      table: ce2
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE4
      database: db
      table: ce4
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE5
      database: db
      table: ce5
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    let col = |name: &str, ty: &str| {
        crate::graph_catalog::column_info::ColumnInfo::new(name.to_string(), ty.to_string())
    };
    columns.insert("db", "pairs", vec![col("k", "String"), col("v", "Date")]);
    // Walk one (CE1|CE2): part 1 folds Date/DateTime to DateTime64(3); part 0
    // is String everywhere.
    columns.insert(
        "db",
        "ce1",
        vec![
            col("kk", "String"),
            col("vv", "DateTime"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce2",
        vec![
            col("kk", "String"),
            col("vv", "Date"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    // Walk two (CE4|CE5): part 0 folds to String through the FixedString ends;
    // part 1 is Date everywhere.
    columns.insert(
        "db",
        "ce4",
        vec![
            col("kk", "FixedString(8)"),
            col("vv", "Date"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce5",
        vec![
            col("kk", "String"),
            col("vv", "Date"),
            col("kk2", "FixedString(8)"),
            col("vv2", "Date"),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p1=(a:Pair)-[:CE1|CE2*1..2]->(b:Pair) WITH count(p1) AS c1 \
         MATCH p2=(d:Pair)-[:CE4|CE5*1..2]->(e:Pair) RETURN count(p2)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // Each walk's own fold, read from its own segment: the first walk runs
    // until the second's node CTE, and the second from there on (they share
    // the `e` row alias, so a fragment alone cannot tell them apart).
    let walk1 = match got.find("vlp_v6_nodes") {
        Some(to) => &got[..to],
        None => panic!("the second walk is not in\n{got}"),
    };
    let walk2 = match got.find("vlp_v6_nodes AS (") {
        Some(at) => &got[at..],
        None => panic!("the second walk's CTE is not in\n{got}"),
    };
    assert!(
        walk1.contains("CAST(e.v AS DateTime64(3))") && !walk1.contains("CAST(e.k AS String)"),
        "the first walk folds its part 1 only:\n{got}"
    );
    assert!(
        walk2.contains("CAST(e.k AS String)") && !walk2.contains("CAST(e.v AS"),
        "the second walk folds its part 0 only:\n{got}"
    );
}

/// #1360, review round 3, blocker 1: a filtered or bound END of a by-keys
/// union walk spells its seed rows before `walk_nodes` can record the fold —
/// so the seed (`walk_key`) read the fold from a label map no walk had
/// written yet and spelled the RAW tuple, which its own casted node CTE can
/// never match (the issue's own filtered query returned no rows, and queries
/// main gets right lost them). The fold is now recorded from the walk's own
/// arms before the first restriction is spelled; this pins the seed's key as
/// folded, and the walked CTE's key it must equal.
///
/// Same schema as the walks above: part 0 is String everywhere (never cast),
/// part 1 folds Date/DateTime to `DateTime64(3)`.
#[test]
fn a_filtered_seed_spells_the_folds_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE1
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE2
      database: db
      table: ce2
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    let col = |name: &str, ty: &str| {
        crate::graph_catalog::column_info::ColumnInfo::new(name.to_string(), ty.to_string())
    };
    columns.insert("db", "pairs", vec![col("k", "String"), col("v", "Date")]);
    columns.insert(
        "db",
        "ce1",
        vec![
            col("kk", "String"),
            col("vv", "DateTime"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce2",
        vec![
            col("kk", "String"),
            col("vv", "Date"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH (a:Pair {k: 'a'})-[:CE1|CE2*1..2]->(b:Pair) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // The seed's rows are THIS walk's fold: the key restricted to the node's
    // own table spells part 1 as the fold does.
    assert!(
        got.contains(
            "concat('Pair', ':', \
             toString(tuple(v0.k, CAST(v0.v AS DateTime64(3)))))"
        ),
        "the filtered seed spells the folded key:\n{got}"
    );
    // ...never the raw tuple, which the walked key (cast) can surely differ
    // from -- the fold must exist BEFORE the seed, or nothing joins.
    assert!(
        !got.contains("toString(tuple(v0.k, v0.v))"),
        "the seed never spells the raw tuple:\n{got}"
    );
    assert!(
        got.contains(
            "(SELECT DISTINCT 'Pair' AS \"label\", concat('Pair', ':', toString(tuple(v0.k, \
             CAST(v0.v AS DateTime64(3))))) AS \"id\" FROM db.pairs AS v0 WHERE v0.k = 'a')"
        ),
        "the seed's rows are the node's own table narrowed by its conjuncts:\n{got}"
    );
}

/// #1360, review round 3, blocker 1, the two-walk shape: the folds are keyed
/// by LABEL only so a later walk over the same label used to READ the first
/// walk's fold while spelling its own seed — right here, `{k: 'd'}`'s seed
/// for a walk whose own fold casts part 0 to String would borrow part 1's
/// `DateTime64(3)` from the walk before it, and restrict rows its own node
/// CTE spells raw. Each walk records its folds under its own arms and drops
/// them when it returns, so the second walk spells only its own.
///
/// Same schema as the test above, plus the CE4|CE5 arms of the two-walks
/// test: part 0 folds to String through the FixedString ends, part 1 is Date
/// everywhere (needs no cast).
#[test]
fn a_later_walks_filtered_seed_spells_its_own_fold_1360() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_composite_keys
graph_schema:
  nodes:
    - label: Pair
      database: db
      table: pairs
      node_id: [k, v]
      auto_discover_columns: true
      property_mappings: {}
  edges:
    - type: CE1
      database: db
      table: ce1
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE2
      database: db
      table: ce2
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE4
      database: db
      table: ce4
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
    - type: CE5
      database: db
      table: ce5
      from_id: [kk, vv]
      to_id: [kk2, vv2]
      from_node: Pair
      to_node: Pair
      auto_discover_columns: true
      property_mappings: {}
"#,
    )
    .unwrap();
    let mut columns = crate::graph_catalog::column_info::DiscoveredColumns::default();
    let col = |name: &str, ty: &str| {
        crate::graph_catalog::column_info::ColumnInfo::new(name.to_string(), ty.to_string())
    };
    columns.insert("db", "pairs", vec![col("k", "String"), col("v", "Date")]);
    columns.insert(
        "db",
        "ce1",
        vec![
            col("kk", "String"),
            col("vv", "DateTime"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce2",
        vec![
            col("kk", "String"),
            col("vv", "Date"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce4",
        vec![
            col("kk", "FixedString(8)"),
            col("vv", "Date"),
            col("kk2", "String"),
            col("vv2", "Date"),
        ],
    );
    columns.insert(
        "db",
        "ce5",
        vec![
            col("kk", "String"),
            col("vv", "Date"),
            col("kk2", "FixedString(8)"),
            col("vv2", "Date"),
        ],
    );
    let schema = config.to_graph_schema_with_columns(&columns).unwrap();
    let got = lowered(
        "MATCH p1=(a:Pair)-[:CE1|CE2*1..2]->(b:Pair) WITH count(p1) AS c1 \
         MATCH p2=(d:Pair {k: 'd'})-[:CE4|CE5*1..2]->(e:Pair) RETURN count(p2)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    // The FIRST walk's node CTE widens part 1 (Date against DateTime)...
    assert!(
        got.contains("CAST(e.v AS DateTime64(3))"),
        "the first walk folds its part 1:\n{got}"
    );
    // ...yet the SECOND walk's filtered seed spells its OWN fold: part 0
    // cast to String (its arms' FixedString ends widened it), part 1 raw --
    // never the first walk's fold (a DateTime64 part 1), which it used to
    // read from a label still holding it, and whose widened key its own
    // node CTE spells raw. Pinned row for row:
    assert!(
        got.contains(
            "(SELECT DISTINCT 'Pair' AS \"label\", concat('Pair', ':', \
             toString(tuple(CAST(v5.k AS String), v5.v))) AS \"id\" FROM db.pairs AS v5 \
             WHERE v5.k = 'd')"
        ),
        "the second walk's seed spells its own fold only:\n{got}"
    );
    // The borrow: while spelling its seed, the second walk's PART 1 must
    // read raw. If the first walk's fold were still readable it would spell
    // `CAST(<alias>.v AS DateTime64(3))` in the seed's row select.
    let walk2 = match got.find("vlp_v6_nodes") {
        Some(at) => &got[at..],
        None => panic!("the second walk is not in\n{got}"),
    };
    assert!(
        !walk2.contains("AS DateTime64(3)"),
        "the second walk spells no DateTime64 fold at all:\n{walk2}"
    );
    assert!(
        got.contains("concat('Pair', ':', toString(tuple(CAST(e.k AS String), e.v)))"),
        "the second walk spells its own folded key:\n{got}"
    );
}

/// S7b3: a variable-length relationship of several types walks their union
/// from the restricted end, its nodes keyed by label and id. The first
/// node is restricted by its own conjuncts (a node of several labels too),
/// the relationship's property map holds inside the walk, and its nodes and
/// relationships are values when the path is.
#[test]
fn a_variable_length_relationship_of_several_types_walks_their_union() {
    let got = rels_sql(
        "MATCH p = (a:Person)-[:KNOWS|LIKES*1..3 {since: 2015}]->(b) WHERE a.id = 1 RETURN p",
    );
    for part in [
        // The first node: its label, and its own conjuncts' rows.
        "WHERE (start_node.__cg_label = 'Person') AND ((start_node.__cg_label, \
         start_node.__cg_id_0) IN (SELECT DISTINCT 'Person' AS \"label\", v0.id AS \"id\" \
         FROM db.people AS v0 WHERE v0.id = 1))",
        // Each definition's own spelling of the property map's property.
        "CAST(coalesce(rel.p2_v1_since__cg0, rel.p2_v1_since__cg1, rel.p2_v1_since__cg2)",
        "e.liked_on AS \"p2_v1_since__cg1\"",
        // Values: each node's and relationship's own.
        "[start_node.__cg_value] AS path_node_values",
        "arrayConcat(vp.path_rel_values, [rel.__cg_value]) AS path_rel_values",
        "FROM db.likes_co AS e FINAL",
        "FROM db.likes(tenant = 't1') AS e",
        "arraySlice(v1.path_node_values, 2)",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // A first node of several labels: by label and id (each label the walk
    // visits is one of its: no condition on the label).
    let got = rels_sql(
        "MATCH (a:Person|Post)-[:KNOWS|MENTIONS*1..2]->(b) WHERE a.title = 'x' \
         RETURN count(*)",
    );
    for part in [
        "WHERE ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT \
         v0.__cg_label AS \"label\", v0.__cg_id_0 AS \"id\" FROM v0_labels AS v0",
        "v1.start_label = v0.__cg_label",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // Walked from the right end, its lists are reversed.
    let got = rels_sql("MATCH p = (a)-[:KNOWS|MENTIONS*1..2]->(b:Person) WHERE b.id = 1 RETURN p");
    for part in [
        // From a person, against the direction: each definition reversed.
        "e.b AS \"__cg_start_0\", e.a AS \"__cg_end_0\"",
        "(start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 'Person' AS \"label\", \
         v2.id AS \"id\" FROM db.people AS v2 WHERE v2.id = 1)",
        "arrayReverse(v1.path_rel_values)",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

/// S7b3: relationships of one MATCH are distinct (§4.6.3) by their
/// identities as texts when one is a walk of several definitions; every
/// scan spells a relationship as its definition and identity, so a path's
/// identity is one however its variable-length parts split it.
#[test]
fn a_walk_of_several_types_is_unique_and_spelled_by_definition() {
    for (q, part) in [
        (
            "MATCH (a:Person)-[:KNOWS|LIKES*1..2]->(b)<-[r:KNOWS]-(c) RETURN count(*)",
            "NOT has(v1.path_edges, concat('KNOWS:Person:Person:', toString(tuple(v3.a, v3.b))))",
        ),
        (
            "MATCH (a:Person)-[:KNOWS|LIKES*1..2]->(b)<-[r:KNOWS|LIKES]-(c) RETURN count(*)",
            "NOT has(v1.path_edges, v3.__cg_key)",
        ),
        (
            "MATCH (a:Person)-[:KNOWS|LIKES*1..2]->(b)<-[:KNOWS*1..2]-(c:Person) RETURN count(*)",
            "NOT hasAny(v1.path_edges, arrayMap(__x -> concat('KNOWS:Person:Person:', \
             toString(__x)), v3.path_edges))",
        ),
        (
            "MATCH (a:Person)-[:KNOWS|LIKES*1..2]->(b)<-[*1..2]-(c) RETURN count(*)",
            "NOT hasAny(v1.path_edges, v3.path_edges)",
        ),
    ] {
        let got = rels_sql(q);
        assert!(got.contains(part), "{q}\nmissing `{part}` in\n{got}");
    }
    // No definition in common: nothing to compare.
    let got = rels_sql("MATCH (a:Person)-[:LIKES*1..2]->(b)<-[r:MENTIONS]-(c) RETURN count(*)");
    assert!(!got.contains("NOT has(v1."), "{got}");
    // A path's identity: a union's relationship by its own key column, a
    // single table's and a walk's by type, end labels and identity.
    let got = rels_sql(
        "MATCH p = (a:Person)-[:KNOWS*0..1]->(m)-[r:KNOWS|LIKES]->(n)-[:KNOWS|MENTIONS*0..1]->(b) \
         WITH DISTINCT p RETURN count(*)",
    );
    for part in [
        "arrayMap(__x -> concat('KNOWS:Person:Person:', toString(__x)), v1.path_edges)",
        "[v3.__cg_key]",
        "v5.path_edges",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

/// S7b3: a walk of several types carried through a WITH keeps the labels of
/// its ends.
#[test]
fn a_carried_walk_of_several_types_keeps_its_end_labels() {
    rels_has(
        "MATCH p = (a:Person)-[:KNOWS|LIKES*1..2]->(b) WITH p, b RETURN p, labels(b) AS l",
        &["AS \"v1__start_label\"", "AS \"v1__end_label\""],
    );
}

/// S7b3a review: a walk prunes definitions by the labels the schema gives
/// their ends; a polymorphic edge's rows carry their own (`$any`). S8c: the
/// lowering reads it as a definition per end label, its rows those its label
/// column names, closed to `from_label_values`; a walk of them is the walk of
/// those definitions.
#[test]
fn a_walk_over_a_polymorphic_edge_walks_its_definitions() {
    let schema = poly_schema();
    let got = translate_bound_plan(
        "MATCH (u:User)-[:MEMBER_OF*1..5]->(g:Group) RETURN g.name",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for part in [
        "'User' AS \"__cg_start_label\"",
        "'Group' AS \"__cg_start_label\"",
        "FROM db.memberships AS e\nWHERE ((e.\"member_type\" = 'User'))",
        "FROM db.memberships AS e\nWHERE ((e.\"member_type\" = 'Group'))",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // Closed to its label values: no other label's rows.
    assert!(!got.contains("'Doc' AS \"__cg_start_label\""), "{got}");
}

/// S8c: the schema the polymorphic tests read: a closed polymorphic end
/// (`MEMBER_OF`), a polymorphic table of several types and both ends
/// open (`interactions`), and two labels sharing a table by a label column.
fn poly_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: poly
graph_schema:
  nodes:
    - { label: User, database: db, table: users, node_id: id, property_mappings: { id: id, name: name } }
    - { label: Group, database: db, table: groups, node_id: id, property_mappings: { id: id, name: name } }
    - { label: Doc, database: db, table: objects, node_id: id, label_column: kind, label_value: doc, property_mappings: { id: id, title: title } }
    - { label: Dir, database: db, table: objects, node_id: id, label_column: kind, label_value: dir, property_mappings: { id: id, title: title } }
  edges:
    - polymorphic: true
      database: db
      table: memberships
      from_id: member_id
      to_id: group_id
      from_label_column: member_type
      from_label_values: [User, Group]
      to_node: Group
      type_values: [MEMBER_OF]
      property_mappings: {}
    - polymorphic: true
      database: db
      table: interactions
      from_id: from_id
      to_id: to_id
      type_column: kind
      from_label_column: from_type
      to_label_column: to_type
      type_values: [OWNS, VIEWED]
      property_mappings: { at: ts }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

/// S8c: a relationship of a polymorphic table is the rows its type and end
/// label columns name.
#[test]
fn a_polymorphic_relationship_is_its_discriminated_rows() {
    let schema = poly_schema();
    let got = translate_bound_plan(
        "MATCH (u:User)-[r:VIEWED]->(d:Doc) RETURN u.name, d.title, r.at",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for part in [
        "FROM db.users AS v0",
        "JOIN db.interactions AS v1",
        "(((v1.\"kind\" = 'VIEWED') AND (v1.\"from_type\" = 'User')) AND (v1.\"to_type\" = 'Doc'))",
        "JOIN db.objects AS v2",
        "v2.\"kind\" = 'doc'",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

/// S8c: an end of a polymorphic relationship whose label is not written is
/// each label the schema allows there: every label for an open end, the
/// label values for a closed one.
#[test]
fn a_polymorphic_end_is_each_label_it_allows() {
    let schema = poly_schema();
    let got = translate_bound_plan(
        "MATCH (m)-[:MEMBER_OF]->(g:Group) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for label in ["User", "Group"] {
        assert!(
            got.contains(&format!("\"member_type\" = '{label}'")),
            "{label}: {got}"
        );
    }
    for label in ["Doc", "Dir"] {
        assert!(!got.contains(&format!("'{label}'")), "{label}: {got}");
    }
    let got = translate_bound_plan(
        "MATCH (u:User)-[:OWNS]->(x) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for label in ["User", "Group", "Doc", "Dir"] {
        assert!(
            got.contains(&format!("\"to_type\" = '{label}'")),
            "{label}: {got}"
        );
    }
}

/// S8c: a type both a table of its own and a polymorphic table's between
/// the same labels is two definitions the catalog holds one of: that pair is
/// not lowered (it read the table's rows only); the polymorphic table's other
/// pairs are.
#[test]
fn a_polymorphic_pair_another_definition_joins_is_not_lowered() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: both
graph_schema:
  nodes:
    - { label: User, database: db, table: users, node_id: id, property_mappings: { id: id } }
    - { label: Post, database: db, table: posts, node_id: id, property_mappings: { id: id } }
  edges:
    - { type: LIKES, database: db, table: likes, from_node: User, to_node: User, from_id: a, to_id: b, property_mappings: {} }
    - polymorphic: true
      database: db
      table: interactions
      from_id: from_id
      to_id: to_id
      type_column: kind
      from_label_column: from_type
      to_label_column: to_type
      type_values: [LIKES]
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let err = translate_bound_plan(
        "MATCH (a:User)-[:LIKES]->(b:User) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("not the standard layout (S8)"), "{err}");
    let got = translate_bound_plan(
        "MATCH (a:User)-[:LIKES]->(b:Post) RETURN count(*)",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    assert!(got.contains("\"to_type\" = 'Post'"), "{got}");
}

/// S8c review: an FK edge's rows are its owner's rows. On a table labels
/// share, they are the owner's label's rows (another label's row of the same
/// id is no reference); of two labels on the table, the owner is the end
/// whose columns hold its id.
#[test]
fn an_fk_edge_on_a_shared_table_reads_its_owners_rows() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: shared_fk
graph_schema:
  nodes:
    - { label: A, database: db, table: ent, node_id: id, label_column: kind, label_value: A, property_mappings: { id: id } }
    - { label: B, database: db, table: ent, node_id: id, label_column: kind, label_value: B, property_mappings: { id: id } }
  edges:
    - { type: IN_A, database: db, table: ent, from_node: B, to_node: A, from_id: id, to_id: parent, property_mappings: {} }
    - { type: HOLDS, database: db, table: ent, from_node: A, to_node: B, from_id: parent, to_id: id, property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    for (q, edge) in [
        ("MATCH (b:B)-[:IN_A]->(a:A) RETURN count(*)", "v1"),
        ("MATCH (a:A)-[:HOLDS]->(b:B) RETURN count(*)", "v1"),
    ] {
        let got = translate_bound_plan(q, &schema, &ReadOptions::default())
            .unwrap()
            .sql;
        let owner = format!("{edge}.\"kind\" = 'B'");
        let other = format!("{edge}.\"kind\" = 'A'");
        assert!(got.contains(&owner), "{q}: {got}");
        assert!(!got.contains(&other), "{q}: {got}");
    }
}

/// S8c: labels sharing a table are its rows of their label value; a
/// discriminator the filter grammar cannot hold leaves its table to legacy.
#[test]
fn labels_sharing_a_table_are_its_rows_of_their_value() {
    let schema = poly_schema();
    let got = translate_bound_plan(
        "MATCH (n) WHERE n.title = 'a' RETURN labels(n), n.id",
        &schema,
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for part in ["(e.\"kind\" = 'doc')", "(e.\"kind\" = 'dir')"] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let quoted = GraphSchemaConfig::from_yaml_str(
        r#"
name: quoted
graph_schema:
  nodes:
    - { label: Doc, database: db, table: objects, node_id: id, label_column: kind, label_value: "it's", property_mappings: { id: id } }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let err = translate_bound_plan(
        "MATCH (d:Doc) RETURN d.id",
        &quoted,
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("not the standard layout (S8)"), "{err}");
}

#[test]
fn a_walk_of_an_unknown_type_from_zero_is_the_path_of_none() {
    // The binder drops the type the schema doesn't have: only the path of no
    // relationship can match, and it has no type to check.
    for q in [
        "MATCH (a:Person)-[:NOPE*0..2]->(b) RETURN count(*)",
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:NOPE*0..1]->(b) RETURN a.id, b.id",
    ] {
        let got = rels_sql(q);
        assert!(got.contains("_trails"), "not walked: {q}\n{got}");
        assert!(!got.contains("WHERE false"), "matches nothing: {q}\n{got}");
    }
    // From one hop it matches nothing.
    let got = rels_sql("MATCH (a:Person)-[:NOPE*1..2]->(b) RETURN count(*)");
    assert!(!got.contains("_trails"), "walked: {got}");
}

#[test]
fn a_shortest_path_over_several_types_searches_their_union() {
    // Nodes by their identities as texts, through every definition.
    let got = rels_sql(
        "MATCH p = shortestPath((a:Person)-[:KNOWS|LIKES*]->(b)) WHERE a.id = 1 RETURN length(p)",
    );
    for part in [
        "concat('Person', ':', toString(e.pid)) AS \"__cg_start_key\", \
         concat('Company', ':', toString(e.cid)) AS \"__cg_end_key\" FROM db.likes_co AS e FINAL",
        "SELECT DISTINCT start_node.__cg_key AS start_id, start_node.__cg_key AS node",
        "JOIN v1_rels AS rel ON rel.__cg_start_key = f.node \
         JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = rel.__cg_end_key",
        // The pairs' ends: the nodes of those identities.
        "vlp_v1_ends AS ( SELECT start_node.__cg_label AS start_label, \
         start_node.__cg_id_0 AS start_id, end_node.__cg_label AS end_label, \
         end_node.__cg_id_0 AS end_id, p.hop_count AS hop_count FROM vlp_v1_path AS p \
         JOIN vlp_v1_nodes AS start_node ON start_node.__cg_key = p.start_id \
         JOIN vlp_v1_nodes AS end_node ON end_node.__cg_key = p.end_id )",
        "FROM vlp_v1_ends AS v1",
        "v1.end_label = v2.__cg_label AND v1.end_id = v2.__cg_id_0",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // Every node the walk visits can be its last: nothing to reach first.
    assert!(!got.contains("countIf"), "{got}");
    // A restricted last node bounds the search; every path is walked back.
    let got = rels_sql(
        "MATCH p = allShortestPaths((a:Person)-[:KNOWS|MENTIONS*]-(b:Post {id: 2})) \
         WHERE a.id = 1 RETURN p",
    );
    for part in [
        "HAVING countIf(node IN (SELECT end_node.__cg_key FROM vlp_v1_nodes AS end_node \
         WHERE (end_node.__cg_label = 'Post') AND ((end_node.__cg_label, end_node.__cg_id_0) \
         IN (SELECT DISTINCT 'Post' AS \"label\", v2.id AS \"id\" FROM db.posts AS v2 \
         WHERE v2.id = 2))))",
        "groupUniqArray(f.node) AS parents",
        "arrayConcat([rel.__cg_key], w.path_edges) AS path_edges",
        "arrayConcat([end_node.__cg_value], w.path_node_values) AS path_node_values",
        "JOIN vlp_v1_nodes AS start_node ON start_node.__cg_key = w.start_id",
        "p.path_nodes AS path_nodes, p.path_edges AS path_edges",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // Conditions: the trails of the pairs whose distance fails them, keyed
    // as the search is; an end they read joined by its identity as a text.
    let got = rels_sql(
        "MATCH p = shortestPath((a:Person)-[:KNOWS|LIKES*]->(b:Company)) \
         WHERE length(p) > 1 AND length(p) < b.id RETURN p",
    );
    for part in [
        "JOIN db.companies AS v2 ON concat('Company', ':', toString(v2.id)) = v1.end_id",
        "start_node.__cg_key AS start_key, start_node.__cg_key AS end_key",
        "vp.start_key AS start_key, end_node.__cg_key AS end_key",
        "SELECT start_key AS start_id, end_key AS end_id, hop_count",
        "start_node.__cg_key IN (SELECT start_id FROM (SELECT v1.start_id AS start_id",
        "vlp_v1_shortest",
        "FROM vlp_v1_shortest AS p",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // An end of several labels: its union, by label and id.
    let got = rels_sql(
        "MATCH p = shortestPath((a:Person)-[:KNOWS|LIKES*]->(b:Post|Company)) \
         WHERE length(p) < size(b.title) RETURN length(p)",
    );
    let part = "JOIN v2_labels AS v2 ON concat(v2.__cg_label, ':', toString(v2.__cg_id_0)) \
                = v1.end_id";
    assert!(got.contains(part), "missing `{part}` in\n{got}");
    // OPTIONAL, a condition reads the path for NULL by a column its search's
    // relations have.
    let got = rels_sql(
        "MATCH (a:Person) OPTIONAL MATCH p = shortestPath((a)-[:KNOWS|LIKES*]->(b:Company)) \
         WHERE length(p) > 1 RETURN a.id, length(p)",
    );
    let part = "FROM vlp_v1_near AS v1 JOIN db.companies AS v2 ON concat('Company', ':', \
                toString(v2.id)) = v1.end_id WHERE NOT coalesce((CASE WHEN (v2.id IS NULL OR \
                v1.start_id IS NULL)";
    assert!(got.contains(part), "missing `{part}` in\n{got}");
}

#[test]
fn a_shortest_path_condition_reads_an_end_as_its_label_has_it() {
    // An end a condition reads is joined as its rows are: through its
    // `filter:` and FINAL, one row per node (review of S7b3b: the raw table
    // joined a stale version once more).
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_filtered_end
graph_schema:
  nodes:
    - { label: A, database: db, table: a, node_id: id, filter: "cur = 1", use_final: true,
        property_mappings: { id: id, name: name } }
    - { label: B, database: db, table: b, node_id: id, property_mappings: { id: id } }
  edges:
    - { type: R, database: db, table: r, from_id: s, to_id: d, from_node: A, to_node: B,
        property_mappings: {} }
    - { type: S, database: db, table: s, from_id: s, to_id: d, from_node: B, to_node: A,
        property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = squash(
        &translate_bound_plan(
            "MATCH p = shortestPath((a:A)-[:R|S*]->(b:B)) WHERE length(p) > 2 OR a.name = 'x' \
             RETURN a.id, b.id, length(p)",
            &schema,
            &ReadOptions::default(),
        )
        .map(|t| t.sql)
        .unwrap_or_else(|e| e),
    );
    // `*` reads ALIAS / MATERIALIZED columns too (S11k review).
    let part = "JOIN (SELECT * FROM db.a AS e FINAL WHERE ((e.cur = 1)) SETTINGS \
                asterisk_include_alias_columns = 1, asterisk_include_materialized_columns = 1) \
                AS v0 ON concat('A', ':', toString(v0.id)) = v1.start_id";
    assert!(got.contains(part), "missing `{part}` in\n{got}");
}

/// S7c: UNWIND repeats each row once per element (`ARRAY JOIN`) in a CTE
/// exporting the scope plus the element.
#[test]
fn an_unwind_repeats_each_row_per_element() {
    // The one row: the elements in list order.
    has(
        "UNWIND [1, 2, 3] AS x RETURN x",
        &[
            r#"WITH with_w1 AS ( SELECT __cg_element AS "v0", __cg_position AS "__o0" FROM system.one AS __cg_one ARRAY JOIN [1, 2, 3] AS __cg_element, arrayEnumerate([1, 2, 3]) AS __cg_position )"#,
            r#"SELECT w1.v0 AS "x" FROM with_w1 AS w1 ORDER BY w1.__o0 ASC"#,
        ],
    );
    // Rows in no order: no position (and no order a later clause may use).
    has(
        "MATCH (a:User) UNWIND [1, 2] AS x RETURN a.name, x",
        &[
            r#"SELECT v0.user_id AS "v0__user_id", v0.full_name AS "p2_v0_name", __cg_element AS "v1" FROM test_integration.users_test AS v0 ARRAY JOIN [1, 2] AS __cg_element )"#,
            r#"SELECT w1.p2_v0_name AS "a.name", w1.v1 AS "x" FROM with_w1 AS w1"#,
        ],
    );
    assert!(!sql("MATCH (a:User) UNWIND [1, 2] AS x RETURN a.name, x").contains("ORDER BY"));
    // A LIMIT takes the elements in order: the rows are numbered in any
    // order first, each row's elements following it in list order (S11g).
    has(
        "MATCH (a:User) UNWIND [1, 2] AS x RETURN x LIMIT 1",
        &[
            r#"row_number() OVER () AS "__cg_row" FROM test_integration.users_test AS v0 )"#,
            r#"w1.__cg_row AS "__o0", __cg_position AS "__o1" FROM with_w1 AS w1 ARRAY JOIN [1, 2] AS __cg_element, arrayEnumerate([1, 2]) AS __cg_position )"#,
            "ORDER BY w2.__o0 ASC, w2.__o1 ASC LIMIT 1",
        ],
    );
    // So does a collect(); a count() does not.
    has(
        "MATCH (a:User) WITH a, [1, 2] AS l UNWIND l AS x WITH a, collect(x) AS c RETURN c",
        &["row_number() OVER ()"],
    );
    assert!(
        !sql("MATCH (a:User) WITH a, [1, 2] AS l UNWIND l AS x RETURN count(*) AS n")
            .contains("row_number"),
    );
    // An ORDER BY in between: the order before it does not matter.
    assert!(
        !sql("MATCH (a:User) UNWIND [1, 2] AS x RETURN x ORDER BY x LIMIT 1")
            .contains("row_number")
    );
    // Nor does a MATCH in between, which leaves its rows in no order.
    for q in [
        "MATCH (a:User) UNWIND [1, 2] AS x MATCH (a)-[:FOLLOWS]->(b:User) RETURN x, b.name LIMIT 1",
        "MATCH (a:User) UNWIND [1, 2] AS x OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
         RETURN collect(x) AS c",
    ] {
        assert!(!sql(q).contains("row_number"), "{q}");
    }
    // The numbered rows read twice (an OPTIONAL MATCH's or a subquery's
    // drive, a path's seed, a collected list joined back): ClickHouse
    // numbers them anew on each read, and the reads would disagree in no
    // order. Lowered stable (#1395): numbered in the order of every column.
    for q in [
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a, x LIMIT 3 \
         OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) WHERE b.user_id > x RETURN x, b.name",
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a, x LIMIT 3 \
         WHERE EXISTS { MATCH (a)-[:FOLLOWS]->(b:User) WHERE b.user_id > x } RETURN count(*) AS c",
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a, x LIMIT 3 \
         MATCH (a)-[:FOLLOWS*1..2]->(b:User) RETURN count(*) AS c",
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a.country AS g, collect(x) AS c \
         OPTIONAL MATCH (b:User) WHERE b.user_id = 5 AND size(c) > 0 RETURN count(b) AS m",
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a, x LIMIT 3 \
         RETURN x, [(a)-[:FOLLOWS]->(b:User) WHERE b.user_id > x | b.name] AS ns",
    ] {
        let got = sql(q);
        assert!(
            got.contains("row_number() OVER (ORDER BY v0.user_id ASC)") && !got.contains("OVER ()"),
            "{q}\n{got}"
        );
    }
    // Read twice before the paging, which is the final SELECT's: the rows
    // are the same on each read, only their numbers differ; numbered
    // stable all the same (a CTE that numbers rows is read twice).
    has(
        "MATCH (a:User) UNWIND [1, 2] AS x \
         RETURN x, [(a)-[:FOLLOWS]->(b:User) WHERE b.user_id > x | b.name] AS ns LIMIT 3",
        &["row_number() OVER (ORDER BY v0.user_id ASC)", "LIMIT 3"],
    );
    // Read once, after the paging: lowered.
    has(
        "MATCH (a:User) UNWIND [1, 2] AS x WITH a, x LIMIT 3 \
         MATCH (a)-[:FOLLOWS]->(b:User) RETURN x, b.name",
        &["row_number() OVER ()"],
    );
    // After an aggregation with no grouping item: one row.
    has(
        "MATCH (a:User) WITH collect(a.name) AS ns UNWIND ns AS n RETURN n",
        &[
            "FROM with_w1 AS w1 ARRAY JOIN w1.v1 AS __cg_element, arrayEnumerate(w1.v1) AS __cg_position",
            "ORDER BY w2.__o0 ASC",
        ],
    );
    // A list of lists: each element is a list.
    has(
        "UNWIND [[1, 2], [3]] AS l UNWIND l AS x RETURN x",
        &["ARRAY JOIN w2.v0 AS __cg_element, arrayEnumerate(w2.v0) AS __cg_position"],
    );
    // An unwound value ties a later MATCH.
    has(
        "UNWIND [1, 2] AS i MATCH (a:User {user_id: i}) RETURN a.name",
        &["FROM with_w1 AS w1 CROSS JOIN test_integration.users_test AS v1 WHERE v1.user_id = w1.v0"],
    );
}

/// S7c: rows in an order keep it, each row's elements after it in list
/// order: the rows are numbered first.
#[test]
fn an_unwind_keeps_the_order_of_ordered_rows() {
    has(
        "MATCH (a:User) WITH a ORDER BY a.name UNWIND [1, 2] AS x RETURN a.name, x",
        &[
            r#"row_number() OVER (ORDER BY w1.__o0 ASC) AS "__cg_row" FROM with_w1 AS w1 )"#,
            r#"w2.__cg_row AS "__o0", __cg_position AS "__o1" FROM with_w2 AS w2 ARRAY JOIN [1, 2] AS __cg_element, arrayEnumerate([1, 2]) AS __cg_position )"#,
            "ORDER BY w3.__o0 ASC, w3.__o1 ASC",
        ],
    );
    // `collect` relies on that order: the values in the order of the
    // element's position, a number the lowering made (no numbering of its
    // own).
    let l = sql("UNWIND [3, 1] AS x RETURN collect(x) AS l");
    assert!(
        squash(&l).contains(
            "SELECT arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w1.__o0, w1.v0), \
             NOT isNull(w1.v0)))) AS \"l\" FROM with_w1 AS w1"
        ),
        "{l}"
    );
    assert!(!l.contains("row_number"), "{l}");
    // Rows in no order, numbered for the UNWIND: by the row's number, then
    // the position.
    has(
        "MATCH (a:User) UNWIND [1, 2] AS x RETURN collect(x) AS l",
        &["groupArrayIf((tuple(w2.__o0, w2.__o1), w2.v1), NOT isNull(w2.v1))"],
    );
}

/// S7c: an empty list or NULL gives no rows; a value that is not a list is
/// the list of itself (none when NULL); a value of unknown type must be a
/// list.
#[test]
fn an_unwind_of_what_is_not_a_list() {
    has(
        "UNWIND [] AS x RETURN x",
        &[r#"SELECT NULL AS "x" WHERE false"#],
    );
    has(
        "UNWIND null AS x RETURN x",
        &[r#"SELECT NULL AS "x" WHERE false"#],
    );
    // NULL by the schema: a property of an element that matches nothing.
    has(
        "MATCH (c:Nope) UNWIND c.tags AS x RETURN count(*)",
        &["WHERE false"],
    );
    // An empty list carried by a WITH: no rows.
    has(
        "WITH [] AS l UNWIND l AS x RETURN count(x)",
        &["CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END", "WHERE false"],
    );
    has(
        "UNWIND 5 AS x RETURN x",
        &["ARRAY JOIN if(isNull(5), [], [5]) AS __cg_element"],
    );
    // `isNull()`, not `IS NULL`: `NOT a IS NULL` is `NOT (a IS NULL)`.
    has(
        "MATCH (a:User) UNWIND NOT (a.age > 2) AS b RETURN b",
        &["ARRAY JOIN if(isNull(NOT v0.age > 2), [], [NOT v0.age > 2]) AS __cg_element"],
    );
    // A boolean element shows as true / false.
    has(
        "MATCH (a:User) UNWIND [a.age > 2, a.age IS NULL] AS b RETURN b",
        &[r#"CAST(__cg_element AS Nullable(Bool)) AS "v1""#],
    );
    // `split()` of NULL is NULL: no rows (ClickHouse's is `['']`).
    has(
        "MATCH (a:User) UNWIND split(a.name, ',') AS s RETURN s",
        &["ARRAY JOIN CASE WHEN isNull(v0.full_name) OR isNull(',') THEN [] ELSE"],
    );
    has(
        "MATCH (a:User) UNWIND a.name AS x RETURN x",
        &["ARRAY JOIN arrayConcat(v0.full_name) AS __cg_element"],
    );
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: typed
graph_schema:
  nodes:
    - label: A
      database: db
      table: a
      node_id: id
      property_mappings: { id: id, age: age }
      property_types: { age: integer }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = lowered(
        "MATCH (a:A) UNWIND a.age AS x RETURN x",
        &schema,
        &LowerOptions::default(),
    );
    assert!(
        got.contains("ARRAY JOIN if(isNull(v0.age), [], [v0.age]) AS __cg_element"),
        "{got}"
    );
}

/// S7c: a list of nodes, relationships or paths, and a property of an
/// element, are lists S7e lowers.
#[test]
fn an_unwind_of_graph_values_is_not_lowered_yet() {
    // A walk of several types carries no element tuples (S7e2b).
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS|LIKED*1..2]->(b) UNWIND r AS x RETURN x",
        "list other than",
    );
    // A map of keys not known (a parameter's).
    not_lowered("UNWIND $rows AS m RETURN m.k AS k", "a property of a value");
}

/// The SQL of `q` over `schema` on Databricks (S12).
fn databricks_sql(q: &str, schema: &GraphSchema) -> Result<String, String> {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(schema.clone()));
        translate_bound_plan(q, schema, &ReadOptions::default()).map(|t| t.sql)
    })
}

/// S12a: Spark has no ARRAY JOIN: an UNWIND is a `LATERAL VIEW`, its
/// positions from `posexplode` (the list read once: review), from one row
/// `(SELECT 1)`; a scalar is a list of itself or none.
#[test]
fn databricks_unwind_is_a_lateral_view() {
    let got = squash(&databricks_sql("UNWIND [1, 2] AS x RETURN x", &social()).unwrap());
    for part in [
        "FROM (SELECT 1) AS __cg_one",
        "LATERAL VIEW posexplode(array(1, 2)) AS __cg_position, __cg_element",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let got =
        squash(&databricks_sql("MATCH (u:User) UNWIND [u.name] AS n RETURN n", &social()).unwrap());
    assert!(got.contains("LATERAL VIEW explode("), "{got}");
    assert!(!got.contains("ARRAY JOIN"), "{got}");
}

/// S12a: a node embedded in edge tables on Databricks: its rows are its
/// sources' rows, grouped by id; a property several sources hold is their
/// `coalesce` under a type guard, any of the rows' values not NULL.
#[test]
fn databricks_embedded_nodes() {
    let got =
        squash(&databricks_sql("MATCH (i:IP) RETURN i.ip AS ip", &embedded_schema()).unwrap());
    assert!(
        got.contains("WHERE NOT isnull(e.ip) GROUP BY e.ip"),
        "{got}"
    );
    let got = squash(
        &databricks_sql("MATCH (a:Airport) RETURN a.city AS c", &embedded_schema()).unwrap(),
    );
    assert!(got.contains("any_value("), "{got}");
    assert!(got.contains(", true)"), "{got}");
    assert!(got.contains("raise_error("), "{got}");
}

/// #1426: a node embedded in several sources takes any of its sources'
/// non-NULL values: ClickHouse `any` keeps a `Variant`'s NULL (`NULL UNION
/// ALL` an `Array` column is a `Variant`), so a source without the property
/// could win; a group of NULLs only stays NULL (`-OrNull`: `anyIf` of a
/// `Nullable(Tuple)` is its default, review).
#[test]
fn embedded_property_skips_variant_nulls() {
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Airport) RETURN a.city AS c",
            &embedded_schema(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    assert!(got.contains("anyIfOrNull("), "{got}");
    assert!(got.contains(", NOT isNull("), "{got}");
    assert!(!got.contains(" any("), "{got}");
}

/// #531: the rows of one id of an embedded node holding several values of a
/// property are no node's. By default any of the values is read (main's
/// SQL); checked (`check_node_properties`), they are an error naming the
/// label and the property, counted by DISTINCT's keys (up to two hashes on
/// ClickHouse, their `min` and `max` on Spark). The id is read as before.
#[test]
fn embedded_property_of_several_values_is_an_error_when_checked() {
    let message = "ClickGraph: rows of one :Airport node hold several values of its property city";
    let checked = ReadOptions {
        check_node_properties: Some(true),
        ..ReadOptions::default()
    };
    let read = |q: &str, options: &ReadOptions| {
        squash(
            &translate_bound_plan(q, &embedded_schema(), options)
                .unwrap()
                .sql,
        )
    };
    let unchecked = read(
        "MATCH (a:Airport) RETURN a.city AS c",
        &ReadOptions::default(),
    );
    assert!(!unchecked.contains("throwIf("), "{unchecked}");
    assert!(unchecked.contains("anyIfOrNull("), "{unchecked}");
    let got = read("MATCH (a:Airport) RETURN a.city AS c", &checked);
    assert!(got.contains("arrayElement([anyIfOrNull("), "{got}");
    assert!(
        got.contains(
            "1 + throwIf(uniqUpToIf(1)(cityHash64(tuple(multiIf(replaceRegexpOne(dynamicType("
        ),
        "{got}"
    );
    assert!(got.contains(message), "{got}");
    // The id is the group's key, not a property's aggregate.
    let got = read("MATCH (i:IP) RETURN i.ip AS ip", &checked);
    assert!(!got.contains("throwIf(uniqUpToIf"), "{got}");
    // Spark: the least and greatest of DISTINCT's keys of the non-NULL
    // values (a VARIANT has no DISTINCT of its own).
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let schema = embedded_schema();
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(schema.clone()));
        translate_bound_plan("MATCH (a:Airport) RETURN a.city AS c", &schema, &checked)
            .map(|t| squash(&t.sql))
    })
    .unwrap();
    assert!(got.contains("CASE WHEN min(CASE WHEN "), "{got}");
    assert!(got.contains(") <> max(CASE WHEN "), "{got}");
    assert!(got.contains("schema_of_variant("), "{got}");
    assert!(got.contains(&format!("raise_error('{message}")), "{got}");
}

/// S12a review: Spark `sequence` is never empty (`sequence(1, 0)` is
/// `[1, 0]`): `range` is empty when its step leads away from its end, each
/// argument bound once;
/// an empty `split` delimiter stays empty (characters, no trailing "").
#[test]
fn databricks_range_and_split() {
    let got = databricks_sql("RETURN range(5, 1) AS r", &social()).unwrap();
    assert!(
        got.contains(
            "element_at(transform(array(named_struct('s', 5, 'e', 1, 't', 1)), __cg_r -> \
             CASE WHEN (__cg_r.t > 0 AND __cg_r.s > __cg_r.e) OR (__cg_r.t < 0 AND __cg_r.s < \
             __cg_r.e) THEN array() ELSE sequence(__cg_r.s, __cg_r.e, __cg_r.t) END), 1)"
        ),
        "{got}"
    );
    let got = databricks_sql("RETURN split('abc', '') AS s", &social()).unwrap();
    assert!(
        got.contains("split('abc', CASE WHEN '' = '' THEN '' ELSE concat("),
        "{got}"
    );
}

/// S12a: `size()` of a list is the dialect's list size (Spark `length` is
/// of a string only).
#[test]
fn databricks_size_of_a_list() {
    let got = databricks_sql(
        "MATCH (u:User) RETURN size(split(u.email, '@')) AS n",
        &social(),
    )
    .unwrap();
    assert!(got.contains("array_size(split("), "{got}");
    has(
        "MATCH (u:User) RETURN size(split(u.email, '@')) AS n",
        &["length(splitByChar('@', v0.email_address))"],
    );
}

/// S12b: Spark spells comprehensions with `filter` / `transform`, an ordered
/// `collect()` by sorting (key, value) structs on the key alone, a list's
/// node as a struct of positional fields, a pattern comprehension that
/// matches nothing as `[]` (Spark's unmatched LEFT JOIN column is NULL), and
/// a map as a `MAP<STRING, VARIANT>`.
#[test]
fn databricks_lists() {
    let got = squash(
        &databricks_sql(
            "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 10] AS l",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains("transform(filter(array(1, 2, 3), v1 -> v1 > 1), v0 -> v0 * 10)"),
        "{got}"
    );
    let got = squash(
        &databricks_sql(
            "MATCH (u:User) WITH u ORDER BY u.user_id RETURN collect(u.name) AS names",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains(
            "transform(array_sort(collect_list(CASE WHEN NOT isnull(w2.p2_v1_name) THEN \
             named_struct('k', w2.__cg_row, 'v', w2.p2_v1_name) END), (__cg_l, __cg_r) -> \
             CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 ELSE 0 END), \
             __cg_t -> __cg_t.v)"
        ),
        "{got}"
    );
    let got = squash(
        &databricks_sql(
            "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.user_id, [x IN collect(f) | x.name] AS n",
            &social(),
        )
        .unwrap(),
    );
    for part in [
        "collect_list(CASE WHEN NOT isnull(v2.user_id) THEN named_struct('_1', v2.user_id, '_2', v2.age,",
        "v3 -> (v3)._7)",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let got = squash(
        &databricks_sql(
            "MATCH (u:User) RETURN u.user_id, [(u)-[:FOLLOWS]->(f:User) | f.name] AS names",
            &social(),
        )
        .unwrap(),
    );
    for part in [
        "transform(collect_list(named_struct('v', v2.full_name)), __cg_t -> __cg_t.v)",
        "coalesce(s1.__value, array()) AS `names`",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let got = squash(
        &databricks_sql(
            "RETURN {b: 'x', a: [1]} AS m, keys({b: 1, a: 2}) AS k",
            &social(),
        )
        .unwrap(),
    );
    for part in [
        "to_variant_object(map('a', CAST(array(1) AS VARIANT), 'b', CAST('x' AS VARIANT))) AS `m`",
        "map_keys(CAST(to_variant_object(map('a', CAST(2 AS VARIANT), 'b', CAST(1 AS VARIANT))) \
         AS MAP<STRING, VARIANT>)) AS `k`",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // Spark's IN takes a parenthesized list only (review); past a list's
    // end is NULL, not an error under ANSI mode.
    let got = squash(
        &databricks_sql(
            "MATCH (u:User) WHERE NOT 'x' IN [(u)-[:FOLLOWS]->(f:User) | f.name] \
             RETURN head([(u)-[:FOLLOWS]->(f:User) | f.name]) AS h, [1, 2][5] AS e",
            &social(),
        )
        .unwrap(),
    );
    for part in [
        "WHEN array_contains(coalesce(s1.__value, array()), 'x') THEN true",
        "try_element_at(coalesce(s2.__value, array()), 1) AS `h`",
        "try_element_at(array(1, 2), 6) AS `e`",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // A map value is made a VARIANT by `to_variant_object`; an empty list's
    // elements are of no type, which no VARIANT holds.
    let got = squash(&databricks_sql("RETURN {a: {b: 1}, c: [{d: 2}]} AS m", &social()).unwrap());
    assert!(
        got.contains(
            "to_variant_object(map('a', CAST(to_variant_object(map('b', CAST(1 AS VARIANT))) \
             AS VARIANT), 'c', CAST(array(to_variant_object(map('d', CAST(2 AS VARIANT)))) \
             AS VARIANT)))"
        ),
        "{got}"
    );
    let err = databricks_sql("RETURN {l: [[]]} AS m", &social()).unwrap_err();
    assert!(err.contains("a map holding an empty list"), "{err}");
    // Spark groups and de-duplicates no VARIANT: a map by its key (S12d).
    for q in [
        "MATCH (u:User) RETURN DISTINCT {c: u.country} AS m",
        "MATCH (u:User) RETURN {c: u.country} AS m, count(*) AS n",
    ] {
        let got = squash(&databricks_sql(q, &social()).unwrap());
        assert!(got.contains("GROUP BY CASE WHEN CAST("), "{q}: {got}");
        assert!(
            got.contains("concat('m:', to_json(transform_values("),
            "{q}: {got}"
        );
    }
}

/// S12e: on Spark a shortestPath search's steps are one parenthesized
/// recursive term (Spark takes one), with no early stop (no recursive
/// reference in an aggregate); a walk over several types is lowered.
/// S12f: `allShortestPaths` and a shortest path's nodes keep the paths in
/// the search (no grouped step, no second recursion reading it): a row per
/// shortest path, one per pair picked by its nodes from the last.
#[test]
fn databricks_shortest_path_search() {
    let got = squash(
        &databricks_sql(
            "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) \
             WHERE a.user_id = 1 AND b.user_id = 9 RETURN length(p) AS l",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains("UNION ALL (SELECT DISTINCT f.start_id AS start_id"),
        "{got}"
    );
    assert!(
        got.contains("WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1)) )"),
        "{got}"
    );
    assert!(!got.contains("HAVING"), "{got}");
    // A NULL id starts no search: Spark's `(a, b) NOT IN` of a row holding
    // NULL rejects every pair (review: 0 rows for 98).
    assert!(
        got.contains("WHERE start_node.user_id IS NOT NULL AND"),
        "{got}"
    );
    // Counted: a row per path, without their columns.
    let got = squash(
        &databricks_sql(
            "MATCH p = allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) \
             WHERE a.user_id = 1 AND b.user_id = 9 RETURN count(*) AS n",
            &social(),
        )
        .unwrap(),
    );
    assert!(!got.contains("path_nodes"), "{got}");
    assert!(!got.contains("GROUP BY f.start_id"), "{got}");
    assert!(
        got.contains("SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs"),
        "{got}"
    );
    // The carried nodes reached, once each.
    assert!(
        got.contains("SELECT DISTINCT start_id, node, depth FROM vlp_v1_bfs"),
        "{got}"
    );
    // A path's nodes: the paths kept with their columns, one per pair
    // picked from the last.
    let got = squash(
        &databricks_sql(
            "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) \
             WHERE a.user_id = 1 AND b.user_id = 9 RETURN nodes(p) AS ns",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains("concat(f.path_nodes, array(end_node.user_id)) AS path_nodes"),
        "{got}"
    );
    assert!(!got.contains("GROUP BY f.start_id"), "{got}");
    assert!(
        got.contains(
            "ROW_NUMBER() OVER (PARTITION BY start_id, node \
             ORDER BY reverse(path_nodes), reverse(path_edges)) AS pick"
        ),
        "{got}"
    );
    databricks_sql(
        "MATCH (a:User)-[:FOLLOWS|AUTHORED*1..2]->(x) WHERE a.user_id = 1 RETURN count(*) AS n",
        &social(),
    )
    .unwrap();
}

/// #1427: a RETURN's ORDER BY reads a returned path's length (the item's
/// elements are the path's); returned with DISTINCT or an aggregation, as
/// any of its group's (the path, the group's key, determines it).
#[test]
fn order_by_length_of_a_returned_path() {
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN p ORDER BY length(p)",
        &["ORDER BY v1.hop_count ASC"],
    );
    // A fixed path's length is a constant: no order to apply.
    sql("MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN p ORDER BY length(p) DESC");
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN DISTINCT p ORDER BY length(p)",
        &["ORDER BY anyIfOrNull(v1.hop_count, NOT isNull(v1.hop_count)) ASC"],
    );
    // Two walks: their lengths' sum is one aggregate, one row's (review).
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User)-[:FOLLOWS*1..2]->(c:User) \
         RETURN DISTINCT p ORDER BY length(p)",
        &["ORDER BY anyIfOrNull(v1.hop_count + v3.hop_count, NOT isNull(v1.hop_count + v3.hop_count)) ASC"],
    );
    // Its nodes are not the group's columns: refused, not an SQL error.
    for q in [
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN DISTINCT p \
         ORDER BY [n IN nodes(p) | n.name]",
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN p, count(*) AS c \
         ORDER BY [n IN nodes(p) | n.name]",
    ] {
        assert!(
            translate_bound_plan(q, &social(), &ReadOptions::default()).is_err(),
            "{q}"
        );
    }
    sql("MATCH p = (a:User)-[:FOLLOWS]->(b:User) WITH p RETURN p ORDER BY length(p)");
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH p RETURN p ORDER BY length(p)",
        &["ORDER BY w2.v1__hop_count ASC"],
    );
}

/// UNION (S7d): each arm is a CTE of its RETURN's columns in the UNION's
/// order (matched by name); a CTE is the UNION ALL of the arms, each value
/// read as a value of any type; UNION groups rows by Cypher's equality.
#[test]
fn a_union_reads_each_arm_from_its_own_cte() {
    has(
        "MATCH (u:User) RETURN u.name AS n, u.user_id AS i \
         UNION ALL MATCH (p:Post) RETURN p.post_id AS i, p.title AS n",
        &[
            r#"WITH with_w1 AS ( SELECT v0.full_name AS "__cg_w1_c0", v0.user_id AS "__cg_w1_c1" FROM test_integration.users_test AS v0 )"#,
            // The second arm's columns in the UNION's order.
            r#"with_w2 AS ( SELECT v3.post_title AS "__cg_w2_c0", v3.post_id AS "__cg_w2_c1" FROM"#,
            r#"with_w3 AS ( SELECT CAST(w1.__cg_w1_c0, 'Dynamic') AS "__cg_c0", CAST(w1.__cg_w1_c1, 'Dynamic') AS "__cg_c1" FROM with_w1 AS w1 UNION ALL SELECT CAST(w2.__cg_w2_c0, 'Dynamic') AS "__cg_c0", CAST(w2.__cg_w2_c1, 'Dynamic') AS "__cg_c1" FROM with_w2 AS w2 )"#,
            r#"SELECT w3.__cg_c0 AS "n", w3.__cg_c1 AS "i" FROM with_w3 AS w3"#,
        ],
    );
    // UNION: one row per group of equal rows, in no order.
    let got = squash(&sql("RETURN 1 AS a UNION RETURN 1.0 AS a"));
    assert!(
        got.contains(
            r#"SELECT any(w3.__cg_c0) AS "a" FROM with_w3 AS w3 GROUP BY tuple(multiIf(replaceRegexpOne(dynamicType(CAST(w3.__cg_c0, 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1') = 'None', '', "#
        ) && got.ends_with(r#"toString(w3.__cg_c0)))"#),
        "{got}"
    );
    // Three arms; the arms' WITH segments continue the CTE names.
    has(
        "MATCH (u:User) WITH u.name AS n RETURN n UNION ALL RETURN 'x' AS n \
         UNION ALL UNWIND ['y'] AS n RETURN n",
        &[
            "with_w1 AS ( SELECT v0.full_name AS \"v1\"",
            "with_w2 AS ( SELECT w1.v1 AS \"__cg_w2_c0\" FROM with_w1 AS w1 )",
            "with_w3 AS ( SELECT 'x' AS \"__cg_w3_c0\" )",
            "with_w5 AS ( SELECT w4.v4 AS \"__cg_w5_c0\"",
            "FROM with_w5 AS w5 ) SELECT w6.__cg_c0 AS \"n\" FROM with_w6 AS w6",
        ],
    );
}

/// An arm's ORDER BY, SKIP and LIMIT are its own (Neo4j: an ORDER BY /
/// LIMIT after the last arm is that arm's); UNION ALL keeps each arm's rows
/// in turn, in their order.
#[test]
fn a_union_arm_orders_and_pages_its_own_rows() {
    has(
        "MATCH (u:User) RETURN u.name AS n \
         UNION ALL MATCH (u:User) RETURN u.name AS n ORDER BY n DESC LIMIT 2",
        &[
            // The first arm has no order.
            r#"with_w1 AS ( SELECT v0.full_name AS "__cg_w1_c0" FROM test_integration.users_test AS v0 )"#,
            r#"with_w2 AS ( SELECT v2.full_name AS "__cg_w2_c0", v2.full_name AS "__cg_w2_o0" FROM test_integration.users_test AS v2 ORDER BY v2.full_name DESC NULLS FIRST LIMIT 2)"#,
            // Each arm's rows numbered in its order, after its LIMIT.
            r#"SELECT CAST(w1.__cg_w1_c0, 'Dynamic') AS "__cg_c0", 0 AS "__cg_arm", 0 AS "__cg_row" FROM with_w1 AS w1 UNION ALL SELECT CAST(w2.__cg_w2_c0, 'Dynamic') AS "__cg_c0", 1 AS "__cg_arm", row_number() OVER (ORDER BY w2.__cg_w2_o0 DESC NULLS FIRST) AS "__cg_row" FROM with_w2 AS w2"#,
            r#"ORDER BY w3.__cg_arm ASC, w3.__cg_row ASC"#,
        ],
    );
    // An ordered arm with no SKIP / LIMIT needs no ORDER BY in its CTE.
    let got = squash(&sql(
        "RETURN 1 AS n UNION ALL MATCH (u:User) RETURN u.user_id AS n ORDER BY n",
    ));
    assert!(
        got.contains(r#"with_w2 AS ( SELECT v1.user_id AS "__cg_w2_c0", v1.user_id AS "__cg_w2_o0" FROM test_integration.users_test AS v1 )"#),
        "{got}"
    );
    // UNION has no order: the arms export no sort key.
    let got = squash(&sql(
        "RETURN 1 AS n UNION MATCH (u:User) RETURN u.user_id AS n ORDER BY n LIMIT 1",
    ));
    assert!(
        got.contains(r#"with_w2 AS ( SELECT v1.user_id AS "__cg_w2_c0" FROM test_integration.users_test AS v1 ORDER BY v1.user_id ASC LIMIT 1)"#)
            && !got.contains("__cg_arm"),
        "{got}"
    );
}

/// Nodes and relationships keep their columns (one label or type in every
/// arm), and the result shape is the first arm's.
#[test]
fn a_union_of_elements() {
    let (sql, shape) = shaped(
        "MATCH (a:User) RETURN a, a.name AS n UNION MATCH (b:User)-[:FOLLOWS]->(:User) \
         RETURN b.name AS n, b AS a",
        &social(),
    );
    assert_eq!(
        kinds(&shape),
        vec![column("a", user()), column("n", ResultKind::Value)]
    );
    assert!(
        sql.contains(r#"SELECT any(w3.__cg_c0) AS "a.age","#)
            // The node's columns keep their types; the value is any type.
            && sql.contains(r#"SELECT w1.__cg_w1_c0 AS "__cg_c0","#)
            && sql.contains(r#"CAST(w2.__cg_w2_c8, 'Dynamic') AS "__cg_c8", w2.__cg_w2_k0 AS "__cg_k0" FROM with_w2 AS w2"#),
        "{sql}"
    );
    let (_, shape) = shaped(
        "MATCH (:T)-[c:C]->(:T) RETURN c UNION ALL MATCH (:T)-[c:C]->(:T) RETURN c",
        &shapes_schema(),
    );
    assert_eq!(
        shape[0].columns[0],
        ("from_id_1".to_string(), "c.from_id_1".to_string())
    );
}

/// A map literal's entries are in key order: maps of the same entries are
/// equal (DISTINCT, grouping, UNION), whatever order they were written in.
#[test]
fn a_map_literal_is_in_key_order() {
    // Values keep their types (S7e3).
    has(
        "RETURN {b: 2, a: 1} AS m",
        &["map('a', CAST(1, 'Dynamic'), 'b', CAST(2, 'Dynamic')) AS \"m\""],
    );
}

/// Every arm's RETURN reads the properties of the elements it returns whole
/// (here from an OPTIONAL MATCH's CTE).
#[test]
fn a_union_arm_reads_what_it_returns() {
    has(
        "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN f AS x \
         UNION MATCH (x:User) RETURN x",
        &[r#"o1.p2_v2_name AS "__cg_w2_c5""#],
    );
}

#[test]
fn a_union_of_columns_read_differently_is_not_lowered() {
    not_lowered(
        "MATCH (a:User) RETURN a AS x UNION MATCH ()-[r:FOLLOWS]->() RETURN r AS x",
        "UNION column `x` of a different kind or label in another arm",
    );
    not_lowered(
        "MATCH (a:User) RETURN a AS x UNION ALL RETURN 1 AS x",
        "UNION column `x` of a different kind or label in another arm",
    );
}

/// UNION groups by each returned element's identity too: rows equal in
/// every returned column can be different relationships (the `edge_id` is
/// not a property).
#[test]
fn a_union_keeps_elements_apart_by_identity() {
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN k, 1 AS one UNION MATCH (:U)-[k:K]->(:U) RETURN k, 1 AS one",
        &shapes_schema(),
    );
    assert!(
        sql.contains(r#"'-') AS "__cg_w1_c3", 1 AS "__cg_w1_c4", v1.kid AS "__cg_w1_k0" FROM"#)
            && sql.contains(r#"w2.__cg_w2_k0 AS "__cg_k0" FROM with_w2 AS w2"#)
            // Grouped by every column's key and the identity.
            && sql.contains(r#"SELECT any(w3.__cg_c0) AS "k.from_id","#)
            && sql.contains(r#"GROUP BY tuple(multiIf(replaceRegexpOne(dynamicType(CAST(w3.__cg_c0, "#)
            && sql.ends_with(r#"toString(w3.__cg_c4))), w3.__cg_k0"#),
        "{sql}"
    );
    // A relationship of several types is a graph value, whose `elementId`
    // is its ends': its identity is exported too.
    let (sql, _) = shaped(
        "MATCH (:User)-[r:FOLLOWS|LIKED]->() RETURN r \
         UNION MATCH (:User)-[r:LIKED|FOLLOWS]->() WHERE r.follow_date IS NULL RETURN r",
        &social(),
    );
    assert!(
        sql.contains(r#"v1.__cg_rid_0 AS "__cg_w3_k3""#)
            && sql.ends_with(
                "toString(w7.__cg_c0))), w7.__cg_k0, w7.__cg_k1, w7.__cg_k2, w7.__cg_k3"
            ),
        "{sql}"
    );
    // UNION ALL needs no identity.
    let (sql, _) = shaped(
        "MATCH (:U)-[k:K]->(:U) RETURN k UNION ALL MATCH (:U)-[k:K]->(:U) RETURN k",
        &shapes_schema(),
    );
    assert!(!sql.contains("__cg_k0"), "{sql}");
}

/// S12d: on Spark a UNION's column of values is a VARIANT, grouped by a
/// key of its kind and text (Spark groups no VARIANT): `1` = `1.0`, not
/// `true` or `'1'`.
#[test]
fn databricks_union() {
    let got = squash(&databricks_sql("RETURN 1 AS x UNION RETURN 1.0 AS x", &social()).unwrap());
    for part in [
        "SELECT CAST(w1.__cg_w1_c0 AS VARIANT) AS `__cg_c0` FROM with_w1 AS w1 UNION ALL",
        "SELECT any_value(w3.__cg_c0) AS `x` FROM with_w3 AS w3 GROUP BY CASE WHEN",
        "WHEN schema_of_variant(CAST(w3.__cg_c0 AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' \
         THEN concat('n:', CAST(CAST(CAST(w3.__cg_c0 AS VARIANT) AS BIGINT) AS STRING))",
        // Any other number as a Cypher number: its double (review).
        "RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN \
         variant_get(CAST(w3.__cg_c0 AS VARIANT), '$', 'DOUBLE') = \
         floor(variant_get(CAST(w3.__cg_c0 AS VARIANT), '$', 'DOUBLE'))",
        // A list by its elements' keys; any other scalar by its type.
        "concat('l:', to_json(transform(CAST(CAST(w3.__cg_c0 AS VARIANT) AS ARRAY<VARIANT>), \
         __cg_e0 ->",
        "ELSE concat(schema_of_variant(CAST(w3.__cg_c0 AS VARIANT)), ':', \
         CAST(CAST(w3.__cg_c0 AS VARIANT) AS STRING)) END",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
}

/// S7e1: a list comprehension over values filters and maps by the
/// dialect's spellings; its parameter is printed by its generated name.
#[test]
fn a_list_comprehension_filters_and_maps() {
    has(
        "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS l",
        &[r#"SELECT arrayMap(v0 -> v0 * 2, arrayFilter(v1 -> v1 > 1, [1, 2, 3])) AS "l""#],
    );
    // NULL of a NULL list; a comprehension of a comprehension.
    has(
        "WITH [[1, 2], [3]] AS l RETURN [x IN null | x] AS n, [x IN l | [y IN x | y * 10]] AS m",
        &[
            r#"SELECT NULL AS "n", arrayMap(v2 -> arrayMap(v3 -> v3 * 10, v2), [[1, 2], [3]]) AS "m""#,
        ],
    );
    // A property and an aggregate in the list; a column read in the body.
    has(
        "MATCH (a:User) RETURN a.country AS c, [n IN collect(a.name) WHERE n <> a.country] AS l",
        &[
            "arrayFilter(v1 -> v1 <> v0.country, coalesce(groupArray(v0.full_name), [])) AS \"l\"",
            "GROUP BY v0.country",
        ],
    );
    // In a WHERE, and unwound.
    has(
        "UNWIND [x IN range(1, 4) WHERE x % 2 = 0] AS y RETURN y",
        &["ARRAY JOIN arrayFilter(v0 -> v0 % 2 = 0, range(1, (4) + 1)) AS __cg_element"],
    );
}

/// S7e1: ClickHouse holds a comparison as `UInt8`: a list's boolean
/// elements are cast, so the list shows booleans.
#[test]
fn a_list_of_booleans_shows_booleans() {
    has(
        "RETURN [1 > 0, false] AS l, [x IN [1, 2] | x > 1] AS m",
        &[
            r#"[CAST(1 > 0 AS Nullable(Bool)), false] AS "l""#,
            r#"arrayMap(v0 -> CAST(v0 > 1 AS Nullable(Bool)), [1, 2]) AS "m""#,
        ],
    );
    // `DISTINCT` applies to the whole argument (the parser reads it as a
    // prefix of the first operand).
    has(
        "MATCH (a:User) RETURN collect(DISTINCT a.user_id > 3) AS l",
        &[r#"coalesce(groupArray(DISTINCT CAST(v0.user_id > 3 AS Nullable(Bool))), []) AS "l""#],
    );
}

/// S7e1: `collect(DISTINCT x)` over ordered rows keeps each value where it
/// first occurs.
#[test]
fn an_ordered_collect_distinct_keeps_the_first_of_each() {
    has(
        "MATCH (a:User) WITH a ORDER BY a.name RETURN collect(DISTINCT a.country) AS l",
        &[
            "SELECT arrayDistinct(arrayMap(t -> t.2, arraySort(t -> t.1, \
           groupArrayIf((w2.__cg_row, w2.p2_v1_country), NOT isNull(w2.p2_v1_country))))) AS \"l\"",
        ],
    );
}

/// S7e1 review: `+` with a list concatenates (ClickHouse `plus` of arrays
/// adds them element by element); a value is appended as a list of itself,
/// and only when it is never NULL (`[1] + NULL` is NULL in Cypher).
#[test]
fn a_list_plus_concatenates() {
    has(
        "RETURN [x IN [1, 2] | x] + [x IN [3] | x] AS a, [x IN [1] | x] + 3 AS b, \
         [x IN [1] | x] + null AS c",
        &[
            r#"arrayConcat(arrayMap(v0 -> v0, [1, 2]), arrayMap(v1 -> v1, [3])) AS "a""#,
            r#"arrayConcat(arrayMap(v2 -> v2, [1]), [3]) AS "b""#,
            r#"NULL AS "c""#,
        ],
    );
    has(
        "UNWIND [1, 2] AS x RETURN collect(x) + collect(x * 10) AS c",
        &["arrayConcat(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w1.__o0, w1.v0)"],
    );
    not_lowered(
        "MATCH (a:User) RETURN [x IN [1] | x] + (a.user_id - 1) AS l",
        "`+` of a list and a value that may be NULL",
    );
}

/// S7e1 review: comprehension parameters are printed as written, so the
/// emitter does not take `x = y` for a comparison of nodes.
#[test]
fn comprehension_parameters_compare_as_values() {
    has(
        "RETURN [x IN [1, 2] | [y IN [1, 2] WHERE y <> x]] AS l",
        &[r#"arrayMap(v0 -> arrayFilter(v1 -> v1 <> v0, [1, 2]), [1, 2]) AS "l""#],
    );
}

/// #1377 / #1364 / #1454 (user 2026-10-13): ClickHouse has no NULL array.
/// Where a list's NULL is known, what tells it from `[]` reads that
/// condition, the list as it takes it held with `[]` for NULL; a WITH
/// carries both beside the list as anything else reads it (`{v}__null`,
/// `{v}__held`); the final RETURN returns NULL where the condition holds.
#[test]
fn a_null_list_is_told_by_its_source() {
    has(
        "UNWIND [1, 2] AS x WITH x, CASE WHEN x > 1 THEN [x] END AS l \
         RETURN l, l IS NULL AS n, size(l) AS s, l + [9] AS p, {k: l} AS m",
        &[
            r#"CASE WHEN w1.v0 > 1 THEN [w1.v0] END AS "v2""#,
            r#"CASE WHEN w1.v0 > 1 THEN false ELSE true END AS "v2__null""#,
            r#"CASE WHEN w1.v0 > 1 THEN [w1.v0] ELSE [] END AS "v2__held""#,
            r#"if(w2.v2__null, NULL, CAST(w2.v2__held, 'Dynamic')) AS "l""#,
            r#"w2.v2__null AS "n""#,
            r#"CASE WHEN w2.v2__null THEN NULL ELSE length(w2.v2__held) END AS "s""#,
            "arrayFilter(__cg_e -> NOT (w2.v2__null), arrayConcat(w2.v2__held, [9]))",
            // Anything else reads the list as it always did.
            r#"map('k', CAST(w2.v2, 'Dynamic')) AS "m""#,
        ],
    );
    // count() / collect() skip it; grouped by it too.
    has(
        "UNWIND [1, 2] AS x WITH CASE WHEN x > 1 THEN [x] END AS l \
         RETURN count(l) AS c, count(DISTINCT l) AS d, collect(l) AS cl",
        &[
            r#"count(CASE WHEN w2.v1__null THEN NULL ELSE 1 END) AS "c""#,
            r#"length(arrayDistinct(groupArrayIf(w2.v1__held, NOT w2.v1__null))) AS "d""#,
            r#"groupArrayIf((w2.__o0, w2.v1__held), NOT w2.v1__null))), []) AS "cl""#,
        ],
    );
    has(
        "UNWIND [1, 2] AS x WITH CASE WHEN x > 1 THEN [x] END AS l RETURN l, count(*) AS c",
        &[
            r#"any(if(w2.v1__null, NULL, CAST(w2.v1__held, 'Dynamic'))) AS "l""#,
            "toString(w2.v1__held))), w2.v1__null",
        ],
    );
    // split() of a CASE's NULL: NULL; of a column, as it always was.
    has(
        "UNWIND [1, 2] AS x RETURN split(CASE WHEN x > 1 THEN 'a,b' END, ',') IS NULL AS n, \
         split(toString(x), ',') AS t",
        &[
            r#"CASE WHEN w1.v0 > 1 THEN false ELSE true END AS "n""#,
            r#"splitByChar(',', toString(w1.v0)) AS "t""#,
        ],
    );
    // ORDER BY: NULL last ascending (its condition first); reduce() over
    // it NULL; carried into an OPTIONAL MATCH and joined on.
    has(
        "UNWIND [1, 2] AS x WITH split(CASE WHEN x > 1 THEN 'a,b' END, ',') AS l \
         RETURN reduce(t = '', y IN l | t + y) AS r ORDER BY l",
        &[
            "CASE WHEN w2.v1__null THEN NULL ELSE arrayFold(",
            "ORDER BY w2.v1__null ASC, w2.v1 ASC",
        ],
    );
    has(
        "UNWIND [1, 2] AS x WITH x, split(CASE WHEN x > 1 THEN 'a,b' END, ',') AS l \
         OPTIONAL MATCH (u:User) WHERE u.user_id = 1 AND l IS NULL RETURN x, u.name",
        &[
            "d3.v2__null",
            "(w2.v2__null = o4.v2__null OR (w2.v2__null IS NULL AND o4.v2__null IS NULL))",
        ],
    );
    // A grouping projection's WHERE (HAVING) / ORDER BY read its item's
    // group: any one of the held lists, and the flag, a key (review).
    has(
        "UNWIND [1, 2] AS x WITH CASE WHEN x > 1 THEN [x] END AS l, count(*) AS c \
         WHERE size(l) > 0 RETURN l, c",
        &[
            "HAVING CASE WHEN CASE WHEN w1.v0 > 1 THEN false ELSE true END THEN NULL ELSE \
           length(any(CASE WHEN w1.v0 > 1 THEN [w1.v0] ELSE [] END)) END > 0",
        ],
    );
    // max() of a group with no list that is not NULL: NULL.
    has(
        "UNWIND [1, 2] AS x WITH x % 2 AS g, CASE WHEN x > 5 THEN [x] END AS l \
         RETURN g, max(l) AS m",
        &[
            "if(count(CASE WHEN w2.v2__null THEN NULL ELSE 1 END) = 0, NULL, \
             CAST(maxIf(w2.v2__held, NOT w2.v2__null), 'Dynamic')) AS \"m\"",
        ],
    );
    // A DISTINCT's sort key of a list item is no grouping key (it is an
    // `any()`; main failed with it in the GROUP BY).
    let got = squash(&sql(
        "MATCH (u:User) WITH DISTINCT [u.name] AS l ORDER BY l LIMIT 2 RETURN l",
    ));
    assert!(got.contains(r#"any([v0.full_name]) AS "__o0""#), "{got}");
    assert!(
        !got.contains("GROUP BY tuple(multiIf(replaceRegexpOne(dynamicType(CAST(any("),
        "{got}"
    );
    assert!(!got.contains("toString([v0.full_name]))), any("), "{got}");
    // Not held outside a reader: inside a list a NULL stays NULL (review).
    has(
        "UNWIND [1, 2] AS x RETURN [CASE WHEN x > 1 THEN [x] END, [5]] AS ll",
        &["[CASE WHEN w1.v0 > 1 THEN [w1.v0] END, [5]]"],
    );
    // A list that is never NULL: no condition.
    has(
        "UNWIND [1, 2] AS x WITH collect(x) AS l RETURN l IS NULL AS n, size(l) AS s",
        &[
            r#"CAST(false AS Nullable(Bool)) AS "n""#,
            r#"length(w2.v1) AS "s""#,
        ],
    );
}

/// #1423: `x IN` a list expression is three-valued, as for a list of items
/// (ClickHouse's `x IN l` is false where an element is NULL): NULL where no
/// element is `x` and one is NULL, or `x` is NULL and the list is not empty.
#[test]
fn in_a_list_expression_is_three_valued() {
    let l = "arrayMap(v1 -> v1, [1, NULL])";
    has(
        "UNWIND [2] AS x RETURN x IN [y IN [1, null] | y] AS a",
        &[&format!(
            "CASE WHEN length({l}) = 0 THEN false \
             WHEN (w1.v0) IS NULL THEN NULL WHEN has({l}, w1.v0) THEN true \
             WHEN length(arrayFilter(__cg_in -> __cg_in IS NULL, {l})) > 0 THEN NULL \
             ELSE false END"
        )],
    );
    has(
        "UNWIND [2] AS x RETURN x NOT IN [y IN [1] | y] AS a",
        &[
            "length(arrayMap(v1 -> v1, [1])) = 0 THEN true",
            "has(arrayMap(v1 -> v1, [1]), w1.v0) THEN false",
            "THEN NULL ELSE true END",
        ],
    );
    // A filter's `x IN l` (through ANDs only) is ClickHouse's own: NULL and
    // false drop the row alike, and a walk's start reads it (review).
    has(
        "MATCH (u:User) WHERE u.age > 1 AND u.user_id IN [y IN [1, null] | y] \
         AND NOT u.user_id IN [y IN [2] | y] RETURN u.name",
        &[
            "v0.user_id IN arrayMap(v1 -> v1, [1, NULL])",
            "NOT CAST(CASE WHEN length(arrayMap(v2 -> v2, [2])) = 0",
        ],
    );
    has(
        "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) WHERE a.user_id IN $ids RETURN count(*)",
        &["WHERE (start_node.user_id IN $ids)"],
    );
    // A NULL value is not looked up (Spark refuses a NULL-typed one).
    has(
        "RETURN null IN [y IN [1] | y] AS a",
        &["CASE WHEN length(arrayMap(v0 -> v0, [1])) = 0 THEN false ELSE NULL END"],
    );
    // A NULL list (held as `[]`, `null_list.rs`): NULL by its source.
    has(
        "UNWIND [1, 2] AS x WITH x, CASE WHEN x > 1 THEN [x] END AS l RETURN 5 IN l AS i",
        &["CASE WHEN w2.v2__null THEN NULL WHEN length(w2.v2__held) = 0 THEN false"],
    );
    // `null IN []` is false; `x IN null` is NULL.
    has(
        "UNWIND [2, null] AS x RETURN x IN [] AS a, NOT x IN [] AS b, x IN null AS c",
        &[
            r#"CAST(false AS Nullable(Bool)) AS "a""#,
            r#"CAST(NOT false AS Nullable(Bool)) AS "b""#,
            r#"NULL AS "c""#,
        ],
    );
}

/// As in Neo4j: an aggregate is not evaluated per list element.
#[test]
fn an_aggregate_inside_a_comprehension_is_an_error() {
    not_lowered(
        "MATCH (a:User) RETURN [x IN collect(a.user_id) | x * count(*)] AS l",
        "Can't use aggregating expressions inside of expressions executing over lists",
    );
}

/// S7e2a: a list of nodes is an array of tuples of each node's id and
/// declared properties; an unwound element's property is its slot.
#[test]
fn a_list_of_nodes_holds_tuples_of_their_columns() {
    has(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x RETURN x.name AS n",
        &[
            "SELECT groupArrayIf(tuple(v0.user_id, v0.age, v0.city, v0.country, \
             v0.email_address, v0.is_active, v0.full_name, v0.registration_date, v0.user_id), \
             NOT isNull(v0.user_id)) AS \"v1\" FROM test_integration.users_test AS v0",
            "ARRAY JOIN w1.v1 AS __cg_element",
            r#"SELECT tupleElement(w2.v2, 7) AS "n""#,
        ],
    );
    // A fixed path's nodes, in a comprehension.
    has(
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN [n IN nodes(p) WHERE n.age > 30 | n.name] AS ns",
        &["arrayMap(v4 -> tupleElement(v4, 7), arrayFilter(v5 -> tupleElement(v5, 2) > 30, \
           [tuple(v0.user_id, "],
    );
    // An element equals a node of the relation by identity.
    has(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x MATCH (b:User) WHERE b = x \
         RETURN count(*) AS c",
        &["WHERE v3.user_id = tupleElement(w2.v2, 1)"],
    );
    // Returned, the list is its nodes' values.
    has(
        "MATCH (a:User) RETURN collect(a) AS l",
        &[
            "arrayMap(__cg_e -> if(isNull(tupleElement(__cg_e, 1)), NULL, CAST(map('elementId', \
           CAST(concat('User:', toString(tupleElement(__cg_e, 1)), '-'), 'Dynamic')",
        ],
    );
}

/// S7e2a refusals: an element out of a list's range would be a tuple of
/// defaults; the tuples carry declared properties only; a list's elements
/// are of one label.
#[test]
fn what_a_list_of_nodes_does_not_lower_yet() {
    not_lowered(
        "MATCH (a:User) WITH collect(a) AS l RETURN l[0] AS f",
        "an element or slice of a list of nodes or relationships",
    );
    not_lowered(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x RETURN x.nope AS n",
        "an undeclared property of a list's node or relationship",
    );
    not_lowered(
        "MATCH (a:User) WITH collect(a) AS l RETURN head(l) AS f",
        "head() of nodes or relationships of a list",
    );
    not_lowered(
        "MATCH (a:User), (b:User) RETURN coalesce(a, b) AS n",
        "coalesce() of nodes or relationships of a list",
    );
    // `length()` of a list other than a path's nodes / relationships (S11a
    // reads those as their size): Neo4j 5 refuses it, and the legacy
    // pipeline fails on a variable-length relationship's.
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN length(r) AS l",
        "length() of nodes or relationships of a list",
    );
    not_lowered(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN length(collect(b)) AS l",
        "length() of nodes or relationships of a list",
    );
}

/// S7e2a review: a list's tuples are of one type: a list mixing labels,
/// types or values is not lowered (the tuples would be returned raw), nor
/// is `+` of such lists, nor ORDER BY an element.
#[test]
fn a_list_mixing_labels_or_values_is_not_lowered() {
    for q in [
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN [a, r] AS l",
        "MATCH (a:User), (p:Post) RETURN [a, p] AS l",
        "MATCH (a:User) RETURN [a, 1] AS l",
        "MATCH (a:User) RETURN [a.name, a] AS l",
        "MATCH (a:User), (p:Post) RETURN collect([a, p]) AS l",
        "MATCH (a:User), (p:Post) RETURN [[a], [p]] AS l",
        "MATCH (a:User), (p:Post) RETURN size([a, p]) AS n",
    ] {
        not_lowered(
            q,
            "a list of nodes or relationships of several labels or types",
        );
    }
    not_lowered(
        "MATCH (a:User), (p:Post) RETURN size(collect(a) + collect(p)) AS n",
        "`+` of lists of nodes or relationships of different labels or types",
    );
    not_lowered(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x RETURN x.name AS n ORDER BY x",
        "ORDER BY a node or relationship of a list",
    );
}

/// S7e2a review: a property declared boolean on an integer column is cast
/// in the tuple, as a returned column is.
#[test]
fn a_listed_boolean_property_is_cast() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: listed_bool
graph_schema:
  nodes:
    - label: P
      database: db
      table: p
      node_id: id
      property_mappings: { id: id, ok: ok_flag }
      property_types: { ok: boolean }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = squash(
        &translate_bound_plan(
            "MATCH (p:P) WITH collect(p) AS l UNWIND l AS x RETURN x.ok AS ok",
            &schema,
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    assert!(
        got.contains("tuple(v0.id, v0.id, CAST(v0.ok_flag AS Nullable(Bool)))"),
        "{got}"
    );
}

/// S7e2b: a walk of one definition carries its nodes / relationships as
/// element tuples when they are read as a list's elements; the empty list
/// a recursive CTE's first rows start from has their type.
#[test]
fn a_walk_carries_its_elements_as_tuples() {
    has(
        "MATCH p = (a:User)-[:FOLLOWS*0..2]->(b:User) RETURN [r IN relationships(p) | r.follow_date] AS d",
        &[
            "(SELECT groupArray(tuple(__cg_none.follower_id, __cg_none.followed_id, \
             __cg_none.follow_id, __cg_none.follow_date)) FROM test_integration.user_follows_test \
             AS __cg_none WHERE 0) as path_rel_tuples",
            "arrayConcat(vp.path_rel_tuples, [tuple(rel.follower_id, rel.followed_id, \
             rel.follow_id, rel.follow_date)]) as path_rel_tuples",
        ],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN [n IN nodes(p) | n.name] AS ns",
        &[
            "arrayConcat([tuple(v0.user_id, ",
            "arraySlice(v1.path_node_tuples, 2))",
        ],
    );
    // A shortest path's search carries them too.
    has(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) RETURN [n IN nodes(p) | n.name] AS ns",
        &["(SELECT groupArray(tuple(__cg_none.user_id, ", "AS path_node_tuples"],
    );
    // A MATCH condition reads them (before the walk is built).
    has(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) WHERE size([x IN r WHERE x.follow_date IS NULL]) = 0 \
         RETURN a.name",
        &["WHERE length(arrayFilter(v3 -> tupleElement(v3, 4) IS NULL, v1.path_rel_tuples))"],
    );
    // Returned as a value, a walk's nodes stay values (no tuples).
    let got = squash(&sql(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN nodes(p) AS ns",
    ));
    assert!(!got.contains("path_node_tuples"), "{got}");
}

/// S7e2b review, S8d2c: a walk whose relationships have a composite
/// `edge_id` carries them as tuples with a slot per column, read as a
/// list's elements.
#[test]
fn a_walk_of_a_composite_edge_id_carries_its_tuples() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: composite_edge
graph_schema:
  nodes:
    - label: U
      database: db
      table: u
      node_id: id
      property_mappings: { id: id }
  edges:
    - type: F
      database: db
      table: f
      from_id: a
      to_id: b
      edge_id: [a, b]
      from_node: U
      to_node: U
      property_mappings: { since: since }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let lower = |q: &str| translate_bound_plan(q, &schema, &ReadOptions::default());
    let got = lower("MATCH p = (a:U)-[:F*1..2]->(b:U) WITH relationships(p) AS rs RETURN rs")
        .map(|t| squash(&t.sql));
    // A composite `edge_id` has a tuple layout (a slot per column, S8d2c).
    assert!(
        matches!(&got, Ok(sql) if sql.contains("path_rel_tuples")),
        "{got:?}"
    );
    // Its relationships' properties are their slots.
    let got =
        lower("MATCH p = (a:U)-[:F*1..2]->(b:U) RETURN [r IN relationships(p) | r.since] AS s")
            .map(|t| squash(&t.sql));
    assert!(
        matches!(&got, Ok(sql) if sql.contains("tupleElement(v4, 5)")),
        "{got:?}"
    );
}

/// S7e3: a map literal is a `Map(String, Dynamic)` (as a graph value, S6c):
/// its values keep their types, maps of any keys are of one type; a map
/// that is a temporal or spatial constructor's argument stays as the
/// constructor reads it. A map's values are held as values of any type, so
/// reading one (`m.k`), comparing, sorting or aggregating maps is not
/// lowered (each would compare, sort or count unlike the value's own type).
#[test]
fn a_map_keeps_its_values_types() {
    has(
        "WITH {b: 'x', a: 1} AS m RETURN m",
        &["map('a', CAST(1, 'Dynamic'), 'b', CAST('x', 'Dynamic')) AS \"v0\""],
    );
    // Maps of different keys in one list keep their own keys.
    has(
        "UNWIND [{a: 1}, {b: 2}] AS m RETURN m",
        &["ARRAY JOIN [map('a', CAST(1, 'Dynamic')), map('b', CAST(2, 'Dynamic'))]"],
    );
    has(
        "RETURN {ok: 1 > 0} AS m, {} AS e",
        &[
            "map('ok', CAST(CAST(1 > 0 AS Nullable(Bool)), 'Dynamic'))",
            "CAST(CAST(map(), 'Map(String, String)'), 'Map(String, Dynamic)') AS \"e\"",
        ],
    );
    // Grouped and made distinct by S7d's DISTINCT key: `1` and `1.0` are
    // one.
    has(
        "MATCH (a:User) RETURN DISTINCT {c: a.country} AS m",
        &["toString(map('c', CAST(v0.country, 'Dynamic')))"],
    );
    has(
        "MATCH (a:User) RETURN a.name AS n, collect({c: a.country}) AS l",
        &["groupArray(map('c', CAST(v0.country, 'Dynamic')))"],
    );
    let got = squash(&sql("RETURN duration({days: 5}) AS d"));
    assert!(!got.contains("'Dynamic'"), "{got}");
    has(
        "WITH {b: 1, a: 2} AS m RETURN keys(m) AS k",
        &["mapKeys(w1.v0) AS \"k\""],
    );
    // A key of a map a WITH projected: the key's own column (S11m).
    has(
        "MATCH (u:User) WITH {n: u.name, k: u.user_id} AS m WHERE m.k > 5 \
         RETURN m.n AS n, m['k'] AS k, m.missing AS z",
        &[
            r#"v0.full_name AS "v1__k0""#,
            r#"v0.user_id AS "v1__k1""#,
            "WHERE v0.user_id > 5",
            r#"w1.v1__k0 AS "n", w1.v1__k1 AS "k", NULL AS "z""#,
        ],
    );
    // Through another WITH, and constant keys as they are.
    has(
        "MATCH (u:User) WITH {a: 1, n: u.name} AS m WITH m RETURN m.a AS a, m.n AS n",
        &[r#"w1.v1__k1 AS "v2__k1""#, r#"1 AS "a", w2.v2__k1 AS "n""#],
    );
    // Grouped by the map, its own HAVING / ORDER BY read any one of its
    // group's keys (review: a raw key there is not an aggregate).
    has(
        "MATCH (u:User) WITH {c: u.country} AS m, count(*) AS n WHERE m.c <> 'UK' \
         RETURN m.c AS c, n",
        &["HAVING any(v0.country) <> 'UK'"],
    );
    for (q, why) in [
        // A key of a key's value: not carried (review: read as NULL).
        (
            "MATCH (u:User) WITH {inner: {n: u.name}} AS m RETURN m.inner.n AS n",
            "a property of a map",
        ),
        // A map whose keys are not known (a parameter, a list's element).
        (
            "UNWIND [{a: 1}] AS m RETURN m.a AS a",
            "a property of a map",
        ),
        ("UNWIND [{a: 1}] AS m RETURN m['a'] AS a", "a key of a map"),
        (
            "WITH {days: 5} AS m RETURN duration(m) AS d",
            "duration() of a map",
        ),
        (
            "RETURN {a: 1} = {a: 1.0} AS e",
            "a comparison or operator over maps",
        ),
        (
            "WITH {a: 1} AS m RETURN m IN [m] AS e",
            "a comparison or operator over maps",
        ),
        (
            "MATCH (a:User) RETURN a.name AS n ORDER BY {c: a.country}",
            "ORDER BY a map",
        ),
        (
            "MATCH (a:User) RETURN collect(DISTINCT {c: a.country}) AS l",
            "collect() of maps",
        ),
        ("RETURN size({a: 1}) AS s", "size() of a map"),
    ] {
        not_lowered(q, why);
    }
}

/// S8d: the schema of the composite-id walks: accounts by `(bank, num)`.
fn composite_walk_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        r#"
name: composite_walk
graph_schema:
  nodes:
    - { label: Acct, database: db, table: acct, node_id: [bank, num], property_mappings: { bank: bank, num: num, owner: owner } }
  edges:
    - { type: SENT, database: db, table: sent, from_node: Acct, to_node: Acct, from_id: [fb, fnum], to_id: [tb, tnum], property_mappings: {} }
    - { type: PAID, database: db, table: paid, from_node: Acct, to_node: Acct, from_id: [fb, fnum], to_id: [tb, tnum], edge_id: tid, property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

/// I6b: the server's `id()` of a composite node names its element id, whose
/// parts are escaped (a `|` in one is no separator) and a NULL one `\\N`:
/// the node of those parts, its NULL part `IS NULL`; a text of other parts
/// names none.
#[test]
fn an_id_of_a_composite_node_reads_its_escaped_parts() {
    let server = LowerOptions {
        id_lookup: Some(IdLookup {
            node: std::sync::Arc::new(|id| match id {
                1 => Some(("Acct".to_string(), "b\\|1|7".to_string())),
                2 => Some(("Acct".to_string(), "b|\\N".to_string())),
                3 => Some(("Acct".to_string(), "a|b|c".to_string())),
                // A part holding a backslash: a value, no escape.
                4 => Some(("Acct".to_string(), "e\\\\f|9".to_string())),
                _ => None,
            }),
            element: std::sync::Arc::new(|_| None),
        }),
        ..LowerOptions::default()
    };
    let got = lowered(
        "MATCH (a:Acct) WHERE id(a) = 1 RETURN a.owner",
        &composite_walk_schema(),
        &server,
    );
    assert!(got.contains("v0.bank = 'b|1' AND v0.num = '7'"), "{got}");
    let got = lowered(
        "MATCH (a:Acct) WHERE id(a) = 2 RETURN a.owner",
        &composite_walk_schema(),
        &server,
    );
    assert!(got.contains("v0.bank = 'b' AND v0.num IS NULL"), "{got}");
    let got = lowered(
        "MATCH (a:Acct) WHERE id(a) = 3 RETURN a.owner",
        &composite_walk_schema(),
        &server,
    );
    assert!(got.contains("WHERE false"), "{got}");
    let got = lowered(
        "MATCH (a:Acct) WHERE id(a) = 4 RETURN a.owner",
        &composite_walk_schema(),
        &server,
    );
    assert!(got.contains("v0.bank = 'e\\\\f' AND v0.num = '9'"), "{got}");
}

/// I5a (#1387): `id(n)` of a composite node, an item or a value, is one
/// text: the id's parts as its element id spells them (escaped, joined by
/// `|`, a NULL part `\N`), so no two ids share one and Bolt, which numbers it
/// from the element id, gives a node the number of its value. NULL where an
/// OPTIONAL MATCH found no node. (Before, a refusal.)
#[test]
fn an_id_of_a_composite_node_as_a_value_is_its_element_id_text() {
    let lower = |q: &str| {
        translate_bound_plan(q, &composite_walk_schema(), &ReadOptions::default())
            .map(|t| squash(&t.sql))
            .unwrap_or_else(|e| panic!("{q}: {e}"))
    };
    let text = |a: &str| {
        format!(
            "concat(CASE WHEN {a}.bank IS NULL THEN '\\\\N' ELSE \
             replaceAll(replaceAll(replaceAll(toString({a}.bank), '\\\\', '\\\\\\\\'), \
             ':', '\\\\:'), '|', '\\\\|') END, '|', CASE WHEN {a}.num IS NULL THEN '\\\\N' ELSE"
        )
    };
    // An item, and one a WITH carries or an ORDER BY reads.
    for q in [
        "MATCH (a:Acct) RETURN id(a) AS i",
        "MATCH (a:Acct) RETURN a.owner, id(a) AS i ORDER BY id(a)",
        "MATCH (a:Acct) WITH id(a) AS i RETURN i ORDER BY i",
        "MATCH (a:Acct) RETURN count(DISTINCT id(a)) AS n",
    ] {
        let got = lower(q);
        assert!(got.contains(&text("v0")), "{q}:\n{got}");
        assert!(
            !got.contains("v0.bank AS"),
            "{q}: the first part alone:\n{got}"
        );
    }
    // ORDER BY reads the whole text, not the first part.
    let got = lower("MATCH (a:Acct) RETURN a.owner ORDER BY id(a)");
    let order = got.split("ORDER BY").nth(1).unwrap_or_default();
    assert!(
        order.contains(&text("v0")) && order.contains("v0.num"),
        "{got}"
    );
    // NULL where an OPTIONAL MATCH found no node (every part NULL).
    let got = lower("OPTIONAL MATCH (a:Acct) RETURN id(a) AS i");
    assert!(
        got.contains("(o1.v0__bank IS NULL AND o1.v0__num IS NULL) THEN NULL ELSE concat("),
        "{got}"
    );
}

/// I5a review: a composite `id()` is a text, so a comparison with anything
/// but another `id()` is refused, not answered: a number (Bolt's) matches no
/// text, and a text of one label equals another label's. Only `id(a) = id(b)`
/// and a literal the server decodes (`id(a) = 5`) compare it.
#[test]
fn a_composite_id_compares_only_with_another_id() {
    let refused = |q: &str| {
        let err = translate_bound_plan(q, &composite_walk_schema(), &ReadOptions::default())
            .expect_err(q);
        assert!(
            err.contains("composite id compared with a value other than another id()"),
            "{q}: {err}"
        );
    };
    for q in [
        "MATCH (a:Acct) WHERE id(a) IN $ns RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) = $n RETURN a.owner",
        "MATCH (a:Acct) WHERE $n = id(a) RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) <> $n RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) >= $n RETURN a.owner",
        "MATCH (a:Acct) WHERE NOT id(a) IN $ns RETURN a.owner",
        "MATCH (a:Acct) RETURN id(a) = $n AS e",
        "MATCH (a:Acct), (b:Acct) WHERE id(a) IN [id(b)] RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) = 'x|1' RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) NOT IN $ns RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) < $n RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) <= $n RETURN a.owner",
        "MATCH (a:Acct) WHERE id(a) > $n RETURN a.owner",
        "MATCH (a:Acct) WHERE 5 IN [id(a)] RETURN a.owner",
        // A variable a WITH bound to the id is the same text.
        "MATCH (a:Acct) WITH id(a) AS k WHERE k IN $ns RETURN count(*)",
        "MATCH (a:Acct) WITH id(a) AS k WHERE NOT k IN [$n] RETURN count(*)",
        "MATCH (a:Acct) WITH id(a) AS k RETURN k IN [$n] AS e",
        "MATCH (a:Acct), (b:Acct) WITH id(a) AS ka, id(b) AS kb WHERE ka = kb RETURN ka",
        "MATCH (a:Acct) WITH id(a) AS k WITH k AS j WHERE j IN $ns RETURN count(*)",
        // An alias of the RETURN, read by its ORDER BY.
        "MATCH (a:Acct) RETURN a.owner AS o, id(a) AS k ORDER BY k IN [$n] DESC, o",
        "MATCH (a:Acct) RETURN a.owner AS o, id(a) AS k ORDER BY id(a) IN [$n], o",
        "MATCH (a:Acct) WITH id(a) AS k RETURN k ORDER BY k IN [$n]",
    ] {
        refused(q);
    }
    // A single-column node's id, carried or not, and a relationship's, are
    // compared as before (the guard is for the composite text only).
    let (sql, _) = shaped(
        "MATCH (a:User) WITH id(a) AS k WHERE k IN [1, 2] RETURN count(*) AS n",
        &social(),
    );
    assert!(
        sql.contains("IN (1, 2)") || sql.contains("IN [1, 2]"),
        "{sql}"
    );
    // Another `id()`, and the value in a RETURN item, are lowered.
    for q in [
        "MATCH (a:Acct), (b:Acct) WHERE id(a) = id(b) RETURN a.owner",
        "MATCH (a:Acct), (b:Acct) WHERE id(a) <> id(b) RETURN a.owner",
        "MATCH (a:Acct) RETURN id(a) AS i, a.owner ORDER BY i",
    ] {
        translate_bound_plan(q, &composite_walk_schema(), &ReadOptions::default())
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
}

/// S8d: a walk between composite ids joins each step and ties its ends by
/// the ids' columns (their texts in `start_id` / `end_id` two ids share:
/// `('B|1', '2')`, `('B', '1|2')`), restricts its first node by the tuple of
/// them, and a hop beside it is unique against it by its stored ends.
#[test]
fn a_walk_between_composite_ids_goes_by_their_columns() {
    let got = translate_bound_plan(
        "MATCH (a:Acct)-[:SENT]->(b:Acct)-[:SENT*1..2]->(c:Acct) \
         WHERE b.owner = 'x' RETURN a.owner, c.owner",
        &composite_walk_schema(),
        &ReadOptions::default(),
    )
    .unwrap()
    .sql;
    for part in [
        "JOIN db.sent AS rel ON vp.end_bank = rel.fb AND vp.end_num = rel.fnum",
        "(start_node.bank, start_node.num) IN (SELECT DISTINCT",
        "v3.start_bank = v2.bank AND v3.start_num = v2.num",
        "v3.end_bank = v4.bank AND v3.end_num = v4.num",
        "NOT has(v3.path_edges, tuple(v1.fb, v1.fnum, v1.tb, v1.tnum))",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    // (`start_id` / `end_id` / `path_nodes` keep the texts; nothing joins
    // or ties by them.)
    assert!(
        !got.contains("vp.end_id = concat("),
        "a step by the text:\n{got}"
    );
}

/// S8d2: a shortest path between composite ids searches nodes keyed by the
/// text of their id's tuple (not the generator's `|`-joined text, which
/// `('B|1', '2')` and `('B', '1|2')` share), computed where the search reads
/// a node or a relationship's end over the tables as they are, and ties its
/// ends by that key.
#[test]
fn a_shortest_path_between_composite_ids_keys_nodes_by_their_tuples() {
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Acct {owner: 'x'}), (b:Acct {owner: 'v'}) \
             MATCH p = shortestPath((a)-[:SENT*]->(b)) RETURN length(p)",
            &composite_walk_schema(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    for p in [
        "toString(tuple(start_node.bank, start_node.num)) AS start_id",
        "JOIN db.sent AS rel ON toString(tuple(rel.fb, rel.fnum)) = f.node \
         JOIN db.acct AS end_node ON toString(tuple(end_node.bank, end_node.num)) = \
         toString(tuple(rel.tb, rel.tnum))",
        // A row with a NULL end part relates no node (a key's text of NULL
        // would equal itself; review).
        "(rel.fb IS NOT NULL) AND (rel.fnum IS NOT NULL)",
        "v2.start_id = toString(tuple(v0.bank, v0.num))",
        "v2.end_id = toString(tuple(v1.bank, v1.num))",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
    assert!(!got.contains("'|'"), "{got}");
    // With a condition the search cannot hold, the pick reads the trails
    // keyed alike (their `start_<col>` / `end_<col>`), and restricts them to
    // the pairs failing it by that key.
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Acct), (b:Acct) WHERE a <> b \
             MATCH p = shortestPath((a)-[:SENT*]->(b)) WHERE length(p) >= 2 RETURN length(p)",
            &composite_walk_schema(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    for p in [
        "vlp_v2_keyed AS ( SELECT toString(tuple(start_bank, start_num)) AS start_id, \
         toString(tuple(end_bank, end_num)) AS end_id, hop_count, path_edges, path_nodes \
         FROM vlp_v2_path )",
        "FROM vlp_v2_keyed AS v2",
        "toString(tuple(start_node.bank, start_node.num)) IN (SELECT start_id FROM",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
}

/// S8d2b: a walk of several definitions between composite ids goes by keys:
/// each node's id is its identity as a text (its label and the tuple of its
/// id's columns), as are the relationships' ends, which no two ids share; a
/// row whose end has a NULL part relates no node; the walk's first node is
/// restricted, and its ends tied, by that key.
#[test]
fn a_union_walk_between_composite_ids_goes_by_keys() {
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Acct {owner: 'x'})-[:SENT|PAID*1..2]->(b:Acct) RETURN b.owner",
            &composite_walk_schema(),
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    for p in [
        "concat('Acct', ':', toString(tuple(e.bank, e.num))) AS \"__cg_id_0\"",
        "concat('Acct', ':', toString(tuple(e.fb, e.fnum))) AS \"__cg_start_0\"",
        "concat('Acct', ':', toString(tuple(e.tb, e.tnum))) AS \"__cg_end_0\"",
        "e.fb IS NOT NULL AND e.fnum IS NOT NULL",
        "concat('Acct', ':', toString(tuple(v0.bank, v0.num))) AS \"id\"",
        "v1.start_id = concat('Acct', ':', toString(tuple(v0.bank, v0.num)))",
        "v1.end_id = concat('Acct', ':', toString(tuple(v2.bank, v2.num)))",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
}

/// S8d2c: a composite id in a value or a list: its element id joins its
/// parts' texts, escaped, with `|` (the element-id contract, I6b: a NULL
/// part `\\N`, not the text `null`), its identity is the text
/// of its tuple (which no two ids share), and a list's element tuple holds a
/// slot per id column.
#[test]
fn composite_ids_in_values_and_lists() {
    let lower = |q: &str| {
        translate_bound_plan(q, &composite_walk_schema(), &ReadOptions::default())
            .map(|t| squash(&t.sql))
            .unwrap_or_else(|e| panic!("{q}: {e}"))
    };
    // A walk's nodes as values; its identity by the tuple text.
    let got = lower("MATCH p = (a:Acct)-[:SENT*0..1]->(b:Acct) WITH DISTINCT p RETURN p");
    for p in [
        "concat(CASE WHEN start_node.bank IS NULL THEN '\\\\N' ELSE \
         replaceAll(replaceAll(replaceAll(toString(start_node.bank), '\\\\', '\\\\\\\\'), \
         ':', '\\\\:'), '|', '\\\\|') END, '|', CASE WHEN start_node.num IS NULL",
        "toString(tuple(start_node.bank, start_node.num)) as start_id",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
    // A list of composite nodes: a slot per column.
    let got = lower("MATCH (a:Acct)-[:SENT]->(b:Acct) RETURN collect(b) AS l");
    assert!(
        got.contains("tuple(v2.bank, v2.num, v2.bank, v2.num, v2.owner)"),
        "{got}"
    );
    // A node is NULL when every part of its id is: a real node's part can be
    // NULL (review).
    let got = lower("MATCH (a:Acct) WITH collect(a) AS l RETURN size(l) AS n");
    assert!(
        got.contains("(v0.bank IS NOT NULL OR v0.num IS NOT NULL)"),
        "{got}"
    );
    // A shortest path read as a path, with a condition the search cannot
    // hold: the trails carry its columns keyed alike.
    let got = lower(
        "MATCH (a:Acct), (b:Acct) WHERE a <> b \
         MATCH p = shortestPath((a)-[:SENT*]->(b)) WHERE length(p) >= 2 WITH DISTINCT p \
         RETURN count(*)",
    );
    assert!(
        got.contains("hop_count, path_edges, path_nodes FROM vlp_v2_path"),
        "{got}"
    );
}

/// S8d review: refused, not answered wrong or with a ClickHouse error: a
/// walk between composite ids one of whose columns is named `id` (the walk's
/// `start_id`), and a relationship whose end has fewer columns than its
/// node's id (a hop or walk tied part of the id).
#[test]
fn composite_ids_the_walk_cannot_carry_are_refused() {
    let named_id = GraphSchemaConfig::from_yaml_str(
        r#"
name: named_id
graph_schema:
  nodes:
    - { label: N, database: db, table: n, node_id: [id, region], property_mappings: { id: id, region: region } }
  edges:
    - { type: R, database: db, table: r, from_node: N, to_node: N, from_id: [fid, freg], to_id: [tid, treg], property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let err = translate_bound_plan(
        "MATCH (a:N)-[:R*1..2]->(b:N) RETURN count(*)",
        &named_id,
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("with a column `id`"), "{err}");
    let fewer = GraphSchemaConfig::from_yaml_str(
        r#"
name: fewer
graph_schema:
  nodes:
    - { label: Acct, database: db, table: acct, node_id: [bank, num], property_mappings: { bank: bank, num: num } }
  edges:
    - { type: SENT, database: db, table: sent, from_node: Acct, to_node: Acct, from_id: fb, to_id: [tb, tnum], property_mappings: {} }
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    for q in [
        "MATCH (a:Acct)-[:SENT]->(b:Acct) RETURN count(*)",
        "MATCH (a:Acct)-[:SENT*1..2]->(b:Acct) RETURN count(*)",
    ] {
        let err = translate_bound_plan(q, &fewer, &ReadOptions::default()).unwrap_err();
        assert!(err.contains("another number of columns"), "{q}: {err}");
    }
}

// ------------------------------------------------------------- S9 subqueries

#[test]
fn an_exists_subquery_joins_its_matches_grouped_by_the_correlation() {
    // Anchored on the shared node, read from the edge's endpoint column;
    // one group per node, so the rows are neither multiplied nor dropped.
    has(
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(:User) } RETURN a.name",
        &[
            "subquery_s1 AS ( SELECT v1.follower_id AS \"v0__user_id\", \
             CAST(count(*) > 0 AS Nullable(Bool)) AS \"__value\"",
            "GROUP BY v1.follower_id",
            "LEFT JOIN subquery_s1 AS s1 ON v0.user_id = s1.v0__user_id",
            "WHERE coalesce(s1.__value, false)",
        ],
    );
    // A pattern predicate, `NOT`, and `exists()` are the same subquery.
    has(
        "MATCH (a:User) WHERE NOT (a)-[:FOLLOWS]->() RETURN a.name",
        &["WHERE NOT coalesce(s1.__value, false)"],
    );
    has(
        "MATCH (a:User) RETURN a.name, exists((a)-[:FOLLOWS]->()) AS e",
        &[
            "coalesce(s1.__value, false) AS \"e\"",
            "CAST(count(*) > 0 AS Nullable(Bool)) AS \"__value\"",
        ],
    );
}

#[test]
fn a_count_and_a_pattern_comprehension_are_the_groups_count_and_list() {
    has(
        "MATCH (a:User) RETURN a.name, size((a)-[:FOLLOWS]->()) AS c",
        &["coalesce(s1.__value, 0) AS \"c\""],
    );
    // `size()` of a pattern comprehension counts its matches.
    has(
        "MATCH (a:User) RETURN size([(a)-[:FOLLOWS]->(b) WHERE b.age > 3 | b]) AS c",
        &[
            "count(*) AS \"__value\"",
            "coalesce(s1.__value, 0) AS \"c\"",
        ],
    );
    // Every match's value, NULLs kept (`groupArray` skips them).
    has(
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(b) | b.name] AS l",
        &[
            "arrayMap(t -> t.1, groupArray(tuple(v2.full_name))) AS \"__value\"",
            "s1.__value AS \"l\"",
        ],
    );
}

#[test]
fn a_subquery_reading_a_relationship_or_a_value_is_driven_by_a_copy_of_the_rows() {
    // The rows so far, distinct by what the subquery reads, are its input;
    // the segment stays open (no `with_` CTE).
    let got = squash(&sql(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE EXISTS { ()-[r]->(c) WHERE c.age > 3 } \
         RETURN a.name",
    ));
    for p in [
        "subquery_d1 AS ( SELECT DISTINCT",
        "FROM subquery_d1 AS d1",
        "LEFT JOIN subquery_s2 AS s2 ON",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
    assert!(!got.contains("with_w"), "{got}");
    // A value read only by the WHERE can be NULL and decide it: joined
    // NULL-safely.
    has(
        "MATCH (a:User) WITH a.age AS g MATCH (b:User) WHERE EXISTS { MATCH (c:User) WHERE c.age = g } \
         RETURN b.name",
        &["isNotDistinctFrom("],
    );
    // A drive copies the rows without the subqueries applied to them before
    // (each would otherwise copy every earlier one).
    let got = squash(&sql(
        "MATCH (a:User) WITH a.age AS g RETURN g, EXISTS { MATCH (b:User) WHERE b.age = g } AS e, \
         EXISTS { MATCH (b:User) WHERE b.age > g } AS f",
    ));
    let second_drive = got.find("subquery_d4 AS (").expect(&got);
    let its_end = got.find("subquery_s5 AS (").expect(&got);
    let its_body = &got[second_drive..its_end];
    assert!(!its_body.contains("subquery_s3"), "{its_body}");
}

#[test]
fn nested_subqueries_are_applied_to_their_matches() {
    // The inner one, reading the outermost node's property, is applied in
    // the outer one's matches, whose CTE is named after it.
    let got = squash(&sql(
        "MATCH (a:User) WHERE EXISTS { (a)-[:FOLLOWS]->(b) WHERE NOT EXISTS { \
         (b)-[:FOLLOWS]->(c) WHERE c.age < a.age } } RETURN a.name",
    ));
    for p in [
        "subquery_s2 AS (",
        "subquery_s3 AS (",
        "LEFT JOIN subquery_s2 AS s2 ON",
        "WHERE NOT coalesce(s2.__value, false)",
        "LEFT JOIN subquery_s3 AS s3 ON",
    ] {
        assert!(got.contains(p), "missing `{p}` in\n{got}");
    }
}

#[test]
fn a_subquery_in_an_optional_match_decides_its_matches() {
    let got = squash(&sql(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b) WHERE EXISTS { (b)-[:LIKED]->() } \
         RETURN a.name, b.name",
    ));
    let optional = got.find("optional_o").expect(&got);
    let applied = got.find("LEFT JOIN subquery_s1").expect(&got);
    assert!(applied > optional, "applied inside the matches:\n{got}");
}

#[test]
fn a_with_where_reading_a_subquery_filters_the_projected_rows() {
    // After the LIMIT (#1311): the WHERE reads the projected node.
    has(
        "MATCH (a:User) WITH a ORDER BY a.name LIMIT 3 WHERE EXISTS { (a)-[:FOLLOWS]->() } \
         RETURN a.name",
        &["LIMIT 3", "FROM with_w1 AS w1 LEFT JOIN subquery_s2"],
    );
}

#[test]
fn a_subquery_whose_pattern_matches_nothing_is_a_constant() {
    has(
        "MATCH (a:User) RETURN a.name, EXISTS { (a)-[:FOLLOWS]->(:Post) } AS e, \
         size((a)-[:FOLLOWS]->(:Post)) AS c, [(a)-[:FOLLOWS]->(p:Post) | p.title] AS l",
        &[
            "CAST(false AS Nullable(Bool)) AS \"e\"",
            "0 AS \"c\"",
            "[] AS \"l\"",
        ],
    );
}

#[test]
fn subquery_shapes_not_lowered() {
    // In a comprehension, reading none of its elements: one value a row
    // (S11o9).
    has(
        "MATCH (a:User) RETURN [x IN [1, 2] WHERE EXISTS { (a)-[:FOLLOWS]->() }] AS l",
        &["arrayFilter(v4 -> coalesce(s1.__value, false), [1, 2]) AS \"l\""],
    );
    not_lowered(
        "MATCH (a:User) RETURN [x IN [1, 2] WHERE EXISTS { (a)-[:FOLLOWS]->(b) WHERE b.age > x }] \
         AS l",
        "inside a list comprehension",
    );
    not_lowered(
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(b) | b] AS l",
        "a pattern comprehension of nodes",
    );
    // An ORDER BY reading a projected name.
    not_lowered(
        "MATCH (a:User) RETURN a ORDER BY size((a)-[:FOLLOWS]->())",
        "reading a projected name",
    );
    not_lowered(
        "MATCH (a:User), (b:User) WITH a, count(b) AS n ORDER BY EXISTS { (a)-[:FOLLOWS]->() } \
         RETURN a.name",
        "reading a projected name, or of a DISTINCT or aggregating projection",
    );
    // A shortest path's WHERE conjunct reading the path and a subquery.
    not_lowered(
        "MATCH (a:User), (b:User) MATCH p = shortestPath((a)-[:FOLLOWS*]->(b)) \
         WHERE length(p) > size((a)-[:FOLLOWS]->()) RETURN length(p)",
        "a shortestPath's WHERE conjunct",
    );
}

/// `count()` of a path or a path's list, `length()` of a path's nodes /
/// relationships (their size, as Neo4j 4 read it; the legacy pipeline answers
/// it) and `size()` of a carried
/// list an OPTIONAL MATCH may leave NULL (P-4c S11a).
#[test]
fn counts_and_lengths_of_paths_and_their_lists() {
    for q in [
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.name, count(p) AS c",
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN count(DISTINCT p) AS c",
        "MATCH (a:User) OPTIONAL MATCH p = (a)-[:AUTHORED]->(x:Post) RETURN count(p) AS c",
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN count(r) AS c, count(DISTINCT r) AS d",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN length(nodes(p)) AS n",
        "MATCH (a:User) OPTIONAL MATCH p = (a)-[:AUTHORED]->(x:Post) \
         WITH a, relationships(p) AS rs RETURN a.name, size(rs) AS n",
        // A signed size mixes with a negative number in a grouping key.
        "MATCH (a:User) OPTIONAL MATCH p = (a)-[:FOLLOWS*1..2]->(b:User) \
         WITH a, relationships(p) AS rs RETURN coalesce(size(rs), -1) AS s, count(*) AS c",
    ] {
        sql(q);
    }
    // count(p) reads no element value of the walk (review: building them
    // made it 20x slower).
    let counted = sql("MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(p) AS c");
    assert!(!counted.contains("map("), "{counted}");
    // A path that matches nothing counts 0, in one row.
    has(
        "MATCH p = (a:User)-[:NOPE]->(b:User) RETURN count(p) AS c",
        &["CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END AS \"c\""],
    );
}

/// `count()` / `count(DISTINCT)` of a list of nodes or relationships: its
/// rows, or its distinct lists by their elements' tuples in order, as
/// DISTINCT compares them (P-4c S11b, #1375; the legacy fallback miscounted
/// it).
#[test]
fn counts_of_lists_of_nodes_and_relationships() {
    for q in [
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH relationships(p) AS rs \
         RETURN count(DISTINCT rs) AS c, count(rs) AS d",
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) WITH nodes(p) AS ns \
         RETURN count(DISTINCT ns) AS c",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs \
         RETURN count(DISTINCT bs) AS c, count(bs) AS d",
    ] {
        sql(q);
    }
    // A list built over a list that may be NULL (`split(NULL)` is an array
    // here, NULL in Cypher) is not counted: it falls back (review, #1377).
    not_lowered(
        "MATCH (a:User) WITH [x IN split(CASE WHEN a.age > 26 THEN 'x' END, ',') | a] AS l \
         RETURN count(l) AS d",
        "count() of lists of nodes or relationships",
    );
    // Distinct by whole tuples: parallel relationships of a type without
    // `edge_id` (same ends) are told apart by their properties (review).
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs RETURN count(DISTINCT bs) AS c",
        &["count(DISTINCT w1.v4)"],
    );
}

/// Lists of lists of nodes or relationships: `collect()` of a list that is
/// never NULL, and returned lists of lists as their elements' values, a
/// parameter per depth (P-4c S11c).
#[test]
fn lists_of_lists_of_nodes_and_relationships() {
    for q in [
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) WITH a, count(DISTINCT relationships(p)) AS c, \
         collect(relationships(p)) AS rss RETURN a.name, c, size(rss) AS n",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN a.name, collect(nodes(p)) AS nss",
        "MATCH p = (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(nodes(p)) AS nss \
         UNWIND nss AS ns RETURN a.name, size(ns) AS n",
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs RETURN collect(bs) AS bss",
    ] {
        sql(q);
    }
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN [[a, b], [b]] AS l",
        &["arrayMap(__cg_e -> arrayMap(__cg_e1 -> "],
    );
    // A list that may be NULL is not collected (`split(NULL)`, #1377).
    not_lowered(
        "MATCH (a:User) WITH a, [x IN split(CASE WHEN a.age > 26 THEN 'x' END, ',') | a] AS l \
         RETURN collect(l) AS ls",
        "collect() of lists of nodes or relationships",
    );
}

/// `label(n)`: a node's one label as a string (a ClickGraph function, as the
/// legacy pipeline reads it); `id()` of an element that matches nothing is
/// NULL in a query with no rows (P-4c S11d).
#[test]
fn the_label_of_a_node_and_ids_of_what_matches_nothing() {
    has(
        "MATCH (u:User)-[:FOLLOWS]->(x:User) RETURN label(x) AS l",
        &["'User' AS \"l\""],
    );
    has(
        "MATCH (n) RETURN DISTINCT label(n) AS l",
        &["__cg_label AS \"l\""],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN label(b) AS l",
        &["CASE WHEN o1.v2__user_id IS NULL THEN NULL ELSE 'User' END AS \"l\""],
    );
    sql("MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs RETURN [x IN bs | label(x)] AS l");
    has(
        "MATCH (a:Post)-[r:AUTHORED]->(b:User) RETURN type(r), id(a), labels(a) LIMIT 5",
        &["NULL AS \"id(a)\"", "WHERE false"],
    );
    // The value type of what an OPTIONAL MATCH of the impossible left NULL.
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:AUTHORED]->(p:User) RETURN valueType(p) AS t",
        &["'NULL' AS \"t\""],
    );
    // label() of a relationship: neither pipeline reads it.
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN label(r) AS l",
        "label() of a node or relationship",
    );
}

#[test]
fn ids_compared_with_integers() {
    // The library's `id(n)` is the node's key.
    has(
        "MATCH (a:User)-[:FOLLOWS]->(o:User) WHERE id(a) = 2 RETURN o.name",
        &["WHERE v0.user_id = '2'"],
    );
    has(
        "MATCH (a:User) WHERE NOT id(a) IN [1, 2] RETURN a.name",
        &["WHERE NOT (v0.user_id = '1' OR v0.user_id = '2')"],
    );
    has(
        "MATCH (a:User) WHERE 3 <> id(a) RETURN a.name",
        &["NOT v0.user_id = '3'"],
    );
    // Of a node of several labels: its key, of whichever label.
    has(
        "MATCH (a) WHERE id(a) IN [1] RETURN a.name",
        &["toString(v0.__cg_id_0) = '1'"],
    );
    // `x IN []` is false, `x` NULL or not.
    has(
        "MATCH (a:User) WHERE id(a) IN [] RETURN a.name",
        &["WHERE false"],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN NOT id(b) IN [] AS x",
        &["CAST(NOT false AS Nullable(Bool)) AS \"x\""],
    );
    // On the server an integer is one of its ids: the node of that label
    // and key, or none.
    let server = LowerOptions {
        id_lookup: Some(IdLookup {
            node: std::sync::Arc::new(|id| match id {
                101 => Some(("User".to_string(), "2".to_string())),
                102 => Some(("Post".to_string(), "5".to_string())),
                104 => Some(("User".to_string(), "p|q".to_string())),
                _ => None,
            }),
            element: std::sync::Arc::new(|id| match id {
                201 => Some("FOLLOWS:7:1->2-".to_string()),
                202 => Some("LIKED:9:1->5-".to_string()),
                203 => Some("FOLLOWS:a\\:'b:1->2-".to_string()),
                _ => None,
            }),
        }),
        ..LowerOptions::default()
    };
    let on_server = |q: &str, parts: &[&str]| {
        let got = lowered(q, &social(), &server);
        for p in parts {
            assert!(got.contains(p), "{q}\nmissing `{p}` in\n{got}");
        }
    };
    on_server(
        "MATCH (a:User) WHERE id(a) IN [101, 102, 103] RETURN a.name",
        &["WHERE v0.user_id = '2'"],
    );
    on_server(
        "MATCH (a) WHERE id(a) = 102 RETURN a.name",
        &["(v0.__cg_label = 'Post' AND toString(v0.__cg_id_0) = '5')"],
    );
    on_server(
        "MATCH (a:User) WHERE id(a) = 102 RETURN a.name",
        &["WHERE false"],
    );
    // A key of one column is the id's whole text.
    on_server(
        "MATCH (a:User) WHERE id(a) = 104 RETURN a.name",
        &["WHERE v0.user_id = 'p|q'"],
    );
    on_server(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN id(b) = 103 AS x",
        &["CASE WHEN o1.v2__user_id IS NULL THEN NULL ELSE false END AS \"x\""],
    );
    // ORDER BY an `id()` sorts by the key, as its value is.
    has(
        "MATCH (a:User) RETURN a.name, id(a) AS i ORDER BY i DESC",
        &["v0.user_id AS \"i\"", "ORDER BY v0.user_id DESC"],
    );
    has(
        "MATCH (a:User) RETURN a.name ORDER BY id(a)",
        &["ORDER BY v0.user_id ASC"],
    );
    // Ordered against an integer: by the key (S11l).
    has(
        "MATCH (a:User) WHERE id(a) > 3 RETURN a.name",
        &["WHERE v0.user_id > 3"],
    );
    // A relationship's id is Bolt's number for it, only the server's.
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) = 1 RETURN b.name",
        "id() of a relationship other than",
    );
    // On the server (#1410): the relationship of the element id it gave the
    // number, compared with the relationship's (`value.rs`); of another
    // type, or a number it did not give, none.
    let follows = format!(
        "concat('FOLLOWS:', concat({}), ':', toString(v1.follower_id), '->', toString(v1.followed_id), '-')",
        edge_text("v1.follow_id")
    );
    on_server(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) = 201 RETURN b.name",
        &[&format!("WHERE {follows} = 'FOLLOWS:7:1->2-'")],
    );
    on_server(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) IN [201, 202, 203, 204] RETURN b.name",
        &[&format!(
            r"WHERE ({follows} = 'FOLLOWS:7:1->2-' OR {follows} = 'FOLLOWS:a\\:''b:1->2-')"
        )],
    );
    on_server(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE NOT id(r) IN [201] RETURN b.name",
        &[&format!("WHERE NOT {follows} = 'FOLLOWS:7:1->2-'")],
    );
    on_server(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) = 202 RETURN b.name",
        &["WHERE false"],
    );
    // A relationship of several types: by its element id of its row's type.
    on_server(
        "MATCH (a:User)-[r:FOLLOWS|LIKED]->(b) WHERE id(r) = 202 RETURN b",
        &[
            "concat(v1.__cg_type, ':', CASE WHEN",
            "-') = 'LIKED:9:1->5-'",
        ],
    );
    // SQL that read the server's ids holds only while they are known: the
    // translation says so, and the server does not cache it (review).
    let with_mapper = |q: &str| {
        translate_bound_plan(
            q,
            &social(),
            &ReadOptions {
                id_mapper: Some(crate::server::bolt_protocol::id_mapper::IdMapper::new()),
                ..ReadOptions::default()
            },
        )
        .unwrap()
        .session_ids
    };
    assert!(with_mapper(
        "MATCH (a:User)-[r:FOLLOWS]->(b) WHERE id(r) = 201 RETURN b.name"
    ));
    assert!(with_mapper("MATCH (a:User) WHERE id(a) = 5 RETURN a.name"));
    assert!(!with_mapper(
        "MATCH (a:User) WHERE a.user_id = 5 RETURN a.name"
    ));
    // A RETURN item of one is a boolean (review).
    on_server(
        "MATCH (a:User)-[r:FOLLOWS]->(b) RETURN id(r) = 201 AS x",
        &[&format!(
            "CAST({follows} = 'FOLLOWS:7:1->2-' AS Nullable(Bool)) AS \"x\""
        )],
    );
    // An OPTIONAL MATCH's NULL relationship: NULL.
    on_server(
        "MATCH (a:User) OPTIONAL MATCH (a)-[r:FOLLOWS]->(b:User) RETURN id(r) = 999 AS x",
        &["CASE WHEN", "IS NULL THEN NULL ELSE false END AS \"x\""],
    );
}

/// S11l (user 2026-10-11): `id()` as a value. A node's is its key; ids
/// compared with ids compare the elements; a WITH-carried node id is still
/// one when returned; a relationship's RETURN item holds its element id,
/// which the server and the library return as Bolt's number.
#[test]
fn ids_as_values() {
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE id(a) <> id(b) RETURN count(*) AS n",
        &["WHERE v0.user_id <> v2.user_id"],
    );
    // Of different labels: never equal, whatever their keys.
    has(
        "MATCH (a:User)-[:AUTHORED]->(p:Post) WHERE id(a) = id(p) RETURN count(*) AS n",
        &["WHERE false"],
    );
    let carried = translate_bound_plan(
        "MATCH (a:User) WITH id(a) AS i RETURN i",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap();
    assert!(
        matches!(&carried.shape[0].kind, ResultKind::NodeId { label } if label == "User"),
        "{:?}",
        carried.shape
    );
    let rel = translate_bound_plan(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN id(r) AS i",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap();
    assert!(
        matches!(rel.shape[0].kind, ResultKind::ElementIdNumber),
        "{:?}",
        rel.shape
    );
    assert!(
        squash(&rel.sql).contains(
            "toString(v1.follow_id), '\\\\', '\\\\\\\\'), ':', '\\\\:'), '|', '\\\\|') END), ':', \
             toString(v1.follower_id), '->', toString(v1.followed_id), '-') AS \"i\""
        ),
        "{}",
        rel.sql
    );
    // Of several types: its element id of its row's type (I3b).
    let rels = translate_bound_plan(
        "MATCH (a:User)-[r:FOLLOWS|LIKED]->(b) RETURN id(r) AS i",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap();
    assert!(
        matches!(rels.shape[0].kind, ResultKind::ElementIdNumber)
            && squash(&rels.sql).contains("concat(v1.__cg_type, ':', CASE WHEN"),
        "{:?}\n{}",
        rels.shape,
        rels.sql
    );
    // Bolt's number is computed after the query: nothing orders by it.
    for q in [
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN id(r) AS i ORDER BY i",
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN id(r) AS i ORDER BY id(r) DESC",
    ] {
        not_lowered(q, "ORDER BY an id() the server numbers");
    }
    has(
        "MATCH (a:User)-[r:FOLLOWS]->(b:User), (c:User)-[s:FOLLOWS]->(d:User) \
         WHERE id(r) <> id(s) RETURN count(*) AS n",
        &["v1.follow_id <> v4.follow_id"],
    );
}

/// `id(n)` of a node of several labels (I5b, #1383) is the node's element id,
/// which the server and the library return as Bolt's number for it, as a
/// relationship's is; a UNION column of `id()`s of different labels is so in
/// every arm.
#[test]
fn id_of_a_node_of_several_labels_is_its_element_id() {
    let (sql, shape) = shaped("MATCH (n) RETURN id(n) AS i", &social());
    assert!(
        matches!(shape[0].kind, ResultKind::ElementIdNumber)
            && sql.contains(r#"concat(v0.__cg_label, ':', toString(v0.__cg_id_0), '-') AS "i""#),
        "{shape:?}\n{sql}"
    );
    // Sorted by the node's identity, as `ORDER BY id(n)` is.
    let (sql, _) = shaped("MATCH (n) RETURN id(n) AS i ORDER BY i DESC", &social());
    assert!(
        sql.ends_with("ORDER BY v0.__cg_label DESC NULLS FIRST, v0.__cg_id_0 DESC NULLS FIRST"),
        "{sql}"
    );
    // NULL where an OPTIONAL MATCH found no node.
    let (sql, _) = shaped(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS|LIKED]->(m) RETURN id(m) AS i",
        &social(),
    );
    assert!(
        sql.contains(
            "CASE WHEN o1.v2____cg_label IS NULL THEN NULL ELSE \
             concat(o1.v2____cg_label, ':', toString(o1.v2____cg_id_0), '-') END"
        ),
        "{sql}"
    );
    // An expression of the item would read the element id's text.
    for q in [
        "MATCH (n) RETURN id(n) AS i ORDER BY i + 1",
        "MATCH (n) RETURN id(n) AS i ORDER BY toString(i)",
        "MATCH (n) RETURN id(n) AS i ORDER BY [i]",
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN id(r) AS i ORDER BY toString(i)",
    ] {
        not_lowered(q, "ORDER BY an expression of an id() the server numbers");
    }
    // No identity column is a column of a grouped or distinct result.
    for q in [
        "MATCH (n) RETURN id(n) AS i, count(*) AS c ORDER BY i",
        "MATCH (n) RETURN DISTINCT id(n) AS i ORDER BY i",
    ] {
        not_lowered(q, "ORDER BY an id() the server numbers");
    }
    let (sql, _) = shaped("MATCH (n) RETURN id(n) AS i, count(*) AS c", &social());
    assert!(
        sql.contains(r#"GROUP BY concat(v0.__cg_label, ':', toString(v0.__cg_id_0), '-')"#),
        "{sql}"
    );
    // Of different labels in a UNION column: every arm returns the element
    // id, under UNION as under UNION ALL (a text needs no element identity).
    for union in ["UNION ALL", "UNION"] {
        let (sql, shape) = shaped(
            &format!("MATCH (a:User) RETURN id(a) AS x {union} MATCH (p:Post) RETURN id(p) AS x"),
            &social(),
        );
        assert!(
            matches!(shape[0].kind, ResultKind::ElementIdNumber)
                && sql.contains("concat('User:', toString(v0.user_id), '-')")
                && sql.contains("concat('Post:', toString(v2.post_id), '-')"),
            "{union}: {shape:?}\n{sql}"
        );
    }
    // Of one label in every arm: the keys, as before.
    let (_, shape) = shaped(
        "MATCH (a:User) RETURN id(a) AS x UNION ALL MATCH (b:User) RETURN id(b) AS x",
        &social(),
    );
    assert!(
        matches!(&shape[0].kind, ResultKind::NodeId { label } if label == "User"),
        "{shape:?}"
    );
    // A node of several labels and a relationship in one column.
    let (_, shape) = shaped(
        "MATCH (n) RETURN id(n) AS x UNION ALL MATCH ()-[r:FOLLOWS]->() RETURN id(r) AS x",
        &social(),
    );
    assert!(
        matches!(shape[0].kind, ResultKind::ElementIdNumber),
        "{shape:?}"
    );
    // A key a WITH carried cannot be read as the element id.
    not_lowered(
        "MATCH (a:User) WITH id(a) AS i RETURN i AS x \
         UNION ALL MATCH (p:Post) RETURN id(p) AS x",
        "one carried by a WITH",
    );
}

/// A UNION ALL column whose arms hold nodes of different labels, or
/// relationships of different definitions, is a value in every arm (S11f).
#[test]
fn union_columns_of_different_labels_or_definitions() {
    let node = ResultKind::Graph(GraphType::Node);
    let (sql, shape) = shaped(
        "MATCH (u:User) RETURN u AS x, 1 AS k UNION ALL MATCH (p:Post) RETURN p AS x, 2 AS k",
        &social(),
    );
    assert_eq!(
        kinds(&shape),
        vec![column("x", node.clone()), column("k", ResultKind::Value)]
    );
    assert_eq!(sql.matches("map('elementId'").count(), 2, "{sql}");
    // A node of several labels in another arm.
    let (_, shape) = shaped(
        "MATCH (a:User)-->(o) RETURN o UNION ALL MATCH (a:User)<--(o) RETURN o",
        &social(),
    );
    assert_eq!(kinds(&shape), vec![column("o", node)]);
    // UNION (not ALL): the arms identify such elements differently.
    not_lowered(
        "MATCH (u:User) OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) RETURN p AS x \
         UNION MATCH (n) WHERE n.post_id = 2 RETURN n AS x",
        "UNION (not ALL) of a column of nodes of different labels",
    );
    // Its properties as columns: of different names in each arm.
    not_lowered(
        "MATCH (u:User) RETURN u.* UNION ALL MATCH (u:Post) RETURN u.*",
        "`v.*` in a UNION column of different labels",
    );
    // Relationships of different types.
    let (_, shape) = shaped(
        "MATCH ()-[r:FOLLOWS]->() RETURN r AS x UNION ALL MATCH ()-[r:LIKED]->() RETURN r AS x",
        &social(),
    );
    assert_eq!(
        kinds(&shape),
        vec![column("x", ResultKind::Graph(GraphType::Relationship))]
    );
    // A node and a relationship, or a node and a value: not lowered.
    not_lowered(
        "MATCH (u:User) RETURN u AS x UNION ALL MATCH ()-[r:FOLLOWS]->() RETURN r AS x",
        "of a different kind or label",
    );
    not_lowered(
        "MATCH (u:User) RETURN u AS x UNION ALL MATCH (p:Post) RETURN p.title AS x",
        "of a different kind or label",
    );
}

/// S11h: an edge `constraints:` expression holds of every relationship of
/// its definition, read either way, in a union of definitions, along a
/// walk and in a shortest-path search.
#[test]
fn edge_constraints_keep_the_relationships_they_hold_of() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_constraints
graph_schema:
  nodes:
    - label: User
      database: db
      table: users
      node_id: user_id
      property_mappings: { user_id: user_id, age: age, born: born_year }
    - label: Post
      database: db
      table: posts
      node_id: post_id
      property_mappings: { post_id: post_id, ts: ts, label: computed }
  edges:
    - type: FOLLOWS
      database: db
      table: follows
      from_id: src
      to_id: dst
      from_node: User
      to_node: User
      property_mappings: {}
      constraints: "from.age > to.age OR from.born = to.born"
    - type: KNOWS
      database: db
      table: knows
      from_id: src
      to_id: dst
      from_node: User
      to_node: User
      property_mappings: {}
    - type: WROTE
      database: db
      table: wrote
      from_id: uid
      to_id: pid
      from_node: User
      to_node: Post
      property_mappings: {}
      constraints: "from.born <= to.ts"
    - type: TAGGED
      database: db
      table: tagged
      from_id: uid
      to_id: pid
      from_node: User
      to_node: Post
      property_mappings: {}
      constraints: "from.age < to.nonesuch"
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let sql = |q: &str| {
        translate_bound_plan(q, &schema, &ReadOptions::default())
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .sql
    };
    let has = |q: &str, parts: &[&str]| {
        let got = squash(&sql(q));
        for p in parts {
            assert!(got.contains(p), "{q}\nmissing `{p}` in\n{got}");
        }
    };
    // One definition, each way: its stored `from` is the node it leaves.
    has(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id, b.user_id",
        &["WHERE (v0.age > v2.age OR v0.born_year = v2.born_year)"],
    );
    has(
        "MATCH (a:User)<-[:FOLLOWS]-(b:User) RETURN a.user_id, b.user_id",
        &["WHERE (v2.age > v0.age OR v2.born_year = v0.born_year)"],
    );
    has(
        "MATCH (u:User)-[:WROTE]->(p:Post) RETURN p.post_id",
        &["WHERE v0.born_year <= v2.ts"],
    );
    // Read both ways: each row says which way it was stored.
    has(
        "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN count(*) AS n",
        &[
            r#"1 AS "__cg_forward""#,
            r#"0 AS "__cg_forward""#,
            "WHERE CASE WHEN v1.__cg_forward = 1 THEN (v0.age > v2.age OR v0.born_year = \
             v2.born_year) ELSE (v2.age > v0.age OR v2.born_year = v0.born_year) END",
        ],
    );
    // A union's constrained definition keeps its rows the constraint holds
    // of; the other's arm is unchanged.
    let union = squash(&sql(
        "MATCH (a:User)-[:FOLLOWS|KNOWS]->(b:User) RETURN count(*) AS n",
    ));
    assert!(
        union.contains(
            "FROM db.follows AS e JOIN db.users AS __cg_cfrom ON __cg_cfrom.user_id = e.src \
             JOIN db.users AS __cg_cto ON __cg_cto.user_id = e.dst \
             WHERE (__cg_cfrom.age > __cg_cto.age OR __cg_cfrom.born_year = __cg_cto.born_year)"
        ),
        "{union}"
    );
    assert_eq!(union.matches("__cg_cfrom ON").count(), 1, "{union}");
    // A directed walk of one definition from its `from` end: the
    // generator's, forward, carrying what the next step reads of the node it
    // leaves; the constraint parenthesized among the step's conditions (an
    // `OR` would escape the hop limit and uniqueness: review).
    let walk = squash(&sql(
        "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) WHERE a.user_id = 7 RETURN b.user_id",
    ));
    for part in [
        "WHERE (start_node.age > end_node.age OR start_node.born_year = end_node.born_year)",
        "end_node.age as end_cg_age",
        "end_node.born_year as end_cg_born",
        "AND (vp.end_cg_age > end_node.age OR vp.end_cg_born = end_node.born_year)",
        "JOIN db.follows AS rel ON start_node.user_id = rel.src",
    ] {
        assert!(walk.contains(part), "missing `{part}` in\n{walk}");
    }
    // Undirected, a shortest path, or restricted only at its `to` end (the
    // generator would walk forward from every node: review): the walk of
    // its definitions, over the relationships the constraint keeps.
    for q in [
        "MATCH (a:User)-[:FOLLOWS*1..2]-(b:User) RETURN count(*) AS n",
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*1..3]->(b:User)) \
         WHERE a.user_id = 1 AND b.user_id = 2 RETURN length(p)",
        "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) WHERE b.user_id = 7 RETURN a.user_id",
    ] {
        has(
            q,
            &["WHERE (__cg_cfrom.age > __cg_cto.age OR __cg_cfrom.born_year = __cg_cto.born_year)"],
        );
    }
    // An OPTIONAL MATCH reads the node it shares from its table: the
    // constraint reads its properties, also when another relationship of
    // the pattern reaches it first (review).
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.user_id, b.user_id",
        &["JOIN db.users AS v2 ON v1.dst = v2.user_id WHERE (v0.age > v2.age"],
    );
    has(
        "MATCH (a:User) OPTIONAL MATCH (b:User)<-[:KNOWS]-(a)-[:FOLLOWS]->(c:User) \
         RETURN a.user_id, b.user_id, c.user_id",
        &["WHERE (d2.p2_v0_age > v4.age OR d2.p2_v0_born = v4.born_year)"],
    );
    // A drive exports what the constraint reads of the shared node.
    has(
        "MATCH (a:User)-[:KNOWS]->(x:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) \
         WHERE b.user_id > x.user_id RETURN a.user_id, b.user_id",
        &["AS \"p2_v0_age\""],
    );
    // A constraint of a property the node does not declare as a column is
    // not lowered (the generator could not apply it either).
    match translate_bound_plan(
        "MATCH (u:User)-[:TAGGED]->(p:Post) RETURN p.post_id",
        &schema,
        &ReadOptions::default(),
    ) {
        Err(e) if e.contains("edge constraint `from.age < to.nonesuch`") => {}
        other => panic!("expected the constraint refused, got {other:?}"),
    }
}

/// S11n: a lambda written as a higher-order function's argument (the
/// `ch.*` passthrough) is the dialect's lambda over its local parameters,
/// which shadow variables of the same name.
#[test]
fn lambdas_of_higher_order_functions() {
    has(
        "RETURN ch.arrayMap((x, y) -> x + y, [1, 2], [10, 20]) AS s",
        &[r#"arrayMap((v0, v1) -> v0 + v1, [1, 2], [10, 20]) AS "s""#],
    );
    has(
        "MATCH (u:User) WITH collect(u.user_id) AS ids \
         RETURN ch.arrayMap(x -> x * 2, ch.arrayFilter(n -> n > 3, ids)) AS big",
        &["arrayMap(v2 -> v2 * 2, arrayFilter(v3 -> v3 > 3, w1.v1))"],
    );
    // A parameter named as a node shadows it.
    has(
        "MATCH (u:User) RETURN ch.arrayFilter(u -> u > 1, [1, 2]) AS l",
        &["arrayFilter(v1 -> v1 > 1, [1, 2])"],
    );
    // Its value is read as a value: a node, relationship or map the body
    // gives would be read as its tuple (S11n review).
    for q in [
        "MATCH (a:User) RETURN ch.arrayMap(x -> a, [1]) AS r",
        "MATCH (a:User)-[f:FOLLOWS]->(b:User) RETURN ch.arrayMap(x -> f, [1]) AS r",
        "MATCH (a:User) RETURN ch.arrayMap(x -> [a], [1]) AS r",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN ch.arrayMap(x -> nodes(p), [1]) AS r",
        "MATCH (a:User) WITH collect(a) AS us RETURN ch.arrayMap(x -> us, [1]) AS r",
        "MATCH (a:User) RETURN ch.arrayMap(x -> {k: x}, [1]) AS r",
    ] {
        not_lowered(q, "as a lambda's value");
    }
    // Its properties and identity are values.
    has(
        "MATCH (a:User) RETURN ch.arrayMap(x -> a.name, [1]) AS r",
        &["arrayMap(v1 -> v0.full_name, [1])"],
    );
}

/// S12c: on Spark a node, relationship or path value is a VARIANT object
/// (`to_variant_object` of a `MAP<STRING, VARIANT>`), a path an
/// `ARRAY<VARIANT>` of them; a composite identity's key is a JSON object of
/// its parts' texts (`CAST(struct AS STRING)` quotes none).
#[test]
fn databricks_graph_values() {
    let got = squash(
        &databricks_sql(
            "MATCH p = (u:User)-[r:FOLLOWS]->(f:User) RETURN p",
            &social(),
        )
        .unwrap(),
    );
    for part in [
        "SELECT array(to_variant_object(map('elementId', CAST(concat('User:', \
         CAST(v0.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), \
         'properties', CAST(to_variant_object(map_filter(map('age', CAST(v0.age AS VARIANT),",
        "(__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', \
         CAST(concat('FOLLOWS:', concat(CASE WHEN v1.follow_id IS NULL THEN '\\\\N' ELSE \
         replace(replace(replace(CAST(v1.follow_id AS STRING), '\\\\', '\\\\\\\\'), ':', \
         '\\\\:'), '|', '\\\\|') END), ':', CAST(v1.follower_id AS STRING), '->', \
         CAST(v1.followed_id AS STRING), '-') AS VARIANT)",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    let got = squash(
        &databricks_sql(
            "MATCH (a:Acct)-[r:SENT|PAID]->(b:Acct) RETURN r",
            &composite_walk_schema(),
        )
        .unwrap(),
    );
    assert!(
        got.contains(
            "concat('SENT:Acct:Acct:', to_json(named_struct('_1', CAST(e.fb AS STRING), \
             '_2', CAST(e.fnum AS STRING), '_3', CAST(e.tb AS STRING), \
             '_4', CAST(e.tnum AS STRING)), map('ignoreNullFields', 'false')))"
        ),
        "{got}"
    );
}

/// S12c review: a path's key spells a relationship (or a composite node)
/// alike whether a fixed hop or a walk holds it: the walk's typed tuples are
/// cast to strings field by field (`tuple_value_text`), as `tuple_text`
/// spells loose columns. Else one path cut two ways is two keys (DISTINCT
/// paths overcounted on Spark: 22 for 16).
#[test]
fn databricks_composite_keys_agree() {
    let got = squash(
        &databricks_sql(
            "MATCH p = (a:Acct)-[:SENT*0..1]->(b:Acct)-[:SENT]->(c:Acct)-[:SENT*0..1]->(d:Acct) \
             RETURN count(DISTINCT p) AS n",
            &composite_walk_schema(),
        )
        .unwrap(),
    );
    for part in [
        // A walk's relationships, typed tuples of four columns.
        "transform(v1.path_edges, __x -> concat('SENT:Acct:Acct:', \
         to_json(CAST(__x AS STRUCT<_1: STRING, _2: STRING, _3: STRING, _4: STRING>), \
         map('ignoreNullFields', 'false'))))",
        // A walk's composite nodes.
        "to_json(CAST(struct(end_node.bank, end_node.num) AS STRUCT<_1: STRING, _2: STRING>), \
         map('ignoreNullFields', 'false'))",
        // A node's key from its columns.
        "to_json(named_struct('_1', CAST(v0.bank AS STRING), '_2', CAST(v0.num AS STRING)), \
         map('ignoreNullFields', 'false'))",
    ] {
        assert!(got.contains(part), "missing `{part}` in\n{got}");
    }
    assert!(!got.contains("CAST(__x AS STRING)"), "{got}");
}

/// S11o2: a union label (`union_labels:`): `Message` of `Post` and `Comment`.
fn union_schema(extra: &str) -> Result<GraphSchema, String> {
    GraphSchemaConfig::from_yaml_str(&format!(
        r#"
name: union_labels
graph_schema:
  nodes:
    - label: Person
      database: db
      table: person
      node_id: id
      property_mappings: {{ id: id, name: name }}
    - label: Post
      database: db
      table: post
      node_id: id
      property_mappings: {{ id: id, content: content, imageFile: image_file }}
    - label: Comment
      database: db
      table: comment
      node_id: id
      property_mappings: {{ id: id, content: content }}
  edges:
    - type: HAS_CREATOR
      database: db
      table: post_creator
      from_id: post_id
      to_id: person_id
      from_node: Post
      to_node: Person
      property_mappings: {{}}
    - type: HAS_CREATOR
      database: db
      table: comment_creator
      from_id: comment_id
      to_id: person_id
      from_node: Comment
      to_node: Person
      property_mappings: {{}}
    - type: REPLY_OF
      database: db
      table: reply_post
      from_id: comment_id
      to_id: post_id
      from_node: Comment
      to_node: Post
      property_mappings: {{}}
{extra}
  union_labels:
    - label: Message
      labels: [Post, Comment]
"#
    ))
    .map_err(|e| e.to_string())?
    .to_graph_schema()
    .map_err(|e| e.to_string())
}

#[test]
fn union_labels() {
    let schema = union_schema("").unwrap();
    let sql = |q: &str| {
        squash(
            &translate_bound_plan(q, &schema, &ReadOptions::default())
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .sql,
        )
    };
    // A Message is a Post or a Comment: both tables, by label.
    let got = sql("MATCH (m:Message) RETURN m.content AS c");
    for part in [
        "FROM db.comment",
        "FROM db.post",
        "'Comment' AS \"__cg_label\"",
    ] {
        assert!(got.contains(part), "missing `{part}` in {got}");
    }
    // Its label test reads the row's label; `labels()` lists the union
    // label after the node's own.
    let got = sql("MATCH (m:Message) RETURN m:Comment AS c, m:Message AS d, labels(m) AS ls");
    for part in [
        "CAST(v0.__cg_label = 'Comment' AS Nullable(Bool)) AS \"c\"",
        "CAST((v0.__cg_label = 'Comment' OR v0.__cg_label = 'Post') AS Nullable(Bool)) AS \"d\"",
        "caseWithExpression(v0.__cg_label, 'Comment', ['Comment', 'Message'], 'Post', \
         ['Post', 'Message'], [v0.__cg_label]) AS \"ls\"",
    ] {
        assert!(got.contains(part), "missing `{part}` in {got}");
    }
    let got = sql("MATCH (p:Post) RETURN p:Message AS m, labels(p) AS ls");
    assert!(got.contains("['Post', 'Message'] AS \"ls\""), "{got}");
    assert!(!got.contains("__cg_label"), "{got}");
    // A relationship of a member: a Message that replies to a Post is a
    // Comment.
    let got = sql("MATCH (m:Message)-[:REPLY_OF]->(p:Post) RETURN count(*) AS n");
    assert!(got.contains("FROM db.comment"), "{got}");
    assert!(!got.contains("__cg_label"), "{got}");
    // `(m:Message|Person)` is a Post, a Comment or a Person.
    let got = sql("MATCH (m:Message|Person) RETURN count(*) AS n");
    assert!(got.contains("FROM db.person"), "{got}");
    // Refused: a union label as a relationship end, as a node label, of a
    // label the schema lacks.
    let end = "    - type: LIKES\n      database: db\n      table: likes\n      from_id: a\n      \
               to_id: b\n      from_node: Person\n      to_node: Message\n      property_mappings: {}";
    let err = union_schema(end).unwrap_err();
    assert!(
        err.contains("relationship end is the union label `Message`"),
        "{err}"
    );
    let err = GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: Post\n      database: db\n      table: post\n      \
         node_id: id\n      property_mappings: { id: id }\n  union_labels:\n    - label: Post\n      \
         labels: [Post]\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap_err()
    .to_string();
    assert!(err.contains("is also a node label"), "{err}");
    let err = GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: Post\n      database: db\n      table: post\n      \
         node_id: id\n      property_mappings: { id: id }\n  union_labels:\n    - label: Message\n      \
         labels: [Post, Nope]\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap_err()
    .to_string();
    assert!(err.contains("`Nope` is not a node label"), "{err}");
}

/// S11o3: a value used as a node (an UNWIND element of collected nodes, as
/// LDBC IC 6 / IC 9 / BI 8 write it) is the clause's node there, tied to it
/// by identity: in the WHERE of an OPTIONAL MATCH too, and one node for
/// every occurrence in the clause.
#[test]
fn a_value_used_as_a_node() {
    let tie = "v3.user_id = tupleElement(w2.v2, 1)";
    has(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x \
         MATCH (x)-[:FOLLOWS]->(b:User) RETURN x.name AS n, b.name AS m",
        &[tie, "ON v4.follower_id = v3.user_id"],
    );
    has(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x \
         MATCH (x)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(x) RETURN count(*) AS n",
        &[tie, "v6.followed_id = v3.user_id"],
    );
    has(
        "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x \
         OPTIONAL MATCH (x)-[:FOLLOWS]->(b:User) RETURN x.name AS n, b.name AS m",
        &[
            "WHERE v3.user_id = tupleElement(d3.v2, 1)",
            "LEFT JOIN optional_o4",
        ],
    );
    // A value that is no node (a number, a relationship): not lowered
    // (Neo4j: a type error); the name as a relationship too.
    for (q, why) in [
        (
            "UNWIND [1, 2] AS x MATCH (x)-[:FOLLOWS]->(b:User) RETURN count(*) AS n",
            "a value not known to be a node used as a node",
        ),
        (
            "MATCH (a:User)-[r:FOLLOWS]->() WITH collect(r) AS l UNWIND l AS x \
             MATCH (x)-[:FOLLOWS]->(y) RETURN count(*) AS n",
            "a value not known to be a node used as a node",
        ),
        (
            "MATCH (a:User) WITH collect(a) AS l UNWIND l AS x MATCH (x)-[x]->(y) \
             RETURN count(*) AS n",
            "is a node, used as a relationship",
        ),
    ] {
        let err = translate_bound_plan(q, &social(), &ReadOptions::default()).unwrap_err();
        assert!(err.contains(why), "{q}: {err}");
    }
}

/// S11o5: `x IN …` of nodes by identity (LDBC IC 3, IC 5, BI 13): a list
/// literal as `x = a OR x = b`; a collected list, or a path's nodes, by its
/// elements' identities (NULL where `x` or the list is).
#[test]
fn a_node_in_a_list() {
    has(
        "MATCH (a:User), (b:User) WHERE a.user_id = 1 AND b.user_id = 2 \
         MATCH (c:User) WHERE c IN [a, b] RETURN c.name AS n",
        &["(v2.user_id = v0.user_id OR v2.user_id = v1.user_id)"],
    );
    has(
        "MATCH (a:User) WHERE a.user_id < 3 WITH collect(a) AS l \
         MATCH (c:User) WHERE NOT c IN l RETURN count(*) AS n",
        &[
            "NOT CAST(CASE WHEN w1.v1 IS NULL THEN NULL WHEN length(w1.v1) = 0 THEN false \
           WHEN v2.user_id IS NULL THEN NULL ELSE length(arrayFilter(__cg_in -> \
           tupleElement(__cg_in, 1) = v2.user_id, w1.v1)) > 0 END AS Nullable(Bool))",
        ],
    );
    has(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User), (c:User) WHERE c IN nodes(p) RETURN c",
        &["arrayFilter(__cg_in -> tupleElement(__cg_in, 1) = v4.user_id"],
    );
    // Of no item: false.
    has(
        "MATCH (a:User) WHERE a IN [] RETURN count(*) AS n",
        &["WHERE false"],
    );
    // `null IN` an empty collected list: false (Neo4j), as `[]` is empty.
    has(
        "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User {user_id: 99}) \
         WITH a, collect(b) AS l OPTIONAL MATCH (a)-[:AUTHORED]->(p:Post {post_id: 99}) \
         RETURN p IN l AS x",
        &["WHEN length(", ") = 0 THEN false"],
    );
}

/// S11o5 review: a part joined from its bound end whose middle node is read
/// from its relationships' columns (denormalized edges) keeps the tie
/// between them (it was left out: a product, 10 rows for 2).
#[test]
fn a_part_from_its_bound_end_over_denormalized_edges() {
    let schema = GraphSchemaConfig::from_yaml_str(include_str!(
        "../../../schemas/test/denormalized_flights.yaml"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = squash(
        &translate_bound_plan(
            "MATCH (a:Airport {code: 'LAX'}) WITH a \
             MATCH (x:Airport)-[:FLIGHT]->(m:Airport)-[:FLIGHT]->(a) RETURN x.code AS x",
            &schema,
            &ReadOptions::default(),
        )
        .unwrap()
        .sql,
    );
    assert!(!got.contains("CROSS JOIN"), "{got}");
}

/// S11o5: a pattern part whose first node is new and whose last the
/// relation has (an earlier clause's) is joined from its last node, not as
/// a product with the relation (LDBC IC 5 ran past 300 s).
#[test]
fn a_part_joined_from_its_bound_end() {
    let got = squash(&sql("MATCH (a:User) WHERE a.user_id = 1 WITH a \
         MATCH (x:User)-[:FOLLOWS]->(a) RETURN count(*) AS n"));
    assert!(!got.contains("CROSS JOIN"), "{got}");
    assert!(
        got.contains(
            "FROM with_w1 AS w1 JOIN test_integration.user_follows_test AS v3 ON \
             v3.followed_id = w1.v1__user_id JOIN test_integration.users_test AS v2 ON \
             v3.follower_id = v2.user_id"
        ),
        "{got}"
    );
}

/// S11o5: right after an aggregating WITH, an OPTIONAL MATCH correlated on
/// all its keys is not joined on its aggregated values (ClickHouse reads
/// the WITH's CTE again for them: a `collect()` in another order joined no
/// match; LDBC IC 5).
#[test]
fn an_optional_match_after_a_grouping_with_joins_on_the_keys() {
    let got = squash(&sql(
        "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, collect(f) AS fs \
         OPTIONAL MATCH (u)-[:FOLLOWS]->(g:User) WHERE g IN fs \
         RETURN u.name AS n, count(g) AS c",
    ));
    assert!(
        got.contains("LEFT JOIN optional_o3 AS o3 ON w1.v3__user_id = o3.v3__user_id GROUP BY"),
        "joined on the collected list: {got}"
    );
}

/// S11o6: `collect(CASE … THEN null ELSE [..] END)` collects the lists only
/// (ClickHouse holds no NULL array: the NULL read `[]`, which it kept), and
/// a list of values grouped by is grouped by its key (its items may be of
/// several types, a `Variant`, which ClickHouse groups by no more than
/// `Dynamic`) (LDBC IC 1).
#[test]
fn a_collect_of_lists_or_null_and_a_list_grouping_key() {
    has(
        "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) \
         RETURN u.user_id AS id, collect(CASE WHEN f IS NULL THEN null ELSE [f.name, f.age] END) AS l",
        &["coalesce(groupArrayIf(CASE WHEN o1.v2__user_id IS NULL THEN [o1.p2_v2_name, \
           o1.p2_v2_age] ELSE [o1.p2_v2_name, o1.p2_v2_age] END, CASE WHEN o1.v2__user_id IS \
           NULL THEN false ELSE true END), [])"],
    );
    // Over ordered rows (S11o6 review): the ordered collect has the flag.
    let got = squash(&sql(
        "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) WITH u, f ORDER BY f.user_id \
         RETURN u.user_id AS id, collect(CASE WHEN f IS NULL THEN null ELSE [f.name, f.age] END) AS l",
    ));
    assert!(!got.contains("THEN [] ELSE"), "{got}");
    assert!(got.contains("THEN false ELSE true END"), "{got}");
    let got = squash(&sql(
        "MATCH (u:User) WITH [u.name, u.age] AS l, count(*) AS c RETURN l, c",
    ));
    assert!(
        got.contains("GROUP BY tuple(multiIf(replaceRegexpOne(dynamicType(CAST("),
        "{got}"
    );
}

/// S11o6: `ORDER BY id(o)` of a node of several labels (the Neo4j
/// Browser's node expand) sorts by its label, then its key: its identity
/// (two labels' keys may be equal: no one number).
#[test]
fn order_by_id_of_a_node_of_several_labels() {
    has(
        "MATCH (a:User) WHERE a.user_id = 1 WITH a, size([(a)--() | 1]) AS allNeighboursCount \
         MATCH path = (a)--(o) RETURN path, allNeighboursCount ORDER BY id(o) LIMIT 100",
        &["ORDER BY v7.__cg_label ASC, v7.__cg_id_0 ASC"],
    );
}

/// S11o7: `p IS [NOT] NULL` of a path of the relation (LDBC IC 13): NULL
/// where a node of it is (an OPTIONAL MATCH left it so).
#[test]
fn a_path_is_null() {
    has(
        "MATCH (a:User {user_id: 1}) OPTIONAL MATCH p = (a)-[:FOLLOWS]->(b:User {user_id: 99}) \
         RETURN p IS NULL AS n",
        &["CAST((v0.user_id IS NULL OR o1.v2__user_id IS NULL) AS Nullable(Bool)) AS \"n\""],
    );
}

/// S11o8: `head(collect({…}))` / `collect({…})[i]` carried by a WITH (LDBC
/// IC 7, BI 14): its keys each a column, from the collected tuples of the
/// map's values (in the rows' order where they have one); a key's node read
/// by its properties (`m.k.p`); a node of several labels carried as the
/// properties read of it; the map read whole, not lowered.
#[test]
fn a_collected_map_read_by_its_keys() {
    has(
        "MATCH (u:User) WITH collect({a: u.name, b: u.age})[0] AS top RETURN top.a AS a, top.b AS b",
        &["CASE WHEN length(arrayMap(t -> t.1, groupArray(tuple(tuple(v0.full_name, \
           v0.age))))) > 0 THEN tupleElement(arrayElementOrNull(arrayMap(t -> t.1, \
           groupArray(tuple(tuple(v0.full_name, v0.age)))), 1), 1) ELSE NULL END AS \"v1__k0\""],
    );
    let got = squash(&sql(
        "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, f ORDER BY f.user_id \
         WITH u, head(collect({msg: f, t: f.age})) AS l RETURN u.name AS n, l.t AS t, l.msg.name AS m",
    ));
    assert!(got.contains("arraySort("), "in the rows' order: {got}");
    // A node of several labels: the properties read of it.
    has(
        "MATCH (u:User)-[:AUTHORED|FOLLOWS]->(x) WITH u, head(collect({m: x})) AS l \
         RETURN l.m.name AS n",
        &["groupArray(tuple(tuple(v2.p2_v2_name)))), 1), 1) ELSE NULL END AS \"v4__k0\""],
    );
    // A missing row (an index past the end): a node's property reads NULL
    // (S11o8 review: a tuple's defaults).
    has(
        "MATCH (u:User)-[:FOLLOWS]->(x:User) WITH u, collect({n: x})[2] AS m \
         RETURN m.n.name AS n",
        &["CASE WHEN w1.v4__k0 THEN", "ELSE NULL END AS \"n\""],
    );
    for (q, why) in [
        (
            "MATCH (u:User) WITH collect({a: u.name})[0] AS top RETURN top",
            "a collected map other than by its keys",
        ),
        (
            "MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, head(collect({n: f})) AS m RETURN m.n",
            "a node or relationship a collected map holds, read whole",
        ),
        (
            "MATCH (u:User)-[:FOLLOWS]->(x:User) WITH u, collect({l: [x.name]})[5] AS m \
             RETURN m.l",
            "a list or map in a collected map",
        ),
        (
            "MATCH (u:User)-[:AUTHORED|FOLLOWS]->(x) WITH u, head(collect({m: x})) AS l \
             RETURN l.m AS m",
            "read whole",
        ),
    ] {
        let err = translate_bound_plan(q, &social(), &ReadOptions::default()).unwrap_err();
        assert!(err.contains(why), "{q}: {err}");
    }
}

/// S11o9: a pattern predicate of a list comprehension's element (LDBC
/// IC 10's `[p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]`)
/// is whether the element is one of the nodes in its place of the matches,
/// by its label and identity: looked for from the list's elements (a row per
/// element, the rows joined by a hash of the list's identities); false for a
/// NULL element, and when the node cannot have the element's label.
#[test]
fn a_pattern_predicate_of_a_comprehension_element() {
    let follows =
        "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) WITH u, collect(f) AS fs ";
    has(
        &format!("{follows}RETURN size([x IN fs WHERE (x)-[:AUTHORED]->()]) AS a"),
        &[
            "cityHash64(arraySort(arrayDistinct(arrayMap(__cg_e -> tuple(tupleElement(__cg_e, \
             1)), w2.v4)))) AS \"__cg_members\"",
            "ARRAY JOIN arrayDistinct(arrayMap(__cg_e -> tuple(tupleElement(__cg_e, 1)), w2.v4)) \
             AS __cg_member",
            "JOIN test_integration.posts_test AS v6 ON v6.author_id = d3.v5__user_id",
            "arrayDistinct(arrayMap(t -> t.1, groupArray(tuple(tuple('User', d3.v5__user_id)))))",
            "CASE WHEN tupleElement(v9, 1) IS NULL THEN false WHEN has(arrayIntersect(s4.__value, \
             arrayMap(__cg_e -> tuple('User', tupleElement(__cg_e, 1)), w2.v4)), tuple('User', \
             tupleElement(v9, 1))) THEN true ELSE false END",
            "LEFT JOIN subquery_s4 AS s4 ON cityHash64(arraySort(arrayDistinct(arrayMap(__cg_e -> \
             tuple(tupleElement(__cg_e, 1)), w2.v4)))) = s4.__cg_members",
        ],
    );
    // The node may have several labels: the element's only.
    let got = squash(&sql(&format!(
        "{follows}RETURN size([x IN fs WHERE (x)<-[:FOLLOWS|AUTHORED]-()]) AS a"
    )));
    assert!(got.contains("ARRAY JOIN"), "{got}");
    assert!(!got.contains("_labels"), "no union of labels: {got}");
    // A label the element cannot have: false, no subquery.
    has(
        &format!("{follows}RETURN size([x IN fs WHERE (x:Post)<-[:LIKED]-()]) AS z"),
        &["length(arrayFilter(v9 -> false, w2.v4)) AS \"z\""],
    );
    // A list reading another comprehension's element: the matches in the
    // whole graph.
    let got = squash(&sql(&format!(
        "{follows}RETURN [x IN fs | size([y IN [x] WHERE (y)-[:AUTHORED]->()])] AS a"
    )));
    assert!(!got.contains("ARRAY JOIN"), "{got}");
    assert!(got.contains("has(arrayIntersect("), "{got}");
    not_lowered(
        &format!("{follows}RETURN [x IN fs WHERE size([y IN fs WHERE (x)-->(y)]) > 0] AS a"),
        "a subquery expression inside a list comprehension, reduce or lambda",
    );
    not_lowered(
        &format!("{follows}RETURN [x IN fs WHERE EXISTS {{ (x)-->(y) WHERE x.age > 3 }}] AS a"),
        "a subquery expression inside a list comprehension, reduce or lambda",
    );
    not_lowered(
        &format!("{follows}RETURN [x IN fs WHERE (x)-[:FOLLOWS]->(y)] AS a"),
        "PatternExpressions are not allowed to introduce new variables: 'y'",
    );
    // A shortest path reading the element reads it (review: it was bound as
    // one value per row, `x` a new or the outer variable).
    for q in [
        "RETURN size([x IN fs WHERE EXISTS { shortestPath((x)-[:FOLLOWS*]->(:User {user_id: 1})) }]) AS n",
        "RETURN [x IN fs | EXISTS { allShortestPaths((x)-[:FOLLOWS*]->(:User {user_id: 1})) }] AS n",
    ] {
        not_lowered(
            &format!("{follows}{q}"),
            "a subquery expression inside a list comprehension, reduce or lambda",
        );
    }
    not_lowered(
        "MATCH (x:User {user_id: 1}) MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) \
         WITH x, u, collect(f) AS fs \
         RETURN size([x IN fs WHERE EXISTS { shortestPath((u)-[:FOLLOWS*]->(x)) }]) AS n",
        "a subquery expression inside a list comprehension, reduce or lambda",
    );
}

/// `datetime({epochMillis: x})` is `x`; a component of it converts it
/// first, inline as through a WITH (LDBC IC 10's `birthday.month`).
#[test]
fn a_component_of_an_epoch_millis_datetime() {
    has(
        "MATCH (u:User) WITH u, datetime({epochMillis: u.age}) AS b \
         WHERE b.month = 6 RETURN b.day AS d, datetime({epochMillis: u.age}).year AS y",
        &[
            "toMonth(fromUnixTimestamp64Milli(v0.age)) = 6",
            "toDayOfMonth(fromUnixTimestamp64Milli(w1.v2)) AS \"d\"",
            "toYear(fromUnixTimestamp64Milli(w1.p2_v1_age)) AS \"y\"",
        ],
    );
}

/// #1470: a comparison's value is a boolean, which ClickHouse holds as
/// `UInt8`. `toString()` of it is `'true'`; compared with a number or a
/// string it is not equal and has no order (NULL with a NULL operand), as
/// in Neo4j; `x IN [ … ]` of the two is an `=` per element.
#[test]
fn a_boolean_compared_with_a_number_is_a_boolean() {
    has(
        "WITH 1 < 2 AS b RETURN toString(b) AS s, b < 3 AS lt, b = 1 AS eq, \
         1 <> b AS ne, b IN [1, true] AS i, toString(true) AS t, toString(1) AS n",
        &[
            "toString(CAST(1 < 2 AS Nullable(Bool))) AS \"s\"",
            "NULL AS \"lt\"",
            "CASE WHEN (1 < 2) IS NULL THEN NULL ELSE false END AS \"eq\"",
            "CASE WHEN (1 < 2) IS NULL THEN NULL ELSE true END AS \"ne\"",
            "(CASE WHEN (1 < 2) IS NULL THEN NULL ELSE false END OR (1 < 2) = true)",
            "toString(true) AS \"t\"",
            "toString(1) AS \"n\"",
        ],
    );
    // Literals: no NULL test.
    has(
        "RETURN true < 1 AS a, true = 1 AS b",
        &["NULL AS \"a\"", "CAST(false AS Nullable(Bool)) AS \"b\""],
    );
    // A boolean against a boolean, a number against a number: as before.
    has(
        "WITH 1 < 2 AS b, 2 AS n RETURN b = true AS x, n < 3 AS y",
        &["(1 < 2) = true", "2 < 3"],
    );
    for (q, why) in [
        (
            "WITH 1 < 2 AS b RETURN [b] = [1] AS w",
            "a list of booleans compared with a list of other values",
        ),
        (
            "WITH 1 < 2 AS b RETURN b IN range(0, 3) AS w",
            "a boolean looked up in a list of other values (IN)",
        ),
        (
            "WITH 1 < 2 AS b RETURN [b] < [1, 2] AS w",
            "a list of booleans compared with a list of other values",
        ),
        (
            "WITH 1 < 2 AS b RETURN 'a' + b AS w",
            "arithmetic of a boolean",
        ),
        (
            "WITH 1 < 2 AS b RETURN b - 1 AS w",
            "arithmetic of a boolean",
        ),
    ] {
        not_lowered(q, why);
    }
    // With NULL, `+` is NULL; lists of different lengths are not equal.
    has(
        "WITH 1 < 2 AS b RETURN b + null AS r, [b] = [true, false] AS l",
        &["(1 < 2) + NULL AS \"r\""],
    );
    // A simple CASE's branch of the other kind never matches.
    has(
        "WITH 1 < 2 AS b RETURN CASE b WHEN 1 THEN 'one' ELSE 'other' END AS c, \
         CASE b WHEN 1 THEN 'one' END AS d, \
         CASE b WHEN true THEN 't' WHEN 1 THEN 'one' END AS e",
        &[
            "'other' AS \"c\"",
            "NULL AS \"d\"",
            "CASE WHEN (1 < 2) = true THEN 't' END AS \"e\"",
        ],
    );
}

/// #1412: a simple CASE compares with `=`, three-valued: a NULL input or
/// `WHEN` value never matches (ClickHouse's `caseWithExpression` takes NULL
/// for equal to NULL).
#[test]
fn a_simple_case_never_matches_null() {
    has(
        "UNWIND [1, null] AS x RETURN CASE x WHEN null THEN 'n' WHEN 1 THEN 'one' \
         ELSE 'other' END AS a, CASE x WHEN x THEN 'self' END AS b, \
         CASE x WHEN null THEN 'n' END AS c",
        &[
            r#"CASE WHEN w1.v0 = 1 THEN 'one' ELSE 'other' END AS "a""#,
            r#"CASE WHEN w1.v0 = w1.v0 THEN 'self' END AS "b""#,
            r#"NULL AS "c""#,
        ],
    );
    // So in `collect()` of a CASE of lists; a node is compared by identity,
    // and of NULL is not compared (review).
    has(
        "UNWIND [1, null] AS x RETURN collect(CASE x WHEN null THEN [0] WHEN 1 THEN [1] END) AS c",
        &["CASE WHEN w1.v0 = 1 THEN [1] ELSE [0] END), CASE WHEN w1.v0 = 1 THEN true ELSE false END"],
    );
    has(
        "MATCH (n:User), (m:User) RETURN sum(CASE n WHEN null THEN 1 ELSE 0 END) AS a, \
         sum(CASE n WHEN m THEN 1 ELSE 0 END) AS b",
        &[
            "coalesce(sum(0), 0) AS \"a\"",
            "sum(CASE WHEN v0.user_id = v1.user_id THEN 1 ELSE 0 END), 0) AS \"b\"",
        ],
    );
}

/// #1470: a property the schema declares boolean or numeric is compared
/// with the other kind as Cypher does, in a pattern's property map as in a
/// WHERE.
#[test]
fn a_declared_property_against_the_other_kind() {
    let schema = GraphSchemaConfig::from_yaml_str(
        r#"
name: typed
graph_schema:
  nodes:
    - label: P
      database: db
      table: p
      node_id: id
      property_mappings: { id: id, ok: ok_flag, n: n }
      property_types: { ok: boolean, n: integer }
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = |q: &str| {
        squash(
            &translate_bound_plan(q, &schema, &ReadOptions::default())
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .sql,
        )
    };
    for q in [
        "MATCH (p:P {ok: 1}) RETURN p.id AS i",
        "MATCH (p:P) WHERE p.ok = 1 RETURN p.id AS i",
    ] {
        let sql = got(q);
        assert!(
            sql.contains("CASE WHEN v0.ok_flag IS NULL THEN NULL ELSE false END"),
            "{q}: {sql}"
        );
    }
    let sql = got("MATCH (p:P {n: true}) WHERE NOT p.ok IN [1, 2] RETURN p.id AS i");
    for part in [
        "CASE WHEN v0.n IS NULL THEN NULL ELSE false END",
        "NOT (CASE WHEN v0.ok_flag IS NULL THEN NULL ELSE false END \
         OR CASE WHEN v0.ok_flag IS NULL THEN NULL ELSE false END)",
    ] {
        assert!(sql.contains(part), "missing `{part}` in {sql}");
    }
    // Of one kind: compared as before.
    assert!(got("MATCH (p:P {ok: true}) RETURN p.id AS i").contains("v0.ok_flag = true"));
}

/// A comparison, `IN`, `IS NULL` or `NOT` operand of another is
/// parenthesized: ClickHouse reads `false = 1 < 2` as `(false = 1) < 2`
/// and `a IS NULL = b IS NULL` as `((a IS NULL) = b) IS NULL`.
#[test]
fn a_comparison_operand_of_a_comparison_is_parenthesized() {
    has(
        "WITH 1 < 2 AS b RETURN false = b AS x",
        &["false = (1 < 2)"],
    );
    has(
        "UNWIND [true, null] AS a \
         RETURN (a AND true) IS NULL = (true AND a) IS NULL AS r",
        &["((w1.v0 AND true) IS NULL) = ((true AND w1.v0) IS NULL)"],
    );
}

/// #1355: a parenthesized comparison on either side of '=' must render as a
/// plain boolean equality, NOT as a chained-comparison desugar. The old parser
/// produced '(u.age > 20) AND (20 = u.age > 28)'. Renders through the bound
/// plan (both dialects print from the same `needs_comparison_parens` rule).
#[test]
fn paren_comparison_equal_not_chained_1355() {
    let q = "MATCH (u:User) RETURN (u.age > 20) = (u.age > 28) AS x";
    let got = squash(&sql(q));
    assert!(
        got.contains("age > 20") && got.contains("age > 28"),
        "#1355: both comparisons must render:\n{got}"
    );
    assert!(
        !got.contains(" AND "),
        "#1355: no chain desugar allowed:\n{got}"
    );
    assert!(
        got.contains(") = ("),
        "#1355: nested comparisons keep parens:\n{got}"
    );
}

/// #1355: over a variable-length walk, a parenthesized comparison compared
/// with a boolean stays one comparison (main read it as
/// `a.age > 20 AND 20 = false`).
#[test]
fn paren_comparison_in_vlp_where_renders_1355() {
    let q =
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE (a.age > 20) = false RETURN count(*) AS c";
    let got = squash(&sql(q));
    assert!(
        got.contains("age > 20) = false") && !got.contains("20 = false"),
        "#1355: the comparison is compared, not chained:\n{got}"
    );
}

/// VALUE_TYPES.md T1 (#1360 item 1): a schema that declares its properties
/// gets the same walk-key casts once its tables' column types are read (or
/// declared in `column_types`), and none without them.
#[test]
fn declared_union_walk_casts_mixed_type_end_keys_with_read_types() {
    use crate::graph_catalog::column_info::{ColumnInfo, DiscoveredColumns};
    const YAML: &str = r#"
name: lower_declared_mixed_keys
graph_schema:
  nodes:
    - label: Day
      database: db
      table: days
      node_id: d
      property_mappings: {d: d, nm: nm}
  edges:
    - type: E1
      database: db
      table: e1
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      property_mappings: {}
    - type: E2
      database: db
      table: e2
      from_id: d1
      to_id: d2
      from_node: Day
      to_node: Day
      property_mappings: {}
"#;
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(YAML).unwrap();
    let col = |n: &str, t: &str| ColumnInfo::new(n.to_string(), t.to_string());
    let mut columns = DiscoveredColumns::default();
    columns.insert("db", "days", vec![col("d", "Date"), col("nm", "String")]);
    columns.insert(
        "db",
        "e1",
        vec![col("d1", "DateTime"), col("d2", "DateTime")],
    );
    columns.insert("db", "e2", vec![col("d1", "Date"), col("d2", "Date")]);
    let q = "MATCH p = shortestPath((a:Day)-[:E1|E2*1..]->(b:Day)) RETURN count(*)";
    let options = crate::bound_plan::lower::LowerOptions::default();

    let read = lowered(
        q,
        &config.to_graph_schema_with_columns(&columns).unwrap(),
        &options,
    );
    for cast in [
        "CAST(e.d1 AS DateTime64(3))",
        "CAST(e.d2 AS DateTime64(3))",
        "CAST(e.d AS DateTime64(3))",
    ] {
        assert!(read.contains(cast), "{cast}:\n{read}");
    }

    // The same types declared in the YAML, with no connection.
    let declared = YAML
        .replace(
            "      node_id: d\n",
            "      node_id: d\n      column_types: {d: Date}\n",
        )
        .replace(
            "      table: e1\n",
            "      table: e1\n      column_types: {d1: DateTime, d2: DateTime}\n",
        )
        .replace(
            "      table: e2\n",
            "      table: e2\n      column_types: {d1: Date, d2: Date}\n",
        );
    let declared = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(&declared)
        .unwrap()
        .to_graph_schema()
        .unwrap();
    assert_eq!(lowered(q, &declared, &options), read);

    // Unknown types: the SQL is what it was before T1.
    let unknown = lowered(q, &config.to_graph_schema().unwrap(), &options);
    assert!(!unknown.contains("CAST(e.d"), "{unknown}");
}

/// A part whose type is unknown on one side tells nothing: it is not read as
/// an `Integer` (whose `Bool` / `Integer` rule would cast the known `Bool`
/// side to `Int64`), while the part known on every side is still folded.
#[test]
fn an_unknown_key_part_is_not_folded() {
    let config = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_partly_typed_keys
graph_schema:
  nodes:
    - label: P
      database: db
      table: ps
      node_id: [b, d]
      column_types: {b: Bool, d: Date}
      property_mappings: {b: b, d: d}
  edges:
    - type: E
      database: db
      table: es
      from_id: [fb, fd]
      to_id: [tb, td]
      from_node: P
      to_node: P
      column_types: {fd: DateTime, td: DateTime}
      property_mappings: {}
"#,
    )
    .unwrap();
    let schema = config.to_graph_schema().unwrap();
    let got = lowered(
        "MATCH p = shortestPath((a:P)-[:E*1..]->(b:P)) RETURN count(*)",
        &schema,
        &crate::bound_plan::lower::LowerOptions::default(),
    );
    assert!(
        !got.contains("AS Int64)"),
        "the Bool part is not cast:\n{got}"
    );
    assert!(
        got.contains("CAST(start_node.d AS DateTime64(3))"),
        "the Date part is folded with the DateTime ends:\n{got}"
    );
}

/// #1528: where an id part is one SQL alone spells (a Float, a FixedString, a
/// 128-bit integer, a Decimal of 16 digits or more), a node returned whole
/// carries its element id from SQL and its relationship's ends are texts;
/// any other id's SQL is as it was.
#[test]
fn an_id_part_sql_alone_spells_is_spelled_in_sql() {
    let schema = |node_type: &str| {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(&format!(
            r#"
name: lower_exotic_ids
graph_schema:
  nodes:
    - label: P
      database: db
      table: ps
      node_id: k
      column_types: {{k: "{node_type}"}}
      property_mappings: {{k: k, name: name}}
  edges:
    - type: E
      database: db
      table: es
      from_id: fk
      to_id: tk
      from_node: P
      to_node: P
      column_types: {{fk: "{node_type}", tk: "{node_type}"}}
      property_mappings: {{}}
"#
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap()
    };
    for exotic in [
        "Float64",
        "Float32",
        "FixedString(4)",
        "Int128",
        "UInt256",
        "Decimal(18, 2)",
        "Decimal128(4)",
    ] {
        let schema = schema(exotic);
        let (sql, shape) = shaped("MATCH (a:P) RETURN a", &schema);
        assert!(
            sql.contains(r#"AS "a.elementId""#) && sql.contains("concat('P:', toString("),
            "{exotic}: {sql}"
        );
        assert!(
            shape[0]
                .columns
                .iter()
                .any(|(k, c)| k == crate::bound_plan::lower::NODE_ELEMENT_ID_KEY
                    && c == "a.elementId"),
            "{exotic}: {:?}",
            shape[0].columns
        );
        // The ends of its relationship are texts.
        let (sql, _) = shaped("MATCH (a:P)-[r:E]->(b:P) RETURN r", &schema);
        assert!(
            sql.contains(r#"toString(v1.fk) AS "r.from_id""#)
                && sql.contains(r#"toString(v1.tk) AS "r.to_id""#),
            "{exotic}: {sql}"
        );
        // id(n) is the key, as it was.
        let (_, shape) = shaped("MATCH (a:P) RETURN id(a) AS i", &schema);
        assert!(
            matches!(shape[0].kind, ResultKind::NodeId { .. }),
            "{exotic}: {shape:?}"
        );
    }
    // Other ids: no element id column, the ends as they are, id(n) the key.
    for plain in [
        "Int64",
        "UInt64",
        "String",
        "Decimal(10, 2)",
        "Date",
        "UUID",
    ] {
        let schema = schema(plain);
        let (sql, _) = shaped("MATCH (a:P) RETURN a", &schema);
        assert!(!sql.contains("a.elementId"), "{plain}: {sql}");
        let (sql, _) = shaped("MATCH (a:P)-[r:E]->(b:P) RETURN r", &schema);
        assert!(sql.contains(r#"v1.fk AS "r.from_id""#), "{plain}: {sql}");
        let (_, shape) = shaped("MATCH (a:P) RETURN id(a) AS i", &schema);
        assert!(
            matches!(shape[0].kind, ResultKind::NodeId { .. }),
            "{plain}: {shape:?}"
        );
    }
}

/// #1528: composite ids with one such part, an OPTIONAL MATCH that finds no
/// node, the node's and the end's types known on one side only, and Databricks.
#[test]
fn sql_spelled_ids_by_layout() {
    let schema = |node_types: &str, edge_types: &str| {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(&format!(
            r#"
name: lower_exotic_layouts
graph_schema:
  nodes:
    - label: C
      database: db
      table: cs
      node_id: [b, k]
      {node_types}
      property_mappings: {{b: b, k: k}}
  edges:
    - type: E
      database: db
      table: es
      from_id: [fb, fk]
      to_id: [tb, tk]
      from_node: C
      to_node: C
      {edge_types}
      property_mappings: {{}}
"#
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap()
    };
    // One part of two is a Float64: the node and its ends are spelled in SQL;
    // the other end part (a String) is as it was.
    let both = schema(
        "column_types: {b: String, k: Float64}",
        "column_types: {fb: String, fk: Float64, tb: String, tk: Float64}",
    );
    let (sql, _) = shaped("MATCH (a:C) RETURN a", &both);
    assert!(sql.contains(r#"AS "a.elementId""#), "{sql}");
    let (sql, _) = shaped("MATCH (a:C)-[r:E]->(b:C) RETURN r", &both);
    assert!(
        sql.contains(r#"v1.fb AS "r.from_id_1""#)
            && sql.contains(r#"toString(v1.fk) AS "r.from_id_2""#)
            && sql.contains(r#"toString(v1.tk) AS "r.to_id_2""#),
        "{sql}"
    );
    // ... NULL where an OPTIONAL MATCH found no node (a NULL part of a
    // composite id is `\\N` in its text, never a NULL).
    let (sql, _) = shaped("MATCH (m:C) OPTIONAL MATCH (m)-[:E]->(n:C) RETURN n", &both);
    assert!(
        sql.contains(") THEN NULL ELSE concat('C:', ") && sql.contains(r#"END AS "n.elementId""#),
        "{sql}"
    );
    // The node's type decides with the end's: a plain node (known) with an
    // exotic stored end: the end is text (the node spells its id plain, the
    // JSON of a FixedString is padded); an exotic node with a plain end: text.
    let plain_node = schema(
        "column_types: {b: String, k: Int64}",
        "column_types: {fb: String, fk: Float64, tb: String, tk: Int64}",
    );
    let (sql, _) = shaped("MATCH (a:C)-[r:E]->(b:C) RETURN r", &plain_node);
    assert!(
        !sql.contains("a.elementId")
            && sql.contains(r#"toString(v1.fk) AS "r.from_id_2""#)
            && sql.contains(r#"v1.tk AS "r.to_id_2""#),
        "{sql}"
    );
    let exotic_node = schema(
        "column_types: {b: String, k: Float64}",
        "column_types: {fb: String, fk: Int64, tb: String, tk: Int64}",
    );
    let (sql, _) = shaped("MATCH (a:C)-[r:E]->(b:C) RETURN r", &exotic_node);
    assert!(
        sql.contains(r#"toString(v1.fk) AS "r.from_id_2""#)
            && sql.contains(r#"toString(v1.tk) AS "r.to_id_2""#),
        "{sql}"
    );
    // The node's types unknown (a node embedded in an edge table's are), or
    // only the other part's known: the ends are as they were, spelled in
    // Rust with the node.
    let part_known = schema(
        "column_types: {b: String}",
        "column_types: {fb: String, fk: Float64, tb: String, tk: Float64}",
    );
    let (sql, _) = shaped("MATCH (a:C)-[r:E]->(b:C) RETURN r", &part_known);
    assert!(
        !sql.contains("toString(v1.fk)") && sql.contains(r#"v1.fk AS "r.from_id_2""#),
        "{sql}"
    );
    let unknown_node = schema(
        "",
        "column_types: {fb: String, fk: Float64, tb: String, tk: Float64}",
    );
    let (sql, _) = shaped("MATCH (a:C)-[r:E]->(b:C) RETURN r", &unknown_node);
    assert!(
        !sql.contains("toString(v1.fk)") && sql.contains(r#"v1.fk AS "r.from_id_2""#),
        "{sql}"
    );
    // Databricks spells the text in its own function.
    let got = squash(&databricks_sql("MATCH (a:C) RETURN a", &both).unwrap());
    assert!(
        got.contains("AS STRING") && got.contains(r#"AS `a.elementId`"#),
        "{got}"
    );
    let got = squash(&databricks_sql("MATCH (a:C)-[r:E]->(b:C) RETURN r", &both).unwrap());
    assert!(
        got.contains("AS `r.from_id_2`") && !got.contains("toString("),
        "{got}"
    );
}

/// An escaped (backtick-quoted) name is the name: at every place a query
/// names something it lowers as the plain spelling does (#1489, #1421,
/// #1381).
#[test]
fn escaped_names_lower_as_the_names_1489() {
    for (escaped, plain) in [
        (
            "MATCH (a:`User`) RETURN a.name AS n",
            "MATCH (a:User) RETURN a.name AS n",
        ),
        (
            "MATCH (a:User)-[r:`FOLLOWS`]->(b) RETURN count(*) AS c",
            "MATCH (a:User)-[r:FOLLOWS]->(b) RETURN count(*) AS c",
        ),
        (
            "MATCH (a:User)-[r:`FOLLOWS`|`LIKED`]->(b) RETURN count(*) AS c",
            "MATCH (a:User)-[r:FOLLOWS|LIKED]->(b) RETURN count(*) AS c",
        ),
        (
            "MATCH (`my node`:User)-[`r`:FOLLOWS]->(b) \
             RETURN `my node`.`name` AS `x`, `r`.follow_date AS d",
            "MATCH (a:User)-[r:FOLLOWS]->(b) RETURN a.name AS x, r.follow_date AS d",
        ),
        (
            "MATCH (a:User {`name`: 'Alice'}) RETURN a.user_id AS u",
            "MATCH (a:User {name: 'Alice'}) RETURN a.user_id AS u",
        ),
        (
            "MATCH `p` = (a:User)-[:FOLLOWS]->(b) RETURN length(`p`) AS l",
            "MATCH p = (a:User)-[:FOLLOWS]->(b) RETURN length(p) AS l",
        ),
        (
            "MATCH (a:User) WITH a.age AS `the age` WHERE `the age` > 30 RETURN `the age` AS x",
            "MATCH (a:User) WITH a.age AS t WHERE t > 30 RETURN t AS x",
        ),
        (
            "UNWIND [1, 2] AS `x` RETURN `x` AS y",
            "UNWIND [1, 2] AS x RETURN x AS y",
        ),
        (
            "RETURN [`x y` IN [1, 2] WHERE `x y` > 1 | `x y` * 2] AS z",
            "RETURN [x IN [1, 2] WHERE x > 1 | x * 2] AS z",
        ),
        (
            "RETURN reduce(`s` = 0, `x` IN [1, 2] | `s` + `x`) AS z",
            "RETURN reduce(s = 0, x IN [1, 2] | s + x) AS z",
        ),
        (
            "MATCH (a:User) WHERE a:`User` RETURN `toUpper`(a.name) AS n",
            "MATCH (a:User) WHERE a:User RETURN toUpper(a.name) AS n",
        ),
        (
            "MATCH (a:User) WHERE a.user_id = $`id` RETURN a.name AS n",
            "MATCH (a:User) WHERE a.user_id = $id RETURN a.name AS n",
        ),
        // A keyword is a name where a name is expected; escaped, the
        // literals `true` / `null` are variables.
        (
            "WITH 1 AS `true`, 2 AS `null` RETURN `true` + `null` AS s",
            "WITH 1 AS t, 2 AS n RETURN t + n AS s",
        ),
        // A name may start with `_` in a pattern too.
        (
            "MATCH (_n:User)-[_r:FOLLOWS]->(b) RETURN _n.name AS n",
            "MATCH (n:User)-[r:FOLLOWS]->(b) RETURN n.name AS n",
        ),
        // `a.b.c` is `(a.b).c`: a variable's key path, as before.
        (
            "MATCH (a:User) RETURN a.`name`.`x` AS n",
            "MATCH (a:User) RETURN a.name.x AS n",
        ),
    ] {
        assert_eq!(sql(escaped), sql(plain), "{escaped}");
    }
}

/// An escaped name may hold any character; in the SQL a column or alias is
/// always quoted unless plain, a quote or backslash escaped (#1489).
#[test]
fn escaped_names_are_quoted_in_the_sql_1489() {
    has(
        "RETURN 1 AS `a\\`, 2 AS `c\"d`, 3 AS `e``f`, {`it's`: 1, `k``l`: 2} AS m",
        &[
            r#"1 AS "a\\""#,
            r#"2 AS "c""d""#,
            r#"3 AS "e`f""#,
            r"'it\'s', CAST(1, 'Dynamic')",
            r"'k`l', CAST(2, 'Dynamic')",
        ],
    );
    // Undeclared properties read the same-named column, quoted.
    has(
        "MATCH (a:User) WHERE a.`q x` = 1 RETURN a.`é` AS e, a.`x;y` AS s",
        &[r#"v0."é" AS "e""#, r#"v0."x;y" AS "s""#, r#"v0."q x" = 1"#],
    );
}

/// What an escaped name can name that ClickGraph cannot carry is refused,
/// never read as another name (#1489).
#[test]
fn escaped_names_that_are_refused_1489() {
    // `v.k1.k2` is carried as the key path `k1.k2`.
    not_lowered(
        "MATCH (a:User) RETURN a.`q.x` AS q",
        "a property key containing '.'",
    );
    not_lowered(
        "WITH {a: {b: 1}} AS m RETURN m.a.`b.c` AS q",
        "a property key containing '.'",
    );
    // A parameter's value is put in its place by a plain name.
    not_lowered("RETURN $`p q` AS p", "a parameter named `p q`");
}

/// A property of a list's element, of a call, of a parenthesized
/// expression (#1381): its key of the value, as `e['key']`.
#[test]
fn a_property_of_any_value_1381() {
    assert_eq!(
        sql("WITH [{name: 'x'}, {name: 'y'}] AS ms RETURN ms[0].name AS n, ms[-1].name AS m"),
        sql("WITH [{name: 'x'}, {name: 'y'}] AS ms RETURN ms[0]['name'] AS n, ms[-1]['name'] AS m"),
    );
    assert_eq!(
        sql("WITH [[{name: 'a'}]] AS nss RETURN [x IN nss | x[-1].name] AS l, nss[0][0].name AS m"),
        sql("WITH [[{name: 'a'}]] AS nss RETURN [x IN nss | x[-1]['name']] AS l, nss[0][0]['name'] AS m"),
    );
    assert_eq!(
        sql("MATCH (a:User) RETURN (a).name AS n"),
        sql("MATCH (a:User) RETURN a.name AS n"),
    );
    // Of a list of nodes: not lowered (#1378), as `bs[0]` is not.
    not_lowered(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs RETURN bs[0].name AS f",
        "an element or slice of a list of nodes or relationships",
    );
}

/// Review of #1489: the empty name, dotted keys in a pattern map, a
/// parameter's property.
#[test]
fn escaped_names_review_1489() {
    not_lowered("RETURN 1 AS `` ", "an empty name (``) as a column");
    not_lowered("MATCH (a:User) RETURN a.`` AS x", "an empty property key");
    not_lowered(
        "MATCH (a:User {`q.x`: 1}) RETURN a.name AS n",
        "a property key containing '.'",
    );
    // `$p.year` is the accessor; `$p.days` a property, no parse error.
    has(
        "RETURN $p.year AS y",
        &["toYear(fromUnixTimestamp64Milli($p))"],
    );
    if let Err(e) = translate_bound_plan("RETURN $p.days AS d", &social(), &ReadOptions::default())
    {
        assert!(!e.contains("Parse error"), "{e}");
    }
}

/// Comparison chains and predicate binding as in Neo4j (#1398): a chain of
/// any length, `<>` included, is the conjunction of its adjacent
/// comparisons; IN and IS NULL bind tighter than `=` and apply to an
/// additive operand.
#[test]
fn comparison_chains_and_predicates_1398() {
    has(
        "MATCH (a:User), (b:User) RETURN a.age <> b.age <> a.age AS x",
        &["(v0.age <> v1.age AND v1.age <> v0.age)"],
    );
    has(
        "MATCH (a:User), (b:User), (c:User) RETURN a.age < b.age < c.age < b.age AS x",
        &["((v0.age < v1.age AND v1.age < v2.age) AND v2.age < v1.age)"],
    );
    has(
        "MATCH (a:User) RETURN true = a.age IN [25, 30] AS x, a.age + 1 IS NULL AS y",
        &["true = (v0.age IN [25, 30])", "v0.age + 1 IS NULL"],
    );
    // A parenthesized NOT is no node pattern.
    has(
        "WITH true AS x RETURN (NOT false) = x AS y",
        &["(NOT false) = true"],
    );
}

/// XOR (#1398): between AND and OR, three-valued; ClickHouse `xor`, Spark
/// `<>` of the two booleans.
#[test]
fn xor_1398() {
    has(
        "MATCH (a:User) WHERE a.age > 30 XOR a.country = 'USA' OR a.age < 20 RETURN a.name AS n",
        &["(xor(v0.age > 30, v0.country = 'USA') OR v0.age < 20)"],
    );
    let got = squash(
        &databricks_sql(
            "MATCH (a:User) WHERE a.age > 30 XOR a.country = 'USA' RETURN a.name AS n",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains("(CAST(v0.age > 30 AS BOOLEAN) <> CAST(v0.country = 'USA' AS BOOLEAN))"),
        "{got}"
    );
}

/// Review of #1507: clause keywords are words too (`RETURN distinctcountry`
/// read as `RETURN DISTINCT country`), ASCENDING / DESCENDING, and an
/// operator without an operand leaves the error at the operator.
#[test]
fn keywords_are_words_in_clauses_1398() {
    assert_eq!(
        sql("MATCH (n:User) WITH n.country AS country, n.user_id AS distinctcountry \
             RETURN distinctcountry"),
        sql("MATCH (n:User) WITH n.country AS country, n.user_id AS dc RETURN dc AS distinctcountry"),
    );
    assert!(
        !sql("MATCH (n:User) WITH n.user_id AS distinctx RETURN distinctx").contains("DISTINCT")
    );
    assert_eq!(
        sql("MATCH (n:User) RETURN n.age AS a ORDER BY a DESCENDING"),
        sql("MATCH (n:User) RETURN n.age AS a ORDER BY a DESC"),
    );
    assert_eq!(
        sql("MATCH (n:User) RETURN n.age AS a ORDER BY a ASCENDING"),
        sql("MATCH (n:User) RETURN n.age AS a ORDER BY a"),
    );
    let err = translate_bound_plan(
        "MATCH (u:User) WHERE u.name = 'x' AND u.age > ? RETURN u",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.contains("> ? RETURN u") && !err.contains("at `MATCH"),
        "{err}"
    );
    let err = translate_bound_plan(
        "MATCH (u:User) WHERE u.age IN ? RETURN u",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.contains("IN ? RETURN u") && !err.contains("at `MATCH"),
        "{err}"
    );
    // `AS` is a word: `ASage` is no alias.
    assert!(translate_bound_plan(
        "MATCH (n:User) RETURN n.age ASage",
        &social(),
        &ReadOptions::default()
    )
    .is_err());
}

/// The list predicates (#1420) lower as their list comprehensions do, over
/// values and over a list of nodes, on both dialects; a NULL list is NULL.
#[test]
fn list_predicates_1420() {
    has(
        "MATCH (u:User) WHERE any(x IN [1, 2] WHERE x = u.user_id) RETURN u.name AS n",
        &[
            "CASE WHEN length(arrayFilter(v1 -> true, [1, 2])) IS NULL THEN NULL",
            "WHEN length(arrayFilter(v2 -> v2 = v0.user_id, [1, 2])) > 0 THEN true",
            "WHEN length(arrayFilter(v3 -> (v3 = v0.user_id) IS NULL, [1, 2])) > 0 THEN NULL \
             ELSE false END",
        ],
    );
    for q in [
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, collect(b) AS bs \
         RETURN all(x IN bs WHERE x.age > 30) AS l, single(x IN bs WHERE x.age > 30) AS s",
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User) \
         RETURN none(n IN nodes(p) WHERE n.age > 30) AS x",
        "WITH [[1, 2], [3]] AS nss RETURN all(x IN nss WHERE size(x) > 0) AS a",
    ] {
        sql(q);
        databricks_sql(q, &social()).unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    let got = squash(
        &databricks_sql("RETURN single(x IN [1, null] WHERE x > 0) AS s", &social()).unwrap(),
    );
    assert!(
        got.contains("CASE WHEN array_size(filter(array(1, NULL), "),
        "{got}"
    );
}

/// Review of #1508: on Databricks the NULL-list guard is the size of a
/// comprehension (`array_size(filter(…)) IS NULL`), never `length(list)`
/// (strings only there) nor `size(list)` (-1 of NULL without ANSI).
#[test]
fn list_predicate_null_guard_on_databricks_1420() {
    let schema = GraphSchemaConfig::from_yaml_str(include_str!(
        "../../../schemas/test/array_property_probe.yaml"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let got = squash(
        &databricks_sql(
            "MATCH (i:Item) RETURN any(t IN i.tags WHERE t = 'a') AS a",
            &schema,
        )
        .unwrap(),
    );
    assert!(
        got.contains("CASE WHEN array_size(filter(v0.tags, ") && !got.contains("length("),
        "{got}"
    );
    let got = squash(
        &databricks_sql(
            "WITH CASE WHEN false THEN [1] END AS l RETURN all(x IN l WHERE x > 0) AS a",
            &social(),
        )
        .unwrap(),
    );
    assert!(
        got.contains("CASE WHEN array_size(filter(w1.v0, ") && !got.contains(" size("),
        "{got}"
    );
}

/// `COUNT { }` lowers as `size()` of its pattern does, `COLLECT { … RETURN e }`
/// as the pattern comprehension `[pattern WHERE c | e]` (#1492); other bodies
/// are refused by name.
#[test]
fn count_and_collect_subqueries_1492() {
    for (sub, equivalent) in [
        (
            "MATCH (a:User) RETURN COUNT { (a)-[:FOLLOWS]->() } AS c",
            "MATCH (a:User) RETURN size([(a)-[:FOLLOWS]->() | 1]) AS c",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) WHERE b.age > 30 } AS c",
            "MATCH (a:User) RETURN size([(a)-[:FOLLOWS]->(b) WHERE b.age > 30 | 1]) AS c",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) RETURN b.name } AS c",
            "MATCH (a:User) RETURN size([(a)-[:FOLLOWS]->(b) | 1]) AS c",
        ),
        (
            "MATCH (a:User) RETURN COLLECT { MATCH (a)-[:FOLLOWS]->(b) WHERE b.age > 30 \
             RETURN b.user_id } AS c",
            "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(b) WHERE b.age > 30 | b.user_id] AS c",
        ),
    ] {
        assert_eq!(sql(sub), sql(equivalent), "{sub}");
        databricks_sql(sub, &social()).unwrap_or_else(|e| panic!("{sub}: {e}"));
    }
    sql("MATCH (a:User) WHERE COUNT { (a)-[:FOLLOWS]->() } > 1 RETURN count(*) AS c");
    sql("MATCH (a:User) WITH a, COUNT { (a)-[:FOLLOWS]->() } AS c RETURN sum(c) AS s");
    for (q, why) in [
        (
            "MATCH (a:User) RETURN COLLECT { MATCH (a)-[:FOLLOWS]->(b) RETURN b.name ORDER BY b.name } AS c",
            "DISTINCT, ORDER BY, SKIP or LIMIT",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) RETURN DISTINCT b.country } AS c",
            "DISTINCT, ORDER BY, SKIP or LIMIT",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) WITH b RETURN b } AS c",
            "of other than one MATCH",
        ),
        (
            "MATCH (a:User) RETURN COLLECT { (a)-[:FOLLOWS]->(b) } AS c",
            "a COLLECT { } without RETURN",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) MATCH (b)-[:FOLLOWS]->(c) } AS c",
            "of other than one pattern or one MATCH",
        ),
        (
            "MATCH (a:User) RETURN COUNT { (a)-[:FOLLOWS]->(b), (b)-[:FOLLOWS]->(c) } AS c",
            "of other than one pattern or one MATCH",
        ),
    ] {
        not_lowered(q, why);
    }
}

/// A call of a function of fixed arity with too few or too many arguments is
/// refused, as Neo4j does (#1413: `count()` read as `count(*)`).
#[test]
fn function_arity_1413() {
    for (q, msg) in [
        (
            "MATCH (a:User) RETURN count() AS c",
            "Insufficient parameters for function 'count'",
        ),
        (
            "MATCH (a:User) RETURN COUNT() AS c",
            "Insufficient parameters for function 'COUNT'",
        ),
        (
            "RETURN size([1], 2) AS c",
            "Too many parameters for function 'size'",
        ),
        (
            "RETURN rand(1) AS c",
            "Too many parameters for function 'rand'",
        ),
        (
            "RETURN substring('a') AS c",
            "Insufficient parameters for function 'substring'",
        ),
        (
            "MATCH (a:User) WITH a WHERE size([x IN [1] WHERE toUpper() = 'A']) > 0 RETURN a",
            "Insufficient parameters for function 'toUpper'",
        ),
        // A COUNT { }'s RETURN items are bound for their errors.
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) RETURN size() } AS c",
            "Insufficient parameters for function 'size'",
        ),
        (
            "MATCH (a:User) RETURN COUNT { MATCH (a)-[:FOLLOWS]->(b) RETURN nope } AS c",
            "Variable `nope` not defined",
        ),
    ] {
        match translate_bound_plan(q, &social(), &ReadOptions::default()) {
            Err(e) if e.contains(msg) => {}
            other => panic!("{q}: {:?}", other.map(|t| t.sql)),
        }
    }
    for q in [
        "MATCH (a:User) RETURN count(*) AS c, substring(a.name, 1) AS s, round(1.25, 1) AS r",
        "RETURN coalesce(null, null, 1) AS c, range(1, 5, 2) AS g, rand() AS x",
    ] {
        sql(q);
    }
}
/// VALUE_TYPES.md T2 (#1485): a property whose column is an `Array` is a
/// list, so an OPTIONAL MATCH's unmatched node reads it as NULL (its source)
/// rather than as the `[]` ClickHouse holds; without the column's type it
/// reads as before.
#[test]
fn an_array_column_property_is_a_list_1485() {
    const YAML: &str = r#"
name: lower_array_property
graph_schema:
  nodes:
    - label: Person
      database: db
      table: person
      node_id: id
      column_types: {id: Int64, tags: 'Array(String)', name: 'Nullable(String)'}
      property_mappings: {id: id, tags: tags, name: name}
  edges:
    - type: KNOWS
      database: db
      table: knows
      from_id: a
      to_id: b
      from_node: Person
      to_node: Person
      property_mappings: {}
"#;
    let schema = |yaml: &str| {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(yaml)
            .unwrap()
            .to_graph_schema()
            .unwrap()
    };
    let options = crate::bound_plan::lower::LowerOptions::default();
    let q = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) \
             RETURN q.tags IS NULL AS n, size(q.tags) AS s";
    let typed = lowered(q, &schema(YAML), &options);
    assert!(
        typed.contains(r#"CAST(o1.v2__id IS NULL AS Nullable(Bool)) AS "n""#),
        "{typed}"
    );
    assert!(
        typed.contains("CASE WHEN o1.v2__id IS NULL THEN NULL ELSE length(o1.p2_v2_tags) END"),
        "{typed}"
    );
    // A scalar column's property keeps its kind (unknown) and its SQL.
    let scalar = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN q.name IS NULL";
    let untyped_yaml = YAML.replace(
        "      column_types: {id: Int64, tags: 'Array(String)', name: 'Nullable(String)'}\n",
        "",
    );
    assert_eq!(
        lowered(scalar, &schema(YAML), &options),
        lowered(scalar, &schema(&untyped_yaml), &options)
    );
    // Without the column's type: as before (the list unknown).
    let untyped = lowered(q, &schema(&untyped_yaml), &options);
    assert!(
        !untyped.contains("o1.v2__id IS NULL AS Nullable(Bool)"),
        "{untyped}"
    );
}

/// T2a review findings: an `Array` column's property in a collected map
/// reads as before (not refused), `isEmpty()` of one an OPTIONAL MATCH may
/// leave NULL is a boolean, and one never NULL groups as its column.
#[test]
fn array_column_properties_keep_what_they_did_1485() {
    let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_array_property_review
graph_schema:
  nodes:
    - label: Person
      database: db
      table: person
      node_id: id
      column_types: {id: Int64, tags: 'Array(String)', m: 'Array(Map(String, Int64))', ad: 'Array(Dynamic)'}
      property_mappings: {id: id, tags: tags, m: m, ad: ad}
  edges:
    - type: KNOWS
      database: db
      table: knows
      from_id: a
      to_id: b
      from_node: Person
      to_node: Person
      property_mappings: {}
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let options = crate::bound_plan::lower::LowerOptions::default();
    let sql = |q: &str| lowered(q, &schema, &options);
    sql(
        "MATCH (p:Person) WITH p ORDER BY p.id WITH collect({t: p.tags, i: p.id})[0] AS m \
         RETURN m.t, m.i",
    );
    let empty =
        sql("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN isEmpty(q.tags) AS e");
    assert!(
        empty.contains("ELSE CAST(empty(o1.p2_v2_tags) AS Nullable(Bool)) END"),
        "{empty}"
    );
    let grouped = sql("MATCH (p:Person) RETURN p.tags, count(*)");
    assert!(grouped.contains("GROUP BY v0.tags"), "{grouped}");
    // Lists made of such a property read as before too (round 2).
    for e in [
        "p.tags + ['z']",
        "tail(p.tags)",
        "coalesce(p.tags, [])",
        "p.tags[0..1]",
    ] {
        sql(&format!(
            "MATCH (p:Person) WITH p ORDER BY p.id WITH collect({{t: {e}}})[0] AS m RETURN m.t"
        ));
    }
    // So do they carried through a WITH or an UNWIND (round 3).
    for q in [
        "MATCH (p:Person) WITH p, p.tags AS t ORDER BY p.id WITH collect({x: t})[0] AS m RETURN m.x",
        "MATCH (p:Person) WITH p.id AS i, tail(p.tags) AS t ORDER BY i WITH collect({x: t})[1] AS m RETURN m.x",
        "MATCH (p:Person) UNWIND [p.tags] AS n WITH p, n ORDER BY p.id WITH collect({x: n})[0] AS m RETURN m.x",
        // A pattern comprehension's / COLLECT { }'s list (round 4).
        "MATCH (p:Person) WITH p ORDER BY p.id \
         WITH collect({x: head([(p)-[:KNOWS]->(q:Person) | q.tags])})[0] AS m RETURN m.x",
        "MATCH (p:Person) WITH p, [(p)-[:KNOWS]->(q:Person) | q.tags] AS l ORDER BY p.id \
         WITH collect({x: head(l)})[0] AS m RETURN m.x",
    ] {
        sql(q);
    }
    // An `Array(Dynamic)` is not of one type: keyed, as before.
    let dynamic = sql("MATCH (p:Person) RETURN p.ad, count(*)");
    assert!(!dynamic.contains("GROUP BY v0.ad"), "{dynamic}");
}

/// VALUE_TYPES.md T4a (#1396): a list of a `UInt64` and a signed column has
/// no ClickHouse common type (a `Variant`, in which equal integers are two
/// values), so its elements meet in `Int128`; lists with a common type, or
/// with an element of unknown type, keep their SQL; Spark has no unsigned
/// integers.
#[test]
fn list_elements_of_mixed_signedness_meet_in_int128_1396() {
    let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(
        r#"
name: lower_mixed_signedness_list
graph_schema:
  nodes:
    - label: Item
      database: db
      table: item
      node_id: id
      column_types: {id: UInt64, k: Int64, u: UInt32, b: Bool}
      property_mappings: {id: id, k: k, u: u, s: s, b: b}
  edges: []
"#,
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let options = crate::bound_plan::lower::LowerOptions::default();
    let sql = |q: &str| lowered(q, &schema, &options);
    let mixed = sql("MATCH (n:Item) UNWIND [n.id, n.k] AS x RETURN count(DISTINCT x)");
    assert!(
        mixed.contains("[CAST(v0.id AS Nullable(Int128)), CAST(v0.k AS Nullable(Int128))]"),
        "{mixed}"
    );
    // A negative literal is signed too.
    let literal = sql("MATCH (n:Item) RETURN [n.id, -1]");
    assert!(
        literal.contains("CAST(v0.id AS Nullable(Int128))"),
        "{literal}"
    );
    for q in [
        "MATCH (n:Item) RETURN [n.id, 1]",
        "MATCH (n:Item) RETURN [n.u, n.k]",
        "MATCH (n:Item) RETURN [n.id, n.k, n.s]",
        // A boolean is no integer: `[1, -20, true]`, not `[1, -20, 1]`.
        "MATCH (n:Item) RETURN [n.id, n.k, n.b]",
    ] {
        assert!(!sql(q).contains("Int128"), "{q}:\n{}", sql(q));
    }
    let spark = databricks_sql(
        "MATCH (n:Item) UNWIND [n.id, n.k] AS x RETURN count(DISTINCT x)",
        &schema,
    )
    .unwrap();
    assert!(!spark.contains("CAST("), "{spark}");
}

/// ELEMENT_IDENTITY.md I1 (#1358): where a `UInt64` node id meets `Int64`
/// relationship ends, every producer of an identity part for comparison casts
/// it to the label's comparable type (`Int128`), so every site agrees: a
/// hop's tie, an undirected relationship's two directions, a union of types,
/// a several-labels node, a directed walk's edge, a union walk's node CTE, an
/// OPTIONAL MATCH (no elided endpoint). Without a gap, the SQL is as before.
#[test]
fn identity_parts_meet_in_one_comparable_type_1358() {
    const YAML: &str = r#"
name: lower_identity_comparable
graph_schema:
  nodes:
    - label: U
      database: db
      table: u
      node_id: id
      column_types: {id: UInt64}
      property_mappings: {id: id, name: name}
    - label: T
      database: db
      table: t
      node_id: id
      column_types: {id: Int64}
      property_mappings: {id: id, name: name}
    - label: V
      database: db
      table: v
      node_id: id
      column_types: {id: UInt64}
      property_mappings: {id: id, name: name}
    - label: X
      database: db
      table: x
      node_id: id
      column_types: {id: UInt128}
      property_mappings: {id: id, name: name}
  edges:
    - type: XX
      database: db
      table: xx
      from_node: X
      to_node: X
      from_id: src
      to_id: dst
      column_types: {src: UInt128, dst: UInt128}
      property_mappings: {}
    - type: VV
      database: db
      table: vv
      from_node: V
      to_node: V
      from_id: src
      to_id: dst
      column_types: {src: UInt64, dst: UInt64}
      property_mappings: {}
    - type: F
      database: db
      table: fe
      from_node: U
      to_node: U
      from_id: src
      to_id: dst
      column_types: {src: UInt64, dst: Int64}
      property_mappings: {}
    - type: H
      database: db
      table: h
      from_node: U
      to_node: T
      from_id: uid
      to_id: tid
      column_types: {uid: UInt64, tid: Int64}
      property_mappings: {}
"#;
    let schema = |yaml: &str| {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(yaml)
            .unwrap()
            .to_graph_schema()
            .unwrap()
    };
    let mixed = schema(YAML);
    let options = crate::bound_plan::lower::LowerOptions::default();
    let sql = |q: &str| lowered(q, &mixed, &options);
    let has = |q: &str, parts: &[&str]| {
        let got = sql(q);
        for p in parts {
            assert!(got.contains(p), "{q}: `{p}` missing:\n{got}");
        }
    };
    has(
        "MATCH (a:U)-[:F]->(b:U) RETURN a.name, b.name",
        &["CAST(v1.dst AS Int128) = CAST(v2.id AS Int128)"],
    );
    has(
        "MATCH (a:U)-[:F]-(b:U) RETURN count(*)",
        &[
            r#"CAST(e.src AS Int128) AS "__cg_start_0""#,
            r#"CAST(e.dst AS Int128) AS "__cg_start_0""#,
        ],
    );
    has(
        "MATCH (x:U|T)-[:H]-(y) RETURN x.name, y.name",
        &[r#"CAST(e.id AS Int128) AS "__cg_id_0""#],
    );
    has(
        "MATCH (a:U)-[:F*1..2]->(b:U) RETURN count(*)",
        &[
            "v1_ends AS ( SELECT",
            r#"CAST(e.dst AS Int128) AS "__cg_end_0""#,
        ],
    );
    has(
        "MATCH (x)-[:H*1..2]-(y) RETURN count(*)",
        &[
            "vlp_v1_nodes AS ( SELECT",
            r#"CAST(e.id AS Int128) AS "__cg_id_0""#,
        ],
    );
    has(
        "MATCH (a:U) OPTIONAL MATCH (a)<-[:F]-(b:U) RETURN a.name, b.name",
        &["AS Int128) = CAST("],
    );
    // A label whose own ids and ends agree keeps its SQL, whatever other
    // labels' gaps (a cast join key blocks ClickHouse's key pushdown).
    for q in [
        "MATCH (v:V)-[:VV]->(w:V) WHERE v.id = 1 RETURN w.name",
        "MATCH (v:V)-[:VV*1..2]->(w:V) RETURN count(*)",
        "MATCH (v:V) OPTIONAL MATCH (v)<-[:VV]-(w:V) RETURN v.name, w.name",
    ] {
        assert!(!sql(q).contains("Int128"), "{q}:\n{}", sql(q));
    }
    // A wider id is never cast to `Int128` (it would wrap): in a column that
    // mixes it with a gap label, it meets the cast ids as itself.
    let wide = sql("MATCH (a:X|U) WITH a MATCH (a)-[:XX]->(b) RETURN a.name, b.name");
    assert!(wide.contains("CAST(e.id AS Int128)"), "{wide}");
    assert!(
        !wide.contains("CAST(e.id AS Int128) AS \"__cg_id_0\" FROM db.x"),
        "{wide}"
    );
    assert!(!wide.contains("CAST(v1.src AS Int128)"), "{wide}");
    // No gap (every id and end UInt64): the SQL of before.
    let agreeing = schema(&YAML.replace("Int64}", "UInt64}"));
    for q in [
        "MATCH (a:U)-[:F]->(b:U) RETURN a.name, b.name",
        "MATCH (a:U)-[:F]-(b:U) RETURN count(*)",
        "MATCH (a:U)-[:F*1..2]->(b:U) RETURN count(*)",
        "MATCH (x)-[:H*1..2]-(y) RETURN count(*)",
    ] {
        let got = lowered(q, &agreeing, &options);
        assert!(!got.contains("Int128"), "{q}:\n{got}");
        assert!(!got.contains("_ends AS"), "{q}:\n{got}");
    }
}

/// ELEMENT_IDENTITY.md I2 (#1393): where the schema's labels hold an id part
/// in values of several kinds (an integer id and a string one), a column
/// that mixes labels holds it as a text, so the string `'01'` is not read as
/// the integer `1` (a `Variant`'s), joins of it meet, and it groups. Each
/// label spells its text in the type its ids and ends meet in (a `Date` id
/// with `DateTime` ends: the `DateTime`'s), and a typed side compared with
/// such a column spells it so too; a node's element id reads its text back in
/// its own type. Ids of one kind: the SQL of before.
#[test]
fn ids_of_several_kinds_meet_as_texts_1393() {
    const YAML: &str = r#"
name: lower_identity_texts
graph_schema:
  nodes:
    - label: A
      database: db
      table: a
      node_id: id
      column_types: {id: UInt64}
      property_mappings: {name: name}
    - label: B
      database: db
      table: b
      node_id: code
      column_types: {code: String}
      property_mappings: {name: name}
    - label: C
      database: db
      table: c
      node_id: id
      column_types: {id: Int64}
      property_mappings: {name: name}
    - label: D
      database: db
      table: d
      node_id: day
      column_types: {day: Date}
      property_mappings: {name: name}
  edges:
    - type: AB
      database: db
      table: ab
      from_node: A
      to_node: B
      from_id: a_id
      to_id: b_code
      column_types: {a_id: UInt64, b_code: String}
      property_mappings: {}
    - type: CA
      database: db
      table: ca
      from_node: C
      to_node: A
      from_id: c_id
      to_id: a_id
      column_types: {c_id: Int64, a_id: UInt64}
      property_mappings: {}
    - type: DB
      database: db
      table: dbe
      from_node: D
      to_node: B
      from_id: at
      to_id: b_code
      column_types: {at: DateTime, b_code: String}
      property_mappings: {}
"#;
    let schema = |yaml: &str| {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(yaml)
            .unwrap()
            .to_graph_schema()
            .unwrap()
    };
    let mixed = schema(YAML);
    let options = crate::bound_plan::lower::LowerOptions::default();
    let sql = |q: &str| lowered(q, &mixed, &options);
    let has = |q: &str, parts: &[&str]| {
        let got = sql(q);
        for p in parts {
            assert!(got.contains(p), "{q}: `{p}` missing:\n{got}");
        }
    };
    // A node of several labels: each arm's id as a text (a string's as
    // itself), a `Date` in its ends' `DateTime`.
    has(
        "MATCH (n) RETURN count(DISTINCT n)",
        &[
            r#"CAST(e.id AS Nullable(String)) AS "__cg_id_0" FROM db.a"#,
            r#"e.code AS "__cg_id_0" FROM db.b"#,
            r#"CAST(CAST(e.day AS Nullable(DateTime64(3))) AS Nullable(String)) AS "__cg_id_0" FROM db.d"#,
        ],
    );
    // Its ties to one label's ends, an equality with a node of one label,
    // a walk's ties and restriction, and the two orientations of a
    // relationship between two labels spell those as texts too.
    has(
        "MATCH (n) WITH n MATCH (n)-[:AB]->(b) RETURN b.name",
        &["ON CAST(v2.a_id AS Nullable(String)) = w2.v1____cg_id_0"],
    );
    has(
        "MATCH (a:A), (n) WHERE a = n RETURN count(*)",
        &["CAST(v0.id AS Nullable(String)) = v1.__cg_id_0"],
    );
    has(
        "MATCH (x:C)-[*1..2]-(y) RETURN y.name",
        &[
            r#"CAST(e.c_id AS Nullable(String)) AS "__cg_start_0""#,
            "ON v1.start_id = CAST(v0.id AS Nullable(String))",
        ],
    );
    has(
        "MATCH (x:C) WHERE x.name = 'c' MATCH (x)-[*1..2]-(y) RETURN y.name",
        &[r#"CAST(v0.id AS Nullable(String)) AS "id""#],
    );
    has(
        "MATCH ()-[r:AB]->() MATCH (x)-[r]-(y) RETURN x.name",
        &["THEN v1.b_code ELSE CAST(v1.a_id AS Nullable(String)) END = v3.__cg_id_0"],
    );
    // An element id reads its text back in its label's own type: a `D`'s
    // is its `Date`'s, as of a node of label `D`.
    has(
        "MATCH (n) RETURN n",
        &["CASE WHEN v0.__cg_label = 'D' THEN CAST(CAST(CAST(v0.__cg_id_0 AS Nullable(DateTime64(3))) AS Nullable(Date)) AS Nullable(String)) ELSE v0.__cg_id_0 END"],
    );
    // Times are spelled as instants of one zone: a `DateTime` id with ends
    // in another zone meets them (review finding: each side's own zone's
    // wall clock matched nothing).
    let zoned = schema(
        &YAML
            .replace(
                "{at: DateTime, b_code: String}",
                "{at: 'DateTime(''Asia/Tokyo'')', b_code: String}",
            )
            .replace(
                "column_types: {day: Date}",
                "column_types: {day: 'DateTime(''UTC'')'}",
            ),
    );
    let got = lowered("MATCH (n) RETURN count(DISTINCT n)", &zoned, &options);
    assert!(
        got.contains(
            r#"CAST(CAST(e.day AS Nullable(DateTime64(0, 'UTC'))) AS Nullable(String)) AS "__cg_id_0""#
        ),
        "{got}"
    );
    let got = lowered(
        "MATCH (n) WITH n MATCH (n)-[:DB]->(b) RETURN b.name",
        &zoned,
        &options,
    );
    assert!(
        got.contains(".at AS Nullable(DateTime64(0, 'UTC'))) AS Nullable(String)) = w"),
        "{got}"
    );
    // A `Date` id with times of several zones: a time is its date's text
    // where it is midnight in its own zone, else its time's (no one cast).
    let several = schema(&format!(
        "{}    - type: DD\n      database: db\n      table: dd\n      from_node: D\n      to_node: D\n      from_id: s\n      to_id: t\n      column_types: {{s: 'DateTime(''UTC'')', t: 'DateTime(''UTC'')'}}\n      property_mappings: {{}}\n    - type: DT\n      database: db\n      table: dt\n      from_node: D\n      to_node: D\n      from_id: s\n      to_id: t\n      column_types: {{s: 'DateTime(''Asia/Tokyo'')', t: 'DateTime(''Asia/Tokyo'')'}}\n      property_mappings: {{}}\n",
        YAML.replace(
            "{at: DateTime, b_code: String}",
            "{at: 'DateTime(''Asia/Tokyo'')', b_code: String}",
        )
    ));
    let got = lowered("MATCH (n) RETURN count(DISTINCT n)", &several, &options);
    assert!(
        got.contains(
            r#"replaceRegexpOne(toString(e.day), ' 00:00:00(\\.0+)?$', '') AS "__cg_id_0""#
        ),
        "{got}"
    );
    // So does a union of relationships between nodes of that one label,
    // its columns typed one zone otherwise, and the node side meeting them.
    let got = lowered(
        "MATCH (x:D)-[r:DD|DT]->(y:D) RETURN y.name",
        &several,
        &options,
    );
    assert!(
        got.contains(
            r#"replaceRegexpOne(toString(e.s), ' 00:00:00(\\.0+)?$', '') AS "__cg_from_0""#
        ),
        "{got}"
    );
    assert!(got.contains("replaceRegexpOne(toString(v0.day)"), "{got}");
    // So does a walk: its nodes, its relationships' ends and its keys.
    let got = lowered(
        "MATCH (x:D)-[:DD|DT*1..2]->(y:D) RETURN y.name",
        &several,
        &options,
    );
    for part in [
        r#"replaceRegexpOne(toString(e.day), ' 00:00:00(\\.0+)?$', '') AS "__cg_id_0""#,
        r#"replaceRegexpOne(toString(e.s), ' 00:00:00(\\.0+)?$', '') AS "__cg_start_0""#,
        "concat('D', ':', toString(replaceRegexpOne(toString(e.day)",
    ] {
        assert!(got.contains(part), "`{part}` missing:\n{got}");
    }
    // Spark spells that text with its own functions.
    let got = squash(
        &databricks_sql("MATCH (x:D)-[:DD|DT*1..2]->(y:D) RETURN y.name", &several).unwrap(),
    );
    assert!(
        got.contains("regexp_replace(CAST(e.day AS STRING), ' 00:00:00(\\\\.0+)?$', '')"),
        "{got}"
    );
    assert!(!got.contains("replaceRegexpOne"), "{got}");
    // A walk whose ends are times of several zones keys them as instants of
    // one zone (each in its own zone's wall clock matched nothing).
    let got = lowered(
        "MATCH (x:D)-[:DB|DD*1..2]->(y) RETURN y.name",
        &zoned,
        &options,
    );
    assert!(
        got.contains("toString(CAST(e.day AS DateTime64(0, 'UTC')))"),
        "{got}"
    );
    // Ids of one kind (no `B`, no `D`): the SQL of before.
    let one_kind = schema(
        &YAML
            .replace(
                "column_types: {code: String}",
                "column_types: {code: UInt64}",
            )
            .replace(
                "{a_id: UInt64, b_code: String}",
                "{a_id: UInt64, b_code: UInt64}",
            )
            .replace(
                "{at: DateTime, b_code: String}",
                "{at: UInt64, b_code: UInt64}",
            )
            .replace("column_types: {day: Date}", "column_types: {day: UInt64}"),
    );
    for q in [
        "MATCH (n) RETURN count(DISTINCT n)",
        "MATCH (n) WITH n MATCH (n)-[:AB]->(b) RETURN b.name",
        "MATCH (a:A), (n) WHERE a = n RETURN count(*)",
        "MATCH (x:C)-[*1..2]-(y) RETURN y.name",
        "MATCH (n) RETURN n",
    ] {
        let got = lowered(q, &one_kind, &options);
        assert!(!got.contains("AS Nullable(String))"), "{q}:\n{got}");
    }
    // Spark: as before.
    let got = squash(&databricks_sql("MATCH (n) RETURN count(DISTINCT n)", &mixed).unwrap());
    assert!(!got.contains("AS STRING)"), "{got}");
}

/// ELEMENT_IDENTITY.md I3 (#1334): a relationship whose schema declares an
/// `edge_id` carries it in its element id (escaped) and returns it as a
/// column, from which Bolt spells the same; its ends' ids are spelled as
/// their nodes spell theirs (a `Date` node's `D:2020-01-01-`, not its
/// `DateTime` end's text), in its value, its columns and `id(r)`.
#[test]
fn relationship_element_ids_carry_edge_ids_and_their_nodes_ids_1334() {
    const YAML: &str = r#"
name: lower_rel_element_ids
graph_schema:
  nodes:
    - label: U
      database: db
      table: u
      node_id: id
      column_types: {id: UInt64}
      property_mappings: {name: name}
    - label: D
      database: db
      table: d
      node_id: day
      column_types: {day: Date}
      property_mappings: {name: name}
    - label: T
      database: db
      table: t
      node_id: ts
      column_types: {ts: "DateTime('Asia/Tokyo')"}
      property_mappings: {name: name}
  edges:
    - type: P
      database: db
      table: p
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: pid
      column_types: {a: UInt64, b: UInt64, pid: String}
      property_mappings: {w: w}
    - type: DU
      database: db
      table: du
      from_node: D
      to_node: U
      from_id: at
      to_id: u
      column_types: {at: DateTime, u: UInt64}
      property_mappings: {}
    - type: TT
      database: db
      table: tt
      from_node: T
      to_node: T
      from_id: at
      to_id: bt
      column_types: {at: "DateTime('UTC')", bt: "DateTime('Asia/Tokyo')"}
      property_mappings: {}
    - type: PE
      database: db
      table: pe
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: pid
      column_types: {a: UInt64, b: UInt64, pid: String, x: String}
      property_mappings: {edge_id: pid, elementId: x}
    - type: FD
      database: db
      table: fd
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: e
      column_types: {a: UInt64, b: UInt64, e: Date}
      property_mappings: {}
    - type: FDT
      database: db
      table: fdt
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: e
      column_types: {a: UInt64, b: UInt64, e: DateTime}
      property_mappings: {}
    - type: FDT64
      database: db
      table: fdt64
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: e
      column_types: {a: UInt64, b: UInt64, e: "DateTime64(3)"}
      property_mappings: {}
    - type: FI
      database: db
      table: fi
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: e
      column_types: {a: UInt64, b: UInt64, e: Int64}
      property_mappings: {}
    - type: FU
      database: db
      table: fu
      from_node: U
      to_node: U
      from_id: a
      to_id: b
      edge_id: e
      column_types: {a: UInt64, b: UInt64, e: UInt64}
      property_mappings: {}
    - type: TM
      database: db
      table: tm
      from_node: T
      to_node: T
      from_id: at
      to_id: bt
      column_types: {at: "DateTime64(3, 'Asia/Tokyo')", bt: "DateTime('Asia/Tokyo')"}
      property_mappings: {}
"#;
    let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(YAML)
        .unwrap()
        .to_graph_schema()
        .unwrap();
    let options = LowerOptions::default();
    let has = |q: &str, parts: &[&str]| {
        let got = lowered(q, &schema, &options);
        for p in parts {
            assert!(got.contains(p), "{q}: `{p}` missing:\n{got}");
        }
    };
    let edge = format!("concat({}), ':'", edge_text("v1.pid"));
    // Returned whole: its element id a column after its properties (Bolt's),
    // a value's element id, `id(r)`.
    has(
        "MATCH (a:U)-[r:P]->(b:U) RETURN r",
        &[&format!(
            r#"v1.b AS "r.to_id", v1.w AS "r.w", concat('P:', {edge}, toString(v1.a), '->', toString(v1.b), '-') AS "r.elementId" FROM"#
        )],
    );
    has(
        "MATCH p = (a:U)-[r:P]->(b:U) RETURN p",
        &[&format!(
            "concat('P:', {edge}, toString(v1.a), '->', toString(v1.b), '-')"
        )],
    );
    has(
        "MATCH (a:U)-[r:P]->(b:U) RETURN id(r) AS i",
        &[&format!("concat('P:', {edge}, toString(v1.a)")],
    );
    // In a list and a walk.
    has(
        "MATCH (a:U)-[r:P]->(b:U) WITH collect(r) AS rs RETURN rs",
        &["concat('P:', concat(CASE WHEN tupleElement("],
    );
    has(
        "MATCH p = (a:U)-[:P*1..2]->(b:U) RETURN p",
        &[&format!(
            "concat('P:', concat({}), ':'",
            edge_text("rel.pid")
        )],
    );
    // A `DateTime` end of a `Date` node: the node's text where `=` holds.
    let day = r#"replaceRegexpOne(toString(v1.at), ' 00:00:00(\\.0+)?$', '')"#;
    has(
        "MATCH (a:D)-[r:DU]->(b:U) RETURN r",
        &[&format!(
            r#"SELECT {day} AS "r.from_id", v1.u AS "r.to_id""#
        )],
    );
    has(
        "MATCH p = (a:D)-[r:DU]->(b:U) RETURN p",
        &[
            &format!("'elementId', CAST(concat('DU:', toString({day}), '->'"),
            &format!("'startNodeElementId', CAST(concat('D:', toString({day}), '-')"),
        ],
    );
    // Rows equal in its columns are one relationship: the spelling is one to
    // one, so DISTINCT with an aggregation still lowers.
    has(
        "MATCH (a:D)-[r:DU]->(b:U) RETURN DISTINCT r, count(*) AS c",
        &[&format!("GROUP BY {day}, v1.u, v1.at")],
    );
    // A time of another zone: as an instant in the node's; of its own, as is.
    has(
        "MATCH (a:T)-[r:TT]->(b:T) RETURN r",
        &[
            r#"SELECT CAST(v1.at AS Nullable(DateTime('Asia/Tokyo'))) AS "r.from_id", v1.bt AS "r.to_id""#,
        ],
    );
    // Properties named `edge_id` and `elementId` keep their columns; the
    // element id's is another, under a key no property has.
    let (sql, shape) = {
        let t = crate::server::query_context::with_query_context_sync(
            crate::server::query_context::QueryContext::new(None),
            || {
                crate::server::query_context::set_current_schema(std::sync::Arc::new(
                    schema.clone(),
                ));
                translate_bound_plan(
                    "MATCH (a:U)-[r:PE]->(b:U) RETURN r",
                    &schema,
                    &ReadOptions::default(),
                )
                .unwrap()
            },
        );
        (squash(&t.sql), t.shape)
    };
    assert!(
        sql.contains(r#"v1.pid AS "r.edge_id", v1.x AS "r.elementId", concat('PE:'"#)
            && sql.contains(r#"AS "r.elementId_2" FROM"#),
        "{sql}"
    );
    assert!(
        shape[0]
            .columns
            .iter()
            .any(|(k, c)| k == crate::bound_plan::lower::REL_ELEMENT_ID_KEY && c == "r.elementId_2"),
        "{:?}",
        shape[0].columns
    );
    // A union holds a `DateTime` edge id with a `DateTime64` one in the
    // wider type: each read back in its own.
    has(
        "MATCH (a:U)-[r:FDT|FDT64]->(b:U) RETURN r",
        &["CASE WHEN CAST(v1.__cg_rid_0 AS Nullable(DateTime)) IS NULL"],
    );
    // I3c: edge ids of several kinds (a `Date` and a `DateTime`, a string
    // and an integer) are held as texts, which a GROUP BY and a walk take
    // (a `Variant` they refuse); integers of no common type meet in
    // `Int128`.
    has(
        "MATCH (a:U)-[r:FD|FDT]->(b:U) RETURN DISTINCT r",
        &[
            r#"'FD' AS "__cg_type", 'U' AS "__cg_from_label", 'U' AS "__cg_to_label", CAST(e.e AS Nullable(String)) AS "__cg_rid_0""#,
            r#"'FDT' AS "__cg_type", 'U' AS "__cg_from_label", 'U' AS "__cg_to_label", CAST(e.e AS Nullable(String)) AS "__cg_rid_0""#,
        ],
    );
    has(
        "MATCH (a:U)-[r:P|FI]->(b:U) RETURN DISTINCT r",
        &[
            r#"CAST(e.pid AS Nullable(String)) AS "__cg_rid_0""#,
            r#"'FI' AS "__cg_type", 'U' AS "__cg_from_label", 'U' AS "__cg_to_label", CAST(e.e AS Nullable(String)) AS "__cg_rid_0""#,
        ],
    );
    // A union in a wider type compared with one of texts: each definition's
    // part read back in its own type first, so it meets its own text (a
    // `DateTime` as a `DateTime64` spells its fraction; review).
    has(
        "MATCH ()-[r1:FDT|FDT64]->() MATCH ()-[r2:FDT|FI]->() WHERE r1 = r2 RETURN count(*) AS c",
        &["THEN CAST(CAST(v1.__cg_rid_0 AS Nullable(DateTime)) AS Nullable(String))"],
    );
    // A walk over one definition of a union held as texts: the union's
    // part read back in the walk's type on that definition's rows (review).
    has(
        "MATCH (a:U)-[r1:FD|FDT]-(b:U)-[r2:FDT*1..2]-(c:U) RETURN count(*) AS c",
        &["CASE WHEN (v1.__cg_type = 'FDT' AND v1.__cg_from_label = 'U' AND v1.__cg_to_label = 'U') THEN CAST(v1.__cg_rid_0 AS Nullable(DateTime)) ELSE NULL END"],
    );
    has(
        "MATCH p = (a:U)-[:FI|FU*1..2]->(b:U) RETURN p",
        &[r#"CAST(e.e AS Nullable(Int128)) AS "__cg_rid_0", e.a AS "__cg_from_0""#],
    );
    // A time finer than its node's: the node's text where its value in the
    // node's type is it, else its own.
    has(
        "MATCH (a:T)-[r:TM]->(b:T) RETURN r",
        &[
            r#"SELECT CASE WHEN CAST(v1.at AS Nullable(DateTime('Asia/Tokyo'))) = v1.at THEN toString(CAST(v1.at AS Nullable(DateTime('Asia/Tokyo')))) ELSE toString(v1.at) END AS "r.from_id""#,
        ],
    );
    // Spark spells the escapes with `replace`.
    let got = squash(&databricks_sql("MATCH p = (a:U)-[r:P]->(b:U) RETURN p", &schema).unwrap());
    assert!(
        got.contains(r#"concat('P:', concat(CASE WHEN v1.pid IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.pid AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':'"#),
        "{got}"
    );
}

/// ELEMENT_IDENTITY.md I6 / I6c (#1354; user 2026-10-19, revised
/// 2026-10-24): a row whose composite id has a NULL part is a node, whose
/// part is compared NULL-safely where it is the node's own identity (`a =
/// b`, a walk's ends, a subquery's correlation, a list's elements) and with
/// `=` where a relationship's end ties to it; a row whose id is all NULL, or
/// a single-column NULL id, is no node, in every scan of its label. Where
/// its id's columns cannot be NULL, or their types are unknown, the SQL is
/// as before.
#[test]
fn a_composite_id_with_a_null_part_is_a_node_1354() {
    const YAML: &str = r#"
name: lower_null_id_parts
graph_schema:
  nodes:
    - label: M
      database: db
      table: m
      node_id: [a, b, c]
      column_types: {a: String, b: UInt32, c: "Nullable(String)"}
      property_mappings: {name: name}
    - label: K
      database: db
      table: k
      node_id: [a, b, c]
      column_types: {a: String, b: UInt32, c: String}
      property_mappings: {name: name}
    - label: U
      database: db
      table: u
      node_id: [a, b, c]
      property_mappings: {name: name}
    - label: A
      database: db
      table: an
      node_id: [a, b, c]
      column_types: {a: "Nullable(String)", b: "Nullable(UInt32)", c: "Nullable(String)"}
      property_mappings: {name: name}
    - label: S
      database: db
      table: s
      node_id: id
      column_types: {id: "Nullable(Int64)"}
      property_mappings: {name: name}
  edges:
    - type: E
      database: db
      table: e
      from_node: M
      to_node: M
      from_id: [fa, fb, fc]
      to_id: [ta, tb, tc]
      column_types: {fa: String, fb: UInt32, fc: "Nullable(String)", ta: String, tb: UInt32, tc: "Nullable(String)"}
      property_mappings: {}
"#;
    let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(YAML)
        .unwrap()
        .to_graph_schema()
        .unwrap();
    let options = LowerOptions::default();
    let sql = |q: &str| lowered(q, &schema, &options);
    // A part that may be NULL is no filter: such a row is a node.
    let got = sql("MATCH (n:M) RETURN n.name");
    assert!(!got.contains("IS NOT NULL"), "{got}");
    // An id that is all NULL, or of one NULL column, is none.
    let got = sql("MATCH (n:A) RETURN n.name");
    assert!(
        got.contains("(v0.a IS NOT NULL OR v0.b IS NOT NULL OR v0.c IS NOT NULL)"),
        "{got}"
    );
    let got = sql("MATCH (n:S) RETURN n.name");
    assert!(got.contains("WHERE v0.id IS NOT NULL"), "{got}");
    // Each arm of a node of several labels.
    let got = sql("MATCH (n:M|A) RETURN count(n) AS c");
    assert!(
        got.contains(
            "FROM db.an AS e WHERE (e.a IS NOT NULL OR e.b IS NOT NULL OR e.c IS NOT NULL)"
        ),
        "{got}"
    );
    assert!(!got.contains("FROM db.m AS e WHERE"), "{got}");
    // A node is itself: its NULL-able part compared NULL-safely, the others
    // with `=`; of an id that cannot be NULL, or of unknown types, as before.
    let got = sql("MATCH (x:M), (y:M) WHERE x = y RETURN x.name");
    assert!(
        got.contains("v0.a = v1.a AND v0.b = v1.b AND (isNotDistinctFrom(v0.c, v1.c) AND true)"),
        "{got}"
    );
    let got = sql("MATCH (x:M), (y:M) WHERE x <> y RETURN x.name");
    assert!(
        got.contains("NOT (isNotDistinctFrom(v0.c, v1.c) AND true)"),
        "{got}"
    );
    for label in ["K", "U"] {
        let got = sql(&format!(
            "MATCH (x:{label}), (y:{label}) WHERE x = y RETURN x.name"
        ));
        assert!(!got.contains("isNotDistinctFrom"), "{got}");
    }
    // A relationship's end ties with `=` (none attaches to a NULL part); a
    // walk's ends, read from its nodes' rows, are its nodes.
    let got = sql("MATCH (x:M)-[:E]->(y:M) RETURN x.name, y.name");
    assert!(!got.contains("isNotDistinctFrom"), "{got}");
    let got = sql("MATCH (x:M)-[:E*0..2]->(y:M) RETURN x.name, y.name");
    assert!(
        got.contains("(isNotDistinctFrom(v1.start_c, v0.c) AND true)"),
        "{got}"
    );
    assert!(got.contains("vp.end_c = rel.fc"), "{got}");
    // A subquery's correlation and a list's elements.
    let got = sql("MATCH (x:M) WHERE EXISTS { MATCH (x) WHERE x.name = 'n' } RETURN x.name");
    assert!(got.contains("isNotDistinctFrom(v0.c, s1.v0__c)"), "{got}");
    let got = sql("MATCH (x:M) WITH collect(x) AS l MATCH (y:M) WHERE y IN l RETURN y.name");
    assert!(
        got.contains("isNotDistinctFrom(tupleElement(__cg_in, 3), v2.c)"),
        "{got}"
    );
    assert!(
        got.contains("WHEN v2.a IS NULL AND v2.b IS NULL AND v2.c IS NULL THEN NULL"),
        "{got}"
    );
    // Of an id that cannot be NULL, a list's `x` is NULL where any column
    // is, as before.
    let got = sql("MATCH (x:K) WITH collect(x) AS l MATCH (y:K) WHERE y IN l RETURN y.name");
    assert!(
        got.contains("WHEN v2.a IS NULL OR v2.b IS NULL OR v2.c IS NULL THEN NULL"),
        "{got}"
    );
    // A walk from earlier rows: its first nodes by their tuple's text (a
    // tuple holding NULL is `IN` no rows).
    let got = sql("MATCH (x:M) WITH x MATCH (x)-[:E*0..2]->(y:M) RETURN x.name, y.name");
    assert!(
        got.contains(
            "toString(tuple(start_node.a, start_node.b, start_node.c)) IN (SELECT DISTINCT"
        ),
        "{got}"
    );
    let got = sql("MATCH (x:K) WITH x MATCH (x)-[:E*0..2]->(y:M) RETURN x.name");
    assert!(!got.contains("toString(tuple(start_node"), "{got}");
    // Spark spells it `<=>`.
    let got =
        squash(&databricks_sql("MATCH (x:M), (y:M) WHERE x = y RETURN x.name", &schema).unwrap());
    assert!(got.contains("((v0.c <=> v1.c) AND true)"), "{got}");
    // On the server, `id(n) = N`'s part the node's may hold NULL in is false,
    // not NULL, where it is NULL.
    let server = LowerOptions {
        id_lookup: Some(IdLookup {
            node: std::sync::Arc::new(|id| match id {
                1 => Some(("M".to_string(), "x|1|y".to_string())),
                _ => None,
            }),
            element: std::sync::Arc::new(|_| None),
        }),
        ..LowerOptions::default()
    };
    let got = lowered(
        "MATCH (x:M) WHERE id(x) <> 1 RETURN x.name",
        &schema,
        &server,
    );
    assert!(
        got.contains("v0.a = 'x' AND v0.b = '1' AND coalesce(v0.c = 'y', false)"),
        "{got}"
    );
}

/// ELEMENT_IDENTITY.md I6d (#1354 comment): a relationship whose `edge_id`
/// may be NULL is identified by its stored ends and its `edge_id` (as its
/// element id `TYPE:\N:from->to-` tells it apart), the `edge_id` compared
/// NULL-safely; it is present where its ends are (`count(r)`); its element
/// id spells the `edge_id` alone. An `edge_id` that cannot be NULL, or of
/// unknown types, keeps its SQL.
#[test]
fn a_relationship_whose_edge_id_has_a_null_part_is_one() {
    const YAML: &str = r#"
name: lower_null_edge_id
graph_schema:
  nodes:
    - {label: N, database: db, table: n, node_id: id, column_types: {id: Int64}, property_mappings: {name: name}}
  edges:
    - type: E
      database: db
      table: e
      from_node: N
      to_node: N
      from_id: fa
      to_id: ta
      edge_id: eid
      column_types: {eid: "Nullable(Int64)", fa: Int64, ta: Int64}
      property_mappings: {w: w}
    - type: K
      database: db
      table: k
      from_node: N
      to_node: N
      from_id: fa
      to_id: ta
      edge_id: eid
      column_types: {eid: Int64, fa: Int64, ta: Int64}
      property_mappings: {w: w}
"#;
    let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(YAML)
        .unwrap()
        .to_graph_schema()
        .unwrap();
    let rs = schema.get_rel_schema("E").unwrap();
    assert!(rs.edge_id_nullable());
    assert_eq!(rs.identity_columns(), vec!["fa", "ta", "eid"]);
    let options = LowerOptions::default();
    let sql = |q: &str| lowered(q, &schema, &options);
    // Present where its ends are; its identity's tuple counted.
    let got = sql("MATCH ()-[r:E]->() RETURN count(r) AS c, count(DISTINCT r) AS d");
    assert!(got.contains("tuple(v1.fa, v1.ta, v1.eid)"), "{got}");
    assert!(got.contains("WHEN v1.fa IS NULL THEN NULL"), "{got}");
    // Relationships of one MATCH differ NULL-safely.
    let got = sql("MATCH (a)-[r1:E]->(b)-[r2:E]->(c) RETURN count(*) AS c");
    assert!(
        got.contains("(v1.fa <> v3.fa OR v1.ta <> v3.ta OR NOT (isNotDistinctFrom(v1.eid, v3.eid) AND true))"),
        "{got}"
    );
    // A walk's relationships by its ends and `edge_id`.
    let got = sql("MATCH (a:N)-[:E*1..2]->(b:N) RETURN count(*) AS c");
    assert!(got.contains("tuple(rel.fa, rel.ta, rel.eid)"), "{got}");
    // Its element id: the `edge_id` alone before its ends.
    let got = sql("MATCH ()-[r:E]->() RETURN r");
    assert!(
        got.contains("'E:', concat(CASE WHEN v1.eid IS NULL"),
        "{got}"
    );
    // An `edge_id` that cannot be NULL: by it alone, as before.
    let got = sql("MATCH (a)-[r1:K]->(b)-[r2:K]->(c) RETURN count(*) AS c");
    assert!(got.contains("v1.eid <> v3.eid"), "{got}");
    assert!(!got.contains("isNotDistinctFrom"), "{got}");
    // A union of types: its identity past its definition NULL-safely,
    // two-valued (review).
    let got = sql("MATCH (x)-[r1:E|K]->(y), (x)-[r2:E|K]->(y) RETURN count(*) AS c");
    assert!(got.contains("NOT (isNotDistinctFrom("), "{got}");
    let got = sql("MATCH (x)-[r1:E|K]->(y) MATCH (x)-[r2:E|K]->(y) RETURN r1 = r2 AS s");
    assert!(got.contains("(isNotDistinctFrom("), "{got}");
    assert!(!got.contains(" IS NULL AND "), "{got}");
    // An OPTIONAL MATCH after it joins on its properties too (two with a
    // NULL `edge_id` and the same ends are one).
    let got =
        sql("MATCH (a)-[r:E]->(b) OPTIONAL MATCH (b)-[s:K]->(c) WHERE s.w > r.w RETURN r.w, s.w");
    assert!(
        got.contains("(w1.p2_v1_w = o3.p2_v1_w OR (w1.p2_v1_w IS NULL AND o3.p2_v1_w IS NULL))"),
        "{got}"
    );
}

/// #1495: a value drawn in a CTE read more than once (rand() /
/// randomUUID() / the clock draw again on every read) decides the result
/// across the reads when it is joined back as a join key of the reads.
#[test]
fn a_drawn_value_joined_back_across_two_reads_is_refused() {
    let err = translate_bound_plan(
        "MATCH (a:User) WITH a, rand() AS r \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id AND r < 2 \
         RETURN count(*), count(b)",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.contains("draw again on every read") && err.contains("is a join key of its reads"),
        "{err}"
    );
    // A literal on the other side does not agree the reads either.
    let err = translate_bound_plan(
        "MATCH (a:User) WITH a, rand() AS r \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id AND r = 0.5 \
         RETURN count(*), count(b)",
        &social(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("is a join key of its reads"), "{err}");
}

/// #1495: the same on Databricks, whose drawn spellings are `rand` and
/// `current_timestamp` (the refusal machinery is dialect-shared).
#[test]
fn a_databricks_drive_that_joins_on_a_drawn_value_is_refused() {
    let err = databricks_sql(
        "MATCH (a:User) WITH a, rand() AS r \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id AND r < 2 \
         RETURN count(*), count(b)",
        &social(),
    )
    .unwrap_err();
    assert!(err.contains("is a join key of its reads"), "{err}");
}

/// #1495: a drawn call that filters (orders, groups) the rows of a CTE
/// read more than once keeps a different row set per read (the counts of
/// the drive and of the final read disagree).
#[test]
fn a_drawn_filter_of_a_reread_cte_is_refused() {
    for q in [
        "MATCH (a:User) WHERE rand() < 0.5 \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*), count(b)",
        "MATCH (a:User) WITH a WHERE rand() < 0.5 \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*), count(b)",
    ] {
        let err = translate_bound_plan(q, &social(), &ReadOptions::default()).unwrap_err();
        assert!(
            err.contains("draw again on every read")
                && err.contains("selects different rows on every read"),
            "{q}: {err}"
        );
    }
}

/// #1495: a drawn value that cannot decide across the reads lowers as
/// before: dropped by the drive, read once, summed or counted, or a
/// same-day draw whose reads agree.
#[test]
fn a_drawn_value_that_cannot_decide_across_reads_lowers_as_before() {
    // Carried and unread by the drive: the drive drops the column, and
    // the join is by identity alone.
    let got = sql("MATCH (a:User) WITH a, rand() AS r \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*), count(b)");
    assert!(got.contains(r#"randCanonical() AS "v2""#), "{got}");
    assert!(!got.contains("w1.v2 = o3.v2"), "{got}");
    // Read once, by the inner MATCH only (no re-read is left to
    // disagree): one external draw.
    let got = sql("MATCH (a:User) WITH a, rand() AS r \
         MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*) AS c, sum(r) AS s");
    assert!(got.contains(r#"coalesce(sum(w1.v2), 0) AS "s""#), "{got}");
    // Read again, but its drawn column is only summed over the final
    // SELECT's own rows: never a join key.
    has(
        "MATCH (a:User) WITH a, rand() AS r \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*) AS c, sum(r) AS s",
        &[
            r#"coalesce(sum(w1.v2), 0) AS "s""#,
            "ON w1.v1__user_id = o3.v1__user_id",
        ],
    );
    // The clock drawn carried (a nested drawn call), unread.
    let got = sql("MATCH (a:User) WITH a, timestamp() AS t \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*) AS c, count(t) AS n");
    assert!(got.contains(r#"toUnixTimestamp(now()) AS "v2""#), "{got}");
    // randomUUID() is spelled through unmapped; carried, unread.
    let got = sql("MATCH (a:User) WITH a, randomUUID() AS u \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id \
         RETURN count(*) AS c, count(u) AS n");
    assert!(got.contains(r#"randomUUID() AS "v2""#), "{got}");
    // A same-day draw whose drawn column IS carried and joined back: the
    // reads agree within the day, so this is correct as it is.
    let got = sql("MATCH (a:User) WITH a, date() AS d \
         OPTIONAL MATCH (b:User) WHERE b.user_id = a.user_id AND d < date('2030-01-01') \
         RETURN count(*), count(b)");
    assert!(
        got.contains(r#"toDate(today()) AS "v2""#) && got.contains("w1.v2 = o3.v2"),
        "{got}"
    );
}
