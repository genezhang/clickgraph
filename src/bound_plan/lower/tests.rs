//! Lowering tests: the SQL each lowered construct produces, and
//! `Unsupported` for what is not lowered yet. Row-level correctness is
//! checked against Neo4j by the oracle (`scripts/oracle/run_corpus.py` with
//! `CLICKGRAPH_BOUND_PLAN=on`).

use std::collections::HashMap;

use crate::bound_plan::lower::{
    lower_statement, GraphType, LowerOptions, ResultColumn, ResultKind,
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
    // And with its node's FINAL, which a joined read cannot print yet: not
    // lowered, rather than read without it (stale row versions).
    let err = translate_bound_plan(
        "MATCH (a:Ver)-[:ABOUT]->(b:Comment) RETURN count(*)",
        &fk_schema(),
        &ReadOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("FINAL on a joined table"), "{err}");
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
    not_lowered("MATCH (a:User) WHERE id(a) = 1 RETURN a.name", "id()");
    not_lowered("MATCH (n) RETURN n.*", "`v.*` of an element of several");
    not_lowered(
        "MATCH (:User)-[r:FOLLOWS|LIKED]->() RETURN r.*",
        "`v.*` of an element of several",
    );
    not_lowered("MATCH (n) RETURN id(n)", "id()");
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
             FROM test_integration.users_test AS start_node WHERE (start_node.user_id = 1) \
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
            "WHERE (start_node.user_id = 1)",
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
            "WHERE (start_node.user_id = 5)",
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

#[test]
fn what_shortest_paths_are_not_lowered() {
    not_lowered(
        "MATCH p = shortestPath((a:User)-[:FOLLOWS]->(b:User)) RETURN count(*)",
        "shortestPath over a fixed-length relationship",
    );
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
fn databricks_shortest_path_is_not_lowered() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    // Searched, and picked among trails.
    for q in [
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) RETURN count(*)",
        "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE length(p) > 1 \
         RETURN count(*)",
    ] {
        let got = with_query_context_sync(ctx.clone(), || {
            set_current_schema(std::sync::Arc::new(social()));
            translate_bound_plan(q, &social(), &ReadOptions::default())
        });
        assert!(
            matches!(&got, Err(e) if e.contains("shortestPath in this SQL dialect")),
            "{q}: {got:?}"
        );
    }
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
    let rel = "map('elementId', CAST(concat('FOLLOWS:', toString(v1.follower_id), '->', \
               toString(v1.followed_id), '-'), 'Dynamic'), 'startNodeElementId', \
               CAST(concat('User:', toString(v1.follower_id), '-'), 'Dynamic'), \
               'endNodeElementId', CAST(concat('User:', toString(v1.followed_id), '-'), \
               'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', \
               CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', \
               CAST(v1.follow_date, 'Dynamic'))), 'Dynamic'))";
    for part in [squash(node), squash(rel)] {
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
            "arrayConcat(vp.path_rel_values, [map('elementId', CAST(concat('FOLLOWS:', toString(rel.follower_id)",
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
    not_lowered(
        "MATCH p = (a:User)-[:FOLLOWS*1..2]->(b:User), (c:User) WHERE c IN nodes(p) RETURN c",
        "as an operand",
    );
}

#[test]
fn databricks_path_values_are_not_lowered() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "MATCH p = (a:User)-[:FOLLOWS]->(b:User) RETURN p",
            &social(),
            &ReadOptions::default(),
        )
    });
    assert!(
        matches!(&got, Err(e) if e.contains("as a value on this dialect")),
        "{got:?}"
    );
    // A list passed through and not read as a value needs no spelling.
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
        "map('elementId', CAST(concat(v1.__cg_type, ':', toString(v1.__cg_from_0), '->', toString(v1.__cg_to_0), '-'), 'Dynamic')",
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
    let part = "JOIN (SELECT * FROM db.a AS e FINAL WHERE ((e.cur = 1))) AS v0 ON \
                concat('A', ':', toString(v0.id)) = v1.start_id";
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
    not_lowered(
        "MATCH (a:User) UNWIND [1, 2] AS x RETURN x LIMIT 1",
        "SKIP / LIMIT over ordered rows after DISTINCT, aggregation or UNWIND",
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
    // `collect` relies on that order: the rows are numbered in it.
    has(
        "UNWIND [3, 1] AS x RETURN collect(x) AS l",
        &[
            r#"row_number() OVER (ORDER BY w1.__o0 ASC) AS "__cg_row" FROM with_w1 AS w1 )"#,
            "SELECT arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w2.__cg_row, w2.v0), \
             NOT isNull(w2.v0)))) AS \"l\" FROM with_w2 AS w2",
        ],
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
    // Elements that are all NULL: the value is NULL (ClickHouse cannot
    // project the element of an `Array(Nothing)`).
    has(
        "WITH [] AS l UNWIND l AS x RETURN count(x)",
        &[
            "ARRAY JOIN [] AS __cg_element",
            "CASE WHEN count(*) >= 0 THEN 0 ELSE 0 END",
        ],
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

#[test]
fn databricks_unwind_is_not_lowered() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "UNWIND [1, 2] AS x RETURN x",
            &social(),
            &ReadOptions::default(),
        )
        .map(|t| t.sql)
    });
    assert!(
        matches!(&got, Err(e) if e.contains("UNWIND in this SQL dialect")),
        "{got:?}"
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
            r#"SELECT any(w3.__cg_c0) AS "a" FROM with_w3 AS w3 GROUP BY tuple(multiIf(dynamicType(CAST(w3.__cg_c0, 'Dynamic')) = 'None', '', "#
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
        "MATCH (a:User) RETURN a AS x UNION MATCH (p:Post) RETURN p AS x",
        "UNION column `x` of a different kind or label in another arm",
    );
    not_lowered(
        "MATCH (a:User) RETURN a AS x UNION ALL RETURN 1 AS x",
        "UNION column `x` of a different kind or label in another arm",
    );
    not_lowered(
        "MATCH (a:User) RETURN id(a) AS x UNION ALL MATCH (p:Post) RETURN id(p) AS x",
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
        sql.contains(r#"v1.weight AS "__cg_w1_c2", 1 AS "__cg_w1_c3", v1.kid AS "__cg_w1_k0" FROM"#)
            && sql.contains(r#"w2.__cg_w2_k0 AS "__cg_k0" FROM with_w2 AS w2"#)
            // Grouped by every column's key and the identity.
            && sql.contains(r#"SELECT any(w3.__cg_c0) AS "k.from_id","#)
            && sql.contains(r#"GROUP BY tuple(multiIf(dynamicType(CAST(w3.__cg_c0, "#)
            && sql.ends_with(r#"toString(w3.__cg_c3))), w3.__cg_k0"#),
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

#[test]
fn databricks_union_is_not_lowered() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "RETURN 1 AS x UNION RETURN 2 AS x",
            &social(),
            &ReadOptions::default(),
        )
        .map(|t| t.sql)
    });
    assert!(
        matches!(&got, Err(e) if e.contains("UNION in this SQL dialect")),
        "{got:?}"
    );
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
        &["arrayConcat(arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf((w2.__cg_row, w2.v0)"],
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
    // `length()` takes a path; Neo4j refuses a list (`size()` counts one).
    not_lowered(
        "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN length(r) AS l",
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

/// S7e2b review: a walk whose elements have no tuple layout (a composite
/// `edge_id`) carries none: its values (S6c) are still lowered, and reading
/// them as a list's elements is not.
#[test]
fn a_walk_without_a_tuple_layout_still_carries_values() {
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
    assert!(
        matches!(&got, Ok(sql) if sql.contains("path_rel_values") && !sql.contains("path_rel_tuples")),
        "{got:?}"
    );
    let got =
        lower("MATCH p = (a:U)-[:F*1..2]->(b:U) RETURN [r IN relationships(p) | r.since] AS s");
    assert!(
        matches!(&got, Err(e) if e.contains("relationships() of a path other than")),
        "{:?}",
        got.map(|t| t.sql)
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
    for (q, why) in [
        ("WITH {a: 1} AS m RETURN m.a AS a", "a property of a map"),
        ("WITH {a: 1} AS m RETURN m['a'] AS a", "a key of a map"),
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

#[test]
fn databricks_comprehension_is_not_lowered() {
    use crate::server::query_context::{set_current_schema, with_query_context_sync, QueryContext};
    let ctx = QueryContext {
        dialect: crate::sql_generator::SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let got = with_query_context_sync(ctx, || {
        set_current_schema(std::sync::Arc::new(social()));
        translate_bound_plan(
            "RETURN [x IN [1, 2] | x * 2] AS l",
            &social(),
            &ReadOptions::default(),
        )
        .map(|t| t.sql)
    });
    assert!(
        matches!(&got, Err(e) if e.contains("a list comprehension in this SQL dialect")),
        "{got:?}"
    );
}
