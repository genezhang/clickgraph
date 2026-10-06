//! Binder tests. Every accepted / rejected case was checked against Neo4j
//! 5.26 (the error texts follow Neo4j's).

use std::collections::BTreeSet;

use crate::graph_catalog::config::GraphSchemaConfig;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::clause_list::parse_clause_statement;
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};

use super::*;

fn schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(include_str!("../../schemas/test/social_integration.yaml"))
        .unwrap()
        .to_graph_schema()
        .unwrap()
}

fn bind(q: &str) -> Result<BoundStatement, BindError> {
    let (_, stmt) = parse_clause_statement(q).unwrap_or_else(|e| panic!("parse {q}: {e:?}"));
    bind_statement(&stmt, &schema())
}

fn ok(q: &str) -> BoundStatement {
    bind(q).unwrap_or_else(|e| panic!("{q}: {e}"))
}

/// All bindings with this user name, in creation order.
fn named(s: &BoundStatement, name: &str) -> Vec<Binding> {
    s.bindings
        .iter()
        .filter(|b| b.name.as_deref() == Some(name))
        .cloned()
        .collect()
}

fn labels(s: &BoundStatement, name: &str) -> BTreeSet<String> {
    match &named(s, name)[0].kind {
        BindingKind::Node { labels } => labels.clone(),
        k => panic!("{name} is {k:?}"),
    }
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// The MATCH operators of a plan, outermost last.
fn matches(op: &BoundOp) -> Vec<&BoundOp> {
    let mut out = Vec::new();
    let mut cur = op;
    loop {
        match cur {
            BoundOp::Unit => break,
            BoundOp::Match { input, .. } => {
                out.push(cur);
                cur = input;
            }
            BoundOp::Unwind { input, .. }
            | BoundOp::Project { input, .. }
            | BoundOp::Sort { input, .. }
            | BoundOp::Skip { input, .. }
            | BoundOp::Limit { input, .. } => cur = input,
            BoundOp::Union { .. } => break,
        }
    }
    out.reverse();
    out
}

// ------------------------------------------------------------------ scope

#[test]
fn a_name_rebound_after_with_is_a_new_variable() {
    // #1304's shape: the second `a` is not the first.
    let s = ok("MATCH (a:User)-[:FOLLOWS]->(c:User) WITH c MATCH (c)-[:FOLLOWS]->(a:User) RETURN a.user_id");
    let a = named(&s, "a");
    assert_eq!(a.len(), 2, "the first MATCH's a and the second MATCH's a");
    assert_ne!(a[0].id, a[1].id);
    let ms = matches(&s.plan);
    let BoundOp::Match {
        pattern,
        introduces,
        ..
    } = ms[1]
    else {
        unreachable!()
    };
    let c = &pattern.parts[0].nodes[0];
    assert!(c.bound_before, "c is carried through WITH");
    assert!(!introduces.contains(&c.var));
    assert!(introduces.contains(&a[1].id));
    // the carried c is the WITH's output binding, not the first MATCH's c
    let c_bindings = named(&s, "c");
    assert_eq!(c.var, c_bindings[1].id);
}

#[test]
fn a_with_scalar_reusing_a_node_name_is_a_value() {
    // #1263's shape
    let s = ok("MATCH (a:User) WITH a.user_id AS a RETURN a");
    let a = named(&s, "a");
    assert!(matches!(a[0].kind, BindingKind::Node { .. }));
    assert_eq!(a[1].kind, BindingKind::Value);
}

#[test]
fn undefined_variables_are_errors() {
    assert_eq!(
        bind("MATCH (a:User)-->(b) WITH a RETURN b").unwrap_err(),
        BindError::UndefinedVariable("b".into())
    );
    assert_eq!(
        bind("UNWIND [3,1] AS x WITH x AS y LIMIT 2 ORDER BY x RETURN y").unwrap_err(),
        BindError::UndefinedVariable("x".into()),
        "a free-standing ORDER BY sees only the previous clause's output"
    );
}

#[test]
fn redeclarations_and_type_conflicts_are_errors() {
    assert_eq!(
        bind("MATCH (a:User) UNWIND [1] AS a RETURN a").unwrap_err(),
        BindError::AlreadyDeclared("a".into())
    );
    assert!(matches!(
        bind("MATCH (a:User)-[a]->() RETURN a").unwrap_err(),
        BindError::TypeMismatch { .. }
    ));
    assert_eq!(
        bind("MATCH p = (a:User)-[:FOLLOWS]->(b) WITH p MATCH p = (c)-->(d) RETURN 1").unwrap_err(),
        BindError::AlreadyDeclared("p".into())
    );
    assert_eq!(
        bind("MATCH (a:User) WITH a.name RETURN 1").unwrap_err(),
        BindError::MissingAlias("WITH")
    );
    assert_eq!(
        bind("RETURN 1 AS x, 2 AS x").unwrap_err(),
        BindError::DuplicateColumn("x".into())
    );
}

#[test]
fn order_by_and_where_visibility_after_with() {
    ok("MATCH (u:User) WITH u.name AS n ORDER BY u.age RETURN n");
    ok("MATCH (u:User) WITH u.name AS n ORDER BY n, u.age RETURN n");
    ok("MATCH (u:User) WITH u.name AS n WHERE u.age > 1 RETURN n");
    ok("MATCH (u:User) WITH u.age AS a, count(*) AS c WHERE u.age > 1 RETURN a, c");
    // DISTINCT: a pre-projection reference must be a projected expression
    let s = ok("MATCH (u:User) WITH DISTINCT u.name AS n ORDER BY u.name RETURN n");
    let BoundOp::Project { input, .. } = &s.plan else {
        unreachable!()
    };
    let BoundOp::Project { projection, .. } = &**input else {
        unreachable!()
    };
    let n = projection.items[0].var;
    assert_eq!(
        projection.order_by[0].expr,
        crate::query_planner::logical_expr::LogicalExpr::TableAlias(
            crate::query_planner::logical_expr::TableAlias(n.name())
        ),
        "rewritten to the projected item"
    );
    assert!(matches!(
        bind("MATCH (u:User) WITH DISTINCT u.name AS n ORDER BY u.age RETURN n").unwrap_err(),
        BindError::Invalid(m) if m.contains("DISTINCT or an aggregation")
    ));
}

#[test]
fn implicit_grouping_rules() {
    ok("MATCH (u:User) RETURN u.age, u.age + count(*)");
    ok("MATCH (a:User) RETURN count(a.age + 1) AS c, a.age + 1 AS k");
    assert!(matches!(
        bind("MATCH (u:User) RETURN u.name, u.age + count(*)").unwrap_err(),
        BindError::Invalid(m) if m.contains("implicit grouping")
    ));
    // Neo4j 5.26: a key buried in CASE needs a projected grouping key.
    ok("MATCH (a)-[r]-(b) RETURN a.name AS k, CASE WHEN count(r) > 5 THEN a.name ELSE 'x' END AS m");
    assert!(matches!(
        bind("MATCH (a)-[r]-(b) RETURN CASE WHEN count(r) > 5 THEN a.name ELSE 'x' END AS m")
            .unwrap_err(),
        BindError::Invalid(m) if m.contains("implicit grouping")
    ));
}

#[test]
fn comprehension_variables_shadow_outer_ones() {
    let s = ok("MATCH (x:User) RETURN [x IN [1,2] | x + 1] AS l, x.name");
    let xs = named(&s, "x");
    assert!(xs
        .iter()
        .any(|b| matches!(b.kind, BindingKind::Node { .. })));
    assert!(xs.iter().any(|b| b.source == BindingSource::Local));
}

#[test]
fn star_expands_the_scope() {
    let s = ok("MATCH (a:User) WITH *, 1 AS one RETURN a.name, one");
    assert_eq!(named(&s, "one").len(), 2);
    ok("WITH 1 AS x RETURN *");
}

#[test]
fn optional_match_bindings_are_nullable() {
    let s = ok("MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b) RETURN b");
    assert!(!named(&s, "a")[0].nullable);
    assert!(named(&s, "b")[0].nullable);
    assert!(named(&s, "b")[1].nullable, "carried through RETURN");
}

#[test]
fn bound_relationship_is_reused() {
    let s = ok("MATCH ()-[r:FOLLOWS]->() WITH r MATCH (a)-[r]-(b) RETURN count(*)");
    let ms = matches(&s.plan);
    let BoundOp::Match { pattern, .. } = ms[1] else {
        unreachable!()
    };
    assert!(pattern.parts[0].rels[0].bound_before);
}

#[test]
fn variable_length_relationships_bind_lists() {
    let s = ok("MATCH (a:User)-[r:FOLLOWS*1]->(b) RETURN size(r)");
    assert!(matches!(
        named(&s, "r")[0].kind,
        BindingKind::Rel {
            length: Some((1, Some(1))),
            ..
        }
    ));
}

#[test]
fn anonymous_nodes_shared_between_steps_are_one_variable() {
    let s = ok("MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) RETURN count(*)");
    let ms = matches(&s.plan);
    let BoundOp::Match { pattern, .. } = ms[0] else {
        unreachable!()
    };
    let nodes = &pattern.parts[0].nodes;
    assert_eq!(nodes.len(), 3);
    assert_ne!(nodes[0].var, nodes[1].var);
    assert_ne!(nodes[1].var, nodes[2].var);
}

#[test]
fn unions() {
    ok("RETURN 1 AS a, 2 AS b UNION RETURN 2 AS b, 1 AS a");
    assert!(matches!(
        bind("RETURN 1 AS a UNION RETURN 2 AS b").unwrap_err(),
        BindError::Invalid(m) if m.contains("same return column names")
    ));
    assert!(matches!(
        bind("RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a").unwrap_err(),
        BindError::Invalid(m) if m.contains("UNION and UNION ALL")
    ));
    let s = ok("MATCH (a:User) RETURN a.name AS n UNION MATCH (a:Post) RETURN a.title AS n");
    let a = named(&s, "a");
    assert_ne!(a[0].id, a[1].id, "each arm binds its own `a`");
}

#[test]
fn unsupported_constructs_fall_back() {
    for q in [
        "MATCH (a:User) WHERE (a)-[:FOLLOWS]->() RETURN a",
        "MATCH (a:User) RETURN [(a)-[:FOLLOWS]->(f) | f.name] AS fs",
        "CALL db.labels()",
    ] {
        match bind(q) {
            Err(e) if e.is_unsupported() => {}
            other => panic!("{q}: expected Unsupported, got {other:?}"),
        }
    }
}

// ----------------------------------------------------------------- labels

#[test]
fn labels_are_inferred_from_relationship_types() {
    let s = ok("MATCH (a)-[:LIKED]->(b) RETURN count(*)");
    assert_eq!(labels(&s, "a"), set(&["User"]));
    assert_eq!(labels(&s, "b"), set(&["Post"]));
    let s = ok("MATCH (a:User)-[:AUTHORED]->(p) RETURN p");
    assert_eq!(labels(&s, "p"), set(&["Post"]));
    let s = ok("MATCH (a:Post)-[:AUTHORED]-(b) RETURN b");
    assert_eq!(labels(&s, "b"), set(&["User"]), "undirected");
}

#[test]
fn impossible_patterns_match_nothing_without_an_error() {
    // Neo4j answers count 0 for both; the legacy planner refuses them.
    let s = ok("MATCH (p:Post)-[:LIKED]->(u) RETURN count(*)");
    assert!(labels(&s, "p").is_empty());
    let s = ok("MATCH (n:NonExistent) RETURN count(*)");
    assert!(labels(&s, "n").is_empty());
}

#[test]
fn variable_length_label_inference() {
    let s = ok("MATCH (a:User)-[:FOLLOWS*1..2]->(b) RETURN b");
    assert_eq!(labels(&s, "b"), set(&["User"]));
    // zero hops: the end may be the start
    let s = ok("MATCH (p:Post)-[:FOLLOWS*0..2]->(b) RETURN b");
    assert_eq!(labels(&s, "b"), set(&["Post"]));
    let s = ok("MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x) RETURN x");
    assert_eq!(labels(&s, "x"), set(&["Post", "User"]));
}

#[test]
fn a_bound_variable_is_not_narrowed_by_a_later_optional_match() {
    let s = ok("MATCH (n) OPTIONAL MATCH (n:Post)-[:FOLLOWS]->(m) RETURN n");
    assert_eq!(labels(&s, "n"), set(&["Post", "User"]));
}

#[test]
fn a_closed_pattern_needs_one_label_at_both_ends() {
    // One node is both ends: only FOLLOWS (User->User) can close the loop.
    // Assigning each end's set to the shared slot in turn never converged.
    let s = ok("MATCH (a)-[r]->(a) RETURN count(*)");
    assert_eq!(labels(&s, "a"), set(&["User"]));
    let s = ok("MATCH (a)-[:AUTHORED]->(a) RETURN count(*)");
    assert!(labels(&s, "a").is_empty());
    let s = ok("MATCH (a)-[:FOLLOWS|AUTHORED*2..3]->(a) RETURN count(*)");
    assert_eq!(labels(&s, "a"), set(&["User"]));
}

#[test]
fn huge_hop_bounds_infer_without_stepping_every_hop() {
    let s = ok("MATCH (a:User)-[:FOLLOWS*100000000..200000000]->(b) RETURN b");
    assert_eq!(labels(&s, "b"), set(&["User"]));
    let s = ok("MATCH (a:User)-[:FOLLOWS|AUTHORED*4000000000..]->(b) RETURN b");
    // FOLLOWS hops, then optionally one AUTHORED hop last.
    assert_eq!(labels(&s, "b"), set(&["Post", "User"]));
    let s = ok("MATCH (a:User)-[:AUTHORED*4000000000..]->(b) RETURN b");
    assert!(labels(&s, "b").is_empty(), "AUTHORED never chains");
}

// ------------------------------------------------- review findings (Neo4j 5.26)

fn final_projection(s: &BoundStatement) -> &Projection {
    let BoundOp::Project { projection, .. } = &s.plan else {
        panic!("not a projection: {:?}", s.plan)
    };
    projection
}

fn invalid(q: &str, needle: &str) {
    match bind(q) {
        Err(BindError::Invalid(m)) if m.contains(needle) => {}
        other => panic!("{q}: expected Invalid containing {needle:?}, got {other:?}"),
    }
}

fn unsupported(q: &str) {
    match bind(q) {
        Err(e) if e.is_unsupported() => {}
        other => panic!("{q}: expected Unsupported, got {other:?}"),
    }
}

#[test]
fn a_variable_length_list_and_a_relationship_do_not_mix() {
    // Neo4j: "r defined with conflicting type List<Relationship> (expected
    // Relationship)", and the reverse.
    assert!(matches!(
        bind("MATCH (a)-[r*]->(b) MATCH (c)-[r]->(d) RETURN r").unwrap_err(),
        BindError::TypeMismatch {
            bound: "List<Relationship>",
            used: "Relationship",
            ..
        }
    ));
    assert!(matches!(
        bind("MATCH (a)-[r]->(b) MATCH (c)-[r*]->(d) RETURN count(*)").unwrap_err(),
        BindError::TypeMismatch {
            bound: "Relationship",
            used: "List<Relationship>",
            ..
        }
    ));
    // Neo4j accepts re-matching a list (7484 rows); not bound yet.
    unsupported("MATCH (a)-[r*]->(b) MATCH (c)-[r*]->(d) RETURN count(*)");
}

#[test]
fn order_by_prefers_a_projected_expression_over_a_shadowing_alias() {
    // Neo4j sorts by the projected a.age.
    let s = ok("MATCH (a:User) RETURN a.age AS a, count(*) AS c ORDER BY a.age DESC");
    let p = final_projection(&s);
    assert_eq!(
        p.order_by[0].expr,
        LogicalExpr::TableAlias(TableAlias(p.items[0].var.name()))
    );
    let s = ok("MATCH (a:User) WITH a.age AS a ORDER BY a.age RETURN a");
    let BoundOp::Project { input, .. } = &s.plan else {
        unreachable!()
    };
    let BoundOp::Project { projection, .. } = &**input else {
        unreachable!()
    };
    assert_eq!(
        projection.order_by[0].expr,
        LogicalExpr::TableAlias(TableAlias(projection.items[0].var.name()))
    );
    // Not where the expression is not projected: `a` is the string item.
    let s = ok("MATCH (a:User) RETURN a.name AS a ORDER BY a.age");
    let p = final_projection(&s);
    let LogicalExpr::PropertyAccessExp(pa) = &p.order_by[0].expr else {
        panic!("{:?}", p.order_by[0].expr)
    };
    assert_eq!(pa.table_alias.0, p.items[0].var.name());
}

#[test]
fn star_columns_are_in_name_order_before_explicit_items() {
    let cols = |q: &str| -> Vec<String> { ok(q).columns.iter().map(|(n, _)| n.clone()).collect() };
    assert_eq!(cols("MATCH (b:User), (a:Post) RETURN *"), ["a", "b"]);
    assert_eq!(
        cols("MATCH (b:User), (a:Post) RETURN *, 1 AS a0"),
        ["a", "b", "a0"]
    );
    assert_eq!(cols("MATCH (b:User), (a:Post) RETURN *, a"), ["b", "a"]);
    assert_eq!(
        cols("MATCH (b:User), (a:Post) RETURN *, b.name AS a"),
        ["b", "a"]
    );
    assert_eq!(cols("MATCH (b:User), (a:Post) WITH * RETURN *"), ["a", "b"]);
    invalid("MATCH (b:User) RETURN 1 AS z, *", "must be the first");
}

#[test]
fn aggregates_only_where_neo4j_allows_them() {
    let misplaced = "Aggregations should not be used like this";
    invalid("MATCH (a:User) WHERE count(*) > 1 RETURN a", misplaced);
    invalid(
        "MATCH (a:User) WITH a WHERE count(*) > 1 RETURN a",
        misplaced,
    );
    invalid("MATCH (a:User {age: count(*)}) RETURN a", misplaced);
    invalid(
        "MATCH (a:User) WITH a ORDER BY count(*) RETURN a",
        "no aggregate expressions",
    );
    invalid(
        "MATCH (a:User) RETURN a.name AS n ORDER BY count(*)",
        "no aggregate expressions",
    );
    invalid(
        "MATCH (a:User) WITH a ORDER BY count(*) LIMIT 1 RETURN a",
        "no aggregate expressions",
    );
    invalid(
        "MATCH (a:User) WITH a, count(*) AS c ORDER BY sum(a.age) RETURN a",
        "Illegal aggregation",
    );
    invalid("UNWIND [count(*)] AS x RETURN x", "executing over lists");
    // Equal to a projected aggregate: allowed (Neo4j: 30 and "Alice", 1).
    ok("MATCH (a:User) WITH a, count(*) AS c WHERE count(*) > 0 RETURN count(*)");
    ok("MATCH (a:User) RETURN a.name AS n, count(*) AS c ORDER BY count(*) DESC");
}

#[test]
fn free_standing_order_by_rejects_aggregates() {
    invalid(
        "MATCH (a:User) WITH a LIMIT 5 ORDER BY count(*) RETURN a",
        "no aggregate expressions",
    );
}

#[test]
fn comprehension_locals_are_not_outer_variables() {
    // Neo4j: 90; a list; accepted.
    ok("MATCH (a:User) RETURN count(*) * reduce(s = 0, x IN [1, 2] | s + x) AS r");
    ok("MATCH (a:User) RETURN [x IN collect(a.age) | x] AS l");
    ok("MATCH (a:User) RETURN DISTINCT a.name AS n ORDER BY size([x IN [1, 2] | x])");
    // An outer variable inside the comprehension still counts.
    invalid(
        "MATCH (a:User) RETURN count(*) * reduce(s = 0, x IN [a.age] | s + x) AS r",
        "implicit grouping",
    );
    // Neo4j: "Variable `x` already declared".
    assert!(matches!(
        bind("RETURN reduce(x = 0, x IN [1] | x) AS r").unwrap_err(),
        BindError::AlreadyDeclared(n) if n == "x"
    ));
}

#[test]
fn properties_of_a_grouping_key_variable() {
    // Neo4j accepts both; `RETURN a.name, count(*) + a.age` stays an error.
    ok("MATCH (a:User) RETURN a, count(*) + a.age AS c");
    let s = ok("MATCH (a:User) RETURN a AS b, count(*) AS c ORDER BY a.age");
    let p = final_projection(&s);
    let LogicalExpr::PropertyAccessExp(pa) = &p.order_by[0].expr else {
        panic!("{:?}", p.order_by[0].expr)
    };
    assert_eq!(pa.table_alias.0, p.items[0].var.name(), "rewritten to b");
    invalid(
        "MATCH (a:User) RETURN a.name, count(*) + a.age AS c",
        "implicit grouping",
    );
}

#[test]
fn value_variables_used_as_graph_elements_fall_back() {
    // Neo4j accepts all of these (types are checked at runtime).
    unsupported("MATCH (a:User) WITH collect(a) AS ns UNWIND ns AS n MATCH (n)-->(m) RETURN m");
    unsupported("MATCH (a:User) WITH coalesce(a, a) AS c MATCH (c)-->(m) RETURN m");
    unsupported("MATCH ()-[rs*]->() UNWIND rs AS r MATCH ()-[r]->() RETURN count(*)");
    unsupported("MATCH ()-[r]->() WITH collect(r) AS rs MATCH ()-[rs*]->() RETURN count(*)");
    // A path is never a node.
    assert!(matches!(
        bind("MATCH p = (a)-->(b) MATCH (p)-->(c) RETURN c").unwrap_err(),
        BindError::TypeMismatch { .. }
    ));
}

#[test]
fn the_same_relationship_twice_in_one_match_falls_back() {
    // Neo4j: 0 rows (OPTIONAL MATCH: one null row).
    unsupported("MATCH (a)-[r]->(b), (c)-[r]->(d) RETURN count(*)");
    unsupported("OPTIONAL MATCH (a)-[r]->(b), (c)-[r]->(d) RETURN count(*)");
}

#[test]
fn zero_hop_segment_with_no_feasible_type_keeps_its_endpoints() {
    // Neo4j: 30 rows, all zero-hop. The endpoint keeps its labels; the empty
    // type set means only the zero-hop match.
    let s = ok("MATCH (a:User)-[r:NOPE*0..]->(b) RETURN b");
    assert_eq!(labels(&s, "b"), set(&["User"]));
    let BindingKind::Rel { types, length } = &named(&s, "r")[0].kind else {
        unreachable!()
    };
    assert!(types.is_empty());
    assert_eq!(*length, Some((0, None)));
}

#[test]
fn order_by_star_is_not_a_sort_key() {
    assert!(bind("MATCH (a:User) RETURN a ORDER BY *").is_err());
}
