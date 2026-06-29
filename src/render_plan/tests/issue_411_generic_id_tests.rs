//! Regression tests for issue #411 — generic `.id` dropped from / mis-resolved in
//! WITH→CTE projection when a schema's node_id property is **renamed**.
//!
//! Cypher's `.id` is a synonym for node identity, but property pruning, CTE column
//! naming, and post-WITH resolution all key on the Cypher property name. When the
//! `User` node's id is the property `user_id` (column `user_id`) — with no property
//! literally named `id` — a generic `b.id` reference used to be:
//!   1. pruned out of the WITH-CTE projection (the literal `"id"` requirement never
//!      matched the schema's `"user_id"` property), and
//!   2. mis-resolved in the outer SELECT to a raw DB column the CTE never exposed
//!      (`b.user_id`), or — for graph-rel shapes whose CTE variable lost its label —
//!      to an unrelated node's id column (`b.post_id`, a Post id!).
//!
//! The fix spans three chokepoints:
//!   (A) `PropertyRequirementsAnalyzer`: a generic `"id"` requirement also requires the
//!       node's real node_id Cypher property, so pruning keeps the id by name.
//!   (B) `variable_scope::resolve`: a missed generic `.id` lookup retries with the
//!       node's actual node_id property (both the by-alias and by-cte-name branches).
//!   (C) `build_chained_with_match_cte_plan`: the WITH CTE variable's labels are
//!       recovered from the WITH bodies when the rewritten plan no longer exposes the
//!       source node — without labels, (B) can't find the node_id property.
//! Plus a VLP-specific fix: the VLP CTE always names its identity column `start_id`/
//! `end_id`, so a renamed node_id must reference `end_id`, not `end_user_id`.

use crate::{
    clickhouse_query_generator,
    graph_catalog::config::Identifier,
    graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    graph_catalog::schema_types::SchemaType,
    open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};
use std::collections::HashMap;

fn cypher_to_sql(cypher: &str) -> String {
    cypher_to_sql_with_schema(cypher, &renamed_id_schema())
}

fn cypher_to_sql_with_schema(cypher: &str, graph_schema: &GraphSchema) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");

    let (logical_plan, mut plan_ctx) = build_logical_plan(&ast, graph_schema, None, None, None)
        .expect("Failed to build logical plan");

    use crate::query_planner::analyzer;
    use crate::query_planner::optimizer;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    let render_plan = logical_plan
        .to_render_plan(graph_schema)
        .expect("Failed to build render plan");

    clickhouse_query_generator::generate_sql(render_plan, 100)
}

fn prop_col(name: &str) -> crate::graph_catalog::expression_parser::PropertyValue {
    crate::graph_catalog::expression_parser::PropertyValue::Column(name.to_string())
}

/// Schema where `User`'s node_id is the **renamed** property `user_id` (column
/// `user_id`) — there is no property literally named `id`. A `Post` node (node_id
/// `post_id`) is present to catch label-confusion regressions.
fn renamed_id_schema() -> GraphSchema {
    schema_with_user_node_id("user_id")
}

/// Contrast schema where `User`'s node_id is literally `id`. The same queries must
/// keep emitting the historical (already-correct) SQL.
fn id_named_schema() -> GraphSchema {
    schema_with_user_node_id("id")
}

fn schema_with_user_node_id(id_prop: &str) -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    let user_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "users".to_string(),
        column_names: vec![
            id_prop.to_string(),
            "full_name".to_string(),
            "age".to_string(),
        ],
        primary_keys: id_prop.to_string(),
        node_id: NodeIdSchema::single(id_prop.to_string(), SchemaType::Integer),
        property_mappings: [
            (id_prop.to_string(), prop_col(id_prop)),
            ("name".to_string(), prop_col("full_name")),
            ("age".to_string(), prop_col("age")),
        ]
        .into_iter()
        .collect(),
        view_parameters: None,
        engine: None,
        use_final: None,
        filter: None,
        is_denormalized: false,
        from_properties: None,
        to_properties: None,
        denormalized_source_table: None,
        label_column: None,
        label_value: None,
        node_id_types: None,
        source: None,
        property_types: HashMap::new(),
        id_generation: None,
    };
    nodes.insert("User".to_string(), user_node);

    let post_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "posts".to_string(),
        column_names: vec!["post_id".to_string(), "title".to_string()],
        primary_keys: "post_id".to_string(),
        node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
        property_mappings: [
            ("post_id".to_string(), prop_col("post_id")),
            ("title".to_string(), prop_col("title")),
        ]
        .into_iter()
        .collect(),
        view_parameters: None,
        engine: None,
        use_final: None,
        filter: None,
        is_denormalized: false,
        from_properties: None,
        to_properties: None,
        denormalized_source_table: None,
        label_column: None,
        label_value: None,
        node_id_types: None,
        source: None,
        property_types: HashMap::new(),
        id_generation: None,
    };
    nodes.insert("Post".to_string(), post_node);

    let follows_rel = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "follows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_node_table: "users".to_string(),
        to_node_table: "users".to_string(),
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
        from_node_id_dtype: SchemaType::Integer,
        to_node_id_dtype: SchemaType::Integer,
        property_mappings: HashMap::new(),
        view_parameters: None,
        engine: None,
        use_final: None,
        filter: None,
        edge_id: None,
        type_column: None,
        from_label_column: None,
        to_label_column: None,
        from_node_properties: None,
        to_node_properties: None,
        from_label_values: None,
        to_label_values: None,
        is_fk_edge: false,
        constraints: None,
        edge_id_types: None,
        source: None,
        property_types: HashMap::new(),
    };
    relationships.insert("FOLLOWS::User::User".to_string(), follows_rel);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

/// Every CTE column the outer SELECT references must be defined by the CTE body.
/// Catches the core #411 failure: outer references `b.<col>` for a `<col>` the CTE
/// never projected, producing invalid SQL.
fn assert_outer_refs_are_defined(sql: &str) {
    // Collect CTE column aliases (`... AS "p1_b_user_id"`). CTE body aliases use the
    // p{N}_alias_prop form; the final SELECT uses dotted display aliases (`AS "b.id"`).
    let defined: std::collections::HashSet<String> = sql
        .match_indices("AS \"")
        .filter_map(|(i, _)| {
            let rest = &sql[i + 4..];
            let end = rest.find('"')?;
            let alias = &rest[..end];
            // Only CTE-style aliases (no dot) are columns later referenced as b.<alias>.
            if alias.contains('.') {
                None
            } else {
                Some(alias.to_string())
            }
        })
        .collect();

    // Any `b.p{N}_...` reference in the final SELECT must be a defined CTE column.
    for (i, _) in sql.match_indices("b.p") {
        let rest = &sql[i + 2..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        let col = &rest[..end];
        assert!(
            defined.contains(col),
            "outer references CTE column `b.{col}` that the CTE never defines; got:\n{sql}"
        );
    }
}

// ===========================================================================
// Repro A — non-VLP simple `WITH b`
// ===========================================================================

#[test]
fn repro_a_non_vlp_simple_with_generic_id() {
    let sql = cypher_to_sql("MATCH (b:User) WITH b WHERE b.age > 0 RETURN b.id, b.name");
    // CTE must project the id under its real node_id property name.
    assert!(
        sql.contains("AS \"p1_b_user_id\""),
        "CTE must project the renamed node_id as p1_b_user_id; got:\n{sql}"
    );
    // Outer must reference the CTE column, preserving the `b.id` display alias.
    assert!(
        sql.contains("p1_b_user_id AS \"b.id\""),
        "outer must map generic .id to the CTE id column; got:\n{sql}"
    );
    // Must NOT reach past the CTE to a raw base column.
    assert!(
        !sql.contains("b.user_id AS \"b.id\""),
        "outer must not reference the raw base column through the CTE; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

#[test]
fn repro_a_explicit_user_id_still_works() {
    let sql = cypher_to_sql("MATCH (b:User) WITH b RETURN b.user_id, b.name");
    assert!(
        sql.contains("p1_b_user_id AS \"b.user_id\""),
        "explicit b.user_id must resolve to the CTE id column; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

// ===========================================================================
// Repro B — graph-rel `WITH b` (also exercised the b.post_id label-confusion)
// ===========================================================================

#[test]
fn repro_b_graph_rel_with_generic_id() {
    let sql = cypher_to_sql(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH b WHERE b.age > 0 RETURN b.id, b.name",
    );
    assert!(
        sql.contains("AS \"p1_b_user_id\""),
        "CTE must project the renamed node_id; got:\n{sql}"
    );
    assert!(
        sql.contains("p1_b_user_id AS \"b.id\""),
        "outer must map generic .id to the CTE id column; got:\n{sql}"
    );
    // The label-confusion regression: b is a User, never a Post.
    assert!(
        !sql.contains("post_id"),
        "b must not be mis-resolved to a Post id; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

// ===========================================================================
// Repro C — VLP endpoint
// ===========================================================================

#[test]
fn repro_c_vlp_endpoint_explicit_user_id() {
    let sql = cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WITH b RETURN b.user_id");
    // The VLP CTE always names the identity `end_id` — never `end_user_id`.
    assert!(
        sql.contains("end_id AS \"p1_b_user_id\""),
        "VLP CTE must reference end_id (not end_user_id) for the renamed node_id; got:\n{sql}"
    );
    assert!(
        !sql.contains("end_user_id"),
        "VLP CTE must not prefix the renamed node_id property; got:\n{sql}"
    );
    assert!(
        sql.contains("p1_b_user_id AS \"b.user_id\""),
        "outer must reference the CTE id column; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

#[test]
fn repro_c2_vlp_endpoint_generic_id() {
    let sql = cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WITH b RETURN b.id");
    assert!(
        sql.contains("end_id AS \"p1_b_user_id\""),
        "VLP CTE must reference end_id for the renamed node_id; got:\n{sql}"
    );
    assert!(
        sql.contains("p1_b_user_id AS \"b.id\""),
        "outer must map generic .id to the CTE id column; got:\n{sql}"
    );
    assert!(
        !sql.contains("post_id"),
        "VLP endpoint b must not be mis-resolved to a Post id; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

// ===========================================================================
// Contrast — id-named schema must keep its historical (correct) output
// ===========================================================================

#[test]
fn contrast_id_named_non_vlp_unchanged() {
    let sql = cypher_to_sql_with_schema(
        "MATCH (b:User) WITH b WHERE b.age > 0 RETURN b.id, b.name",
        &id_named_schema(),
    );
    assert!(
        sql.contains("p1_b_id AS \"b.id\""),
        "id-named schema must keep p1_b_id mapping; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}

#[test]
fn contrast_id_named_vlp_unchanged() {
    let sql = cypher_to_sql_with_schema(
        "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WITH b RETURN b.id",
        &id_named_schema(),
    );
    assert!(
        sql.contains("end_id AS \"p1_b_id\""),
        "id-named VLP must keep end_id → p1_b_id; got:\n{sql}"
    );
    assert!(
        sql.contains("p1_b_id AS \"b.id\""),
        "id-named VLP outer must map .id to p1_b_id; got:\n{sql}"
    );
    assert_outer_refs_are_defined(&sql);
}
