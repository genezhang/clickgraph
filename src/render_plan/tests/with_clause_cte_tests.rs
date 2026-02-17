//! Regression tests for WITH clause → CTE processing
//!
//! These tests verify fixes for three critical bugs:
//!
//! 1. **Infinite loop prevention** (commit 29e4cd2): `find_all_with_clauses_grouped()` was
//!    finding already-processed aliases, causing `build_chained_with_match_cte_plan()` to
//!    iterate forever. Fix: skip aliases already in `processed_cte_aliases`.
//!
//! 2. **CTE column name resolution** (commit 32247d9): `create_cte_reference()` used
//!    `strip_prefix()` which failed for multi-alias WITH clauses (e.g., "fids_p" prefix
//!    wrongly strips from "fids_p_id"). Fix: use `parse_cte_column()` for p{N} format.
//!
//! 3. **Per-alias projection remapping** (commit 32247d9): After WITH→CTE replacement,
//!    `remap_property_access_for_cte()` only matched the composite alias ("fids_p"),
//!    but RETURN references individual aliases ("p.id"). Fix: build per-alias property
//!    mappings from CTE columns and remap against each individual alias.

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

/// Helper: parse Cypher, build logical plan, run all analyzer/optimizer passes, generate SQL
fn cypher_to_sql(cypher: &str) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");
    let graph_schema = setup_test_graph_schema();

    let (logical_plan, mut plan_ctx) = build_logical_plan(&ast, &graph_schema, None, None, None)
        .expect("Failed to build logical plan");

    use crate::query_planner::analyzer;
    use crate::query_planner::optimizer;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    let render_plan = logical_plan
        .to_render_plan(&graph_schema)
        .expect("Failed to build render plan");

    clickhouse_query_generator::generate_sql(render_plan, 100)
}

fn setup_test_graph_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    let user_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "users".to_string(),
        column_names: vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "user_id".to_string(),
        ],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
        property_mappings: [
            ("name".to_string(), prop_col("name")),
            ("age".to_string(), prop_col("age")),
            ("user_id".to_string(), prop_col("user_id")),
            ("id".to_string(), prop_col("id")),
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
    };
    nodes.insert("User".to_string(), user_node);

    let post_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "posts".to_string(),
        column_names: vec!["id".to_string(), "title".to_string(), "content".to_string()],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
        property_mappings: [
            ("id".to_string(), prop_col("id")),
            ("title".to_string(), prop_col("title")),
            ("content".to_string(), prop_col("content")),
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
    };
    relationships.insert("FOLLOWS::User::User".to_string(), follows_rel);

    let authored_rel = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "authored".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "User".to_string(),
        to_node: "Post".to_string(),
        from_node_table: "users".to_string(),
        to_node_table: "posts".to_string(),
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
    };
    relationships.insert("AUTHORED::User::Post".to_string(), authored_rel);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

fn prop_col(name: &str) -> crate::graph_catalog::expression_parser::PropertyValue {
    crate::graph_catalog::expression_parser::PropertyValue::Column(name.to_string())
}

#[cfg(test)]
mod regression_infinite_loop {
    use super::*;

    /// Regression test for infinite loop in build_chained_with_match_cte_plan.
    /// Before fix: query with multiple WITH clauses caused infinite iteration
    /// because already-processed aliases were found again by find_all_with_clauses_grouped.
    #[test]
    fn test_chained_with_clauses_no_infinite_loop() {
        // Multi-WITH query: first WITH exports `friend`, second WITH uses `friend`
        let cypher = r#"
            MATCH (u:User)-[:FOLLOWS]->(friend:User)
            WITH u, friend
            MATCH (friend)-[:AUTHORED]->(p:Post)
            WITH friend, collect(p.id) AS postIds
            RETURN friend.name, postIds
        "#;

        // This should complete without hanging (infinite loop was the bug)
        let sql = cypher_to_sql(cypher);
        println!("Generated SQL:\n{}", sql);

        // Basic sanity: SQL was generated successfully
        assert!(
            sql.contains("SELECT"),
            "SQL should contain SELECT statement"
        );
    }

    /// Verify that a simple WITH clause still works correctly
    #[test]
    fn test_single_with_clause_completes() {
        let cypher = r#"
            MATCH (u:User)-[:FOLLOWS]->(f:User)
            WITH u, count(f) AS friendCount
            RETURN u.name, friendCount
        "#;

        let sql = cypher_to_sql(cypher);
        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("SELECT"),
            "SQL should contain SELECT statement"
        );
    }
}

#[cfg(test)]
mod regression_cte_column_naming {
    use super::*;

    /// Regression test for CTE column name resolution.
    /// Before fix: create_cte_reference used strip_prefix("fids_p_") which
    /// incorrectly decoded CTE columns like "p1_p_id" (the p{N} format).
    /// After fix: uses parse_cte_column() for unambiguous decoding.
    #[test]
    fn test_cte_column_parse_format() {
        // Verify the p{N} naming convention works correctly
        use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};

        // Test basic encoding/decoding roundtrip
        let encoded = cte_column_name("p", "id");
        assert_eq!(encoded, "p1_p_id");

        let (alias, prop) = parse_cte_column(&encoded).unwrap();
        assert_eq!(alias, "p");
        assert_eq!(prop, "id");

        // Test with longer alias names
        let encoded = cte_column_name("friend", "name");
        let (alias, prop) = parse_cte_column(&encoded).unwrap();
        assert_eq!(alias, "friend");
        assert_eq!(prop, "name");

        // Test with alias containing underscores
        let encoded = cte_column_name("my_user", "full_name");
        let (alias, prop) = parse_cte_column(&encoded).unwrap();
        assert_eq!(alias, "my_user");
        assert_eq!(prop, "full_name");
    }

    /// Verify that strip_prefix approach would fail but parse_cte_column works
    /// This is the exact scenario that triggered the bug:
    /// composite alias "fids_p", CTE column "p1_p_id"
    /// strip_prefix("fids_p_") fails to match "p1_p_id"
    #[test]
    fn test_cte_column_not_decodable_by_strip_prefix() {
        use crate::utils::cte_column_naming::parse_cte_column;

        let cte_col = "p1_p_id";

        // strip_prefix with composite alias would fail
        let composite_alias = "fids_p";
        let prefix = format!("{}_", composite_alias);
        assert!(
            cte_col.strip_prefix(&prefix).is_none(),
            "strip_prefix should fail for p{{N}} format columns"
        );

        // But parse_cte_column succeeds
        let (alias, prop) = parse_cte_column(cte_col).unwrap();
        assert_eq!(alias, "p");
        assert_eq!(prop, "id");
    }
}

#[cfg(test)]
mod regression_per_alias_remapping {
    use super::*;

    /// Regression test for per-alias projection remapping.
    /// Before fix: RETURN p.id with composite WITH alias "fids_p" failed because
    /// remap_property_access_for_cte only checked composite alias, not individual "p".
    /// After fix: builds per-alias mappings and remaps against each individual alias.
    #[test]
    fn test_multi_alias_with_return_individual_property() {
        // WITH exports both `fids` (collect result) and `p` (node), composite alias becomes "fids_p"
        // RETURN references p.id — an individual alias property
        let cypher = r#"
            MATCH (u:User)-[:AUTHORED]->(p:Post)
            WITH collect(p.id) AS fids, p
            RETURN p.id, fids
        "#;

        let sql = cypher_to_sql(cypher);
        println!("Generated SQL:\n{}", sql);

        // The generated SQL should NOT reference "fids_p.id" (raw, unremapped)
        // It SHOULD reference the CTE column via the remapped name
        assert!(
            sql.contains("SELECT"),
            "SQL should contain SELECT statement"
        );
        // Should not have unrewritten property access that would cause ClickHouse errors
        assert!(
            !sql.contains("fids_p.id "),
            "Should not have unremapped composite alias property access 'fids_p.id'"
        );
    }

    /// Test that single-alias WITH still works after the per-alias fix
    #[test]
    fn test_single_alias_with_property_access() {
        let cypher = r#"
            MATCH (u:User)-[:FOLLOWS]->(f:User)
            WITH f
            RETURN f.name
        "#;

        let sql = cypher_to_sql(cypher);
        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("SELECT"),
            "SQL should contain SELECT statement"
        );
    }
}
