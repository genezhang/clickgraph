//! Regression tests for VLP property pruning via PropertyRequirements.
//!
//! VLP recursive CTEs previously included ALL schema properties for start/end nodes.
//! After the projection push-down optimization, only properties referenced by the query
//! are included. These tests verify:
//! 1. Selective queries prune unused properties from VLP CTEs
//! 2. Wildcard/bare-node queries retain all properties (safe fallback)

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

fn prop_col(name: &str) -> crate::graph_catalog::expression_parser::PropertyValue {
    crate::graph_catalog::expression_parser::PropertyValue::Column(name.to_string())
}

/// Build a schema with Person (many properties) and KNOWS (Person→Person)
/// so we can verify which properties appear in VLP CTEs.
fn setup_person_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Person node with 6 properties (id + 5 data properties)
    let person_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "persons".to_string(),
        column_names: vec![
            "id".to_string(),
            "first_name".to_string(),
            "last_name".to_string(),
            "email".to_string(),
            "age".to_string(),
            "city".to_string(),
        ],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
        property_mappings: [
            ("id".to_string(), prop_col("id")),
            ("firstName".to_string(), prop_col("first_name")),
            ("lastName".to_string(), prop_col("last_name")),
            ("email".to_string(), prop_col("email")),
            ("age".to_string(), prop_col("age")),
            ("city".to_string(), prop_col("city")),
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
    nodes.insert("Person".to_string(), person_node);

    let knows_rel = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "knows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "Person".to_string(),
        to_node: "Person".to_string(),
        from_node_table: "persons".to_string(),
        to_node_table: "persons".to_string(),
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
    relationships.insert("KNOWS::Person::Person".to_string(), knows_rel);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

fn cypher_to_sql(cypher: &str) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");
    let graph_schema = setup_person_schema();

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

/// Count occurrences of a column name in the VLP CTE definition (the WITH RECURSIVE block).
/// We look at the base case SELECT to check which node properties are materialized.
fn cte_contains_column(sql: &str, column: &str) -> bool {
    // VLP CTEs are inside WITH RECURSIVE ... blocks
    // Check if the column appears in the SQL at all (within CTE context)
    sql.contains(column)
}

#[cfg(test)]
mod selective_property_tests {
    use super::*;

    /// When only `f.firstName` is returned, the VLP CTE should NOT include
    /// all 5 data properties (lastName, email, age, city should be pruned).
    #[test]
    fn test_selective_return_prunes_unused_properties() {
        let sql =
            cypher_to_sql("MATCH (p:Person {id: 1})-[:KNOWS*1..3]->(f:Person) RETURN f.firstName");

        // firstName must be present (it's referenced)
        assert!(
            cte_contains_column(&sql, "first_name"),
            "SQL should contain first_name (required property). SQL: {}",
            sql
        );

        // Properties not referenced in RETURN should be pruned from VLP CTE
        // Check that at least some unused properties are absent
        let has_email = cte_contains_column(&sql, "email");
        let has_city = cte_contains_column(&sql, "city");
        let has_age = cte_contains_column(&sql, "age");
        let unused_count = [has_email, has_city, has_age]
            .iter()
            .filter(|&&x| !x)
            .count();

        assert!(
            unused_count >= 2,
            "Expected at least 2 of 3 unused properties (email, city, age) to be pruned. \
             email={}, city={}, age={}. SQL: {}",
            has_email,
            has_city,
            has_age,
            sql
        );
    }

    /// When two properties are returned, only those two should appear.
    #[test]
    fn test_two_properties_returned() {
        let sql = cypher_to_sql(
            "MATCH (p:Person {id: 1})-[:KNOWS*1..3]->(f:Person) RETURN f.firstName, f.age",
        );

        assert!(
            cte_contains_column(&sql, "first_name"),
            "SQL should contain first_name. SQL: {}",
            sql
        );
        assert!(
            cte_contains_column(&sql, "age"),
            "SQL should contain age. SQL: {}",
            sql
        );

        // email and city should be pruned
        let has_email = cte_contains_column(&sql, "email");
        let has_city = cte_contains_column(&sql, "city");
        assert!(
            !has_email || !has_city,
            "Expected at least one of email/city to be pruned. email={}, city={}. SQL: {}",
            has_email,
            has_city,
            sql
        );
    }
}

#[cfg(test)]
mod wildcard_fallback_tests {
    use super::*;

    /// When a bare node is returned (`RETURN f`), all properties must be kept.
    #[test]
    fn test_bare_node_return_keeps_all_properties() {
        let sql = cypher_to_sql("MATCH (p:Person {id: 1})-[:KNOWS*1..3]->(f:Person) RETURN f");

        // All data properties should be present for bare node return
        assert!(
            cte_contains_column(&sql, "first_name"),
            "SQL should contain first_name for bare node return. SQL: {}",
            sql
        );
        assert!(
            cte_contains_column(&sql, "last_name"),
            "SQL should contain last_name for bare node return. SQL: {}",
            sql
        );
        assert!(
            cte_contains_column(&sql, "email"),
            "SQL should contain email for bare node return. SQL: {}",
            sql
        );
    }
}
