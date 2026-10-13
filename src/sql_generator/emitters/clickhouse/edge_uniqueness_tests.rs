use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::render_plan::variable_length::VariableLengthSpec;
use std::collections::HashMap;

/// Helper to create a minimal test schema for VLC tests
fn create_test_schema() -> GraphSchema {
    GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
}

#[test]
fn test_composite_edge_id() {
    // Composite edge ID — cycle detection now uses edge-uniqueness (path_edges)
    // with the composite key rendered as a tuple.
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 2);
    let edge_id = Some(Identifier::Composite(vec![
        "FlightDate".to_string(),
        "FlightNum".to_string(),
        "Origin".to_string(),
        "Dest".to_string(),
    ]));

    let generator = VariableLengthCteGenerator::for_test(
        &schema,
        spec,
        "airports",
        "airport_code",
        "flights",
        "Origin",
        "Dest",
        "airports",
        "airport_code",
        "a1",
        "a2",
        edge_id,
    );

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!(
            "Expected RawSql content in test_custom_edge_id_tuple_construction, got: {:?}",
            other
        ),
    };

    // Cycle detection uses edge-uniqueness via path_edges, with the composite
    // edge key rendered as a tuple(...) of its columns.
    assert!(
        sql.contains(
            "NOT has(vp.path_edges, tuple(rel.FlightDate, rel.FlightNum, rel.Origin, rel.Dest))"
        ),
        "SQL should check edge uniqueness via composite path_edges tuple. SQL:\n{}",
        sql
    );
    assert!(
        !sql.contains("NOT has(vp.path_nodes,"),
        "Directed range VLP should NOT use node-uniqueness cycle check. SQL:\n{}",
        sql
    );

    println!("\n✅ Composite Edge ID Test SQL:\n{}", sql);
}

#[test]
fn test_simple_edge_id() {
    // Single column edge ID — cycle detection now uses edge-uniqueness (path_edges),
    // keyed by the single edge-id column (no tuple wrapper).
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 2);
    let edge_id = Some(Identifier::Single("transaction_id".to_string()));

    let generator = VariableLengthCteGenerator::for_test(
        &schema,
        spec,
        "accounts",
        "account_id",
        "transactions",
        "from_account",
        "to_account",
        "accounts",
        "account_id",
        "a1",
        "a2",
        edge_id,
    );

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!(
            "Expected RawSql content in test_custom_edge_id_column, got: {:?}",
            other
        ),
    };

    // Cycle detection uses edge-uniqueness via path_edges, keyed by the single
    // edge-id column (rendered bare, no tuple wrapper).
    assert!(
        sql.contains("NOT has(vp.path_edges, rel.transaction_id)"),
        "SQL should check edge uniqueness via single-column path_edges. SQL:\n{}",
        sql
    );
    assert!(
        !sql.contains("NOT has(vp.path_nodes,"),
        "Directed range VLP should NOT use node-uniqueness cycle check. SQL:\n{}",
        sql
    );

    println!("\n✅ Simple Edge ID Test SQL:\n{}", sql);
}
