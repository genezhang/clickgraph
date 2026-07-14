use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::VariableLengthSpec;
use std::collections::HashMap;

/// Helper to create a minimal test schema for VLC tests
fn create_test_schema() -> GraphSchema {
    GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
}

#[test]
fn test_default_edge_id_tuple() {
    // #598 (part 2): directed range VLP now enforces RELATIONSHIP-uniqueness
    // (Cypher default) via path_edges. path_nodes is retained for nodes(p).
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 2);
    let generator = VariableLengthCteGenerator::new(
        &schema,
        spec,
        "users",
        "user_id",
        "follows",
        "follower_id",
        "followed_id",
        "users",
        "user_id",
        "u1",
        "u2",
        vec![],
        None,
        None,
        None,
        None,
        None,
        None,
    );

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!(
            "Expected RawSql content in test_default_tuple_edge_id, got: {:?}",
            other
        ),
    };

    // path_nodes is still maintained for UNWIND nodes(p) support
    assert!(
        sql.contains("path_nodes"),
        "SQL should still contain path_nodes for nodes(p). SQL:\n{}",
        sql
    );

    // path_edges tracks the traversed edges for relationship-uniqueness
    assert!(
        sql.contains("path_edges"),
        "SQL should contain path_edges for relationship-uniqueness. SQL:\n{}",
        sql
    );

    // Cycle detection uses edge-uniqueness via path_edges (Cypher default),
    // not node-uniqueness. Default edge identity is the (from, to) tuple.
    assert!(
        sql.contains("NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))"),
        "SQL should check edge uniqueness via path_edges. SQL:\n{}",
        sql
    );
    assert!(
        !sql.contains("NOT has(vp.path_nodes,"),
        "Directed range VLP should NOT use node-uniqueness cycle check. SQL:\n{}",
        sql
    );

    println!("\n✅ Default Edge ID Test SQL:\n{}", sql);
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

    let generator = VariableLengthCteGenerator::new(
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
        vec![],
        None,
        None,
        None,
        None,
        None,
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

    let generator = VariableLengthCteGenerator::new(
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
        vec![],
        None,
        None,
        None,
        None,
        None,
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

#[test]
fn test_bfs_mode_generates_lightweight_cte() {
    // When use_bfs_mode=true, the generator should produce a BFS CTE
    // with (node_id, hop) columns instead of per-path tracking arrays.
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 5);
    let mut generator = VariableLengthCteGenerator::new(
        &schema,
        spec,
        "Person",
        "id",
        "Person_knows_Person",
        "Person1Id",
        "Person2Id",
        "Person",
        "id",
        "person1",
        "person2",
        vec![],
        Some(crate::clickhouse_query_generator::variable_length_cte::ShortestPathMode::Shortest),
        Some("start_node.id = 123".to_string()),
        Some("end_node.id = 456".to_string()),
        Some("p".to_string()),
        None,
        None,
    );
    generator.use_bfs_mode = true;
    generator.is_undirected = false;

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!("Expected RawSql for BFS mode, got: {:?}", other),
    };

    // BFS CTE should have node_id and hop columns
    assert!(
        sql.contains("node_id") && sql.contains("hop"),
        "BFS CTE should contain node_id and hop columns. SQL:\n{}",
        sql
    );
    // BFS CTE should NOT contain path_nodes or path_relationships arrays
    assert!(
        !sql.contains("arrayConcat"),
        "BFS CTE should not grow path arrays. SQL:\n{}",
        sql
    );
    // Should have a _bfs recursive CTE and a result wrapper
    assert!(
        sql.contains("_bfs"),
        "BFS mode should generate a _bfs recursive CTE. SQL:\n{}",
        sql
    );
    // Result wrapper should have hop_count for length(path) rewriting
    assert!(
        sql.contains("hop_count"),
        "BFS result wrapper should have hop_count. SQL:\n{}",
        sql
    );

    println!("\n✅ BFS Mode Test SQL:\n{}", sql);
}

#[test]
fn test_bfs_mode_not_activated_without_flag() {
    // Without use_bfs_mode=true, even with shortestPath, the standard
    // per-path recursive CTE should be generated (with path_nodes).
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 5);
    let generator = VariableLengthCteGenerator::new(
        &schema,
        spec,
        "Person",
        "id",
        "Person_knows_Person",
        "Person1Id",
        "Person2Id",
        "Person",
        "id",
        "person1",
        "person2",
        vec![],
        Some(crate::clickhouse_query_generator::variable_length_cte::ShortestPathMode::Shortest),
        Some("start_node.id = 123".to_string()),
        Some("end_node.id = 456".to_string()),
        Some("p".to_string()),
        None,
        None,
    );
    // use_bfs_mode defaults to false

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!("Expected RawSql, got: {:?}", other),
    };

    // Standard mode should have path_nodes for per-path tracking
    assert!(
        sql.contains("path_nodes"),
        "Standard mode should contain path_nodes. SQL:\n{}",
        sql
    );
    // Should NOT have a _bfs CTE
    assert!(
        !sql.contains("_bfs"),
        "Standard mode should not generate _bfs CTE. SQL:\n{}",
        sql
    );

    println!("\n✅ BFS Guard Test SQL:\n{}", sql);
}

#[test]
fn test_bfs_mode_undirected_generates_two_branches() {
    // Undirected BFS should produce UNION ALL of forward + reverse traversal
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 5);
    let mut generator = VariableLengthCteGenerator::new(
        &schema,
        spec,
        "Person",
        "id",
        "Person_knows_Person",
        "Person1Id",
        "Person2Id",
        "Person",
        "id",
        "person1",
        "person2",
        vec![],
        Some(crate::clickhouse_query_generator::variable_length_cte::ShortestPathMode::Shortest),
        Some("start_node.id = 123".to_string()),
        Some("end_node.id = 456".to_string()),
        Some("p".to_string()),
        None,
        None,
    );
    generator.use_bfs_mode = true;
    generator.is_undirected = true;

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!("Expected RawSql for undirected BFS, got: {:?}", other),
    };

    // Count UNION ALL occurrences — undirected BFS should have at least 2
    // (one for base UNION ALL recursive, one for forward UNION ALL reverse)
    let union_count = sql.matches("UNION ALL").count();
    assert!(
        union_count >= 2,
        "Undirected BFS should have >=2 UNION ALL (got {}). SQL:\n{}",
        union_count,
        sql
    );

    // Both Person1Id and Person2Id should appear as join columns
    // (forward: Person1Id→Person2Id, reverse: Person2Id→Person1Id)
    assert!(
        sql.contains("Person1Id") && sql.contains("Person2Id"),
        "Undirected BFS should reference both direction columns. SQL:\n{}",
        sql
    );

    println!("\n✅ Undirected BFS Test SQL:\n{}", sql);
}
