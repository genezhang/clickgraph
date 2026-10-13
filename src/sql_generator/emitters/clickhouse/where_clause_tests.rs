// Unit test for the structure of a walk's CTE without filters.

use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::render_plan::variable_length::VariableLengthSpec;
use std::collections::HashMap;

/// Helper to create a minimal test schema for VLC tests
fn create_test_schema() -> GraphSchema {
    GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
}

#[test]
fn test_no_filters_simple_structure() {
    // Use a range pattern starting at 1 to avoid min_hops filtering
    // (Fixed patterns like *2 would use inline JOINs in practice, not CTEs)
    let schema = create_test_schema();
    let spec = VariableLengthSpec::range(1, 3); // *1..3

    let generator = VariableLengthCteGenerator::for_test(
        &schema,
        spec,
        "users",
        "user_id",
        "follows",
        "follower_id",
        "followed_id",
        "users",
        "user_id",
        "a",
        "b",
        None,
    );

    let cte = generator.generate_cte();
    let sql = match &cte.content {
        crate::render_plan::CteContent::RawSql(s) => s,
        other => panic!(
            "Expected RawSql content in test_both_start_and_end_filters, got: {:?}",
            other
        ),
    };

    // Verify simple structure (no _inner, _to_target) when min_hops <= 1
    assert!(
        !sql.contains("_inner AS"),
        "Should NOT have _inner CTE without filters/shortest path when min_hops <= 1"
    );
    assert!(
        !sql.contains("_to_target AS"),
        "Should NOT have _to_target CTE"
    );

    println!(
        "\n✓ Generated SQL without filters (simple structure):\n{}\n",
        sql
    );
}
