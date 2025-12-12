//! Example test using the enhanced mock ClickHouse client

use super::mock_clickhouse_enhanced::MockClickHouseClient;
use crate::graph_catalog::GraphViewDefinition;

#[tokio::test]
async fn test_social_graph_queries() -> anyhow::Result<()> {
    // Initialize mock client
    let client = MockClickHouseClient::new();

    // Create view definition
    let mut view = GraphViewDefinition::new("social_network");

    // Add User node mapping
    let mut user_mapping = crate::graph_catalog::NodeViewMapping::new("users", "user_id");
    user_mapping.add_property("name", "full_name");
    user_mapping.add_property("age", "age");
    user_mapping.set_filter("active = 1");
    view.add_node("User", user_mapping);

    // Test query: "Find users over 30"
    let result = client.execute("
        SELECT full_name, age
        FROM users
        WHERE age > 30 AND active = 1
    ").await?;

    // Verify results
    assert_eq!(result.rows(), 1); // Should only find Bob
    let row = result.get_row(0).unwrap();

    // Print the query results
    println!("Test Query 1 - Users over 30:");
    println!("Found {} users", result.rows());
    println!("Sample user: {:?}", row);

    // Test query: "Find recent followers"
    let result = client.execute("
        SELECT u1.full_name as follower, u2.full_name as following, f.follow_date
        FROM user_follows f
        JOIN users u1 ON f.follower_id = u1.user_id
        JOIN users u2 ON f.following_id = u2.user_id
        WHERE u1.active = 1 AND u2.active = 1
    ").await?;

    println!("\nTest Query 2 - Recent followers:");
    println!("Found {} relationships", result.rows());
    for i in 0..result.rows() {
        println!("Relationship {}: {:?}", i+1, result.get_row(i).unwrap());
    }

    Ok(())
}

/// Run this test to see a demonstration of the view-based queries
#[test]
fn view_query_demo() {
    println!("\nRunning Social Graph View Demo");
    println!("===============================");

    // This will run the async test and print the results
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        match test_social_graph_queries().await {
            Ok(_) => println!("\nDemo completed successfully!"),
            Err(e) => println!("\nDemo failed: {}", e),
        }
    });
}
