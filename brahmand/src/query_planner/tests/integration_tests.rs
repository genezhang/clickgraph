//! Integration tests for view-based queries

use crate::{
    clickhouse_query_generator::generate_sql,
    graph_catalog::{GraphViewDefinition, NodeViewMapping, RelationshipViewMapping},
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::{
        analyzer::{graph_context::GraphContext, view_resolver::ViewResolver},
        evaluate_query,
    },
    server::clickhouse_client::ClickHouseClient,
};

/// Test helper to set up a social network view
fn create_social_network_view() -> GraphViewDefinition {
    let mut view = GraphViewDefinition::new("social_network");
    
    // User nodes
    let mut user_mapping = NodeViewMapping::new("users", "user_id");
    user_mapping.add_property("name", "full_name");
    user_mapping.add_property("age", "age");
    user_mapping.add_property("joined_date", "registration_date");
    user_mapping.set_filter("active = 1"); // Only active users
    view.add_node("User", user_mapping);
    
    // Post nodes
    let mut post_mapping = NodeViewMapping::new("posts", "post_id");
    post_mapping.add_property("title", "post_title");
    post_mapping.add_property("content", "post_content");
    post_mapping.add_property("created_at", "creation_timestamp");
    view.add_node("Post", post_mapping);
    
    // FOLLOWS relationships
    let mut follows_mapping = RelationshipViewMapping::new(
        "user_follows",
        "follower_id",
        "following_id",
    );
    follows_mapping.add_property("since", "follow_date");
    view.add_relationship("FOLLOWS", follows_mapping);
    
    // AUTHORED relationships
    let mut authored_mapping = RelationshipViewMapping::new(
        "user_posts",
        "author_id",
        "post_id",
    );
    authored_mapping.add_property("created_at", "post_timestamp");
    view.add_relationship("AUTHORED", authored_mapping);
    
    view
}

/// Helper to create mock data in ClickHouse
async fn setup_test_data(client: &ClickHouseClient) -> anyhow::Result<()> {
    // Create tables
    client.execute("
        CREATE TABLE IF NOT EXISTS users (
            user_id UInt64,
            full_name String,
            age UInt8,
            registration_date DateTime,
            active UInt8
        ) ENGINE = MergeTree()
        ORDER BY user_id
    ").await?;

    client.execute("
        CREATE TABLE IF NOT EXISTS posts (
            post_id UInt64,
            post_title String,
            post_content String,
            creation_timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY post_id
    ").await?;

    client.execute("
        CREATE TABLE IF NOT EXISTS user_follows (
            follower_id UInt64,
            following_id UInt64,
            follow_date DateTime
        ) ENGINE = MergeTree()
        ORDER BY (follower_id, following_id)
    ").await?;

    client.execute("
        CREATE TABLE IF NOT EXISTS user_posts (
            author_id UInt64,
            post_id UInt64,
            post_timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY (author_id, post_id)
    ").await?;

    // Insert sample data
    client.execute("
        INSERT INTO users VALUES
        (1, 'Alice Smith', 28, '2024-01-01', 1),
        (2, 'Bob Jones', 35, '2024-02-15', 1),
        (3, 'Carol White', 42, '2024-03-20', 0)
    ").await?;

    client.execute("
        INSERT INTO posts VALUES
        (101, 'First Post', 'Hello World', '2024-01-10'),
        (102, 'Graph Views', 'Using ClickHouse as a graph', '2024-02-20')
    ").await?;

    client.execute("
        INSERT INTO user_follows VALUES
        (1, 2, '2024-02-01'),
        (2, 1, '2024-02-05')
    ").await?;

    client.execute("
        INSERT INTO user_posts VALUES
        (1, 101, '2024-01-10'),
        (2, 102, '2024-02-20')
    ").await?;

    Ok(())
}

#[tokio::test]
async fn test_view_based_query() -> anyhow::Result<()> {
    // Setup mock client
    let client = crate::testing::clickhouse::create_mock_client();

    // Create view definition
    let view = create_social_network_view();
    
    // Create graph context with view
    let mut context = GraphContext::new();
    context.add_view(view);

    // Test query: Find Alice's followers who have written posts
    let cypher = "
        MATCH (u:User {name: 'Alice Smith'})<-[f:FOLLOWS]-(follower:User)-[a:AUTHORED]->(p:Post)
        RETURN follower.name, p.title, f.since
    ";

    // Parse and plan query
    let ast = OpenCypherQueryAst::parse(cypher)?;
    let (logical_plan, plan_ctx) = evaluate_query(ast)?;
    
    // Generate SQL
    let render_plan = logical_plan.to_render_plan()?;
    let sql = generate_sql(render_plan);

    // Execute query
    let result = client.query(&sql).await?;
    
    // Verify results
    assert!(result.rows() > 0);
    let row = result.get_row(0)?;
    assert_eq!(row.get::<String>("follower.name")?, "Bob Jones");
    assert_eq!(row.get::<String>("p.title")?, "Graph Views");

    Ok(())
}

#[tokio::test]
async fn test_filtered_view_query() -> anyhow::Result<()> {
    // Setup similar to previous test
    let client = ClickHouseClient::connect("localhost:8123").await?;
    setup_test_data(&client).await?;

    let view = create_social_network_view();
    let mut context = GraphContext::new();
    context.add_view(view);

    // Test query that should respect the active=1 filter
    let cypher = "MATCH (u:User) RETURN u.name";
    
    let ast = OpenCypherQueryAst::parse(cypher)?;
    let (logical_plan, plan_ctx) = evaluate_query(ast)?;
    let render_plan = logical_plan.to_render_plan()?;
    let sql = generate_sql(render_plan);

    let result = client.query(&sql).await?;
    
    // Should only return active users (2 instead of 3)
    assert_eq!(result.rows(), 2);

    Ok(())
}