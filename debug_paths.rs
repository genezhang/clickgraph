use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::query_planner::analyzer::multi_type_vlp_expansion::enumerate_vlp_paths;
use std::collections::HashMap;

fn main() {
    // Load the benchmark schema
    let yaml = r#"
name: social_benchmark

graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users_bench
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        email: email_address
        registration_date: registration_date
        is_active: is_active
        country: country
        city: city

    - label: Post
      database: brahmand
      table: posts_bench
      node_id: post_id
      property_mappings:
        post_id: post_id
        content: content
        date: created_at

  edges:
    - type: FOLLOWS
      database: brahmand
      table: user_follows_bench
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User
      property_mappings:
        follow_date: follow_date

    - type: FRIENDS_WITH
      database: brahmand
      table: friendships
      from_id: user_id_1
      to_id: user_id_2
      from_node: User
      to_node: User
      property_mappings:
        since_date: since
"#;

    let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("Failed to parse YAML");
    let schema = config.to_graph_schema().expect("Failed to build schema");

    println!("Schema relationships:");
    for (key, rel) in schema.get_relationships_schemas() {
        println!("  {}: {} -> {}", key, rel.from_node, rel.to_node);
    }

    // Test path enumeration
    let paths = enumerate_vlp_paths(
        &["User".to_string()],
        &["FOLLOWS".to_string(), "FRIENDS_WITH".to_string()],
        &["User".to_string()],
        1,
        2,
        &schema,
    );

    println!("Found {} paths", paths.len());
    for path in &paths {
        println!("  Path with {} hops:", path.length());
        for hop in &path.hops {
            println!("    {}: {} -> {}", hop.rel_type, hop.from_node_type, hop.to_node_type);
        }
    }
}