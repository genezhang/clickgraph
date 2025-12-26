//! Test SQL generation for pattern comprehensions

use clickgraph::open_cypher_parser::parse_query;
use clickgraph::query_planner::logical_plan::plan_builder::build_logical_plan;
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::clickhouse_query_generator::generate_sql;

fn main() {
    println!("=== Pattern Comprehension SQL Generation Test ===\n");

    // Load benchmark schema
    let schema_path = "benchmarks/social_network/schemas/social_benchmark.yaml";
    let config = GraphSchemaConfig::from_yaml_file(schema_path)
        .expect("Failed to load schema config");
    let graph_schema = config.to_graph_schema()
        .expect("Failed to create graph schema");

    let test_cases = vec![
        (
            "Simple pattern comprehension",
            r#"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends"#,
        ),
        (
            "Pattern comprehension with WHERE",
            r#"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) WHERE f.country = 'USA' | f.name] AS us_friends"#,
        ),
        (
            "Multiple pattern comprehensions",
            r#"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends, [(u)<-[:FOLLOWS]-(follower) | follower.name] AS followers"#,
        ),
    ];

    for (name, cypher) in test_cases {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Test: {}", name);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Cypher:\n  {}\n", cypher);
        
        match parse_query(cypher) {
            Ok(ast) => {
                match build_logical_plan(&ast, &graph_schema, None, None) {
                    Ok((plan, _plan_ctx)) => {
                        match plan.as_ref().to_render_plan(&graph_schema) {
                            Ok(render_plan) => {
                                let sql = generate_sql(render_plan, 10);
                                println!("✅ SQL Generated:\n{}\n", sql);
                            }
                            Err(e) => {
                                println!("❌ Render plan generation failed: {}\n", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Plan build error: {}\n", e);
                    }
                }
            }
            Err(e) => {
                println!("❌ Parse error: {}\n", e);
            }
        }
    }
}
