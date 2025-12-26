//! Clean summary test for pattern comprehension rewriter

use clickgraph::open_cypher_parser::parse_query;
use clickgraph::query_planner::logical_plan::plan_builder::build_logical_plan;
use clickgraph::graph_catalog::config::GraphSchemaConfig;

fn main() {
    println!("=== Pattern Comprehension Rewriter Test ===\n");

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
        (
            "Pattern comprehension with expression",
            r#"MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name + ' - Friend'] AS friends"#,
        ),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (name, cypher) in test_cases {
        print!("Test: {} ... ", name);
        
        match parse_query(cypher) {
            Ok(ast) => {
                match build_logical_plan(&ast, &graph_schema, None, None) {
                    Ok((plan, _plan_ctx)) => {
                        // Check if plan contains collect() calls
                        let plan_str = format!("{:#?}", plan);
                        if plan_str.contains("collect") {
                            println!("✅ PASS (rewritten to collect)");
                            passed += 1;
                        } else {
                            println!("❌ FAIL (no collect found)");
                            failed += 1;
                        }
                    }
                    Err(e) => {
                        println!("❌ FAIL (plan build error: {})", e);
                        failed += 1;
                    }
                }
            }
            Err(e) => {
                println!("❌ FAIL (parse error: {})", e);
                failed += 1;
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Passed: {}/{}", passed, passed + failed);
    println!("Failed: {}", failed);

    if failed > 0 {
        std::process::exit(1);
    }
}
