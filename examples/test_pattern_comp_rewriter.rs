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

    // Test case 1: Simple pattern comprehension
    let cypher1 = r#"
        MATCH (u:User)
        WHERE u.user_id = 1
        RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends
    "#;

    println!("Test 1: Simple pattern comprehension");
    println!("Cypher:\n{}\n", cypher1);

    match parse_query(cypher1) {
        Ok(ast) => {
            println!("✅ Parsed successfully");
            
            match build_logical_plan(&ast, &graph_schema, None, None) {
                Ok((plan, _plan_ctx)) => {
                    println!("✅ Logical plan built successfully");
                    println!("Plan: {:#?}\n", plan);
                }
                Err(e) => println!("❌ Plan build failed: {}\n", e),
            }
        }
        Err(e) => println!("❌ Parse failed: {}\n", e),
    }

    // Test case 2: Pattern comprehension with WHERE
    let cypher2 = r#"
        MATCH (u:User)
        WHERE u.user_id = 1
        RETURN u.name, [(u)-[:FOLLOWS]->(f) WHERE f.country = 'USA' | f.name] AS us_friends
    "#;

    println!("Test 2: Pattern comprehension with WHERE clause");
    println!("Cypher:\n{}\n", cypher2);

    match parse_query(cypher2) {
        Ok(ast) => {
            println!("✅ Parsed successfully");
            
            match build_logical_plan(&ast, &graph_schema, None, None) {
                Ok((plan, _plan_ctx)) => {
                    println!("✅ Logical plan built successfully");
                    println!("Plan: {:#?}\n", plan);
                }
                Err(e) => println!("❌ Plan build failed: {}\n", e),
            }
        }
        Err(e) => println!("❌ Parse failed: {}\n", e),
    }

    // Test case 3: Multiple pattern comprehensions
    let cypher3 = r#"
        MATCH (u:User)
        WHERE u.user_id = 1
        RETURN u.name,
               [(u)-[:FOLLOWS]->(f) | f.name] AS friends,
               [(u)<-[:FOLLOWS]-(follower) | follower.name] AS followers
    "#;

    println!("Test 3: Multiple pattern comprehensions");
    println!("Cypher:\n{}\n", cypher3);

    match parse_query(cypher3) {
        Ok(ast) => {
            println!("✅ Parsed successfully");
            
            match build_logical_plan(&ast, &graph_schema, None, None) {
                Ok((plan, _plan_ctx)) => {
                    println!("✅ Logical plan built successfully");
                    println!("Plan: {:#?}\n", plan);
                }
                Err(e) => println!("❌ Plan build failed: {}\n", e),
            }
        }
        Err(e) => println!("❌ Parse failed: {}\n", e),
    }
}
