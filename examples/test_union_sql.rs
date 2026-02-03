use clickgraph::clickhouse_query_generator::to_sql_query::render_plan_to_sql;
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph::open_cypher_parser::parse_cypher_statement;
use clickgraph::query_planner::logical_plan::evaluate_cypher_statement;
use clickgraph::render_plan::plan_builder::ToRenderPlan;

fn main() {
    // Load schema
    let schema_path = "./benchmarks/social_network/schemas/social_benchmark.yaml";
    let schema = GraphSchema::from_yaml_file(schema_path).expect("Failed to load schema");

    let query = r#"
MATCH (n:User) WHERE n.user_id = 1
RETURN "node" as entity, n.name AS name LIMIT 25 
UNION ALL 
MATCH (p:Post) WHERE p.post_id = 1
RETURN "relationship" AS entity, p.title AS name LIMIT 25
"#;

    println!("Testing UNION ALL query:\n{}\n", query);
    println!("{}", "=".repeat(80));

    // Parse
    let statement = match parse_cypher_statement(query) {
        Ok((_, stmt)) => {
            println!("✅ Parse successful\n");
            stmt
        }
        Err(e) => {
            println!("❌ Parse failed: {:?}", e);
            std::process::exit(1);
        }
    };

    // Logical plan
    let (logical_plan, plan_ctx) =
        match evaluate_cypher_statement(statement, &schema, None, None, None) {
            Ok(result) => {
                println!("✅ Logical plan generation successful\n");
                result
            }
            Err(e) => {
                println!("❌ Logical plan failed: {:?}", e);
                std::process::exit(1);
            }
        };

    println!("Logical Plan:");
    println!("{:#?}\n", logical_plan);
    println!("{}", "=".repeat(80));

    // Render plan
    let render_plan = match logical_plan.to_render_plan_with_ctx(&schema, Some(&plan_ctx)) {
        Ok(plan) => {
            println!("✅ Render plan generation successful\n");
            plan
        }
        Err(e) => {
            println!("❌ Render plan failed: {:?}", e);
            std::process::exit(1);
        }
    };

    println!("Render Plan:");
    println!("{:#?}\n", render_plan);
    println!("{}", "=".repeat(80));

    // SQL generation (render_plan_to_sql returns String directly, not Result)
    let sql = render_plan_to_sql(render_plan, 100); // max_cte_depth = 100

    println!("✅ SQL generation successful\n");
    println!("Generated SQL:");
    println!("{}", sql);
}
