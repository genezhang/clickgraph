// Test to verify ViewScan generation
use brahmand::open_cypher_parser::parse_query;
use brahmand::query_planner::plan_ctx::PlanCtx;
use brahmand::query_planner::logical_plan::QueryPlanner;

fn main() {
    println!("\n=== Testing ViewScan Generation ===\n");
    
    let query = "MATCH (u:User) RETURN u.name";
    println!("Query: {}", query);
    
    match parse_query(query) {
        Ok((_, ast)) => {
            println!("\n✓ Query parsed successfully");
            println!("AST: {:#?}", ast);
            
            let mut plan_ctx = PlanCtx::new();
            match QueryPlanner::plan_query(&ast, &mut plan_ctx) {
                Ok(plan) => {
                    println!("\n✓ Logical plan created:");
                    println!("{:#?}", plan);
                }
                Err(e) => {
                    println!("\n✗ Failed to create logical plan: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("\n✗ Failed to parse query: {:?}", e);
        }
    }
}
