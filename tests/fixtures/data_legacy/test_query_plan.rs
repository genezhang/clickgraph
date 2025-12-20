use brahmand::open_cypher_parser::parse_cypher_query;
use brahmand::query_planner::plan_cypher_query;

fn main() {
    let query = "MATCH (u1:user)-[:FRIEND*1..2]->(u2:user) RETURN u1.name, u2.name";
    match parse_cypher_query(query) {
        Ok(ast) => {
            println!("Parsed: {:?}", ast);
            match plan_cypher_query(&ast) {
                Ok(plan) => println!("Planned: {:?}", plan),
                Err(e) => eprintln!("Planning error: {:?}", e),
            }
        }
        Err(e) => eprintln!("Parse error: {:?}", e),
    }
}
