//! Simple smoke test for pattern comprehension parser

use clickgraph::open_cypher_parser::parse_query;

fn main() {
    println!("Testing pattern comprehension parser...\n");

    // Test 1: Simple pattern comprehension in RETURN
    let query1 = "MATCH (user) RETURN [(user)-[:FOLLOWS]->(follower) | follower.name]";
    match parse_query(query1) {
        Ok(_) => println!("✅ Test 1 PASSED: Simple pattern comprehension in RETURN"),
        Err(e) => {
            eprintln!("❌ Test 1 FAILED: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 2: Pattern comprehension with WHERE
    let query2 = "MATCH (a) RETURN [(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name]";
    match parse_query(query2) {
        Ok(_) => println!("✅ Test 2 PASSED: Pattern comprehension with WHERE"),
        Err(e) => {
            eprintln!("❌ Test 2 FAILED: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 3: Pattern comprehension with relationship property
    let query3 = "MATCH (n) RETURN [(n)-[r]->(m) | r.weight]";
    match parse_query(query3) {
        Ok(_) => println!("✅ Test 3 PASSED: Relationship property projection"),
        Err(e) => {
            eprintln!("❌ Test 3 FAILED: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 4: Pattern comprehension with expression
    let query4 = "MATCH (n) RETURN [(n)-[:RATED]->(m) | m.score * 2]";
    match parse_query(query4) {
        Ok(_) => println!("✅ Test 4 PASSED: Expression projection"),
        Err(e) => {
            eprintln!("❌ Test 4 FAILED: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 5: Regular list should still work
    let query5 = "RETURN [1, 2, 3]";
    match parse_query(query5) {
        Ok(_) => println!("✅ Test 5 PASSED: Regular list still works"),
        Err(e) => {
            eprintln!("❌ Test 5 FAILED: {:?}", e);
            std::process::exit(1);
        }
    }

    println!("\n🎉 All pattern comprehension parser tests PASSED!");
}
