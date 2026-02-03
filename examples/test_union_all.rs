use clickgraph::open_cypher_parser::parse_cypher_statement;

fn main() {
    let query = r#"
MATCH (n) WHERE (n.bytes_sent) IS NOT NULL 
RETURN DISTINCT "node" as entity, n.bytes_sent AS bytes_sent LIMIT 25 
UNION ALL 
MATCH ()-[r]-() WHERE (r.bytes_sent) IS NOT NULL 
RETURN DISTINCT "relationship" AS entity, r.bytes_sent AS bytes_sent LIMIT 25
"#;

    println!("Testing UNION ALL query parsing...\n");

    match parse_cypher_statement(query) {
        Ok((remaining, statement)) => {
            println!("✅ Parse successful!");
            println!(
                "Remaining input: '{}' (len: {})",
                remaining,
                remaining.len()
            );
            println!("\nParsed statement:");
            println!("{:#?}", statement);
        }
        Err(e) => {
            println!("❌ Parse failed:");
            println!("{:?}", e);
            std::process::exit(1);
        }
    }
}
