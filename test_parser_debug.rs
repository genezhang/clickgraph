// Temporary test file to debug parser
use clickgraph::open_cypher_parser::expression::parse_expression;

fn main() {
    let test_cases = vec![
        "size([1,2,3])",
        "100 * size([1,2,3])",
        "size([(t)-[r]-(f) | f])",
        "100 * size([(t)-[r]-(f) | f])",
    ];

    for test in test_cases {
        println!("\n=== Testing: {} ===", test);
        match parse_expression(test) {
            Ok((remaining, expr)) => {
                println!("✅ SUCCESS");
                println!("  Remaining: {:?}", remaining);
                println!("  Expression: {:#?}", expr);
            }
            Err(e) => {
                println!("❌ FAILED: {:?}", e);
            }
        }
    }
}
