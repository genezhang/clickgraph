// Test what gets parsed from labels(u)[1]
// Run with: rustc --edition 2021 test_parser_subscript.rs && ./test_parser_subscript

fn main() {
    // Simulate parsing labels(u)[1]
    let input = "labels(u)[1]";
    println!("Input: {}", input);
    
    // The Cypher parser will:
    // 1. Try to parse as expression
    // 2. Function call: labels(u) - SUCCESS
    // 3. Remaining: [1]
    // 4. [1] looks like start of a new expression (list literal or pattern comprehension)
    
    println!("\nParsing steps:");
    println!("  1. parse_expression('labels(u)[1]')");
    println!("     -> FunctionCall(labels, [Variable(u)])");
    println!("     -> remaining: '[1]'");
    println!("\n  2. [1] is treated as separate expression/statement!");
    println!("     This is the bug!");
}
