#!/bin/bash
# Quick smoke test for pattern comprehension parser

cd "$(dirname "$0")/.."

echo "Testing pattern comprehension parser..."

# Create a simple test program
cat > /tmp/test_pattern_comp.rs << 'EOF'
use clickgraph::open_cypher_parser::expression::parse_expression;
use clickgraph::open_cypher_parser::ast::Expression;

fn main() {
    // Test 1: Simple pattern comprehension
    let query1 = "[(user)-[:FOLLOWS]->(follower) | follower.name]";
    match parse_expression(query1) {
        Ok((rem, Expression::PatternComprehension(_))) => {
            println!("✅ Test 1 PASSED: Simple pattern comprehension");
        }
        Ok((_, other)) => {
            println!("❌ Test 1 FAILED: Wrong variant: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            println!("❌ Test 1 FAILED: Parse error: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 2: Pattern comprehension with WHERE
    let query2 = "[(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name]";
    match parse_expression(query2) {
        Ok((rem, Expression::PatternComprehension(pc))) => {
            if pc.where_clause.is_some() {
                println!("✅ Test 2 PASSED: Pattern comprehension with WHERE");
            } else {
                println!("❌ Test 2 FAILED: WHERE clause not parsed");
                std::process::exit(1);
            }
        }
        Ok((_, other)) => {
            println!("❌ Test 2 FAILED: Wrong variant: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            println!("❌ Test 2 FAILED: Parse error: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 3: Pattern comprehension with relationship property
    let query3 = "[(n)-[r]->(m) | r.weight]";
    match parse_expression(query3) {
        Ok((rem, Expression::PatternComprehension(_))) => {
            println!("✅ Test 3 PASSED: Relationship property projection");
        }
        Ok((_, other)) => {
            println!("❌ Test 3 FAILED: Wrong variant: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            println!("❌ Test 3 FAILED: Parse error: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 4: Pattern comprehension with expression
    let query4 = "[(n)-[:RATED]->(m) | m.score * 2]";
    match parse_expression(query4) {
        Ok((rem, Expression::PatternComprehension(_))) => {
            println!("✅ Test 4 PASSED: Expression projection");
        }
        Ok((_, other)) => {
            println!("❌ Test 4 FAILED: Wrong variant: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            println!("❌ Test 4 FAILED: Parse error: {:?}", e);
            std::process::exit(1);
        }
    }

    // Test 5: Nested pattern comprehension should still be a list
    let query5 = "[1, 2, 3]";
    match parse_expression(query5) {
        Ok((rem, Expression::List(_))) => {
            println!("✅ Test 5 PASSED: Regular list still works");
        }
        Ok((_, other)) => {
            println!("❌ Test 5 FAILED: Regular list broken: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            println!("❌ Test 5 FAILED: Parse error: {:?}", e);
            std::process::exit(1);
        }
    }

    println!("\n🎉 All pattern comprehension parser tests PASSED!");
}
EOF

# Compile and run
rustc --edition 2021 -L target/debug/deps --extern clickgraph=target/debug/libclickgraph.rlib /tmp/test_pattern_comp.rs -o /tmp/test_pattern_comp 2>&1

if [ $? -eq 0 ]; then
    /tmp/test_pattern_comp
    exit_code=$?
    rm -f /tmp/test_pattern_comp /tmp/test_pattern_comp.rs
    exit $exit_code
else
    echo "❌ Failed to compile test"
    exit 1
fi
