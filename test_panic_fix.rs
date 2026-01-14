use clickgraph::open_cypher_parser::ast;
use clickgraph::query_planner::logical_expr::{LogicalExpr, LogicalExprError};

fn main() {
    // Test that PatternComprehension now returns an error instead of panicking
    let pattern_comprehension = ast::Expression::PatternComprehension(Box::new(ast::PatternComprehension {
        variable: "x".to_string(),
        pattern: ast::PathPattern::Node(ast::NodePattern {
            name: Some("n".to_string()),
            labels: None,
            properties: None,
        }),
        where_clause: None,
        expression: Box::new(ast::Expression::Variable("n".to_string())),
    }));
    
    // This should return an error, not panic
    match LogicalExpr::try_from(pattern_comprehension) {
        Ok(_) => {
            println!("ERROR: PatternComprehension should have failed!");
            std::process::exit(1);
        }
        Err(LogicalExprError::PatternComprehensionNotRewritten) => {
            println!("SUCCESS: PatternComprehension properly returns error instead of panicking");
        }
        Err(e) => {
            println!("ERROR: Unexpected error: {:?}", e);
            std::process::exit(1);
        }
    }
    
    // Test that valid expressions still work
    let valid_expr = ast::Expression::Literal(ast::Literal::String("test".to_string()));
    match LogicalExpr::try_from(valid_expr) {
        Ok(LogicalExpr::Literal(_)) => {
            println!("SUCCESS: Valid expressions still work");
        }
        Ok(_) => {
            println!("ERROR: Unexpected success type");
            std::process::exit(1);
        }
        Err(e) => {
            println!("ERROR: Valid expression failed: {:?}", e);
            std::process::exit(1);
        }
    }
    
    println!("All panic fixes verified!");
}
