use clickgraph::open_cypher_parser::{
    ast::{Expression, Operator, OperatorApplication, PatternComprehension},
    expression::parse_expression,
};

#[test]
fn test_parse_pattern_comprehension_simple() {
    // Basic pattern comprehension: [(user)-[:FOLLOWS]->(follower) | follower.name]
    let (rem, expr) = parse_expression("[(user)-[:FOLLOWS]->(follower) | follower.name]").unwrap();
    assert_eq!(rem, "");
    
    if let Expression::PatternComprehension(pc) = expr {
        // Should have a pattern
        assert!(matches!(*pc.pattern, clickgraph::open_cypher_parser::ast::PathPattern { .. }));
        // No WHERE clause
        assert!(pc.where_clause.is_none());
        // Should have projection (property access)
        if let Expression::PropertyAccessExp(_) = *pc.projection {
            // Good
        } else {
            panic!("Expected PropertyAccessExp for projection, got {:?}", pc.projection);
        }
    } else {
        panic!("Expected PatternComprehension, got {:?}", expr);
    }
}

#[test]
fn test_parse_pattern_comprehension_with_where() {
    // Pattern comprehension with WHERE: [(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name]
    let (rem, expr) = parse_expression("[(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name]").unwrap();
    assert_eq!(rem, "");
    
    if let Expression::PatternComprehension(pc) = expr {
        // Should have WHERE clause
        assert!(pc.where_clause.is_some());
        // WHERE clause should be a comparison
        if let Some(where_expr) = pc.where_clause {
            if let Expression::OperatorApplicationExp(op) = *where_expr {
                assert_eq!(op.operator, Operator::GreaterThan);
            } else {
                panic!("Expected OperatorApplicationExp in WHERE, got {:?}", where_expr);
            }
        }
    } else {
        panic!("Expected PatternComprehension, got {:?}", expr);
    }
}

#[test]
fn test_parse_pattern_comprehension_relationship_property() {
    // Pattern comprehension projecting relationship property: [(n)-[r]->(m) | r.weight]
    let (rem, expr) = parse_expression("[(n)-[r]->(m) | r.weight]").unwrap();
    assert_eq!(rem, "");
    
    if let Expression::PatternComprehension(pc) = expr {
        assert!(pc.where_clause.is_none());
        // Projection should be relationship property
        if let Expression::PropertyAccessExp(prop) = *pc.projection {
            assert_eq!(prop.base, "r");
            assert_eq!(prop.key, "weight");
        } else {
            panic!("Expected PropertyAccessExp, got {:?}", pc.projection);
        }
    } else {
        panic!("Expected PatternComprehension, got {:?}", expr);
    }
}

#[test]
fn test_parse_pattern_comprehension_expression_projection() {
    // Pattern comprehension with expression projection: [(n)-[:RATED]->(m) | m.score * 2]
    let (rem, expr) = parse_expression("[(n)-[:RATED]->(m) | m.score * 2]").unwrap();
    assert_eq!(rem, "");
    
    if let Expression::PatternComprehension(pc) = expr {
        // Projection should be operator application (multiplication)
        if let Expression::OperatorApplicationExp(op) = *pc.projection {
            assert_eq!(op.operator, Operator::Multiplication);
        } else {
            panic!("Expected OperatorApplicationExp, got {:?}", pc.projection);
        }
    } else {
        panic!("Expected PatternComprehension, got {:?}", expr);
    }
}

#[test]
fn test_parse_pattern_comprehension_in_return() {
    // Pattern comprehension in RETURN context (would be part of larger query)
    let (rem, expr) = parse_expression("[(user)-[:FOLLOWS]->(f) WHERE f.active | f.name]").unwrap();
    assert_eq!(rem, "");
    
    if let Expression::PatternComprehension(_) = expr {
        // Success - parsed correctly
    } else {
        panic!("Expected PatternComprehension, got {:?}", expr);
    }
}
