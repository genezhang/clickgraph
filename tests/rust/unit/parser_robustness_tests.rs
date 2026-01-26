//! Unit tests for query parsing edge cases and error handling
//!
//! Tests malformed queries, edge cases, and error conditions to ensure
//! robust parsing without panics.

#[cfg(test)]
mod parser_robustness_tests {
    use clickgraph::open_cypher_parser::{parse_query, strip_comments};

    /// Test that malformed queries don't cause panics
    #[test]
    fn test_malformed_queries_no_panic() {
        let malformed_queries = vec![
            "",  // Empty query
            "MATCH",  // Incomplete MATCH
            "MATCH (",  // Unclosed parenthesis
            "MATCH )",  // Wrong parenthesis
            "MATCH (n",  // Unclosed node
            "MATCH n)",  // Wrong node syntax
            "MATCH (n-",  // Incomplete relationship
            "MATCH (n-[]",  // Unclosed relationship
            "MATCH (n-[]-",  // Missing end node
            "MATCH (n)-[]",  // Missing end node
            "RETURN",  // Incomplete RETURN
            "WHERE",  // Incomplete WHERE
            "MATCH (n) RETURN n WHERE",  // Wrong clause order
            "MATCH (n) INVALID_CLAUSE",  // Invalid clause
            "MATCH (n) RETURN n INVALID_KEYWORD",  // Invalid keyword
        ];

        for query in malformed_queries {
            // Should not panic, should return error
            let result = parse_query(query);
            // We don't assert the result type since some might parse partially
            // The important thing is no panic occurs
            let _ = result; // Use result to avoid unused variable warning
        }
    }

    /// Test comment stripping edge cases
    #[test]
    fn test_comment_stripping_edge_cases() {
        let test_cases = vec![
            ("", ""),  // Empty string
            ("SELECT 1", "SELECT 1"),  // No comments
            ("SELECT 1 -- comment", "SELECT 1"),  // Line comment
            ("SELECT 1 # comment", "SELECT 1 # comment"),  // # not stripped
            ("SELECT 1 /* comment */", "SELECT 1"),  // Block comment
            ("/* comment */ SELECT 1", "SELECT 1"),  // Block comment at start
            ("SELECT 1 /* comment */ WHERE", "SELECT 1 WHERE"),  // Block comment in middle
            ("SELECT 1 /* multi\nline\ncomment */ WHERE", "SELECT 1 WHERE"),  // Multi-line block comment
            ("SELECT 1 -- comment\nSELECT 2", "SELECT 1\nSELECT 2"),  // Line comment with newline
            ("SELECT 1 /* comment /* nested */ */ WHERE", "SELECT 1 WHERE"),  // Nested block comments (not supported but shouldn't crash)
        ];

        for (input, expected) in test_cases {
            let result = strip_comments(input);
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }

    /// Test valid queries that should parse successfully
    #[test]
    fn test_valid_queries_parse() {
        let valid_queries = vec![
            "MATCH (n) RETURN n",
            "MATCH (n:User) RETURN n.name",
            "MATCH (n)-[:FOLLOWS]->(m) RETURN n, m",
            "MATCH (n) WHERE n.age > 25 RETURN n",
            "MATCH (n) RETURN n LIMIT 10",
            "MATCH (n) RETURN n ORDER BY n.name",
            "MATCH (n) RETURN n.name AS name",
            "MATCH (n) WITH n.name AS name RETURN name",
            "MATCH (n)-[r]->(m) RETURN r",
            "MATCH (n) RETURN count(n)",
            "MATCH (n) RETURN n UNION MATCH (m) RETURN m",
        ];

        for query in valid_queries {
            let result = parse_query(query);
            assert!(result.is_ok(), "Failed to parse valid query: {}", query);
        }
    }

    /// Test Cypher syntax variations
    #[test]
    fn test_cypher_syntax_variations() {
        let variations = vec![
            "match (n) return n",  // lowercase keywords
            "MATCH (n)\nRETURN n",  // multiline
            "MATCH (n) RETURN n;",  // trailing semicolon
            "MATCH (n) RETURN n ;",  // semicolon with space
            "MATCH   (n)   RETURN   n",  // extra spaces
            "MATCH\n(\nn\n)\nRETURN\nn",  // newlines everywhere
        ];

        for query in variations {
            let result = parse_query(query);
            // These should either parse successfully or fail gracefully
            let _ = result; // Use to avoid warning
        }
    }

    /// Test parameter syntax
    #[test]
    fn test_parameter_syntax() {
        let param_queries = vec![
            "MATCH (n) WHERE n.id = $id RETURN n",
            "MATCH (n) WHERE n.id = $userId AND n.active = $active RETURN n",
            "RETURN $param",
            "MATCH (n) WHERE n.tags IN $tags RETURN n",
        ];

        for query in param_queries {
            let result = parse_query(query);
            assert!(result.is_ok(), "Failed to parse parameter query: {}", query);
        }
    }

    /// Test complex nested expressions
    #[test]
    fn test_complex_expressions() {
        let complex_queries = vec![
            "MATCH (n) WHERE n.age > 18 AND n.active = true RETURN n",
            "MATCH (n) WHERE n.score BETWEEN 0 AND 100 RETURN n",
            "MATCH (n) WHERE n.name =~ '.*john.*' RETURN n",  // Regex (if supported)
            "MATCH (n) RETURN n.name + ' ' + n.surname AS full_name",
            "MATCH (n) RETURN CASE WHEN n.age < 18 THEN 'minor' ELSE 'adult' END",
        ];

        for query in complex_queries {
            let result = parse_query(query);
            // Complex expressions may not all be supported, but shouldn't panic
            let _ = result;
        }
    }

    /// Test USE clause parsing
    #[test]
    fn test_use_clause_parsing() {
        let use_queries = vec![
            "USE default MATCH (n) RETURN n",
            "USE social_network MATCH (n) RETURN n",
            "USE my_schema MATCH (n) RETURN n",
        ];

        for query in use_queries {
            let result = parse_query(query);
            assert!(result.is_ok(), "Failed to parse USE clause query: {}", query);

            let ast = result.unwrap();
            assert!(ast.use_clause.is_some(), "USE clause should be parsed");
        }
    }

    /// Test CALL clause parsing (procedures)
    #[test]
    fn test_call_clause_parsing() {
        let call_queries = vec![
            "CALL my.procedure()",
            "CALL db.labels() YIELD label",
            "CALL db.relationships() YIELD relationship",
        ];

        for query in call_queries {
            let result = parse_query(query);
            // CALL clauses may not be fully implemented, but shouldn't panic
            let _ = result;
        }
    }
}