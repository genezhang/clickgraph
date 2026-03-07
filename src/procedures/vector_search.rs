//! Vector similarity search procedures for Neo4j GraphRAG compatibility.
//!
//! Implements `db.index.vector.queryNodes()` and `db.index.vector.queryRelationships()`
//! by translating to ClickHouse distance functions (cosineDistance, L2Distance).
//!
//! These procedures bypass ProcedureRegistry (like APOC export) because they
//! need async ClickHouse execution rather than sync schema introspection.
//!
//! # Neo4j API
//! ```cypher
//! CALL db.index.vector.queryNodes('index-name', k, [embedding...]) YIELD node, score
//! ```
//!
//! # Generated SQL (cosine)
//! ```sql
//! SELECT *, 1 - cosineDistance(embedding_col, [0.1, 0.2, ...]) AS score
//! FROM db.table
//! ORDER BY cosineDistance(embedding_col, [0.1, 0.2, ...]) ASC
//! LIMIT k
//! ```

use crate::graph_catalog::graph_schema::{GraphSchema, VectorIndexConfig};
use crate::open_cypher_parser::ast::Expression;

/// Check if a procedure name is a vector search procedure
pub fn is_vector_search_procedure(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "db.index.vector.querynodes" || lower == "db.index.vector.queryrelationships"
}

/// Parsed arguments from a vector search procedure call
#[derive(Debug, Clone)]
pub struct VectorSearchArgs {
    /// Vector index name
    pub index_name: String,
    /// Number of results (k)
    pub top_k: u64,
    /// Query embedding vector
    pub query_vector: Vec<f64>,
}

/// Parse arguments from a vector search CALL statement.
///
/// Expected: `db.index.vector.queryNodes('index-name', k, [v1, v2, ...])`
pub fn parse_vector_search_args(args: &[&Expression<'_>]) -> Result<VectorSearchArgs, String> {
    if args.len() < 3 {
        return Err(format!(
            "db.index.vector.queryNodes requires 3 arguments (indexName, numberOfNearestNeighbours, queryVector), got {}",
            args.len()
        ));
    }

    // Arg 1: index name (string literal)
    let index_name = extract_string_literal(args[0])?;

    // Arg 2: top-k (integer literal)
    let top_k = extract_integer_literal(args[1])?;
    if top_k == 0 {
        return Err("numberOfNearestNeighbours must be > 0".to_string());
    }

    // Arg 3: query vector (list of floats)
    let query_vector = extract_float_list(args[2])?;
    if query_vector.is_empty() {
        return Err("queryVector must not be empty".to_string());
    }

    Ok(VectorSearchArgs {
        index_name,
        top_k,
        query_vector,
    })
}

/// Build ClickHouse SQL for vector similarity search
pub fn build_vector_search_sql(
    args: &VectorSearchArgs,
    index_config: &VectorIndexConfig,
) -> String {
    let distance_expr = build_distance_expression(
        &index_config.column,
        &args.query_vector,
        &index_config.similarity,
    );

    let score_expr = build_score_expression(
        &index_config.column,
        &args.query_vector,
        &index_config.similarity,
    );

    format!(
        "SELECT *, {} AS score FROM {} ORDER BY {} ASC LIMIT {}",
        score_expr, index_config.table, distance_expr, args.top_k,
    )
}

/// Look up vector index config by name from GraphSchema
pub fn resolve_vector_index<'a>(
    schema: &'a GraphSchema,
    index_name: &str,
) -> Result<&'a VectorIndexConfig, String> {
    schema.get_vector_index(index_name).ok_or_else(|| {
        let available: Vec<_> = schema.vector_indexes().keys().collect();
        if available.is_empty() {
            format!(
                "Vector index '{}' not found. No vector indexes are configured in the schema. \
                 Add a vector_indexes section to your schema YAML.",
                index_name
            )
        } else {
            format!(
                "Vector index '{}' not found. Available indexes: {:?}",
                index_name, available
            )
        }
    })
}

/// Build the raw distance expression (for ORDER BY)
fn build_distance_expression(column: &str, query_vector: &[f64], similarity: &str) -> String {
    let vec_literal = format_vector_literal(query_vector);
    match similarity {
        "euclidean" => format!("L2Distance({}, {})", column, vec_literal),
        _ => format!("cosineDistance({}, {})", column, vec_literal),
    }
}

/// Build the score expression (for SELECT — higher = more similar)
fn build_score_expression(column: &str, query_vector: &[f64], similarity: &str) -> String {
    let vec_literal = format_vector_literal(query_vector);
    match similarity {
        "euclidean" => format!("1 / (1 + L2Distance({}, {}))", column, vec_literal),
        _ => format!("1 - cosineDistance({}, {})", column, vec_literal),
    }
}

/// Format a vector as a ClickHouse array literal
fn format_vector_literal(vector: &[f64]) -> String {
    let elements: Vec<String> = vector.iter().map(|v| format!("{}", v)).collect();
    format!("[{}]", elements.join(", "))
}

// ─── Expression extraction helpers ───

fn extract_string_literal(expr: &Expression<'_>) -> Result<String, String> {
    match expr {
        Expression::Literal(lit) => match lit {
            crate::open_cypher_parser::ast::Literal::String(s) => Ok(s.to_string()),
            other => Err(format!(
                "Expected string literal for index name, got {:?}",
                other
            )),
        },
        other => Err(format!(
            "Expected string literal for index name, got {:?}",
            other
        )),
    }
}

fn extract_integer_literal(expr: &Expression<'_>) -> Result<u64, String> {
    match expr {
        Expression::Literal(lit) => match lit {
            crate::open_cypher_parser::ast::Literal::Integer(n) => {
                if *n < 0 {
                    Err(format!(
                        "numberOfNearestNeighbours must be positive, got {}",
                        n
                    ))
                } else {
                    Ok(*n as u64)
                }
            }
            other => Err(format!(
                "Expected integer literal for numberOfNearestNeighbours, got {:?}",
                other
            )),
        },
        other => Err(format!(
            "Expected integer literal for numberOfNearestNeighbours, got {:?}",
            other
        )),
    }
}

fn extract_float_list(expr: &Expression<'_>) -> Result<Vec<f64>, String> {
    match expr {
        Expression::List(items) => {
            let mut result = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Expression::Literal(lit) => match lit {
                        crate::open_cypher_parser::ast::Literal::Float(f) => result.push(*f),
                        crate::open_cypher_parser::ast::Literal::Integer(n) => {
                            result.push(*n as f64)
                        }
                        other => {
                            return Err(format!(
                                "Expected numeric literal in queryVector, got {:?}",
                                other
                            ))
                        }
                    },
                    // Handle unary minus: OperatorApplication { operator: Subtraction, operands: [Literal] }
                    Expression::OperatorApplicationExp(op_app) => {
                        if op_app.operator == crate::open_cypher_parser::ast::Operator::Subtraction
                            && op_app.operands.len() == 1
                        {
                            match &op_app.operands[0] {
                                Expression::Literal(lit) => match lit {
                                    crate::open_cypher_parser::ast::Literal::Float(f) => {
                                        result.push(-f)
                                    }
                                    crate::open_cypher_parser::ast::Literal::Integer(n) => {
                                        result.push(-(*n as f64))
                                    }
                                    other => {
                                        return Err(format!(
                                            "Expected numeric literal in queryVector, got -{:?}",
                                            other
                                        ))
                                    }
                                },
                                other => {
                                    return Err(format!(
                                        "Expected numeric literal in queryVector, got -{:?}",
                                        other
                                    ))
                                }
                            }
                        } else {
                            return Err(format!(
                                "Unsupported expression in queryVector: {:?}",
                                op_app
                            ));
                        }
                    }
                    other => {
                        return Err(format!(
                            "Expected numeric literal in queryVector, got {:?}",
                            other
                        ))
                    }
                }
            }
            Ok(result)
        }
        other => Err(format!(
            "Expected list literal for queryVector (e.g., [0.1, 0.2, ...]), got {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast::{Expression, Literal, Operator, OperatorApplication};

    fn make_test_index() -> VectorIndexConfig {
        VectorIndexConfig {
            name: "article-embeddings".to_string(),
            label: "Article".to_string(),
            property: "embedding".to_string(),
            column: "embedding_vec".to_string(),
            table: "brahmand.articles".to_string(),
            dimensions: Some(3),
            similarity: "cosine".to_string(),
        }
    }

    #[test]
    fn test_is_vector_search_procedure() {
        assert!(is_vector_search_procedure("db.index.vector.queryNodes"));
        assert!(is_vector_search_procedure(
            "db.index.vector.queryRelationships"
        ));
        // Case insensitive
        assert!(is_vector_search_procedure("db.index.vector.QUERYNODES"));
        assert!(is_vector_search_procedure("DB.INDEX.VECTOR.QUERYNODES"));
        // Not vector search
        assert!(!is_vector_search_procedure("db.labels"));
        assert!(!is_vector_search_procedure("apoc.export.csv.query"));
        assert!(!is_vector_search_procedure(
            "db.index.vector.createNodeIndex"
        ));
    }

    #[test]
    fn test_build_vector_search_sql_cosine() {
        let args = VectorSearchArgs {
            index_name: "article-embeddings".to_string(),
            top_k: 5,
            query_vector: vec![0.1, 0.2, 0.3],
        };
        let index = make_test_index();
        let sql = build_vector_search_sql(&args, &index);
        assert_eq!(
            sql,
            "SELECT *, 1 - cosineDistance(embedding_vec, [0.1, 0.2, 0.3]) AS score \
             FROM brahmand.articles \
             ORDER BY cosineDistance(embedding_vec, [0.1, 0.2, 0.3]) ASC \
             LIMIT 5"
        );
    }

    #[test]
    fn test_build_vector_search_sql_euclidean() {
        let args = VectorSearchArgs {
            index_name: "article-euclidean".to_string(),
            top_k: 10,
            query_vector: vec![1.0, 2.0],
        };
        let mut index = make_test_index();
        index.similarity = "euclidean".to_string();
        let sql = build_vector_search_sql(&args, &index);
        assert_eq!(
            sql,
            "SELECT *, 1 / (1 + L2Distance(embedding_vec, [1, 2])) AS score \
             FROM brahmand.articles \
             ORDER BY L2Distance(embedding_vec, [1, 2]) ASC \
             LIMIT 10"
        );
    }

    #[test]
    fn test_parse_vector_search_args_valid() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("my-index")),
            Expression::Literal(Literal::Integer(5)),
            Expression::List(vec![
                Expression::Literal(Literal::Float(0.1)),
                Expression::Literal(Literal::Float(0.2)),
                Expression::Literal(Literal::Float(0.3)),
            ]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let result = parse_vector_search_args(&arg_refs).unwrap();
        assert_eq!(result.index_name, "my-index");
        assert_eq!(result.top_k, 5);
        assert_eq!(result.query_vector, vec![0.1, 0.2, 0.3]);
    }

    #[test]
    fn test_parse_vector_search_args_integer_vector() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("idx")),
            Expression::Literal(Literal::Integer(3)),
            Expression::List(vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(2)),
                Expression::Literal(Literal::Integer(3)),
            ]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let result = parse_vector_search_args(&arg_refs).unwrap();
        assert_eq!(result.query_vector, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_parse_vector_search_args_negative_values() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("idx")),
            Expression::Literal(Literal::Integer(3)),
            Expression::List(vec![
                Expression::Literal(Literal::Integer(-1)),
                Expression::Literal(Literal::Float(0.3)),
            ]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let result = parse_vector_search_args(&arg_refs).unwrap();
        assert_eq!(result.query_vector, vec![-1.0, 0.3]);
    }

    #[test]
    fn test_parse_vector_search_args_unary_minus() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("idx")),
            Expression::Literal(Literal::Integer(3)),
            Expression::List(vec![Expression::OperatorApplicationExp(
                OperatorApplication {
                    operator: Operator::Subtraction,
                    operands: vec![Expression::Literal(Literal::Float(0.5))],
                },
            )]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let result = parse_vector_search_args(&arg_refs).unwrap();
        assert_eq!(result.query_vector, vec![-0.5]);
    }

    #[test]
    fn test_parse_vector_search_args_too_few_args() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("my-index")),
            Expression::Literal(Literal::Integer(5)),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let err = parse_vector_search_args(&arg_refs).unwrap_err();
        assert!(err.contains("requires 3 arguments"));
    }

    #[test]
    fn test_parse_vector_search_args_zero_k() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("idx")),
            Expression::Literal(Literal::Integer(0)),
            Expression::List(vec![Expression::Literal(Literal::Float(0.1))]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let err = parse_vector_search_args(&arg_refs).unwrap_err();
        assert!(err.contains("must be > 0"));
    }

    #[test]
    fn test_parse_vector_search_args_empty_vector() {
        let args: Vec<Expression> = vec![
            Expression::Literal(Literal::String("idx")),
            Expression::Literal(Literal::Integer(5)),
            Expression::List(vec![]),
        ];
        let arg_refs: Vec<&Expression> = args.iter().collect();
        let err = parse_vector_search_args(&arg_refs).unwrap_err();
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn test_format_vector_literal() {
        assert_eq!(format_vector_literal(&[0.1, 0.2, 0.3]), "[0.1, 0.2, 0.3]");
        assert_eq!(format_vector_literal(&[1.0, 2.0]), "[1, 2]");
        assert_eq!(format_vector_literal(&[-0.5, 0.0, 0.5]), "[-0.5, 0, 0.5]");
    }
}
