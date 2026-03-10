//! Full-text search procedures for Neo4j GraphRAG compatibility.
//!
//! Implements `db.index.fulltext.queryNodes()` by translating to ClickHouse
//! text search functions (ngramDistance, multiSearchAny, hasToken).
//!
//! These procedures bypass ProcedureRegistry (like vector search) because they
//! need async ClickHouse execution rather than sync schema introspection.
//!
//! # Neo4j API
//! ```cypher
//! CALL db.index.fulltext.queryNodes('index-name', 'search query') YIELD node, score
//! ```
//!
//! # Generated SQL (standard analyzer — ngramDistance + multiSearchAny)
//! ```sql
//! SELECT *, greatest(1 - ngramDistance(col1, 'query'), 1 - ngramDistance(col2, 'query')) AS score
//! FROM db.table
//! WHERE multiSearchAnyCaseInsensitive(concat(col1, ' ', col2), ['term1', 'term2'])
//! ORDER BY score DESC
//! LIMIT 100
//! ```

use crate::graph_catalog::graph_schema::{FulltextIndexConfig, GraphSchema};
use crate::open_cypher_parser::ast::Expression;

/// Default result limit when not specified
const DEFAULT_LIMIT: u64 = 100;

/// Check if a procedure name is a fulltext search procedure
pub fn is_fulltext_search_procedure(name: &str) -> bool {
    name.eq_ignore_ascii_case("db.index.fulltext.querynodes")
}

/// Parsed arguments from a fulltext search procedure call
#[derive(Debug, Clone)]
pub struct FulltextSearchArgs {
    /// Fulltext index name
    pub index_name: String,
    /// Search query text
    pub query_text: String,
    /// Optional result limit (defaults to 100)
    pub limit: u64,
}

/// Parse arguments from a fulltext search CALL statement.
///
/// Expected: `db.index.fulltext.queryNodes('index-name', 'search text')`
/// Optional third arg for limit: `db.index.fulltext.queryNodes('index-name', 'search text', 50)`
pub fn parse_fulltext_search_args(args: &[&Expression<'_>]) -> Result<FulltextSearchArgs, String> {
    if args.len() < 2 {
        return Err(format!(
            "Fulltext search requires at least 2 arguments (indexName, queryString), got {}",
            args.len()
        ));
    }

    // Arg 1: index name (string literal)
    let index_name = extract_string_literal(args[0], "index name")?;

    // Arg 2: query text (string literal)
    let query_text = extract_string_literal(args[1], "query string")?;
    if query_text.trim().is_empty() {
        return Err("queryString must not be empty".to_string());
    }

    // Arg 3 (optional): limit
    let limit = if args.len() >= 3 {
        extract_integer_literal(args[2])?
    } else {
        DEFAULT_LIMIT
    };

    Ok(FulltextSearchArgs {
        index_name,
        query_text,
        limit,
    })
}

/// Build ClickHouse SQL for full-text search.
///
/// Generates different SQL depending on the analyzer configured for the index:
/// - "standard": ngramDistance for fuzzy matching + multiSearchAny pre-filter
/// - "ngram": ngramDistance only (pure fuzzy matching, no pre-filter)
/// - "exact": hasToken for exact token matching
pub fn build_fulltext_search_sql(
    args: &FulltextSearchArgs,
    index_config: &FulltextIndexConfig,
) -> String {
    let escaped_query = escape_sql_string(&args.query_text);

    match index_config.analyzer.as_str() {
        "exact" => build_exact_sql(args, index_config, &escaped_query),
        "ngram" => build_ngram_sql(args, index_config, &escaped_query),
        _ => build_standard_sql(args, index_config, &escaped_query),
    }
}

/// Standard analyzer: ngramDistance scoring + multiSearchAny pre-filter
fn build_standard_sql(
    args: &FulltextSearchArgs,
    config: &FulltextIndexConfig,
    escaped_query: &str,
) -> String {
    let score_expr = build_ngram_score_expr(&config.columns, escaped_query);
    let filter_expr = build_multi_search_filter(&config.columns, &args.query_text);

    format!(
        "SELECT *, {} AS score FROM {} WHERE {} ORDER BY score DESC LIMIT {}",
        score_expr, config.table, filter_expr, args.limit,
    )
}

/// Ngram analyzer: ngramDistance scoring without pre-filter (pure fuzzy)
fn build_ngram_sql(
    args: &FulltextSearchArgs,
    config: &FulltextIndexConfig,
    escaped_query: &str,
) -> String {
    let score_expr = build_ngram_score_expr(&config.columns, escaped_query);

    format!(
        "SELECT * FROM (SELECT *, {} AS score FROM {}) WHERE score > 0 ORDER BY score DESC LIMIT {}",
        score_expr, config.table, args.limit,
    )
}

/// Exact analyzer: hasToken for exact word matching
fn build_exact_sql(
    args: &FulltextSearchArgs,
    config: &FulltextIndexConfig,
    _escaped_query: &str,
) -> String {
    let filter_expr = build_has_token_filter(&config.columns, &args.query_text);

    format!(
        "SELECT *, 1.0 AS score FROM {} WHERE {} LIMIT {}",
        config.table, filter_expr, args.limit,
    )
}

/// Build ngramDistance-based score expression across multiple columns.
/// Uses `greatest()` to pick the best match across all indexed columns.
fn build_ngram_score_expr(columns: &[String], escaped_query: &str) -> String {
    if columns.len() == 1 {
        format!("1 - ngramDistance({}, '{}')", columns[0], escaped_query)
    } else {
        let exprs: Vec<String> = columns
            .iter()
            .map(|col| format!("1 - ngramDistance({}, '{}')", col, escaped_query))
            .collect();
        format!("greatest({})", exprs.join(", "))
    }
}

/// Build multiSearchAnyCaseInsensitive filter for pre-filtering.
/// Tokenizes the query into individual words for broad matching.
fn build_multi_search_filter(columns: &[String], query_text: &str) -> String {
    let tokens = tokenize_query(query_text);
    let escaped_tokens: Vec<String> = tokens.iter().map(|t| escape_sql_string(t)).collect();
    let token_array = format!(
        "[{}]",
        escaped_tokens
            .iter()
            .map(|t| format!("'{}'", t))
            .collect::<Vec<_>>()
            .join(", ")
    );

    if columns.len() == 1 {
        format!(
            "multiSearchAnyCaseInsensitive({}, {})",
            columns[0], token_array
        )
    } else {
        // Concatenate columns with space separator for multi-column search
        let concat_expr = format!(
            "concat({})",
            columns
                .iter()
                .enumerate()
                .flat_map(|(i, col)| {
                    if i == 0 {
                        vec![col.clone()]
                    } else {
                        vec!["' '".to_string(), col.clone()]
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        format!(
            "multiSearchAnyCaseInsensitive({}, {})",
            concat_expr, token_array
        )
    }
}

/// Build hasToken filter for exact word matching across columns.
/// Each query token must appear in at least one column.
fn build_has_token_filter(columns: &[String], query_text: &str) -> String {
    let tokens = tokenize_query(query_text);

    let conditions: Vec<String> = tokens
        .iter()
        .map(|token| {
            let escaped = escape_sql_string(&token.to_lowercase());
            if columns.len() == 1 {
                format!("hasToken(lower({}), '{}')", columns[0], escaped)
            } else {
                let or_parts: Vec<String> = columns
                    .iter()
                    .map(|col| format!("hasToken(lower({}), '{}')", col, escaped))
                    .collect();
                format!("({})", or_parts.join(" OR "))
            }
        })
        .collect();

    conditions.join(" AND ")
}

/// Look up fulltext index config by name from GraphSchema
pub fn resolve_fulltext_index<'a>(
    schema: &'a GraphSchema,
    index_name: &str,
) -> Result<&'a FulltextIndexConfig, String> {
    schema.get_fulltext_index(index_name).ok_or_else(|| {
        let available: Vec<_> = schema.fulltext_indexes().keys().collect();
        if available.is_empty() {
            format!(
                "Fulltext index '{}' not found. No fulltext indexes are configured in the schema. \
                 Add a fulltext_indexes section to your schema YAML.",
                index_name
            )
        } else {
            format!(
                "Fulltext index '{}' not found. Available indexes: {:?}",
                index_name, available
            )
        }
    })
}

// ─── Helpers ───

/// Tokenize a search query into individual words
fn tokenize_query(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Escape single quotes and backslashes in a string for SQL embedding
fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn extract_string_literal(expr: &Expression<'_>, param_name: &str) -> Result<String, String> {
    match expr {
        Expression::Literal(lit) => match lit {
            crate::open_cypher_parser::ast::Literal::String(s) => Ok(s.to_string()),
            other => Err(format!(
                "Expected string literal for {}, got {:?}",
                param_name, other
            )),
        },
        other => Err(format!(
            "Expected string literal for {}, got {:?}",
            param_name, other
        )),
    }
}

fn extract_integer_literal(expr: &Expression<'_>) -> Result<u64, String> {
    match expr {
        Expression::Literal(lit) => match lit {
            crate::open_cypher_parser::ast::Literal::Integer(n) => {
                if *n <= 0 {
                    Err(format!("limit must be positive, got {}", n))
                } else {
                    Ok(*n as u64)
                }
            }
            other => Err(format!(
                "Expected integer literal for limit, got {:?}",
                other
            )),
        },
        other => Err(format!(
            "Expected integer literal for limit, got {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::FulltextIndexConfig;

    fn make_config(columns: Vec<&str>, analyzer: &str) -> FulltextIndexConfig {
        FulltextIndexConfig {
            name: "test-index".to_string(),
            label: "Article".to_string(),
            properties: columns.iter().map(|c| c.to_string()).collect(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            table: "db.articles".to_string(),
            analyzer: analyzer.to_string(),
        }
    }

    // ─── Procedure detection ───

    #[test]
    fn test_is_fulltext_search_procedure() {
        assert!(is_fulltext_search_procedure("db.index.fulltext.queryNodes"));
        assert!(is_fulltext_search_procedure("DB.INDEX.FULLTEXT.QUERYNODES"));
        assert!(is_fulltext_search_procedure("db.index.fulltext.querynodes"));
        assert!(!is_fulltext_search_procedure("db.index.vector.queryNodes"));
        assert!(!is_fulltext_search_procedure(
            "db.index.fulltext.queryRelationships"
        ));
    }

    // ─── SQL generation: standard analyzer ───

    #[test]
    fn test_standard_single_column() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "machine learning".to_string(),
            limit: 10,
        };
        let config = make_config(vec!["title"], "standard");
        let sql = build_fulltext_search_sql(&args, &config);

        assert!(sql.contains("1 - ngramDistance(title, 'machine learning')"));
        assert!(sql.contains("multiSearchAnyCaseInsensitive(title, ['machine', 'learning'])"));
        assert!(sql.contains("ORDER BY score DESC"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_standard_multi_column() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "database".to_string(),
            limit: 5,
        };
        let config = make_config(vec!["title", "content"], "standard");
        let sql = build_fulltext_search_sql(&args, &config);

        assert!(sql.contains("greatest("));
        assert!(sql.contains("1 - ngramDistance(title, 'database')"));
        assert!(sql.contains("1 - ngramDistance(content, 'database')"));
        assert!(sql.contains("concat(title, ' ', content)"));
        assert!(sql.contains("LIMIT 5"));
    }

    // ─── SQL generation: ngram analyzer ───

    #[test]
    fn test_ngram_analyzer() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "fuzzy search".to_string(),
            limit: 20,
        };
        let config = make_config(vec!["title"], "ngram");
        let sql = build_fulltext_search_sql(&args, &config);

        assert!(sql.contains("1 - ngramDistance(title, 'fuzzy search')"));
        // Score computed once in subquery, filtered in outer query
        assert!(sql.contains("SELECT * FROM (SELECT *,"));
        assert!(sql.contains("WHERE score > 0"));
        assert!(!sql.contains("multiSearchAny"));
        assert!(sql.contains("LIMIT 20"));
    }

    // ─── SQL generation: exact analyzer ───

    #[test]
    fn test_exact_single_column() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "clickhouse".to_string(),
            limit: 50,
        };
        let config = make_config(vec!["title"], "exact");
        let sql = build_fulltext_search_sql(&args, &config);

        assert!(sql.contains("hasToken(lower(title), 'clickhouse')"));
        assert!(sql.contains("1.0 AS score"));
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn test_exact_multi_word_multi_column() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "graph database".to_string(),
            limit: 10,
        };
        let config = make_config(vec!["title", "content"], "exact");
        let sql = build_fulltext_search_sql(&args, &config);

        // Each word must match in at least one column
        assert!(
            sql.contains("(hasToken(lower(title), 'graph') OR hasToken(lower(content), 'graph'))")
        );
        assert!(sql.contains(
            "(hasToken(lower(title), 'database') OR hasToken(lower(content), 'database'))"
        ));
        assert!(sql.contains(" AND "));
    }

    // ─── SQL escaping ───

    #[test]
    fn test_sql_injection_prevention() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "O'Reilly's book".to_string(),
            limit: 10,
        };
        let config = make_config(vec!["title"], "standard");
        let sql = build_fulltext_search_sql(&args, &config);

        assert!(sql.contains("O\\'Reilly\\'s book"));
        assert!(!sql.contains("O'Reilly's"));
    }

    // ─── Tokenization ───

    #[test]
    fn test_tokenize_query() {
        assert_eq!(
            tokenize_query("machine learning"),
            vec!["machine", "learning"]
        );
        assert_eq!(
            tokenize_query("  spaces  between  "),
            vec!["spaces", "between"]
        );
        assert_eq!(tokenize_query("single"), vec!["single"]);
        assert!(tokenize_query("").is_empty());
    }

    // ─── Index resolution ───

    #[test]
    fn test_resolve_fulltext_index_not_found() {
        use std::collections::HashMap;
        let schema = GraphSchema::build(1, "db".to_string(), HashMap::new(), HashMap::new());
        let result = resolve_fulltext_index(&schema, "nonexistent");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("No fulltext indexes are configured"));
    }

    // ─── Default limit ───

    #[test]
    fn test_default_limit() {
        let args = FulltextSearchArgs {
            index_name: "test".to_string(),
            query_text: "test".to_string(),
            limit: DEFAULT_LIMIT,
        };
        let config = make_config(vec!["title"], "standard");
        let sql = build_fulltext_search_sql(&args, &config);
        assert!(sql.contains("LIMIT 100"));
    }

    // ─── Argument parsing ───

    #[test]
    fn test_parse_args_valid_two_args() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("my-index"));
        let query = Expression::Literal(Literal::String("search text"));
        let args = parse_fulltext_search_args(&[&idx, &query]).unwrap();
        assert_eq!(args.index_name, "my-index");
        assert_eq!(args.query_text, "search text");
        assert_eq!(args.limit, DEFAULT_LIMIT);
    }

    #[test]
    fn test_parse_args_valid_three_args_with_limit() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("my-index"));
        let query = Expression::Literal(Literal::String("search text"));
        let limit = Expression::Literal(Literal::Integer(25));
        let args = parse_fulltext_search_args(&[&idx, &query, &limit]).unwrap();
        assert_eq!(args.index_name, "my-index");
        assert_eq!(args.query_text, "search text");
        assert_eq!(args.limit, 25);
    }

    #[test]
    fn test_parse_args_too_few() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("my-index"));
        let err = parse_fulltext_search_args(&[&idx]).unwrap_err();
        assert!(err.contains("at least 2 arguments"), "Error: {}", err);
    }

    #[test]
    fn test_parse_args_non_string_index() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::Integer(42));
        let query = Expression::Literal(Literal::String("text"));
        let err = parse_fulltext_search_args(&[&idx, &query]).unwrap_err();
        assert!(err.contains("Expected string literal"), "Error: {}", err);
    }

    #[test]
    fn test_parse_args_empty_query() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("idx"));
        let query = Expression::Literal(Literal::String(""));
        let err = parse_fulltext_search_args(&[&idx, &query]).unwrap_err();
        assert!(err.contains("must not be empty"), "Error: {}", err);
    }

    #[test]
    fn test_parse_args_whitespace_only_query() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("idx"));
        let query = Expression::Literal(Literal::String("   "));
        let err = parse_fulltext_search_args(&[&idx, &query]).unwrap_err();
        assert!(err.contains("must not be empty"), "Error: {}", err);
    }

    #[test]
    fn test_parse_args_zero_limit() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("idx"));
        let query = Expression::Literal(Literal::String("text"));
        let limit = Expression::Literal(Literal::Integer(0));
        let err = parse_fulltext_search_args(&[&idx, &query, &limit]).unwrap_err();
        assert!(err.contains("must be positive"), "Error: {}", err);
    }

    #[test]
    fn test_parse_args_negative_limit() {
        use crate::open_cypher_parser::ast::{Expression, Literal};
        let idx = Expression::Literal(Literal::String("idx"));
        let query = Expression::Literal(Literal::String("text"));
        let limit = Expression::Literal(Literal::Integer(-5));
        let err = parse_fulltext_search_args(&[&idx, &query, &limit]).unwrap_err();
        assert!(err.contains("must be positive"), "Error: {}", err);
    }
}
