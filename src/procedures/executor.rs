//! Procedure executor - handles routing and execution of procedure calls.

use super::{ProcedureRegistry, ProcedureResult};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser;
use crate::open_cypher_parser::ast::{CypherStatement, StandaloneProcedureCall};
use crate::server::GLOBAL_SCHEMAS;
use std::collections::HashMap;
use std::sync::Arc;

/// Execute a standalone procedure call by name
///
/// # Arguments
/// * `procedure_name` - Name of the procedure to execute
/// * `schema_name` - Schema name from HTTP request parameter (defaults to "default")
/// * `registry` - Procedure registry containing available procedures
///
/// # Returns
/// * `Ok(records)` - Vector of record maps (field name -> value)
/// * `Err(message)` - Error message if procedure fails or doesn't exist
pub async fn execute_procedure_by_name(
    procedure_name: &str,
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    // Look up the procedure
    let proc_fn = registry
        .get(procedure_name)
        .ok_or_else(|| format!("Unknown procedure: {}", procedure_name))?;

    // Get the schema
    let schema = get_schema(schema_name).await?;

    // Execute the procedure
    let results = proc_fn(&schema)?;

    // TODO: Apply YIELD clause filtering if present in Phase 3

    Ok(results)
}

/// Execute a standalone procedure call
///
/// # Arguments
/// * `call` - The parsed procedure call AST
/// * `schema_name` - Schema name from HTTP request parameter (defaults to "default")
/// * `registry` - Procedure registry containing available procedures
///
/// # Returns
/// * `Ok(records)` - Vector of record maps (field name -> value)
/// * `Err(message)` - Error message if procedure fails or doesn't exist
pub async fn execute_procedure(
    call: &StandaloneProcedureCall<'_>,
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    execute_procedure_by_name(call.procedure_name, schema_name, registry).await
}

/// Get a schema by name from the global registry
async fn get_schema(schema_name: &str) -> Result<GraphSchema, String> {
    let schemas_guard = GLOBAL_SCHEMAS
        .get()
        .ok_or_else(|| "Schema registry not initialized".to_string())?;

    let schemas = schemas_guard.read().await;

    schemas
        .get(schema_name)
        .cloned()
        .ok_or_else(|| format!("Schema not found: {}", schema_name))
}

/// Check if a parsed statement is a procedure-only UNION query
///
/// Returns true if:
/// - Statement is a Query with union_clauses
/// - Main query has call_clause (no MATCH/WHERE/etc)
/// - All union clauses also have call_clause
/// - All unions are UNION ALL (not DISTINCT)
pub fn is_procedure_union_query(stmt: &CypherStatement<'_>) -> bool {
    log::debug!(
        "is_procedure_union_query called, stmt type: {:?}",
        std::mem::discriminant(stmt)
    );
    match stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            log::debug!("is_procedure_union_query: It's a Query variant");
            // Must have at least one union
            if union_clauses.is_empty() {
                log::debug!("is_procedure_union_query: No union clauses");
                return false;
            }

            // Main query must be a CALL with optional YIELD/RETURN
            let main_is_call = query.call_clause.is_some()
                && query.match_clauses.is_empty()
                && query.optional_match_clauses.is_empty()
                && query.reading_clauses.is_empty()
                && query.where_clause.is_none()
                && query.with_clause.is_none();

            log::debug!(
                "is_procedure_union_query: union_clauses={}, call_clause={:?}, main_is_call={}",
                union_clauses.len(),
                query.call_clause.is_some(),
                main_is_call
            );

            if !main_is_call {
                return false;
            }

            // All unions must also be CALLs with UNION ALL
            union_clauses.iter().all(|union_clause| {
                // Must be UNION ALL
                matches!(
                    union_clause.union_type,
                    crate::open_cypher_parser::ast::UnionType::All
                ) && union_clause.query.call_clause.is_some()
                    && union_clause.query.match_clauses.is_empty()
                    && union_clause.query.optional_match_clauses.is_empty()
                    && union_clause.query.reading_clauses.is_empty()
                    && union_clause.query.where_clause.is_none()
                    && union_clause.query.with_clause.is_none()
            })
        }
        CypherStatement::ProcedureCall(_) => {
            log::debug!("is_procedure_union_query: It's a ProcedureCall variant, returning false");
            false
        }
    }
}

/// Extract procedure names from a UNION query
///
/// # Arguments
/// * `query` - The Cypher UNION query string
///
/// # Returns
/// * `Ok(Vec<String>)` - List of procedure names
/// * `Err(String)` - Parse error
pub fn extract_procedure_names_from_union(query: &str) -> Result<Vec<String>, String> {
    let (_, stmt) = open_cypher_parser::parse_cypher_statement(query)
        .map_err(|e| format!("Parse error: {}", e))?;

    let proc_names = match stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            let mut names = Vec::new();

            // Main query
            if let Some(call_clause) = &query.call_clause {
                names.push(call_clause.procedure_name.to_string());
            }

            // Union clauses
            for union_clause in union_clauses {
                if let Some(call_clause) = &union_clause.query.call_clause {
                    names.push(call_clause.procedure_name.to_string());
                }
            }

            names
        }
        _ => return Err("Not a query statement".to_string()),
    };

    Ok(proc_names)
}

/// Execute a UNION ALL of procedure calls
///
/// Executes each procedure in the union and combines results.
/// Note: This currently only handles CALL ... YIELD ... RETURN patterns.
/// Full RETURN expression evaluation (COLLECT, array slicing) is simplified.
///
/// # Arguments
/// * `proc_names` - List of procedure names to execute
/// * `schema_name` - Schema name to use for all procedure calls
/// * `registry` - Procedure registry
///
/// # Returns
/// * `Ok(records)` - Combined results from all procedures
/// * `Err(message)` - Error if any procedure fails
pub async fn execute_procedure_union(
    proc_names: Vec<String>,
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    // Execute all procedures and combine results
    let mut all_results = Vec::new();
    for proc_name in proc_names {
        let results = execute_procedure_by_name(&proc_name, schema_name, registry).await?;
        all_results.extend(results);
    }

    Ok(all_results)
}

/// Execute a procedure-only query with RETURN clause evaluation
///
/// This handles queries like: `CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)} AS result`
///
/// # Arguments
/// * `query` - The Query AST (must be procedure-only, checked by caller)
/// * `schema_name` - Schema name to use
/// * `registry` - Procedure registry
///
/// # Returns
/// * Transformed results with RETURN clause applied
pub async fn execute_procedure_query(
    query: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>,
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    // Extract procedure name from call_clause
    let call_clause = query
        .call_clause
        .as_ref()
        .ok_or_else(|| "No call clause in procedure query".to_string())?;

    let proc_name = call_clause.procedure_name;

    // Execute the procedure to get raw results
    let raw_results = execute_procedure_by_name(proc_name, schema_name, registry).await?;

    // If there's a RETURN clause, apply transformations
    if let Some(return_clause) = &query.return_clause {
        crate::procedures::return_evaluator::apply_return_clause(raw_results, return_clause)
    } else {
        // No RETURN clause - return raw results
        Ok(raw_results)
    }
}

/// Execute a UNION of procedure-only queries with RETURN clause evaluation
///
/// This handles queries like the Browser's schema query:
/// ```cypher
/// CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)} AS result
/// UNION ALL
/// CALL db.relationshipTypes() YIELD relationshipType RETURN {name:'relationshipTypes', data:COLLECT(relationshipType)} AS result
/// ```
///
/// # Arguments
/// * `main_query` - The main query
/// * `union_clauses` - UNION clauses
/// * `schema_name` - Schema name to use
/// * `registry` - Procedure registry
///
/// # Returns
/// * Combined and transformed results
pub async fn execute_procedure_union_with_return(
    main_query: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>,
    union_clauses: &[crate::open_cypher_parser::ast::UnionClause<'_>],
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    let mut all_results = Vec::new();

    // Execute main query
    let main_results = execute_procedure_query(main_query, schema_name, registry).await?;
    all_results.extend(main_results);

    // Execute each union branch
    for union_clause in union_clauses {
        let branch_results =
            execute_procedure_query(&union_clause.query, schema_name, registry).await?;
        all_results.extend(branch_results);
    }

    Ok(all_results)
}

/// Check if a query (non-UNION) is procedure-only
///
/// Returns true if the query:
/// - Has a call_clause
/// - Has NO match_clauses, optional_match_clauses, or reading_clauses
/// - Has NO create/set/delete/remove clauses
/// - May have RETURN, WITH, WHERE, UNWIND (these project/filter procedure results)
///
/// This is used to determine if a Query AST should be executed as a procedure
/// rather than going through SQL generation.
pub fn is_procedure_only_query(
    query: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>,
) -> bool {
    query.call_clause.is_some()
        && query.match_clauses.is_empty()
        && query.optional_match_clauses.is_empty()
        && query.reading_clauses.is_empty()
        && query.create_clause.is_none()
        && query.set_clause.is_none()
        && query.delete_clause.is_none()
        && query.remove_clause.is_none()
}

/// Check if a statement is procedure-only
///
/// Returns true if:
/// - Statement is ProcedureCall (always procedure-only), OR
/// - Statement is Query with procedure-only main query (and all UNION branches if present)
///
/// This is the main function to use in handlers to determine execution routing:
/// - `true` → execute as procedure
/// - `false` → execute as SQL query
pub fn is_procedure_only_statement(stmt: &CypherStatement<'_>) -> bool {
    match stmt {
        // Standalone procedure calls are always procedure-only
        CypherStatement::ProcedureCall(_) => true,

        // Query statements need deeper inspection
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            // Check main query
            if !is_procedure_only_query(query) {
                return false;
            }

            // If has UNION clauses, check all branches
            if !union_clauses.is_empty() {
                return union_clauses
                    .iter()
                    .all(|union_clause| is_procedure_only_query(&union_clause.query));
            }

            // Single procedure-only query
            true
        }
    }
}

/// Format procedure results as JSON (for HTTP API)
pub fn format_as_json(results: Vec<HashMap<String, serde_json::Value>>) -> serde_json::Value {
    serde_json::json!({
        "records": results,
        "count": results.len()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_as_json() {
        let results = vec![
            HashMap::from([
                ("name".to_string(), serde_json::json!("Alice")),
                ("age".to_string(), serde_json::json!(30)),
            ]),
            HashMap::from([
                ("name".to_string(), serde_json::json!("Bob")),
                ("age".to_string(), serde_json::json!(25)),
            ]),
        ];

        let json = format_as_json(results);
        assert_eq!(json["count"], 2);
        assert_eq!(json["records"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_is_procedure_only_statement_standalone_call() {
        // CALL db.labels()
        let query = "CALL db.labels()";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_only_statement(&stmt),
            "Standalone CALL should be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_call_with_yield() {
        // CALL db.labels() YIELD label
        let query = "CALL db.labels() YIELD label";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_only_statement(&stmt),
            "CALL with YIELD should be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_call_with_return() {
        // CALL db.labels() YIELD label RETURN label
        let query = "CALL db.labels() YIELD label RETURN label";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_only_statement(&stmt),
            "CALL with RETURN should be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_call_with_complex_return() {
        // CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)} AS result
        let query = "CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)[..1000]} AS result";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_only_statement(&stmt),
            "CALL with complex RETURN should be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_union_of_calls() {
        // CALL db.labels() YIELD label RETURN label UNION ALL CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType
        let query = r#"
            CALL db.labels() YIELD label RETURN label
            UNION ALL
            CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType
        "#;
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_only_statement(&stmt),
            "UNION of CALLs should be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_match_query() {
        // MATCH (n) RETURN n
        let query = "MATCH (n:User) RETURN n";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            !is_procedure_only_statement(&stmt),
            "MATCH query should NOT be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_only_statement_mixed_match_and_call() {
        // MATCH (n) CALL db.labels() YIELD label RETURN n, label
        // Note: This might fail to parse currently, but if it does parse, should NOT be procedure-only
        let query = "MATCH (n:User) WITH n CALL db.labels() YIELD label RETURN n.name, label";
        match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => {
                assert!(
                    !is_procedure_only_statement(&stmt),
                    "Mixed MATCH+CALL should NOT be procedure-only"
                );
            }
            Err(_) => {
                // Expected - mixed queries might not parse yet
                // Skip this test
            }
        }
    }

    #[test]
    fn test_is_procedure_only_query_with_create() {
        // CREATE (n:User) RETURN n - has no CALL, should be false
        let query = "CREATE (n:User) RETURN n";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            !is_procedure_only_statement(&stmt),
            "CREATE query should NOT be procedure-only"
        );
    }

    #[test]
    fn test_is_procedure_union_query_detection() {
        // Test the existing is_procedure_union_query function
        let query = r#"
            CALL db.labels() YIELD label RETURN label
            UNION ALL
            CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType
        "#;
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            is_procedure_union_query(&stmt),
            "UNION of CALLs should be detected"
        );
    }

    #[test]
    fn test_is_procedure_union_query_non_union() {
        // Single CALL should return false (not a UNION)
        let query = "CALL db.labels() YIELD label RETURN label";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        assert!(
            !is_procedure_union_query(&stmt),
            "Single CALL should not be detected as UNION"
        );
    }
}
