//! Procedure executor - handles routing and execution of procedure calls.

use super::{ProcedureRegistry, ProcedureResult};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser;
use crate::open_cypher_parser::ast::{CypherStatement, StandaloneProcedureCall};
use std::collections::HashMap;

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

    // Neo4j reserves the `system` database for administrative state; it holds no
    // user graph. Neo4j Browser routes housekeeping/introspection CALLs (id()
    // resolution, db.* metadata) to it, which arrive here with schema_name
    // "system". There is no such user schema, so get_schema() would fail with
    // "Schema not found: system" and surface as a FAILURE popup in Browser.
    // Match Neo4j semantics instead: return an empty result for the reserved
    // system database.
    if schema_name.eq_ignore_ascii_case("system") {
        log::debug!(
            "Procedure '{}' targeted the reserved 'system' database — returning empty result",
            procedure_name
        );
        return Ok(Vec::new());
    }

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

/// Get a schema by name — uses task-local context first, falls back to GLOBAL_SCHEMAS
async fn get_schema(schema_name: &str) -> Result<GraphSchema, String> {
    // Try task-local context first
    if let Some(schema) = crate::server::query_context::get_current_schema() {
        return Ok(schema.as_ref().clone());
    }

    // Fallback to GLOBAL_SCHEMAS for backward compatibility
    let schemas_guard = crate::server::GLOBAL_SCHEMAS
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
        CypherStatement::CopyTo(_) => false,
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

            // Main query must be a CALL; what follows it is applied (or
            // refused) by `apply_statement_tail`
            let main_is_call = query.call_clause.is_some()
                && query.match_clauses.is_empty()
                && query.optional_match_clauses.is_empty()
                && query.reading_clauses.is_empty();

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

    apply_query_tail(raw_results, query)
}

/// Refuse what follows the YIELD of a procedure answered by its own SQL
/// (vector / fulltext search, export): that SQL is built from the call's
/// arguments alone, so a WHERE, RETURN or UNION after it would be dropped
/// (#1411).
pub fn sql_procedure_tail_refused(stmt: &CypherStatement<'_>) -> Result<(), String> {
    let CypherStatement::Query {
        query,
        union_clauses,
    } = stmt
    else {
        return Ok(());
    };
    let clauses = [
        (query.where_clause.is_some(), "WHERE"),
        (query.with_clause.is_some(), "WITH"),
        (!query.unwind_clauses.is_empty(), "UNWIND"),
        (query.return_clause.is_some(), "RETURN"),
        (query.order_by_clause.is_some(), "ORDER BY"),
        (query.skip_clause.is_some(), "SKIP"),
        (query.limit_clause.is_some(), "LIMIT"),
        (!union_clauses.is_empty(), "UNION"),
    ];
    match clauses.iter().find(|(present, _)| *present) {
        Some((_, clause)) => {
            let name = query
                .call_clause
                .as_ref()
                .map_or("the procedure", |c| c.procedure_name);
            Err(format!(
                "Not supported: {clause} after {name} (its rows are returned as the CALL yields them)"
            ))
        }
        None => Ok(()),
    }
}

/// Apply the tail of branch `branch` (0: the first query, then each UNION ALL
/// arm) of the procedure-only `statement` to the rows its procedure returned.
/// A standalone procedure call has none.
pub fn apply_statement_tail(
    rows: Vec<HashMap<String, serde_json::Value>>,
    statement: &str,
    branch: usize,
) -> ProcedureResult {
    let (_, stmt) = open_cypher_parser::parse_cypher_statement(statement)
        .map_err(|e| format!("Parse error: {}", e))?;
    match &stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            if union_clauses.iter().any(|u| {
                matches!(
                    u.union_type,
                    crate::open_cypher_parser::ast::UnionType::Distinct
                )
            }) {
                return Err("Not supported: UNION of procedure calls (UNION ALL is)".to_string());
            }
            let query = match branch {
                0 => query,
                n => {
                    &union_clauses
                        .get(n - 1)
                        .ok_or_else(|| format!("No UNION branch {n}"))?
                        .query
                }
            };
            apply_query_tail(rows, query)
        }
        CypherStatement::ProcedureCall(_) | CypherStatement::CopyTo(_) => Ok(rows),
    }
}

/// Apply what follows `CALL … YIELD` in a procedure-only query to the
/// procedure's rows: the YIELD's columns, the WHERE, the RETURN, ORDER BY,
/// SKIP and LIMIT. A clause this does not evaluate is refused, never dropped
/// (#1411).
pub fn apply_query_tail(
    rows: Vec<HashMap<String, serde_json::Value>>,
    query: &crate::open_cypher_parser::ast::OpenCypherQueryAst<'_>,
) -> ProcedureResult {
    let unevaluated = [
        (query.with_clause.is_some(), "WITH"),
        (!query.unwind_clauses.is_empty(), "UNWIND"),
        (
            query.return_clause.as_ref().is_some_and(|r| r.distinct),
            "RETURN DISTINCT",
        ),
    ];
    if let Some((_, clause)) = unevaluated.iter().find(|(present, _)| *present) {
        return Err(format!("Not supported: {clause} after a procedure call"));
    }

    let mut rows = rows;
    if let Some(columns) = query
        .call_clause
        .as_ref()
        .and_then(|c| c.yield_items.as_ref())
    {
        // A column no row has is not one of the procedure's; one some rows
        // lack is NULL there.
        if let Some(unknown) = columns
            .iter()
            .find(|c| !rows.is_empty() && rows.iter().all(|row| !row.contains_key(**c)))
        {
            return Err(format!("Unknown procedure output: `{unknown}`"));
        }
        rows = rows
            .into_iter()
            .map(|mut row| {
                columns
                    .iter()
                    .map(|c| {
                        let value = row.remove(*c).unwrap_or(serde_json::Value::Null);
                        (c.to_string(), value)
                    })
                    .collect()
            })
            .collect();
    }
    if let Some(where_clause) = &query.where_clause {
        rows = crate::procedures::return_evaluator::apply_where(rows, &where_clause.conditions)?;
    }
    // The records a RETURN returned each row from, for ORDER BY
    let mut inputs = None;
    if let Some(return_clause) = &query.return_clause {
        if !crate::procedures::return_evaluator::return_aggregates(return_clause) {
            inputs = Some(rows.clone());
        }
        rows = crate::procedures::return_evaluator::apply_return_clause(rows, return_clause)?;
    }
    if let Some(order_by) = &query.order_by_clause {
        rows = crate::procedures::return_evaluator::apply_order_by(rows, inputs, order_by)?;
    }
    let count = |n: i64, clause: &str| {
        usize::try_from(n).map_err(|_| format!("{clause} must be a non-negative integer, got {n}"))
    };
    if let Some(skip) = &query.skip_clause {
        let skip = count(skip.skip_item, "SKIP")?;
        rows = rows.into_iter().skip(skip).collect();
    }
    if let Some(limit) = &query.limit_clause {
        rows.truncate(count(limit.limit_item, "LIMIT")?);
    }
    Ok(rows)
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

        // COPY TO is not a procedure call
        CypherStatement::CopyTo(_) => false,

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

    #[tokio::test]
    async fn procedure_against_reserved_system_database_returns_empty() {
        // Neo4j Browser routes housekeeping/introspection CALLs to the reserved
        // `system` database. There is no user schema named "system", so this must
        // return an empty result rather than a "Schema not found: system" error.
        let registry = crate::procedures::ProcedureRegistry::new();
        for db in ["system", "System", "SYSTEM"] {
            let result = execute_procedure_by_name("db.labels", db, &registry).await;
            assert!(
                matches!(result, Ok(ref rows) if rows.is_empty()),
                "procedure against reserved '{db}' database must return Ok(empty), got {result:?}"
            );
        }
    }

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

    fn rows(column: &str, values: &[serde_json::Value]) -> Vec<HashMap<String, serde_json::Value>> {
        values
            .iter()
            .map(|v| HashMap::from([(column.to_string(), v.clone())]))
            .collect()
    }

    fn labels() -> Vec<HashMap<String, serde_json::Value>> {
        rows(
            "label",
            &[serde_json::json!("Post"), serde_json::json!("User")],
        )
    }

    fn tail(rows: Vec<HashMap<String, serde_json::Value>>, q: &str) -> ProcedureResult {
        apply_statement_tail(rows, q, 0)
    }

    fn column(result: ProcedureResult, c: &str) -> Vec<serde_json::Value> {
        result.unwrap().iter().map(|r| r[c].clone()).collect()
    }

    #[test]
    fn a_where_after_yield_filters_the_rows() {
        let q = "CALL db.labels() YIELD label WHERE label = 'User' RETURN label";
        assert_eq!(
            column(tail(labels(), q), "label"),
            [serde_json::json!("User")]
        );
        let q = "CALL db.labels() YIELD label WHERE label STARTS WITH 'P' OR label = 'X'";
        assert_eq!(
            column(tail(labels(), q), "label"),
            [serde_json::json!("Post")]
        );
        let q = "CALL db.labels() YIELD label WHERE NOT label IN ['User', 'X']";
        assert_eq!(
            column(tail(labels(), q), "label"),
            [serde_json::json!("Post")]
        );
    }

    #[test]
    fn a_where_that_is_null_drops_the_row() {
        let input = rows(
            "x",
            &[
                serde_json::json!(1),
                serde_json::Value::Null,
                serde_json::json!(2.0),
            ],
        );
        let kept = |q: &str| column(tail(input.clone(), q), "x");
        let q = |w: &str| format!("CALL p() YIELD x WHERE {w} RETURN x");
        assert_eq!(kept(&q("x = 1")), [serde_json::json!(1)]);
        assert_eq!(kept(&q("x = 2")), [serde_json::json!(2.0)]);
        assert_eq!(kept(&q("NOT x = 1")), [serde_json::json!(2.0)]);
        assert_eq!(kept(&q("x IS NULL")), [serde_json::Value::Null]);
        // NULL IN a list, and a list holding NULL: NULL, never true
        assert_eq!(
            kept(&q("NOT x IN [1, null]")),
            Vec::<serde_json::Value>::new()
        );
        assert_eq!(kept(&q("x < 'a'")), Vec::<serde_json::Value>::new());
        assert_eq!(kept(&q("x > 1 AND x < 3")), [serde_json::json!(2.0)]);
        assert_eq!(kept(&q("x = 1 OR x IS NULL")).len(), 2);
    }

    #[test]
    fn yield_keeps_its_columns() {
        let input = vec![HashMap::from([
            ("a".to_string(), serde_json::json!(1)),
            ("b".to_string(), serde_json::json!(2)),
        ])];
        let out = tail(input.clone(), "CALL p() YIELD a WHERE a = 1").unwrap();
        assert_eq!(
            out,
            [HashMap::from([("a".to_string(), serde_json::json!(1))])]
        );
        let err = tail(input, "CALL p() YIELD c RETURN c").unwrap_err();
        assert!(err.contains("Unknown procedure output"), "{err}");
    }

    #[test]
    fn clauses_not_evaluated_are_refused() {
        for (q, clause) in [
            ("CALL db.labels() YIELD label WITH label RETURN label", "WITH"),
            ("CALL db.labels() YIELD label RETURN DISTINCT label", "RETURN DISTINCT"),
            ("CALL db.labels() YIELD label RETURN label, count(*)", "grouping key"),
            ("CALL db.labels() YIELD label RETURN label UNION CALL db.labels() YIELD label RETURN label", "UNION"),
        ] {
            let err = tail(labels(), q).unwrap_err();
            assert!(err.starts_with("Not supported") && err.contains(clause), "{q}: {err}");
        }
    }

    #[test]
    fn order_by_skip_and_limit_apply_in_turn() {
        let input = rows(
            "x",
            &[
                serde_json::json!(2),
                serde_json::Value::Null,
                serde_json::json!("b"),
                serde_json::json!(1.5),
                serde_json::json!(true),
            ],
        );
        let sorted = |q: &str| column(tail(input.clone(), q), "x");
        assert_eq!(
            sorted("CALL p() YIELD x RETURN x ORDER BY x"),
            [
                serde_json::json!("b"),
                serde_json::json!(true),
                serde_json::json!(1.5),
                serde_json::json!(2),
                serde_json::Value::Null
            ]
        );
        assert_eq!(
            sorted("CALL p() YIELD x RETURN x ORDER BY x DESC SKIP 1 LIMIT 2"),
            [serde_json::json!(2), serde_json::json!(1.5)]
        );
        // A variable from before a RETURN that does not aggregate
        let q = "CALL db.labels() YIELD label RETURN label AS l ORDER BY label DESC";
        assert_eq!(
            column(tail(labels(), q), "l"),
            [serde_json::json!("User"), serde_json::json!("Post")]
        );
        let q = "CALL db.labels() YIELD label RETURN count(*) AS n ORDER BY label";
        let err = tail(labels(), q).unwrap_err();
        assert!(err.contains("Variable not found"), "{err}");
    }

    #[test]
    fn each_union_all_branch_has_its_own_tail() {
        let q = "CALL db.labels() YIELD label WHERE label = 'User' RETURN label \
                 UNION ALL CALL db.labels() YIELD label WHERE label = 'Post' RETURN label";
        assert!(is_procedure_union_query(&parse(q)));
        assert_eq!(
            column(apply_statement_tail(labels(), q, 0), "label"),
            [serde_json::json!("User")]
        );
        assert_eq!(
            column(apply_statement_tail(labels(), q, 1), "label"),
            [serde_json::json!("Post")]
        );
    }

    #[test]
    fn aggregates_skip_nulls() {
        let input = rows("x", &[serde_json::json!(1), serde_json::Value::Null]);
        let q = "CALL p() YIELD x RETURN count(x) AS c, count(*) AS n, collect(x) AS l";
        let out = tail(input, q).unwrap();
        assert_eq!(out[0]["c"], serde_json::json!(1));
        assert_eq!(out[0]["n"], serde_json::json!(2));
        assert_eq!(out[0]["l"], serde_json::json!([1]));
    }

    fn parse(q: &str) -> CypherStatement<'_> {
        open_cypher_parser::parse_cypher_statement(q).unwrap().1
    }

    #[test]
    fn a_clause_after_a_sql_procedure_is_refused() {
        let call = "CALL db.index.vector.queryNodes('idx', 5, [1.0, 2.0]) YIELD node, score";
        assert!(sql_procedure_tail_refused(&parse(call)).is_ok());
        for (rest, clause) in [(" WHERE score > 0.5", "WHERE"), (" RETURN node", "RETURN")] {
            let q = format!("{call}{rest}");
            let err = sql_procedure_tail_refused(&parse(&q)).unwrap_err();
            assert!(
                err.starts_with("Not supported") && err.contains(clause),
                "{err}"
            );
        }
    }
}
