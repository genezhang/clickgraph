//! Procedure executor - handles routing and execution of procedure calls.

use super::{ProcedureRegistry, ProcedureResult};
use crate::graph_catalog::graph_schema::GraphSchema;
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
    match stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            // Must have at least one union
            if union_clauses.is_empty() {
                return false;
            }

            // Main query must be a CALL with optional YIELD/RETURN
            let main_is_call = query.call_clause.is_some()
                && query.match_clauses.is_empty()
                && query.optional_match_clauses.is_empty()
                && query.reading_clauses.is_empty()
                && query.where_clause.is_none()
                && query.with_clause.is_none();

            if !main_is_call {
                return false;
            }

            // All unions must also be CALLs with UNION ALL
            union_clauses.iter().all(|union_clause| {
                // Must be UNION ALL
                matches!(union_clause.union_type, crate::open_cypher_parser::ast::UnionType::All)
                    && union_clause.query.call_clause.is_some()
                    && union_clause.query.match_clauses.is_empty()
                    && union_clause.query.optional_match_clauses.is_empty()
                    && union_clause.query.reading_clauses.is_empty()
                    && union_clause.query.where_clause.is_none()
                    && union_clause.query.with_clause.is_none()
            })
        }
        CypherStatement::ProcedureCall(_) => false, // Standalone call, not a union
    }
}

/// Execute a UNION ALL of procedure calls
///
/// Executes each procedure in the union and combines results.
/// Note: This currently only handles CALL ... YIELD ... RETURN patterns.
/// Full RETURN expression evaluation (COLLECT, array slicing) is simplified.
///
/// # Arguments
/// * `stmt` - The parsed UNION statement (must pass is_procedure_union_query check)
/// * `schema_name` - Schema name to use for all procedure calls
/// * `registry` - Procedure registry
///
/// # Returns
/// * `Ok(records)` - Combined results from all procedures
/// * `Err(message)` - Error if any procedure fails
pub async fn execute_procedure_union<'a>(
    stmt: &CypherStatement<'a>,
    schema_name: &str,
    registry: &ProcedureRegistry,
) -> ProcedureResult {
    // Extract procedure names first (before any awaits)
    let proc_names: Vec<String> = match stmt {
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

    // Now execute all procedures (can await safely)
    let mut all_results = Vec::new();
    for proc_name in proc_names {
        let results = execute_procedure_by_name(&proc_name, schema_name, registry).await?;
        all_results.extend(results);
    }

    Ok(all_results)
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
}
