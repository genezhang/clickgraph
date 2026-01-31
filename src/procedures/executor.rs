//! Procedure executor - handles routing and execution of procedure calls.

use super::{ProcedureRegistry, ProcedureResult};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast::StandaloneProcedureCall;
use crate::server::GLOBAL_SCHEMAS;
use std::collections::HashMap;
use std::sync::Arc;

/// Execute a standalone procedure call by name
///
/// # Arguments
/// * `procedure_name` - Name of the procedure to execute
/// * `schema_name` - Schema to execute against (from USE clause or connection)
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
/// * `schema_name` - Schema to execute against (from USE clause or connection)
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
