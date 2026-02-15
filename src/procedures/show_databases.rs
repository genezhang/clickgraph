//! SHOW DATABASES procedure
//! Returns a list of available databases (schemas) in a format compatible with Neo4j Browser

use serde_json::{json, Value};
use std::collections::HashMap;

/// Execute SHOW DATABASES command
/// Returns available schemas as database records compatible with Neo4j Browser
pub fn execute_show_databases() -> Result<Vec<HashMap<String, Value>>, String> {
    // Get available schemas from global registry
    let mut databases = Vec::new();

    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        // Use blocking_lock() for sync context
        // Note: This is called from sync procedure context, not async
        let schemas_map = schemas_lock.blocking_read();

        // Add each schema as a database record
        for (name, _) in schemas_map.iter() {
            let mut record = HashMap::new();

            // Standard Neo4j SHOW DATABASES response format
            record.insert("name".to_string(), Value::String(name.clone()));
            record.insert("type".to_string(), Value::String("standard".to_string()));
            record.insert("aliases".to_string(), Value::Array(vec![]));
            record.insert(
                "access".to_string(),
                Value::String("read-write".to_string()),
            );
            record.insert(
                "address".to_string(),
                Value::String("localhost:7687".to_string()),
            );
            record.insert("role".to_string(), Value::String("primary".to_string()));
            record.insert("writer".to_string(), Value::Bool(true));
            record.insert(
                "requestedStatus".to_string(),
                Value::String("online".to_string()),
            );
            record.insert(
                "currentStatus".to_string(),
                Value::String("online".to_string()),
            );
            record.insert("statusMessage".to_string(), Value::String("".to_string()));
            record.insert("default".to_string(), Value::Bool(name == "default"));
            record.insert("home".to_string(), Value::Bool(name == "default"));
            record.insert("constituents".to_string(), Value::Array(vec![]));

            databases.push(record);
        }
    } else {
        // Fallback: return just "default" schema
        let mut record = HashMap::new();
        record.insert("name".to_string(), Value::String("default".to_string()));
        record.insert("type".to_string(), Value::String("standard".to_string()));
        record.insert("aliases".to_string(), Value::Array(vec![]));
        record.insert(
            "access".to_string(),
            Value::String("read-write".to_string()),
        );
        record.insert(
            "address".to_string(),
            Value::String("localhost:7687".to_string()),
        );
        record.insert("role".to_string(), Value::String("primary".to_string()));
        record.insert("writer".to_string(), Value::Bool(true));
        record.insert(
            "requestedStatus".to_string(),
            Value::String("online".to_string()),
        );
        record.insert(
            "currentStatus".to_string(),
            Value::String("online".to_string()),
        );
        record.insert("statusMessage".to_string(), Value::String("".to_string()));
        record.insert("default".to_string(), Value::Bool(true));
        record.insert("home".to_string(), Value::Bool(true));
        record.insert("constituents".to_string(), Value::Array(vec![]));

        databases.push(record);
    }

    Ok(databases)
}
