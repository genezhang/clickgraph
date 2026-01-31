//! Stub implementations for dbms.* procedures that Neo4j Browser requires
//!
//! These return empty or minimal results to prevent Browser from failing.
//! Full implementations can be added later as needed.

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::procedures::ProcedureResult;
use std::collections::HashMap;

/// dbms.clientConfig() - Returns empty config for now
pub fn client_config(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![])
}

/// dbms.security.showCurrentUser() - Returns a stub user
pub fn show_current_user(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![HashMap::from([
        ("username".to_string(), serde_json::json!("test_user")),
        ("roles".to_string(), serde_json::json!(["admin"])),
        ("flags".to_string(), serde_json::json!([])),
    ])])
}

/// dbms.procedures() - Returns list of supported procedures
pub fn list_procedures(_schema: &GraphSchema) -> ProcedureResult {
    let procedures = vec![
        ("db.labels", "List all node labels"),
        ("db.relationshipTypes", "List all relationship types"),
        ("db.propertyKeys", "List all property keys"),
        ("dbms.components", "List DBMS components"),
        ("dbms.clientConfig", "Get client configuration"),
        ("dbms.security.showCurrentUser", "Show current user"),
        ("dbms.procedures", "List procedures"),
        ("dbms.functions", "List functions"),
    ];

    Ok(procedures
        .into_iter()
        .map(|(name, description)| {
            HashMap::from([
                ("name".to_string(), serde_json::json!(name)),
                ("signature".to_string(), serde_json::json!(format!("{}()", name))),
                ("description".to_string(), serde_json::json!(description)),
                ("mode".to_string(), serde_json::json!("READ")),
            ])
        })
        .collect())
}

/// dbms.functions() - Returns empty list (we don't support custom functions yet)
pub fn list_functions(_schema: &GraphSchema) -> ProcedureResult {
    Ok(vec![])
}
