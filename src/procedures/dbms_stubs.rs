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
                (
                    "signature".to_string(),
                    serde_json::json!(format!("{}()", name)),
                ),
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

/// dbms.info() - Returns minimal DBMS info row
///
/// Neo4j Browser polls this on startup to render the connection-info pane.
/// Mirrors the Neo4j 5.x record schema (id, name, creationDate, storeId,
/// kernelVersion, kernelStartTime, edition, ...). Empty/zero values are fine
/// for fields we don't track — Browser tolerates them.
pub fn info(_schema: &GraphSchema) -> ProcedureResult {
    let compat = std::env::var("CLICKGRAPH_NEO4J_COMPAT_MODE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let (name, version) = if compat {
        ("Neo4j Kernel".to_string(), "5.8.0".to_string())
    } else {
        (
            "ClickGraph".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        )
    };
    Ok(vec![HashMap::from([
        ("id".to_string(), serde_json::json!("clickgraph")),
        ("name".to_string(), serde_json::json!(name)),
        ("creationDate".to_string(), serde_json::json!("")),
        ("storeId".to_string(), serde_json::json!("")),
        ("kernelVersion".to_string(), serde_json::json!(version)),
        ("kernelStartTime".to_string(), serde_json::json!("")),
        ("edition".to_string(), serde_json::json!("community")),
        ("databaseStatus".to_string(), serde_json::json!("online")),
    ])])
}
