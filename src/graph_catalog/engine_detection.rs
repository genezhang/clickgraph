//! ClickHouse table engine detection for FINAL keyword support
//!
//! This module detects which MergeTree engine family a table uses and determines
//! whether the FINAL keyword should be applied for correct query results.

use clickhouse::Client;
use log::{debug, info, warn};
use thiserror::Error;

/// Errors that can occur during engine detection
#[derive(Debug, Error)]
pub enum EngineDetectionError {
    #[error("Failed to query system.tables for {database}.{table}: {source}")]
    QueryError {
        database: String,
        table: String,
        source: clickhouse::error::Error,
    },
    #[error("Failed to parse engine specification: {message}")]
    ParseError { message: String },
    #[error("Failed to verify FINAL support for {database}.{table}: {source}")]
    VerificationError {
        database: String,
        table: String,
        source: clickhouse::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, EngineDetectionError>;

/// Represents different ClickHouse table engine types
#[derive(Debug, Clone, PartialEq)]
pub enum TableEngine {
    /// Regular MergeTree (no FINAL needed)
    MergeTree,

    /// ReplacingMergeTree - deduplicates rows with same sorting key
    ReplacingMergeTree { version_column: Option<String> },

    /// CollapsingMergeTree - collapses rows using sign column (-1/+1)
    CollapsingMergeTree { sign_column: String },

    /// VersionedCollapsingMergeTree - like Collapsing but with version ordering
    VersionedCollapsingMergeTree {
        sign_column: String,
        version_column: String,
    },

    /// CoalescingMergeTree - newer variant (needs verification)
    CoalescingMergeTree,

    /// AggregatingMergeTree - finalizes pre-aggregated state
    AggregatingMergeTree,

    /// SummingMergeTree - sums numeric columns
    SummingMergeTree { sum_columns: Vec<String> },

    /// Unknown or unsupported engine
    Other(String),
}

impl TableEngine {
    /// Returns true if this engine supports the FINAL keyword
    ///
    /// Note: This is based on known engine types. For unknown engines,
    /// we verify dynamically using `verify_final_support()`.
    pub fn supports_final(&self) -> bool {
        matches!(
            self,
            TableEngine::ReplacingMergeTree { .. }
                | TableEngine::CollapsingMergeTree { .. }
                | TableEngine::VersionedCollapsingMergeTree { .. }
                | TableEngine::CoalescingMergeTree
                | TableEngine::AggregatingMergeTree
                | TableEngine::SummingMergeTree { .. }
        )
    }

    /// Returns true only for engines that require FINAL for correctness
    ///
    /// This is conservative - only returns true for deduplication/collapsing engines
    /// where FINAL is needed to get correct results (not just optimization).
    pub fn requires_final_for_correctness(&self) -> bool {
        matches!(
            self,
            TableEngine::ReplacingMergeTree { .. }
                | TableEngine::CollapsingMergeTree { .. }
                | TableEngine::VersionedCollapsingMergeTree { .. }
                | TableEngine::CoalescingMergeTree
        )
    }

    /// Get engine name for logging/debugging
    pub fn name(&self) -> &str {
        match self {
            TableEngine::MergeTree => "MergeTree",
            TableEngine::ReplacingMergeTree { .. } => "ReplacingMergeTree",
            TableEngine::CollapsingMergeTree { .. } => "CollapsingMergeTree",
            TableEngine::VersionedCollapsingMergeTree { .. } => "VersionedCollapsingMergeTree",
            TableEngine::CoalescingMergeTree => "CoalescingMergeTree",
            TableEngine::AggregatingMergeTree => "AggregatingMergeTree",
            TableEngine::SummingMergeTree { .. } => "SummingMergeTree",
            TableEngine::Other(name) => name,
        }
    }
}

/// Detects the engine type for a given table
///
/// # Arguments
/// * `client` - ClickHouse client
/// * `database` - Database name
/// * `table` - Table name
///
/// # Returns
/// The detected `TableEngine` enum variant
///
/// # Example
/// ```no_run
/// let engine = detect_table_engine(&client, "mydb", "users").await?;
/// if engine.requires_final_for_correctness() {
///     println!("This table needs FINAL for correct results");
/// }
/// ```
pub async fn detect_table_engine(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<TableEngine> {
    debug!("Detecting engine for table {}.{}", database, table);

    let query = format!(
        "SELECT engine, engine_full FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, table
    );

    let row: (String, String) =
        client
            .query(&query)
            .fetch_one()
            .await
            .map_err(|e| EngineDetectionError::QueryError {
                database: database.to_string(),
                table: table.to_string(),
                source: e,
            })?;

    let engine_name = row.0;
    let engine_full = row.1;

    debug!(
        "Table {}.{} uses engine: {} (full: {})",
        database, table, engine_name, engine_full
    );

    let engine = parse_engine(&engine_name, &engine_full)?;

    // For unknown engines or new variants, verify FINAL support dynamically
    if matches!(
        &engine,
        TableEngine::Other(_) | TableEngine::CoalescingMergeTree
    ) {
        let supports_final = verify_final_support(client, database, table).await?;
        info!(
            "Table {}.{} engine {:?} FINAL support verified: {}",
            database, table, engine, supports_final
        );
    }

    Ok(engine)
}

/// Parses engine name and full specification into TableEngine enum
fn parse_engine(engine: &str, engine_full: &str) -> Result<TableEngine> {
    match engine {
        "ReplacingMergeTree" => {
            let version_column = extract_version_column(engine_full);
            Ok(TableEngine::ReplacingMergeTree { version_column })
        }
        "CollapsingMergeTree" => {
            let sign_column = extract_sign_column(engine_full)?;
            Ok(TableEngine::CollapsingMergeTree { sign_column })
        }
        "VersionedCollapsingMergeTree" => {
            let sign_column = extract_sign_column(engine_full)?;
            let version_column = extract_version_column(engine_full).ok_or_else(|| {
                EngineDetectionError::ParseError {
                    message: "VersionedCollapsingMergeTree missing version column".to_string(),
                }
            })?;
            Ok(TableEngine::VersionedCollapsingMergeTree {
                sign_column,
                version_column,
            })
        }
        "CoalescingMergeTree" => {
            // Newer engine - will verify existence dynamically
            Ok(TableEngine::CoalescingMergeTree)
        }
        "AggregatingMergeTree" => Ok(TableEngine::AggregatingMergeTree),
        "SummingMergeTree" => {
            let sum_columns = extract_sum_columns(engine_full);
            Ok(TableEngine::SummingMergeTree { sum_columns })
        }
        "MergeTree" => Ok(TableEngine::MergeTree),
        other => {
            // Unknown engine - will verify FINAL support dynamically
            warn!("Unknown engine type: {}", other);
            Ok(TableEngine::Other(other.to_string()))
        }
    }
}

/// Verifies if a table supports FINAL by attempting a test query
///
/// This is used for unknown engines or new variants where we're not sure
/// if FINAL is supported.
async fn verify_final_support(client: &Client, database: &str, table: &str) -> Result<bool> {
    let test_query = format!("SELECT * FROM {}.{} FINAL LIMIT 0", database, table);

    debug!(
        "Verifying FINAL support for {}.{} with query: {}",
        database, table, test_query
    );

    match client.query(&test_query).execute().await {
        Ok(_) => {
            debug!("FINAL verified: supported for {}.{}", database, table);
            Ok(true)
        }
        Err(e) => {
            let err_msg = e.to_string();
            // Check if error is specifically about FINAL not being supported
            if err_msg.contains("FINAL") || err_msg.contains("not support") {
                debug!(
                    "FINAL verified: not supported for {}.{} (error: {})",
                    database, table, err_msg
                );
                Ok(false)
            } else {
                // Other error - propagate it
                Err(EngineDetectionError::VerificationError {
                    database: database.to_string(),
                    table: table.to_string(),
                    source: e,
                })
            }
        }
    }
}

/// Extracts version column from engine_full specification
///
/// Examples:
/// - "ReplacingMergeTree(version)" -> Some("version")
/// - "VersionedCollapsingMergeTree(sign, version)" -> Some("version")
fn extract_version_column(engine_full: &str) -> Option<String> {
    // Look for pattern: EngineName(params)
    let start = engine_full.find('(')?;
    let end = engine_full.find(')')?;
    let content = &engine_full[start + 1..end];

    if content.is_empty() {
        return None;
    }

    // Check if there are multiple parameters (comma-separated)
    if content.contains(',') {
        // For VersionedCollapsingMergeTree(sign, version), take the second parameter
        let parts: Vec<&str> = content.split(',').collect();
        if parts.len() >= 2 {
            Some(parts[1].trim().to_string())
        } else {
            None
        }
    } else {
        // For ReplacingMergeTree(version), it's just the column name
        Some(content.trim().to_string())
    }
}

/// Extracts sign column from engine_full specification
///
/// Example: "CollapsingMergeTree(sign)" -> "sign"
fn extract_sign_column(engine_full: &str) -> Result<String> {
    let start = engine_full
        .find('(')
        .ok_or_else(|| EngineDetectionError::ParseError {
            message: "Missing opening parenthesis in engine_full".to_string(),
        })?;
    let end = engine_full
        .find(')')
        .ok_or_else(|| EngineDetectionError::ParseError {
            message: "Missing closing parenthesis in engine_full".to_string(),
        })?;
    let content = &engine_full[start + 1..end];

    // For CollapsingMergeTree, first parameter is sign column
    let sign_column = content
        .split(',')
        .next()
        .ok_or_else(|| EngineDetectionError::ParseError {
            message: "No sign column found".to_string(),
        })?
        .trim()
        .to_string();

    if sign_column.is_empty() {
        return Err(EngineDetectionError::ParseError {
            message: "Sign column is empty".to_string(),
        });
    }

    Ok(sign_column)
}

/// Extracts sum columns from engine_full specification
///
/// Example: "SummingMergeTree(amount, quantity)" -> vec!["amount", "quantity"]
fn extract_sum_columns(engine_full: &str) -> Vec<String> {
    if let Some(start) = engine_full.find('(') {
        if let Some(end) = engine_full.find(')') {
            let content = &engine_full[start + 1..end];
            return content
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_replacing_merge_tree() {
        let engine = parse_engine("ReplacingMergeTree", "ReplacingMergeTree(version)").unwrap();
        assert!(matches!(
            engine,
            TableEngine::ReplacingMergeTree {
                version_column: Some(ref col)
            } if col == "version"
        ));
        assert!(engine.supports_final());
        assert!(engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_replacing_merge_tree_no_version() {
        let engine = parse_engine("ReplacingMergeTree", "ReplacingMergeTree").unwrap();
        assert!(matches!(
            engine,
            TableEngine::ReplacingMergeTree {
                version_column: None
            }
        ));
        assert!(engine.supports_final());
        assert!(engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_collapsing_merge_tree() {
        let engine = parse_engine("CollapsingMergeTree", "CollapsingMergeTree(sign)").unwrap();
        assert!(matches!(
            engine,
            TableEngine::CollapsingMergeTree {
                sign_column: ref col
            } if col == "sign"
        ));
        assert!(engine.supports_final());
        assert!(engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_versioned_collapsing_merge_tree() {
        let engine = parse_engine(
            "VersionedCollapsingMergeTree",
            "VersionedCollapsingMergeTree(sign, version)",
        )
        .unwrap();
        assert!(matches!(
            engine,
            TableEngine::VersionedCollapsingMergeTree {
                sign_column: ref s,
                version_column: ref v
            } if s == "sign" && v == "version"
        ));
        assert!(engine.supports_final());
        assert!(engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_coalescing_merge_tree() {
        let engine = parse_engine("CoalescingMergeTree", "CoalescingMergeTree").unwrap();
        assert!(matches!(engine, TableEngine::CoalescingMergeTree));
        assert!(engine.supports_final());
        assert!(engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_aggregating_merge_tree() {
        let engine = parse_engine("AggregatingMergeTree", "AggregatingMergeTree").unwrap();
        assert!(matches!(engine, TableEngine::AggregatingMergeTree));
        assert!(engine.supports_final());
        assert!(!engine.requires_final_for_correctness()); // Optional optimization
    }

    #[test]
    fn test_parse_summing_merge_tree() {
        let engine =
            parse_engine("SummingMergeTree", "SummingMergeTree(amount, quantity)").unwrap();
        assert!(matches!(
            engine,
            TableEngine::SummingMergeTree {
                sum_columns: ref cols
            } if cols == &vec!["amount".to_string(), "quantity".to_string()]
        ));
        assert!(engine.supports_final());
        assert!(!engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_regular_merge_tree() {
        let engine = parse_engine("MergeTree", "MergeTree").unwrap();
        assert!(matches!(engine, TableEngine::MergeTree));
        assert!(!engine.supports_final());
        assert!(!engine.requires_final_for_correctness());
    }

    #[test]
    fn test_parse_unknown_engine() {
        let engine = parse_engine("Memory", "Memory").unwrap();
        assert!(matches!(engine, TableEngine::Other(ref name) if name == "Memory"));
        assert!(!engine.supports_final());
    }

    #[test]
    fn test_extract_version_column() {
        assert_eq!(
            extract_version_column("ReplacingMergeTree(version)"),
            Some("version".to_string())
        );
        assert_eq!(extract_version_column("ReplacingMergeTree"), None);
    }

    #[test]
    fn test_extract_sign_column() {
        assert_eq!(
            extract_sign_column("CollapsingMergeTree(sign)").unwrap(),
            "sign"
        );
        assert_eq!(
            extract_sign_column("VersionedCollapsingMergeTree(sign, version)").unwrap(),
            "sign"
        );
    }

    #[test]
    fn test_extract_sum_columns() {
        let cols = extract_sum_columns("SummingMergeTree(amount, quantity)");
        assert_eq!(cols, vec!["amount", "quantity"]);

        let cols = extract_sum_columns("SummingMergeTree");
        assert_eq!(cols, Vec::<String>::new());
    }

    #[test]
    fn test_engine_name() {
        assert_eq!(TableEngine::MergeTree.name(), "MergeTree");
        assert_eq!(
            TableEngine::ReplacingMergeTree {
                version_column: None
            }
            .name(),
            "ReplacingMergeTree"
        );
    }
}
