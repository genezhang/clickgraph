//! Schema validation module for graph views on ClickHouse tables.
//! 
//! This module provides functionality to validate graph view definitions against
//! actual ClickHouse table schemas. It ensures that:
//! 
//! - Referenced tables exist in ClickHouse
//! - Required columns exist with correct data types
//! - ID columns have compatible types for graph operations
//! 
//! # Example
//! 
//! ```rust,no_run
//! use brahmand::graph_catalog::{SchemaValidator, NodeViewMapping};
//! use clickhouse::Client;
//! 
//! async fn validate_mapping(client: Client) {
//!     let mut validator = SchemaValidator::new(client);
//!     
//!     let mapping = NodeViewMapping {
//!         source_table: "users".to_string(),
//!         id_column: "user_id".to_string(),
//!         property_mappings: [
//!             ("name".to_string(), "full_name".to_string())
//!         ].into_iter().collect(),
//!         label: "User".to_string(),
//!         filter_condition: None,
//!     };
//!     
//!     // Validates table existence and column compatibility
//!     validator.validate_node_mapping(&mapping).await.unwrap();
//! }
//! ```
//! 
//! # Schema Caching
//! 
//! The validator caches table schemas to minimize database queries. Cache is
//! maintained per validator instance and is cleared when the instance is dropped.

use std::collections::HashMap;
use clickhouse::Client;


use super::errors::GraphSchemaError;
use super::graph_schema::{NodeViewMapping, RelationshipViewMapping};

#[cfg(test)]
mod tests;

/// Represents a ClickHouse column definition
use super::column_info::ColumnInfo;

/// Service for validating graph view mappings against ClickHouse schema
pub struct SchemaValidator {
    client: Client,
    // Cache column info to avoid repeated queries
    column_cache: HashMap<String, Vec<ColumnInfo>>,
}

impl SchemaValidator {
    pub fn new(client: Client) -> Self {
        SchemaValidator {
            client,
            column_cache: HashMap::new(),
        }
    }

    /// Validate a node view mapping against ClickHouse schema
    pub async fn validate_node_mapping(&mut self, mapping: &NodeViewMapping) -> Result<(), GraphSchemaError> {
        // Check if table exists and get columns
        let columns = self.get_table_columns(&mapping.source_table).await?;

        // Validate ID column exists and has a valid type
        self.validate_id_column(&mapping.source_table, &mapping.id_column, &columns)?;

        // Validate all mapped property columns exist
        for column_name in mapping.property_mappings.values() {
            self.validate_column_exists(&mapping.source_table, column_name, &columns)?;
        }

        Ok(())
    }

    /// Validate a relationship view mapping against ClickHouse schema
    pub async fn validate_relationship_mapping(
        &mut self,
        mapping: &RelationshipViewMapping,
    ) -> Result<(), GraphSchemaError> {
        // Check if table exists and get columns
        let columns = self.get_table_columns(&mapping.source_table).await?;

        // Validate source and target ID columns exist
        self.validate_column_exists(&mapping.source_table, &mapping.from_column, &columns)?;
        self.validate_column_exists(&mapping.source_table, &mapping.to_column, &columns)?;

        // Validate all mapped property columns exist
        for column_name in mapping.property_mappings.values() {
            self.validate_column_exists(&mapping.source_table, column_name, &columns)?;
        }

        Ok(())
    }

    /// Get column information for a table, using cache if available
    async fn get_table_columns(&mut self, table: &str) -> Result<Vec<ColumnInfo>, GraphSchemaError> {
        if let Some(columns) = self.column_cache.get(table) {
            return Ok(columns.clone());
        }

        // Query ClickHouse for table schema
        let query = format!(
            "SELECT name as name, type as data_type FROM system.columns WHERE table = '{}'",
            table
        );

        let rows = self.client
            .query(&query)
            .fetch_all::<(String, String)>()
            .await
            .map_err(|_e| GraphSchemaError::InvalidSourceTable {
                table: table.to_string(),
            })?;

        let columns = rows.into_iter()
            .map(|(name, data_type)| ColumnInfo::new(name, data_type))
            .collect::<Vec<_>>();

        // Cache the result
        self.column_cache.insert(table.to_string(), columns.clone());
        
        Ok(columns)
    }

    /// Validate that a column exists in the table schema
    fn validate_column_exists(
        &self,
        table: &str,
        column: &str,
        columns: &[ColumnInfo],
    ) -> Result<(), GraphSchemaError> {
        if !columns.iter().any(|c| c.name == column) {
            return Err(GraphSchemaError::InvalidColumn {
                column: column.to_string(),
                table: table.to_string(),
            });
        }
        Ok(())
    }

    /// Validate ID column exists and has a valid type for node IDs
    fn validate_id_column(
        &self,
        table: &str,
        column: &str,
        columns: &[ColumnInfo],
    ) -> Result<(), GraphSchemaError> {
        // Find the column info
        let col_info = columns
            .iter()
            .find(|c| c.name == column)
            .ok_or_else(|| GraphSchemaError::InvalidColumn {
                column: column.to_string(),
                table: table.to_string(),
            })?;

        // Check if type is valid for an ID column (integers, UUID)
        let valid_id_types = ["UInt64", "UInt32", "Int64", "Int32", "UUID"];
        if !valid_id_types.contains(&col_info.data_type.as_str()) {
            return Err(GraphSchemaError::InvalidIdColumnType {
                column: column.to_string(),
                table: table.to_string(),
            });
        }

        Ok(())
    }
}
