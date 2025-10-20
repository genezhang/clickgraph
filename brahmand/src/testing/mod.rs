//! Mock implementations for testing
//! 
//! This module provides mock implementations of core components for testing:
//! - MockClickHouseClient: Simulates a ClickHouse database
//! - MockSchemaValidator: For testing view definitions
//! - MockQueryExecutor: For testing query execution

pub mod clickhouse;
pub mod schema;
pub mod query;