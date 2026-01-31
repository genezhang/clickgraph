//! Brahmand - Graph analysis layer for ClickHouse
//!
//! This crate provides graph analysis capabilities on ClickHouse databases through:
//! - Graph view definitions over existing tables
//! - Cypher query support
//! - Query planning and optimization
//! - SQL generation

/// Debug print macro that only compiles in debug builds.
/// In release builds, this expands to nothing, so there's zero runtime cost.
#[macro_export]
macro_rules! debug_print {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        eprintln!($($arg)*);
    };
}

/// Debug print macro for println-style output (only in debug builds)
#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        println!($($arg)*);
    };
}

pub mod utils;

pub mod clickhouse_query_generator;
pub mod config;
pub mod graph_catalog;
pub mod open_cypher_parser;
pub mod packstream; // Vendored from neo4rs for Bolt protocol support
pub mod procedures;
pub mod query_planner;
pub mod render_plan;
pub mod server;
