//! Brahmand - Graph analysis layer for ClickHouse
//!
//! This crate provides graph analysis capabilities on ClickHouse databases through:
//! - Graph view definitions over existing tables
//! - Cypher query support
//! - Query planning and optimization
//! - SQL generation

/// Debug print macro — delegates to log::debug! (respects RUST_LOG level).
#[macro_export]
macro_rules! debug_print {
    ($($arg:tt)*) => {
        log::debug!($($arg)*);
    };
}

/// Debug print macro for println-style output — delegates to log::debug! (respects RUST_LOG level).
#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        log::debug!($($arg)*);
    };
}

pub mod utils;

pub mod config;
pub mod executor;
pub mod graph_catalog;
pub mod open_cypher_parser;
pub mod packstream; // Vendored from neo4rs for Bolt protocol support
pub mod procedures;
pub mod query_planner;
pub mod render_plan;
pub mod server;
pub mod sql_generator;

/// Deprecated path for the ClickHouse SQL emitter — the code now lives at
/// `crate::sql_generator::emitters::clickhouse` (Phase 0.3 of the
/// DeltaGraph refactor). Kept as a transparent re-export for one release
/// cycle so existing call sites don't churn.
pub use crate::sql_generator::emitters::clickhouse as clickhouse_query_generator;
