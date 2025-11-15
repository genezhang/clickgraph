//! Brahmand - Graph analysis layer for ClickHouse
//!
//! This crate provides graph analysis capabilities on ClickHouse databases through:
//! - Graph view definitions over existing tables
//! - Cypher query support
//! - Query planning and optimization
//! - SQL generation

pub mod utils;

pub mod clickhouse_query_generator;
pub mod config;
pub mod graph_catalog;
pub mod open_cypher_parser;
pub mod packstream; // Vendored from neo4rs for Bolt protocol support
pub mod query_planner;
pub mod render_plan;
pub mod server;
