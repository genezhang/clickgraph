//! Graph-to-SQL translation boundary
//! 
//! This module provides a clean separation between graph concepts (Cypher)
//! and relational concepts (ClickHouse SQL), handling:
//! - Property mapping (graph properties → SQL columns)
//! - Alias resolution (graph aliases → SQL table aliases)
//! - Schema pattern support (standard, denormalized, polymorphic)

pub mod property_resolver;

pub use property_resolver::{PropertyResolver, PropertyResolution, NodePosition, AliasMapping};
