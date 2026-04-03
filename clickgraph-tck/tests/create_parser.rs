//! Re-exports the Cypher CREATE parser from `clickgraph_embedded::cypher_loader`.
//!
//! The parser has been promoted to the embedded crate so it is available to all users.
//! This shim preserves backwards compatibility for tck.rs which still `mod create_parser`s.

pub use clickgraph_embedded::cypher_loader::{
    parse_create_block, EdgeDir, ParsedCreate, ParsedEdge, ParsedNode, PropValue,
};
