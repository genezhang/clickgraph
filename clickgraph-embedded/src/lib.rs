//! ClickGraph Embedded — in-process graph query engine.
//!
//! Provides a Kuzu-compatible synchronous API for running Cypher queries directly
//! in your Rust process, powered by [chdb](https://github.com/chdb-io/chdb) (an
//! embedded ClickHouse engine).
//!
//! ## Quick Start
//!
//! ```no_run
//! use clickgraph_embedded::{Database, Connection, SystemConfig};
//!
//! let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
//! let conn = Connection::new(&db).unwrap();
//!
//! let mut result = conn.query("MATCH (u:User) RETURN u.name LIMIT 5").unwrap();
//! while let Some(row) = result.next() {
//!     println!("{:?}", row);
//! }
//! ```
//!
//! ## Kuzu API Compatibility
//!
//! | Kuzu | ClickGraph Embedded |
//! |------|---------------------|
//! | `Database::new(path, config)` | `Database::new(schema_yaml, config)` |
//! | `Connection::new(&db)` | `Connection::new(&db)` |
//! | `conn.query(cypher)` | `conn.query(cypher)` |
//! | `result.next()` -> `FlatTuple` | `result.next()` -> `Row` |
//! | `row[0]` | `row[0]` |
//!
//! ## Key Differences vs Kuzu
//!
//! - **No data loading required**: ClickGraph reads Parquet/Iceberg/Delta directly.
//! - **Read-only**: ClickGraph is an analytical engine; no `CREATE NODE TABLE` needed.
//! - **YAML schema**: Graph mapping defined in YAML, not Cypher DDL.
//! - **Synchronous API**: chdb FFI is blocking; async wrappers are in the server.

pub mod connection;
pub mod database;
pub mod error;
pub mod export;
pub mod query_result;
pub mod value;
pub mod write_helpers;

pub use connection::Connection;
pub use database::{Database, StorageCredentials, SystemConfig};
pub use error::EmbeddedError;
pub use export::{ExportFormat, ExportOptions};
pub use query_result::{QueryResult, Row};
pub use value::Value;
