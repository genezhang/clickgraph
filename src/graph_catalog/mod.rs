pub mod column_info;
pub mod config;
pub mod engine_detection;
pub mod errors;
pub mod graph_schema;
pub mod schema_validator;

#[cfg(test)]
pub mod testing;

// Re-export commonly used types
// Note: These are public re-exports for library users
#[allow(unused_imports)]
pub use column_info::ColumnInfo;
#[allow(unused_imports)]
pub use config::{GraphSchemaConfig, GraphSchemaDefinition};
pub use engine_detection::{detect_table_engine, TableEngine};
pub use graph_schema::{
    Direction, GraphSchema, GraphSchemaElement, NodeIdSchema, NodeSchema, RelationshipSchema,
};
#[allow(unused_imports)]
pub use schema_validator::SchemaValidator;
