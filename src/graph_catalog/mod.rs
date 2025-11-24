pub mod column_info;
pub mod config;
pub mod engine_detection;
pub mod errors;
pub mod expression_parser;
pub mod graph_schema;
pub mod schema_validator;

#[cfg(test)]
pub mod testing;

// Re-export commonly used types
// Note: These are public re-exports for library users
#[allow(unused_imports)]
pub use column_info::{ColumnInfo, query_table_columns};
#[allow(unused_imports)]
pub use config::{GraphSchemaConfig, GraphSchemaDefinition};
pub use engine_detection::{TableEngine, detect_table_engine};
pub use graph_schema::{
    Direction, GraphSchema, GraphSchemaElement, NodeIdSchema, NodeSchema, RelationshipSchema,
};
#[allow(unused_imports)]
pub use schema_validator::SchemaValidator;
