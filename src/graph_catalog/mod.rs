pub mod column_info;
pub mod config;
pub mod engine_detection;
pub mod errors;
pub mod expression_parser;
pub mod filter_parser;
pub mod graph_schema;
pub mod pattern_schema;
pub mod schema_validator;

#[cfg(test)]
pub mod testing;

#[cfg(test)]
mod composite_id_tests;

// Re-export commonly used types
// Note: These are public re-exports for library users
#[allow(unused_imports)]
pub use column_info::{ColumnInfo, query_table_columns};
#[allow(unused_imports)]
pub use config::{GraphSchemaConfig, GraphSchemaDefinition};
pub use engine_detection::{TableEngine, detect_table_engine};
pub use filter_parser::SchemaFilter;
pub use graph_schema::{
    Direction, GraphSchema, GraphSchemaElement, NodeIdSchema, NodeSchema, RelationshipSchema,
};
pub use pattern_schema::{
    PatternSchemaContext, NodeAccessStrategy, EdgeAccessStrategy, JoinStrategy,
    NodePosition, CoupledEdgeContext,
};
#[allow(unused_imports)]
pub use schema_validator::SchemaValidator;

