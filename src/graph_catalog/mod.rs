pub mod column_info;
pub mod composite_key_utils;
pub mod config;
pub mod constraint_compiler;
pub mod element_id;
pub mod engine_detection;
pub mod errors;
pub mod expression_parser;
pub mod filter_parser;
pub mod graph_schema;
pub mod node_classification;
pub mod pattern_schema;
pub mod schema_discovery;
pub mod schema_types;
pub mod schema_validator;

#[cfg(test)]
pub mod testing;

#[cfg(test)]
mod composite_id_tests;

// Re-export commonly used types
// Note: These are public re-exports for library users
#[allow(unused_imports)]
pub use column_info::{query_table_columns, ColumnInfo};
#[allow(unused_imports)]
pub use schema_discovery::{
    ColumnMetadata, DraftOptions, DraftRequest, EdgeHint, FkEdgeHint, IntrospectResponse,
    NodeHint, SchemaDiscovery, Suggestion, TableMetadata,
};
#[allow(unused_imports)]
pub use composite_key_utils::{
    build_composite_key, extract_type_name, is_composite_key, CompositeKey, CompositeKeyError,
};
#[allow(unused_imports)]
pub use config::{GraphSchemaConfig, GraphSchemaDefinition};
pub use engine_detection::{detect_table_engine, TableEngine};
pub use filter_parser::SchemaFilter;
pub use graph_schema::{
    classify_edge_table_pattern, edge_has_node_properties, is_fully_denormalized_edge_table,
    is_node_denormalized_on_edge, Direction, EdgeTablePattern, GraphSchema, GraphSchemaElement,
    NodeIdSchema, NodeSchema, RelationshipSchema,
};
pub use node_classification::{
    has_denormalized_properties_on_side, is_node_denormalized, rel_has_any_denormalized,
    rel_has_both_nodes_denormalized, rel_has_denormalized_properties,
};
pub use pattern_schema::{
    CoupledEdgeContext, EdgeAccessStrategy, JoinStrategy, NodeAccessStrategy, NodePosition,
    PatternSchemaContext,
};
#[allow(unused_imports)]
pub use schema_validator::SchemaValidator;
