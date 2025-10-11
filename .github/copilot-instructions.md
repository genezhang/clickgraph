# Copilot Instructions for Brahmand

## Project Overview
Brahmand is a stateless graph-analysis layer for ClickHouse, written in Rust. It translates Cypher queries into ClickHouse SQL queries, enabling graph analysis capabilities on ClickHouse databases.

## Feature Development Guide

### Neo4j Bolt Protocol Support
To implement Neo4j Bolt protocol compatibility:
- Create new `bolt_protocol/` module in `server/`
- Implement protocol handlers following Neo4j wire protocol spec
- Entry point: Extend `server/mod.rs` to handle both HTTP and Bolt connections
- Reference: Neo4j Bolt Protocol v4.4 specification

### View-Based Graph Model
For implementing graph views on existing ClickHouse tables:
- Extend `graph_catalog/graph_schema.rs` to support view definitions
- Add view mapping logic in `query_planner/analyzer/schema_inference.rs`
- Schema structure: Define view mappings in `NodeSchema` and `RelationshipSchema`
- Example mapping:
```rust
pub struct GraphViewDefinition {
    source_table: String,
    node_id_column: String,
    property_columns: Vec<String>
}
```

### Code Organization
- Keep new Bolt protocol code isolated in `server/bolt_protocol/`
- Extend existing schema interfaces in `graph_catalog/` for backward compatibility
- Add feature flags in `Cargo.toml` to make new features optional

## Key Architecture Components

### Core Components
- `open_cypher_parser/`: Parses Cypher queries into AST (see `ast.rs`, `mod.rs`)
- `query_planner/`: Transforms Cypher AST into logical plans
  - `analyzer/`: Query validation and optimization passes
  - `logical_plan/`: Core query planning structures
  - `optimizer/`: Query optimization rules
- `clickhouse_query_generator/`: Converts logical plans to ClickHouse SQL
- `server/`: HTTP API server handling query requests
- `graph_catalog/`: Manages graph schema and metadata

### Data Flow
1. Client sends Cypher query → `server/handlers.rs`
2. Query parsed → `open_cypher_parser/mod.rs`
3. Query planned & optimized → `query_planner/`
4. SQL generated → `clickhouse_query_generator/`
5. Results returned via ClickHouse client → `server/clickhouse_client.rs`

## Development Workflow

### Setup
```bash
# Start ClickHouse instance
docker-compose up -d

# Build and run Brahmand
cargo build
cargo run
```

### Key File Patterns
- Rust modules follow a consistent pattern: `mod.rs` for module entry + separate files for major components
- Error types are centralized in `errors.rs` within each module
- AST structures in `open_cypher_parser/ast.rs` mirror the OpenCypher grammar

### Testing
- Integration tests require running ClickHouse instance (see docker-compose.yaml)
- Use `clickhouse::test-util` feature for testing SQL generation

## Project-Specific Conventions

### Error Handling
- Each module has its own error type in `errors.rs`
- Use `thiserror` for error definitions
- Propagate errors up using `?` operator, avoid panics

### Query Planning
- Use builder pattern for plan construction (`logical_plan/plan_builder.rs`)
- Optimization passes are composable via `optimizer/optimizer_pass.rs`
- Graph traversals are planned in `analyzer/graph_traversal_planning.rs`

## Integration Points
- ClickHouse: Via `clickhouse` crate (see `server/clickhouse_client.rs`)
- HTTP API: Using `axum` framework (see `server/handlers.rs`)
- OpenCypher: Grammar defined in `open_cypher_parser/open_cypher_specs/`
- View Integration: Map existing ClickHouse tables through `graph_catalog/graph_schema.rs`
- Neo4j Tools: Connect via Bolt protocol through `server/bolt_protocol/` (planned)

## Development Priorities
1. **View-Based Graph Model**
   - Extend schema definitions to support views
   - Implement view resolution in query planner
   - Add view-specific optimizations

2. **Bolt Protocol Support**
   - Implement core protocol handlers
   - Add connection management
   - Support Neo4j driver authentication

3. **Compatibility Layer**
   - Maintain existing HTTP API
   - Support both direct tables and views
   - Feature flags for opt-in functionality