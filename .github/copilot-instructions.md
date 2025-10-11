# Copilot Instructions for ClickGraph

## Project Overview
ClickGraph is a stateless graph-analysis layer for ClickHouse, written in Rust. It translates Cypher queries into ClickHouse SQL queries, enabling graph analysis capabilities on ClickHouse databases. This is a fork of the original Brahmand project with significant enhancements.

## Current Implementation Status

### ✅ Completed Features

**Neo4j Bolt Protocol v4.4**
- Complete wire protocol implementation in `server/bolt_protocol/`
- Authentication system with multiple schemes (`auth.rs`)
- Message handling for all Bolt operations (`messages.rs`)
- Connection management and error handling (`connection.rs`, `errors.rs`)
- Dual server architecture supporting HTTP and Bolt simultaneously

**View-Based Graph Model** 
- YAML configuration for mapping existing tables to graph entities
- Schema validation and optimization in `graph_catalog/`
- View resolution in `query_planner/analyzer/view_resolver.rs`
- Comprehensive test coverage (374/374 tests passing)

**Production Configuration**
- CLI argument support via clap (`src/main.rs`)
- Environment variable configuration
- Flexible server binding and port configuration
- Protocol enabling/disabling capabilities

### Development Workflow

**Adding New Cypher Features**
- Extend AST in `open_cypher_parser/ast.rs`
- Add parsing rules in relevant `open_cypher_parser/*.rs` files
- Implement logical planning in `query_planner/logical_plan/`
- Add SQL generation in `clickhouse_query_generator/`
- Include optimization passes in `query_planner/optimizer/`

**Bolt Protocol Enhancements**
- Protocol extensions go in `server/bolt_protocol/`
- Authentication schemes in `server/bolt_protocol/auth.rs`
- Message types in `server/bolt_protocol/messages.rs`
- Connection handling in `server/bolt_protocol/handler.rs`

**Performance Optimization**
- Query optimization passes in `query_planner/optimizer/`
- View-specific optimizations in `query_planner/optimizer/view_optimizer.rs`
- ClickHouse SQL generation optimization in `clickhouse_query_generator/`

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

# Set required environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"

# Build and run ClickGraph with default configuration
cargo build
cargo run --bin brahmand

# Or with custom configuration
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688
```

### Key File Patterns
- Rust modules follow a consistent pattern: `mod.rs` for module entry + separate files for major components
- Error types are centralized in `errors.rs` within each module
- AST structures in `open_cypher_parser/ast.rs` mirror the OpenCypher grammar

### Testing
- Integration tests require running ClickHouse instance (see docker-compose.yaml)
- Use `clickhouse::test-util` feature for testing SQL generation
- Current status: 374/374 tests passing with 100% success rate

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
- Neo4j Tools: Connect via Bolt protocol through `server/bolt_protocol/` (implemented)

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