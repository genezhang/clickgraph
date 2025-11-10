# Copilot Instructions for ClickGraph

## Project Overview
ClickGraph is a stateless, **read-only graph query engine** for ClickHouse, written in Rust. It translates Cypher queries into ClickHouse SQL queries, enabling graph analysis capabilities on ClickHouse databases. This is a fork of the original Brahmand project with significant enhancements.

**Project Scope**: Read-only analytical queries only. Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are explicitly out of scope.

## Windows Environment Constraints

**⚠️ IMPORTANT: Known Windows-Specific Issues**

### 1. ClickHouse Docker Volume Write Permission Problem
- **Issue**: ClickHouse container on Windows cannot write to mounted volumes due to permission restrictions
- **Solution**: **Always create tables using `ENGINE = Memory` instead of persistent engines**
- **Example**:
  ```sql
  -- ❌ DO NOT USE (will fail on Windows)
  CREATE TABLE users (...) ENGINE = MergeTree() ORDER BY id;
  
  -- ✅ USE THIS INSTEAD
  CREATE TABLE users (...) ENGINE = Memory;
  ```
- **Impact**: Data is not persisted between container restarts, but this is acceptable for development/testing
- **When to Remember**: Any SQL script creating tables (`setup_demo_data.sql`, test data creation, etc.)

### 2. curl Command Not Available in PowerShell
- **Issue**: `curl` is not available or behaves differently in Windows PowerShell environment
- **Solution**: **Use `Invoke-RestMethod` or `Invoke-WebRequest` PowerShell cmdlets instead**
- **Examples**:
  ```powershell
  # ❌ DO NOT USE (curl doesn't work)
  curl -X POST http://localhost:8080/query -d '{"query":"MATCH (n) RETURN n"}'
  
  # ✅ USE THIS INSTEAD
  Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
    -ContentType "application/json" `
    -Body '{"query":"MATCH (n) RETURN n"}'
  
  # ✅ OR USE Python requests library
  python -c "import requests; print(requests.post('http://localhost:8080/query', json={'query':'MATCH (n) RETURN n'}).json())"
  ```
- **When to Remember**: Testing HTTP endpoints, manual query testing, CI/CD scripts
- **Alternative**: Use Python scripts with `requests` library for cross-platform testing

**Development Reminder**: These constraints have been encountered multiple times. Always check for these patterns when:
- Writing SQL setup scripts → Use `ENGINE = Memory`
- Testing HTTP APIs → Use `Invoke-RestMethod` or Python
- Creating documentation examples → Show both PowerShell and cross-platform alternatives.
- Use Mermaid diagrams for architecture explanations where possible

## Current Implementation Status

### ✅ Completed Features

**Variable-Length Path Queries (Production-Ready)**
- Complete syntax support: `*`, `*2`, `*1..3`, `*..5`, `*2..` patterns
- Recursive CTE generation with `WITH RECURSIVE` keyword
- Configurable recursion depth (10-1000 via CLI/env)
- Property selection in CTEs (two-pass architecture)
- Performance optimization with chained JOINs for exact hops
- Comprehensive testing: 250/251 tests passing (99.6%)
- Full documentation suite (user guide, examples, test scripts)

**OPTIONAL MATCH Support (Production-Ready)**
- Complete LEFT JOIN semantics for optional graph patterns
- Two-word keyword parsing (`OPTIONAL MATCH`)
- Optional alias tracking in `query_planner/plan_ctx/mod.rs`
- Automatic LEFT JOIN generation in `clickhouse_query_generator/`
- All OPTIONAL MATCH tests passing (5/5 basic + 4/4 e2e)
- Full documentation: `docs/optional-match-guide.md`

**Multi-Schema Architecture (Robust)**
- Complete schema isolation support (Nov 9, 2025)
- Per-request schema selection via USE clause or schema_name parameter
- Single source of truth: GLOBAL_SCHEMAS HashMap
- Removed redundant GLOBAL_GRAPH_SCHEMA architecture
- Thread-safe schema flow through entire query execution
- All multi-schema tests passing (100%)

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
- Comprehensive test coverage (325 unit tests passing)
- Fixed label/type_name field usage in `server/graph_catalog.rs`

**Relationship Traversal Support**
- Full relationship pattern support: `MATCH (a)-[r:TYPE]->(b)`
- Multi-hop graph traversals with complex JOIN generation
- All 4 YAML relationship types working (AUTHORED, FOLLOWS, LIKED, PURCHASED)
- Relationship property filtering support

**Multiple Relationship Types Support**
- Alternate relationship patterns: `[:TYPE1|TYPE2]` with UNION SQL generation
- Extended TableCtx from single `label` to `labels` vector throughout codebase
- UNION ALL CTE generation for multiple relationship types
- Comprehensive unit tests and partial end-to-end validation
- Enables complex queries: `MATCH (a)-[:FOLLOWS|FRIENDS_WITH|LIKES]->(b)`

**Shortest Path Algorithms**
- Complete implementation of `shortestPath()` and `allShortestPaths()` functions
- Recursive CTE-based path finding with early termination optimization
- Support for variable-length path patterns with shortest path constraints
- WHERE clause filtering on shortest path results
- Performance optimized for graph analytics workloads

**Robust Configuration System**
- CLI argument support via clap (`src/main.rs`)
- Environment variable configuration
- Flexible server binding and port configuration
- Protocol enabling/disabling capabilities

### Development Workflow

**📋 See `DEVELOPMENT_PROCESS.md` for the complete 5-phase iterative development process.**

**Adding New Cypher Features** (Quick Reference):
- **Phase 1 - Design**: Understand OpenCypher spec, sketch Cypher→SQL examples, identify components
- **Phase 2 - Implement**:
  - Extend AST in `open_cypher_parser/ast.rs`
  - Add parsing rules in relevant `open_cypher_parser/*.rs` files
  - Implement logical planning in `query_planner/logical_plan/`
  - Add SQL generation in `clickhouse_query_generator/`
  - Include optimization passes in `query_planner/optimizer/`
- **Phase 3 - Test**: Manual smoke test → Unit tests → Integration tests
- **Phase 4 - Debug**: Add debug output, use `sql_only`, check server logs
- **Phase 5 - Document**: Update STATUS.md, create feature note, update CHANGELOG.md

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
cargo run --bin clickgraph

# Or with custom configuration
cargo run --bin clickgraph -- --http-port 8081 --bolt-port 7688
```

### Key File Patterns
- Rust modules follow a consistent pattern: `mod.rs` for module entry + separate files for major components
- Error types are centralized in `errors.rs` within each module
- AST structures in `open_cypher_parser/ast.rs` mirror the OpenCypher grammar

### Testing
- Integration tests require running ClickHouse instance (see docker-compose.yaml)
- Use `clickhouse::test-util` feature for testing SQL generation
- Current status: 325/325 unit tests + 32/35 integration tests passing (91.4%)

## Project-Specific Conventions

### Error Handling
- Each module has its own error type in `errors.rs`
- Use `thiserror` for error definitions
- Propagate errors up using `?` operator, avoid panics

### Query Planning
- Use builder pattern for plan construction (`logical_plan/plan_builder.rs`)
- Optimization passes are composable via `optimizer/optimizer_pass.rs`
- Graph traversals are planned in `analyzer/graph_traversal_planning.rs`

### Development Assessment Guidelines
- **Use "robust" instead of "production-ready"** when describing well-tested features
- Always provide realistic assessments of current capabilities and limitations
- Acknowledge when features are experimental, incomplete, or have known edge cases
- Use terms like "development-ready", "demo-ready", "robust", or "working for tested scenarios"
- Be transparent about the scope and robustness of implemented features

## Integration Points
- ClickHouse: Via `clickhouse` crate (see `server/clickhouse_client.rs`)
- HTTP API: Using `axum` framework (see `server/handlers.rs`)
- OpenCypher: Grammar defined in `open_cypher_parser/open_cypher_specs/`
- View Integration: Map existing ClickHouse tables through `graph_catalog/graph_schema.rs`
- Neo4j Tools: Connect via Bolt protocol through `server/bolt_protocol/` (implemented)

## Development Priorities

**Core Read Query Features** (Priority Order):

1. **Integration Test Coverage** (Ongoing)
   - Currently at 32/35 (91.4%)
   - 3 benchmark tests remain (expected - require specific datasets)
   - **Status**: Excellent coverage achieved

2. **Additional Graph Algorithms**
   - ✅ **PageRank** - COMPLETED Oct 23, 2025
   - Centrality measures (betweenness, closeness, degree)
   - Community detection
   - Connected components
   - **Estimated**: 1-2 weeks per algorithm

4. **Pattern Extensions**
   - Path comprehensions: `[(a)-[]->(b) | b.name]`
   - **Estimated**: 3-5 days

**Completed Features**:
- ✅ **Multi-Schema Architecture**: Single source of truth, schema isolation, USE clause - Nov 9, 2025
- ✅ **Path Variables & Functions**: `p = (a)-[*]->(b)`, `length(p)`, `nodes(p)`, `relationships(p)` - Oct 21, 2025
- ✅ **Shortest Path**: `shortestPath()` and `allShortestPaths()` - Oct 20, 2025
- ✅ **Alternate Relationship Types**: `[:TYPE1|TYPE2]` - Oct 21, 2025
- ✅ **PageRank Algorithm**: `CALL pagerank(...)` - Oct 23, 2025
- ✅ **Variable-Length Paths**: `*`, `*2`, `*1..3` patterns - Oct 18, 2025
- ✅ **OPTIONAL MATCH**: LEFT JOIN semantics - Oct 17, 2025

**Out of Scope** (Read-Only Engine):
- ❌ Write operations: `CREATE`, `SET`, `DELETE`, `MERGE`
- ❌ Schema modifications: `CREATE INDEX`, `CREATE CONSTRAINT`
- ❌ Transaction management
- ❌ Data mutations of any kind

## Documentation Standards

**Simplified 3-Document Approach** (as of Oct 21, 2025):

### Core Documents (Always Maintain)

1. **STATUS.md** - Single source of truth for current project state
   - What works now (with examples)
   - What's in progress
   - Known issues
   - Test statistics
   - Next priorities
   - **Update after each feature completion**

2. **CHANGELOG.md** - Release history and feature tracking
   - Follow Keep-a-Changelog format
   - Use emoji prefixes: 🚀 Features, 🐛 Bug Fixes, 📚 Documentation, 🧪 Testing, ⚙️ Infrastructure
   - Update when merging to main or releasing
   - Include test statistics and dates

3. **Feature Notes** (in `notes/` directory)
   - One note per major feature (e.g., `notes/viewscan.md`)
   - Document: Summary, How It Works, Key Files, Design Decisions, Gotchas, Limitations, Future Work
   - Create when feature is complete
   - Keep concise (1-2 pages max)

### Additional Core Documents
- **README.md** - Project overview for users
- **KNOWN_ISSUES.md** - Living document for tracking issues
- **DEV_ENVIRONMENT_CHECKLIST.md** - Development setup procedures
- **NEXT_STEPS.md** - Immediate roadmap and next actions

### Documentation Workflow

**After completing a feature**:
1. Update `STATUS.md` (2 min):
   - Move feature from "In Progress" to "What Works"
   - Update test count
   - Update "Next Priorities"
2. Create feature note in `notes/<feature>.md` (5 min):
   - Document key decisions and gotchas
   - Note limitations and future work
3. Commit: `git commit -m "docs: Update STATUS with <feature>"`

**When releasing** (merging to main):
1. Update `CHANGELOG.md`:
   - Move [Unreleased] items to new version
   - Add release date
2. Tag: `git tag v0.X.Y`

### Archive Policy
- Historical session summaries → `archive/`
- Investigation reports (after implemented) → `archive/`
- Duplicate/outdated docs → `archive/`
- Keep root directory clean (6-8 core docs only)

### What NOT to Do
- ❌ Don't create multiple status documents (SESSION_COMPLETE.md, FEATURE_STATUS.md, etc.)
- ❌ Don't duplicate information across multiple docs
- ❌ Don't create "PROJECT_SUMMARY.md" - use STATUS.md instead
- ❌ Don't create dated session files unless they capture unique debugging stories

### Documentation Structure
```
clickgraph/
├── STATUS.md                    # Current state (THE source of truth)
├── CHANGELOG.md                 # Release history
├── DEVELOPMENT_PROCESS.md       # ⭐ 5-phase feature development workflow
├── KNOWN_ISSUES.md              # Active issues
├── README.md                    # Project overview
├── DEV_ENVIRONMENT_CHECKLIST.md # Pre-session setup
├── NEXT_STEPS.md                # Immediate roadmap
├── TESTING_GUIDE.md             # Testing strategies
├── notes/
│   ├── viewscan.md             # Feature implementation details
│   ├── optional-match.md
│   └── variable-paths.md
└── archive/
    └── (historical docs)
```

**Key Principle**: Keep it simple. One source of truth (STATUS.md), one note per feature, archive everything else.

**Development Workflow**: Follow `DEVELOPMENT_PROCESS.md` for systematic feature development (Design → Implement → Test → Debug → Document).
