# Copilot Instructions for ClickGraph

## Project Overview
ClickGraph is a stateless, **read-only graph query engine** for ClickHouse, written in Rust. It translates Cypher queries into ClickHouse SQL queries, enabling graph analysis capabilities on ClickHouse databases. Codebase has evolved and diverged from the original Brahmand project. Features related to DDLs for special node and edge tables and updates have been removed.

**Project Scope**: Read-only analytical queries only. Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are explicitly out of scope.

## Coding Style Guidelines
- Always prefer idiomatic Rust code styles and best practices, follow Rust's official style guidelines: https://doc.rust-lang.org/1.0.0/style/
- Check if there are existing functions or modules that can be reused before implementing new code
- Follow Boy Scout Rule: refactor code to be clear and maintainable whenever appropriate, before, during, or after implementing features
- Remove unused code, imports, variables, and comments promptly to avoid clutter
- When fixing a bug, step back and consider general patterns, not just the specific instance

### Late-Stage Development Principles (Post-Alpha)
**Current Phase**: Code quality and robustness over new features

- **Investigation Before Claims**: Never make cleanup claims without thorough analysis
  - "44 dead functions" → Actually 0 after proper investigation
  - Check internal usage within same file, not just external references
  - Module-level `#![allow(dead_code)]` can hide actual usage from tooling
  - Use comprehensive grep including the target file itself

- **Evidence-Based Decisions**: Verify before acting
  - Test removal attempts compile before claiming code is dead
  - Document investigation methodology when keeping seemingly large files
  - Accept reasonable file sizes when functions are interconnected utilities

- **No Hacking, No Guessing**: Solid logic only
  - If uncertain, investigate thoroughly
  - Don't rush to "cleanup" without understanding actual usage
  - Large files may be intentional refactoring results (e.g., 16K → 10K + 4K split)

## Development Environment

**Current Platform**: WSL Ubuntu 24.04 (migrated from Windows, November 2025)

### Shell and Script Guidelines
- **Default Shell**: Bash (use bash scripts, not PowerShell)
- **Background Processes**: Use standard bash backgrounding (`nohup`, `&`, `disown`)
- **Server Scripts**: Located in `scripts/server/` (bash versions)
  - `start_server_background.sh` - Start ClickGraph server in background
  - `stop_server.sh` - Stop background server
  - `server_status.sh` - Check server health

### ClickHouse Docker Setup
- **Engine Choice**: Use `ENGINE = Memory` for development/testing
  - Simpler setup, faster restarts
  - Acceptable for non-persistent test data
  - For production schemas, use `MergeTree()` or appropriate engine

### HTTP Testing
- **Use `curl`**: Available and works correctly on Linux
  ```bash
  # ✅ Standard curl syntax works
  curl -X POST http://localhost:8080/query \
    -H "Content-Type: application/json" \
    -d '{"query":"MATCH (n) RETURN n"}'
  ```
- **Alternative**: Python `requests` library for complex test scenarios

**Documentation Style**: Use Mermaid diagrams for architecture explanations

---

## Schema Discipline ⚠️ **[CRITICAL - PREVENTS TIME WASTE]**

**Problem**: Testing with inconsistent schemas wastes significant time debugging "wrong" SQL when the issue is just using the wrong schema file.

**Solution**: **ALWAYS use the benchmark schema for testing/development**

### The One True Schema for Development

**Schema File**: `benchmarks/social_network/schemas/social_benchmark.yaml`

**Tables** (all in `brahmand` database):
- `users_bench` (node)
- `user_follows_bench` (relationship)
- `posts_bench` (node)
- `post_likes_bench` (relationship)

**Property Mappings** (Cypher property → ClickHouse column):
- **User node**:
  - `user_id` → `user_id`
  - `name` → `full_name` ⚠️ (NOT `name`!)
  - `email` → `email_address` ⚠️ (NOT `email`!)
  - `registration_date` → `registration_date`
  - `is_active` → `is_active`
  - `country` → `country`
  - `city` → `city`

- **FOLLOWS relationship**:
  - `follower_id` → `follower_id` (from)
  - `followed_id` → `followed_id` (to)
  - `follow_date` → `follow_date`

**When to Use This Schema**:
- ✅ All manual testing
- ✅ All benchmark queries
- ✅ Integration test development
- ✅ Debug scripts and quick validation
- ✅ Examples in documentation

**Other Schemas** (use ONLY when explicitly needed):
- `schemas/demo/users.yaml` - For demo/tutorial purposes only
- Custom schemas - Only when testing schema-specific features

**Testing Discipline**:
```bash
# ✅ CORRECT: Always set GRAPH_CONFIG_PATH to benchmark schema
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

# ❌ WRONG: Using inconsistent schema
export GRAPH_CONFIG_PATH="./schemas/demo/users.yaml"  # Different property mappings!
```

**Use consistent commands with fewer variations so auto-approval can work without waiting for manual approval**

**Query Examples with Correct Schema**:
```cypher
# ✅ CORRECT (uses full_name mapping from benchmark schema)
MATCH (u:User) WHERE u.user_id = 1 RETURN u.name

# Generated SQL will use: users_bench.full_name

# ✅ CORRECT relationship
MATCH (u1:User)-[:FOLLOWS]->(u2:User) WHERE u1.user_id = 1 RETURN u2.name

# Generated SQL will use: user_follows_bench table
```

**Why This Matters**:
- Prevents "Schema X doesn't have property Y" errors
- Ensures generated SQL matches actual database schema
- Makes benchmark results reproducible
- Saves debugging time from schema mismatches

**Remember**: If you're testing anything except schema loading itself, use the benchmark schema!

---

## File Organization Guidelines

**⚠️ CRITICAL: Keep Root Directory Clean!**

The root directory should contain ONLY essential project files. Before creating any file, determine its proper location:

### Where Files Belong

**✅ Root Directory (15 files maximum)**
- Core configs: `.dockerignore`, `.gitignore`
- Rust workspace: `Cargo.toml`, `Cargo.lock`
- Docker: `docker-compose.yaml`, `docker-compose.test.yaml`, `Dockerfile`, `Dockerfile.test`
- Legal: `LICENSE`
- Entry point: `README.md`
- Key docs: `CHANGELOG.md`, `STATUS.md`, `DEVELOPMENT_PROCESS.md`, `KNOWN_ISSUES.md`, `ROADMAP.md`

**🧪 Test Files** → `tests/`
- Unit tests → `tests/unit/`
- Integration tests → `tests/integration/`
- E2E tests → `tests/e2e/`
- Bolt protocol tests → `tests/integration/bolt/`
- Test data/fixtures → `tests/fixtures/data/`
- **❌ NEVER** create `test_*.py` or `test_*.rs` in root!

**📊 Benchmark Files organized by benchmark** → `benchmarks/social_network/` and `benchmarks/ontime_flights/`
- Data generation → `benchmarks/*/data/`
- Query suites → `benchmarks/*/queries/`
- Benchmark schemas → `benchmarks/*/schemas/`
- Results → `benchmarks/*/results/` (gitignored)

**🛠️ Utility Scripts** → `scripts/`
- Setup scripts → `scripts/setup/`
- Test runners → `scripts/test/`
- Server utilities → `scripts/server/`
- General utilities → `scripts/utils/`
- Debug scripts → `scripts/debug/`

**📚 Documentation** → `docs/`
- Development guides → `docs/development/`
- Feature documentation → `docs/features/`
- API docs → `docs/api/`
- Images/diagrams → `docs/images/`
- User facing docs → `docs/wiki/`

**🗂️ Schemas** → `schemas/`
- Demo schemas → `schemas/demo/`
- Example schemas → `schemas/examples/`

**📝 Feature Notes** → `notes/`
- Implementation details for specific features
- Keep concise (1-2 pages max)

**📦 Archive** → `archive/`
- Completed planning documents
- Historical session summaries
- Outdated documentation

### Quick Decision Tree

**Before creating a file, ask:**
```
Is it a test file?           → tests/
Is it a benchmark?           → benchmarks/
Is it a script/utility?      → scripts/
Is it documentation?         → docs/
Is it a schema?             → schemas/
Is it a feature note?       → notes/
Is it temporary/planning?   → archive/ (when done)
Is it truly essential?      → Maybe root (rare!)
```

### Examples of Proper File Placement

```
✅ GOOD:
tests/integration/test_optional_match.py
benchmarks/queries/suite.py
scripts/utils/load_schema.py
docs/features/bolt-protocol.md
docs/images/architecture.png
schemas/examples/ecommerce.yaml

❌ BAD (clutters root):
test_bolt_simple.py              → tests/integration/bolt/
setup_benchmark_unified.py       → benchmarks/data/
load_schema.py                   → scripts/utils/
BOLT_PROTOCOL_STATUS.md          → docs/features/
architecture.png                 → docs/images/
ecommerce_simple.yaml            → schemas/examples/
```

### Preventing File Proliferation

**When creating files:**
1. ✅ Always use proper directory structure
2. ✅ Use descriptive, categorized names
3. ✅ Archive planning docs when complete
4. ❌ Never create temporary files in root
5. ❌ Never create test files in root
6. ❌ Never create multiple status/summary docs

**When adding to .gitignore:**
```gitignore
# Prevent accidental test file commits in root
/test_*.py
/test_*.rs
/*_test.py
/debug_*.py
/*_debug.py
```

**Maintenance reminder**: Review root directory monthly. If it grows beyond 20 files, reorganize immediately!

---

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

**Neo4j Bolt Protocol v5.8**
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
- **Phase 4 - Debug**: Add debug output, use `sql_only`, check server logs. Run the sql to make sure it works as expected.
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

# Set default graph schema configuration, based on your testing needs
export GRAPH_CONFIG_PATH="./schemas/examples/zeek_merged.yaml"

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

**After completing a feature** (⚠️ CRITICAL - Use checklist!):

**See `docs/FEATURE_DOCUMENTATION_CHECKLIST.md` for complete requirements.**

**Minimum Requirements**:
1. **Cypher Language Reference** (5-10 min):
   - Add to Table of Contents
   - Create dedicated section with syntax, examples, common errors
   - **This is PRIMARY documentation - feature not complete without it!**

2. **STATUS.md** (2 min):
   - Move feature from "In Progress" to "What Works"
   - Update test count
   - Update "Next Priorities"

3. **Feature note** in `notes/<feature>.md` (5 min):
   - Document key decisions and gotchas
   - Note limitations and future work

4. **API docs** (if API-exposed) (3 min):
   - Update `docs/api.md` with new parameters
   - Add request/response examples

5. **Commit**: `git commit -m "docs: Update STATUS with <feature>"`

**⚠️ Documentation Debt = Technical Debt**
- Never merge features without documentation
- Cypher Language Reference MUST be complete
- Use `docs/FEATURE_DOCUMENTATION_CHECKLIST.md` before marking feature complete
- Reference: November 22, 2025 incident (missing USE clause docs led to incorrect regression assessment)

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
├── NEXT_STEPS.md                # Immediate roadmap
├── docs/
│   ├── development/
│   │   ├── environment-checklist.md
│   │   ├── testing.md
│   │   └── git-workflow.md
|   ├── features/
│   |   ├── bolt-protocol.md
│   |   └── packstream.md
|   └─wiki/
├── notes/
│   ├── viewscan.md             # Feature implementation details
│   ├── optional-match.md
│   └── variable-paths.md
└── archive/
    └── (historical docs)
```

**Key Principle**: Keep it simple. One source of truth (STATUS.md), one note per feature, archive everything else.

**Development Workflow**: Follow `DEVELOPMENT_PROCESS.md` for systematic feature development (Design → Implement → Test → Debug → Document).


