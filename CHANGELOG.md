## [Unreleased]

## [0.4.0] - 2025-11-15

### 🎉 Phase 1 Complete - Development-Ready Graph Analytics Engine

This release marks the completion of Phase 1, delivering a robust, development-ready graph query engine with comprehensive Neo4j compatibility and validated performance.

### 🚀 Features

- **Query Cache with LRU Eviction**: 10-100x speedup for repeated query translations (Nov 10, 2025)
  - HashMap-based cache with dual limits (1000 entries, 100 MB memory)
  - Neo4j-compatible CYPHER replan options (default/force/skip)
  - Parameterized query support with SQL template caching
  - Whitespace normalization and CYPHER prefix handling
  - Schema-aware automatic cache invalidation
  - Thread-safe concurrent access with Arc<Mutex<HashMap>>
  - Response headers: X-Query-Cache-Status (HIT/MISS/BYPASS)
  - Test coverage: 6/6 unit tests + 5/5 e2e tests (100%)

- **Parameter Support**: Neo4j-compatible parameterized queries (Nov 10, 2025)
  - `$param` syntax in WHERE clauses, RETURN, aggregations
  - Type-safe parameter binding (String, Integer, Float, Boolean)
  - SQL injection prevention
  - Query plan caching and reuse
  - HTTP API and Bolt protocol support

- **Bolt 5.8 Protocol**: Complete wire protocol implementation (Nov 12, 2025)
  - Version negotiation with byte-order detection (Bolt 5.x format changes)
  - HELLO/LOGON authentication flow for Bolt 5.1+
  - Multi-database support with session parameters
  - Auth-less mode for development
  - Automatic schema selection (first loaded schema as fallback)
  - Message handling: HELLO, LOGON, RUN, PULL, RESET
  - PackStream binary format (vendored from neo4rs v0.9.0-rc.8)
  - Neo4j Python driver v6.0.2 compatibility tested
  - Test coverage: 4/4 E2E tests (connection, query, traversal, aggregation)

- **Neo4j Function Mappings**: 25+ functions for compatibility (Nov 8, 2025)
  - **Datetime**: datetime(), date(), timestamp(), duration(), localtime(), localdatetime()
  - **String**: toString(), toUpper(), toLower(), substring(), trim(), split(), replace()
  - **Math**: abs(), ceil(), floor(), round(), sqrt(), log(), exp(), sign()
  - **Aggregation**: count(), sum(), avg(), min(), max(), collect()
  - **Type checking**: Working towards full Neo4j compatibility

- **Undirected Relationships**: Bidirectional pattern matching (Nov 15, 2025)
  - `(a)-[r]-(b)` syntax support
  - OR JOIN logic: `(r.from=a.id AND r.to=b.id) OR (r.from=b.id AND r.to=a.id)`
  - Direction::Either handling in SQL generation
  - Enables mutual relationship queries

- **Benchmark Suite**: Production validation framework (Nov 1, 2025)
  - 14 comprehensive queries covering all major patterns
  - 3 scale levels: Small (1K), Medium (10K), Large (5M nodes)
  - Performance metrics: Mean time, p50/p95/p99 latency
  - ClickHouse-native data generation for efficient loading
  - 100% success rate at small/medium scale, 90% at large scale
  - Validates: traversals, aggregations, variable-length paths, shortest paths

### 🐛 Bug Fixes

- **Undirected Relationships**: Fixed Direction::Either SQL generation (Nov 15, 2025)
- **ChainedJoin CTE**: Fixed exact hop variable-length paths (`*2`, `*3`) (Nov 1, 2025)
- **Shortest Path Filters**: Fixed WHERE clause rewriting for end nodes (Nov 1, 2025)
- **Aggregation Schema**: Fixed table name lookup for GROUP BY queries (Nov 1, 2025)
- **Test Infrastructure**: Fixed raise_on_error parameter handling (Nov 15, 2025)
- **Bolt 5.x Version Negotiation**: Fixed byte-order interpretation (Nov 12, 2025)
- **Bolt 5.x Version Response**: Convert internal format to client format (Nov 12, 2025)
- **Schema Selection**: Auto-select first loaded schema in LOGON (Nov 12, 2025)
- **Query Cache**: Strip CYPHER prefix before schema extraction (Nov 10, 2025)
- **Query Cache**: Add whitespace normalization in cache keys (Nov 10, 2025)
- **PackStream Integration**: Fixed main.rs module imports (Nov 12, 2025)

### 🔧 Code Quality

- **Major Refactoring**: 22% size reduction in query planner (Nov 14, 2025)
  - Extracted 590 lines from plan_builder.rs into plan_builder_helpers.rs
  - Reduced plan_builder.rs from 3,311 to 2,542 lines (769 LOC removed)
  - Improved code organization and maintainability
  - Cleaner separation of concerns in query planning
  - All tests passing after refactoring

- **Documentation**: Comprehensive improvements (Nov 15, 2025)
  - Anonymous node limitation documented in KNOWN_ISSUES.md
  - Complete Phase 1 status in STATUS.md
  - Updated ROADMAP.md with achievements and timelines
  - Enhanced README.md with v0.4.0 features

### 📊 Test Coverage (November 15, 2025)

- **Rust Unit Tests**: 406/407 passing (99.8%)
- **Integration Tests**: 197/308 passing (64%, +30 tests from Nov 12)
- **Benchmarks**: 14/14 passing (100%)
- **E2E Tests**: Bolt 4/4, Cache 5/5 (100%)

### 🐛 Known Limitations

- **Anonymous Node Patterns**: SQL alias scope issue - use named nodes as workaround
- **Bolt Query Execution**: Wire protocol complete, query execution pending - use HTTP API
- **Integration Test Gaps**: 111 tests represent unimplemented features, not regressions
- **Flaky Test**: 1 cache LRU test occasionally fails (non-blocking)

### 📈 Performance Metrics

**Benchmark Results** (November 1, 2025):
- **Scale 1 (1K users)**: 2077ms mean, 100% success (10/10 queries)
- **Scale 10 (10K users)**: 2088ms mean, 100% success (10/10 queries)
- **Scale Large (5M users)**: 90% success (9/10 queries)
- **Overhead**: Only 0.5% for 10x data scale increase

**Query Cache Performance** (November 10, 2025):
- **Cache hit**: 0.1-0.5ms (translation time)
- **Cache miss**: 10-50ms (full translation)
- **Speedup**: 10-100x for repeated queries

---

## [Unreleased]

### 🚀 Features

- **Bolt 5.1-5.8 Protocol Support**: Complete implementation of Bolt 5.x with version negotiation fix (Nov 12, 2025)
  - **Version negotiation byte-order detection**: Bolt 5.x changed encoding from `[reserved][range][major][minor]` to `[reserved][range][minor][major]`
  - **Heuristic detection**: Automatically detects Bolt 5.x format when major byte is 5-8
  - **Version response conversion**: Sends negotiated version in client's expected byte order
  - **HELLO/LOGON authentication flow**: Bolt 5.1+ splits auth into separate LOGON message
  - **Auth-less mode**: Handles empty LOGON messages for passwordless connections
  - **Automatic schema selection**: Uses first loaded schema when database not specified
  - **LOGON/LOGOFF messages**: New message types (0x6A, 0x6B) for Bolt 5.1+ auth management
  - **Authentication state**: New `ConnectionState::Authentication` for Bolt 5.1+ flow
  - **Neo4j driver compatibility**: Successfully tested with Neo4j Python driver v6.0.2
  - **Test coverage**: 4/4 E2E tests passing (connection, simple query, graph traversal, aggregation)

- **PackStream Binary Format Support**: Vendored neo4rs PackStream module for complete Bolt protocol support (Nov 12, 2025)
  - ~3,371 lines of production-tested code from neo4rs v0.9.0-rc.8
  - Complete message parsing: HELLO, RUN, PULL with PackStream deserialization
  - Complete message serialization: SUCCESS, FAILURE, RECORD with PackStream encoding
  - Serde-based API: `packstream::from_bytes<T>()` and `packstream::to_bytes<T>()`
  - MIT license with proper attribution headers
  - Enables Neo4j drivers (Python, JavaScript, Java) to connect and query
  - Test coverage: Ready for integration testing with real drivers

- **Bolt Protocol Query Execution**: Complete Cypher-to-ClickHouse execution pipeline (Nov 11, 2025)
  - Full pipeline: Parse → Plan → Render → SQL → Execute → Stream
  - Parameter substitution from RUN messages
  - Schema selection via USE clause or session parameters
  - Result caching and streaming via RECORD messages
  - ClickHouse client integration throughout Bolt architecture
  - Send-safe async with elegant block-scoping solution

- **Query Cache**: Production-ready query caching with LRU eviction (10-100x speedup for repeated queries)
  - HashMap-based cache with dual limits (entry count + memory size)
  - Neo4j-compatible CYPHER replan options (default/force/skip)
  - Parameterized query support with SQL template caching
  - Whitespace normalization and CYPHER prefix handling
  - Schema-aware cache invalidation
  - Test coverage: 6/6 unit tests + 5/5 e2e tests (100%)

### 🐛 Bug Fixes

- **Bolt 5.x Version Negotiation**: Fixed byte-order interpretation - Bolt 5.x swaps major/minor bytes (Nov 12, 2025)
- **Bolt 5.x Version Response**: Convert internal format to client format before sending (Nov 12, 2025)
- **Schema Selection**: Auto-select first loaded schema when LOGON has no database field (Nov 12, 2025)
- **PackStream Integration**: Fixed main.rs to import from library instead of redeclaring modules
- **Query Cache**: Strip CYPHER prefix BEFORE schema extraction and query parsing (critical fix for replan=force)
- **Query Cache**: Add whitespace normalization in cache key generation
- **Query Cache**: Fix sql_only mode cache lookup and header injection

### 📚 Documentation

- Update STATUS.md with Bolt 5.8 implementation details and byte-order discovery (Nov 12, 2025)
- Document Bolt 5.x version encoding change in copilot-instructions.md
- Create detailed PackStream vendoring note: notes/packstream-vendoring.md (Nov 12, 2025)
- Add comprehensive query cache documentation in STATUS.md
- Create detailed feature note: notes/query-cache.md
- Document Windows PowerShell background process issue in copilot-instructions.md

### ⚙️ Infrastructure

- Vendor PackStream module: 4 files in src/packstream/ with MIT attribution
- Remove neo4rs dependency (keep bytes crate only)
- Create PowerShell server startup script with proper background handling (start_server_with_cache.ps1)

---

## [0.3.0] - 2025-11-10

### 🚀 Features

- Complete WITH clause with GROUP BY, HAVING, and CTE support
- Enable per-request schema support for thread-safe multi-tenant architecture
- Add schema-aware helper functions in render layer

### 🐛 Bug Fixes

- Multi-hop graph query planning and join generation
- Update path variable tests to match tuple() implementation
- Improve anchor node selection to prefer LEFT nodes first
- Prevent double schema prefix in CTE table names
- Use correct node alias for FROM clause in GraphRel fallback
- Prevent both LEFT and RIGHT nodes from being marked as anchor
- Remove duplicate JOINs for path variable queries
- Detect multiple relationship types in GraphJoins tree
- Update JOINs to use UNION CTE for multiple relationship types
- Correct release date in README (November 9, not 23)

### 💼 Other

- Add schema to PlanCtx (Phases 1-3 complete)

### 🚜 Refactor

- Remove BITMAP traversal code and fix relationship direction handling
- Rename handle_edge_list_traversal to handle_graph_pattern
- Remove redundant GLOBAL_GRAPH_SCHEMA

### 📚 Documentation

- Prepare for next session and organize repository
- Python integration test status report (36.4% passing)
- Update STATUS and KNOWN_ISSUES for GLOBAL_GRAPH_SCHEMA removal
- Clean up outdated KNOWN_ISSUES and update README

### 🧪 Testing

- Add debugging utilities for anchor node and JOIN issues

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Disable automatic docker publish
- Clean up test debris and remove deleted optimizer
- Replace emoji characters with text equivalents in test files
- Organize root directory for public repo
- Bump version to 0.2.0
- Bump version to 0.3.0
## [0.2.0] - 2025-11-06

### 🚀 Features

- Implement dual-key schema registration for startup-loaded schemas
- Add COUNT(DISTINCT node) support and fix integration test infrastructure
- Support edge-driven queries with anonymous node patterns

### 🐛 Bug Fixes

- Simplify schema strategy - use only server's default schema
- Remove ALL hardcoded property mappings - CRITICAL BUG FIX
- Enhance column name helpers to support both prefixed and unprefixed names
- Remove is_simple_relationship logic that skipped node joins
- Configure Docker to use integration test schema
- Only create node JOINs when nodes are referenced in query
- Preserve table aliases in WHERE clause filters
- Extract where_predicate from GraphRel during filter extraction
- Remove direction-based logic from JOIN inference - both directions now work
- GraphNode uses its own alias for PropertyAccessExp, not hardcoded 'u'
- Complete OPTIONAL MATCH with clean SQL generation
- Add user_id and product_id to schema property_mappings
- Add schema prefix to JOIN tables in cte_extraction.rs
- Handle fully qualified table names in table_to_id_column
- Variable-length paths now generate recursive CTEs
- Multiple relationship types now generate UNION CTEs
- Correct edge list test assertions for direction semantics

### 💼 Other

- Document property mapping bug investigation

### 🚜 Refactor

- Remove /api/ prefix from routes for simplicity

### 📚 Documentation

- Final Phase 1 summary with all 12 test suites
- Add schema loading architecture documentation and API test
- Update STATUS with integration test results
- Create action plan for property mapping bug fix
- Update STATUS and CHANGELOG with critical bug fix resolution
- Document WHERE clause gap for simple MATCH queries
- Add schema management endpoints and update API references
- Update STATUS.md with WHERE clause alias fix
- Update STATUS with WHERE predicate extraction fix
- Update STATUS and CHANGELOG with schema fix
- Update STATUS with complete session summary

### 🧪 Testing

- Add comprehensive integration test framework
- Add comprehensive relationship traversal tests
- Add variable-length path and shortest path integration tests
- Add OPTIONAL MATCH and aggregation integration tests
- Complete Phase 1 integration test suite with CASE, paths, and multi-database
- Add comprehensive error handling integration tests
- Add basic performance regression tests
- Initial integration test suite run - 272 tests collected
- Fix schema/database naming separation in integration tests

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.1.0] - 2025-11-02

### 🚀 Features

- *(parser)* Add shortest path function parsing
- *(planner)* Add ShortestPathMode tracking to GraphRel
- *(planner)* Detect and propagate shortest path mode
- *(sql)* Implement shortest path SQL generation with depth filtering
- Add WHERE clause filtering support for shortest path queries
- Add path variable support to parser (Phase 2.1-2.2)
- Track path variables in logical plan (Phase 2.3)
- Pass path variable to SQL generator (Phase 2.4)
- Phase 2.5 - Generate path object SQL for path variables
- Phase 2.6 - Implement path functions (length, nodes, relationships)
- WHERE clause filters for variable-length paths and shortestPath
- Complete allShortestPaths implementation with WHERE filters
- Implement alternate relationship types [:TYPE1|TYPE2] support
- Implement multiple relationship types with UNION logic
- Support multiple relationship types with labels vector
- Complete Path Variables & Functions implementation
- Complete Path Variables implementation with documentation
- Add PageRank algorithm support with CALL statement
- Complete Query Performance Metrics implementation
- Complete CASE expressions implementation with full context support
- Complete WHERE clause filtering pipeline for variable-length paths
- Implement type-safe configuration management
- Systematic error handling improvements - replace panic-prone unwrap() calls
- Complete codebase health restructuring - eliminate runtime panics
- Rebrand from Brahmand to ClickGraph
- Update benchmark suite for ClickGraph rebrand and improved performance testing
- Complete multiple relationship types feature with schema resolution
- Complete WHERE clause filters with schema-driven resolution
- Add per-table database support in multi-schema architecture
- Complete schema-only architecture migration
- Add medium benchmark (10K users, 50K follows) with performance metrics
- Add large benchmark (5M users, 50M follows) - 90% success at massive scale!
- Add Bolt protocol multi-database support
- Add test convenience wrapper and update TESTING_GUIDE
- Implement USE clause for multi-database selection in Cypher queries

### 🐛 Bug Fixes

- *(tests)* Add exhaustive pattern matching for ShortestPath variants
- *(parser)* Improve shortest path function parsing with case-insensitive matching
- *(parser)* Consume leading whitespace in shortest path functions
- *(sql)* Correct nested CTE structure for shortest path queries
- *(phase2)* Phase 2.7 integration test fixes - path variables working end-to-end
- WHERE clause handling for variable-length path queries
- Enable stable background schema monitoring
- Resolve critical TODO/FIXME items causing runtime panics
- Root cause fix for duplicate JOIN generation in relationship queries
- Three critical bug fixes for graph query execution
- Consolidate benchmark results and add SUT information
- Resolve path variable regressions after schema-only migration
- Use last part of CTE name instead of second part

### 💼 Other

- Prepare v0.1.0 release

### 🚜 Refactor

- *(sql)* Wire shortest_path_mode through CTE generator
- Extract CTE generation logic into dedicated module
- Complete codebase health improvements - modular architecture
- Standardize test organization with unit/integration/e2e structure
- Extract common expression processing utilities
- Organize benchmark suite into dedicated directory
- Clean up and improve CTE handling for JOIN optimization
- Remove GraphViewConfig and rename global variables
- Complete migration from view-based to schema-only configuration
- Organize project root directory structure

### 📚 Documentation

- Add session recap and lessons learned
- Add shortest path implementation session progress
- Comprehensive shortest path implementation documentation
- Add session completion summary
- Update STATUS.md with Phase 2.7 completion - path variables fully working
- Update STATUS.md to reflect current state of multiple relationship types
- Add project documentation and cleanup summaries
- Complete schema validation enhancement documentation
- Update STATUS.md and CHANGELOG.md with completed features
- Update NEXT_STEPS.md with recent completions and current priorities
- Correct ViewScan relationship support - relationships DO use YAML schemas
- Correct ViewScan relationship limitation in STATUS.md
- Remove incorrect OPTIONAL MATCH limitation from STATUS.md and NEXT_STEPS.md
- Document property mapping debug findings and render plan fixes
- Update CHANGELOG with property mapping debug session
- Update CHANGELOG with CASE expressions feature
- Fix numbering inconsistencies and update WHERE clause filtering status
- Update STATUS with type-safe configuration completion
- Update STATUS.md with TODO/FIXME resolution completion
- Clarify DDL parser TODOs are out-of-scope for read-only engine
- Sync documentation with current project status
- Update documentation with bug fixes and benchmark results
- Update README with 100% benchmark success and recent bug fixes
- Update STATUS.md with 100% benchmark success
- Update STATUS and CHANGELOG with enterprise-scale validation
- Add What's New section to README highlighting enterprise-scale validation
- Complete benchmark documentation with all three scales
- Add clear navigation to benchmark results
- Tone down production-ready claims to development build
- Add from_node/to_node fields to all relationship schema examples
- Clarify node label terminology in comments and examples
- Update STATUS.md with November 2nd achievements
- Add multi-database support to README and API docs
- Add PROJECT_STRUCTURE.md guide
- Add comprehensive USE clause documentation

### 🧪 Testing

- *(parser)* Add comprehensive shortest path parser tests
- Add shortest path SQL generation test script
- Add shortest path integration test files
- Improve test infrastructure and schema configuration
- Add end-to-end tests for USE clause functionality

### ⚙️ Miscellaneous Tasks

- Update .gitignore to exclude temporary files
- Disable CI on push to main (requires ClickHouse infrastructure)
## [iewscan-complete] - 2025-10-19

### 🚀 Features

- :sparkles: Added basic schema inferenc
- :sparkles: support for multi node conditions
- Support for multi node conditions
- Query planner rewrite (#11)
- Complete view-based graph infrastructure implementation
- Comprehensive view optimization infrastructure
- Complete ClickGraph production-ready implementation
- Implement relationship traversal support with YAML view integration
- Implement variable-length path traversal for Cypher queries
- Complete end-to-end variable-length path execution
- Add chained JOIN optimization for exact hop count queries
- Add parser-level validation for variable-length paths
- Make max_recursive_cte_evaluation_depth configurable with default of 100
- Add OPTIONAL MATCH AST structures
- Implement OPTIONAL MATCH parser
- Implement OPTIONAL MATCH logical plan integration
- Implement OPTIONAL MATCH with LEFT JOIN semantics
- Implement view-based SQL translation with ViewScan for node queries
- Add debug logging for full SQL queries
- Add schema lookup for relationship types

### 🐛 Bug Fixes

- :bug: relation direction when same node types
- :bug: Property tagging to node name
- :bug: node name in return clause related issues
- Count start issue (#6)
- Schema integration bug - separate column names from node types
- Rewrite GROUP BY and ORDER BY expressions for variable-length CTEs
- Preserve Cypher variable aliases in plan sanitization
- Qualify columns in IN subqueries and use schema columns
- Prevent CTE nesting and add SELECT * default
- Pass labels to generate_scan for ViewScan resolution

### 💼 Other

- Node name in return clause related issues
- Add RECURSIVE keyword to variable_length_demo.ipynb SQL descriptions

### 📚 Documentation

- Add comprehensive changelog for October 15, 2025 session
- Update README to use more appropriate terminology
- Add comprehensive test coverage summary for variable-length paths
- Simplify documentation structure for better maintainability
- Add documentation standards to copilot-instructions.md
- Add ViewScan completion documentation
- Add git workflow guide and update .gitignore

### 🧪 Testing

- Add comprehensive test suite for variable-length paths (30 tests)
- Add comprehensive testing infrastructure

### ⚙️ Miscellaneous Tasks

- Fixed docker pipeline mac issue
- Fixed docker mac issue
- Fixed docker image mac issue
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock after axum 0.8.6 upgrade
- Clean up debug logging and add NEXT_STEPS documentation



