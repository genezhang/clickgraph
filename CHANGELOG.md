## [0.4.0] - 2025-11-15

### 🚀 Features

- **Add parameterized views for multi-tenancy** (Tasks 7-11 complete)
  - Schema configuration: `view_parameters` field in NodeDefinition/RelationshipDefinition
  - SQL generation: Generates `view_name(param='value')` syntax with injection protection
  - Bolt protocol support: Extract view_parameters from RUN message extra field
  - Unit tests: 7/7 passing (schema parsing, backward compat, multi-param, edge cases)
  - Test infrastructure: ClickHouse parameterized views + test data + Python test script
  - Documentation: Complete guide in `notes/parameterized-views.md`
  - Commits: 2d1cb04, 7ea4a05, 4ad7563, 8c21fca, a639049
- Add parameter support via HTTP API + identity fallback for properties
- Add production-ready query cache with LRU eviction
- Complete Bolt 5.8 protocol implementation with E2E tests passing
- Add Neo4j function support with 25+ function mappings
- Complete E2E testing infrastructure + critical bug fixes
- Unified benchmark architecture with scale factor parameter
- Adjust post ratio to 20 and add 2 post-related benchmark queries
- Add MergeTree engine support for large-scale benchmarks
- *(benchmark)* Complete MergeTree benchmark infrastructure, discover multi-hop query bug
- Add comprehensive regression test suite (799 tests)
- Add pre-flight checks to test runner
- Pre-load test_integration schema at server startup
- Implement undirected relationship support (Direction::Either)

### 🐛 Bug Fixes

- Multi-hop JOINs, SELECT aliases, SQL quoting + improve benchmark display
- Use correct schema and database for integration tests
- Start server without pre-loaded schema for integration tests
- IS NULL operator in CASE expressions (22/25 tests passing)
- Resolve compilation errors from API changes and incomplete cleanup
- Additional GraphSchema::build() signature fixes in test files
- Remove unused variable in view_resolver_tests.rs
- Update error handling tests to match actual ClickGraph behavior

### 🚜 Refactor

- Archive NEXT_STEPS.md in favor of ROADMAP.md
- Remove inherited DDL generation code (~1250 LOC)
- Remove bitmap index infrastructure (~200 LOC)
- Remove use_edge_list flag (~50 LOC)
- Flatten directory structure - remove brahmand/ wrapper
- Remove expression_utils dead code - visitor pattern + utility functions
- Convert CteGenerationContext to immutable builder pattern
- Create plan_builder_helpers module (preparatory step)
- Integrate plan_builder_helpers module
- Add deprecation markers to duplicate helper functions
- Complete deprecation markers for all helper functions (20/20)
- Remove all deprecated helper functions (~736 LOC, 22% reduction)
- Replace file-based debug logging with standard log::debug! macro

### 📚 Documentation

- Update KNOWN_ISSUES and copilot-instructions - all major issues resolved
- Add comprehensive ROADMAP with real-world features and prioritization
- Architecture decision - Use string substitution for parameters (not ClickHouse .bind())
- Update NEXT_STEPS.md roadmap with query cache completion
- Update README and ROADMAP with query cache completion
- Highlight parameter support in README and add usage restrictions
- Update ROADMAP.md with Bolt 5.8 completion
- Clarify anonymous node/edge pattern as TODO feature
- Document flaky cache LRU eviction test
- Document anonymous node SQL generation bug
- Change 'production-ready' to 'development-ready' for v0.4.0

### 🧪 Testing

- *(benchmark)* Add regression test script for CI/CD

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Complete v0.4.0 release preparation - Phase 1 complete
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
