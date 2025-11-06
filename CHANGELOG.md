## [Unreleased]

### 🚀 Features
- **OPTIONAL MATCH Complete** - Production-ready LEFT JOIN generation (Nov 4-5, 2025)
  - **Parser Fix**: Reordered clause parsing to recognize OPTIONAL MATCH clauses correctly
  - **SQL Generation**: Generates proper LEFT JOINs for optional graph patterns
  - **Clean Output**: No WHERE duplication, proper table prefixes on all tables
  - Test Status: ✅ 11/11 OPTIONAL MATCH parser tests (100%), ✅ 301/319 unit tests (94.4%)
  - Example: `MATCH (a) WHERE a.name='Alice' OPTIONAL MATCH (a)-[:FOLLOWS]->(b) RETURN a.name, b.name`
    ```sql
    SELECT a.name, b.name 
    FROM test_integration.users AS a 
    LEFT JOIN test_integration.follows AS r ON r.follower_id = a.user_id 
    LEFT JOIN test_integration.users AS b ON b.user_id = r.followed_id 
    WHERE a.name = 'Alice'
    ```

### 🐛 Bug Fixes
- **Missing ID Column in Schema** (Nov 5, 2025) **← High-Impact Fix**
  - Fixed: `WHERE u.user_id = 1` failing with "Property 'user_id' not found on node 'User'"
  - Root cause: Schema property_mappings missing ID columns (user_id, product_id)
  - Solution: Added `user_id: user_id` and `product_id: product_id` to test schema
  - Files: `tests/integration/test_integration.yaml`, `schemas/test/test_integration_schema.yaml`
  - Impact: +1 integration test (24/35 → 68.6%), enables all ID-based WHERE filters

- **WHERE Clause Duplication Fix** (Nov 5, 2025)
  - Fixed: `WHERE (a.name = 'Alice') AND (a.name = 'Alice')` duplication
  - Root cause: GraphRel.extract_filters over-collecting from left/center/right filters AND where_predicate
  - Solution: Only extract from where_predicate; node filters stay in ViewScans
  - File: `brahmand/src/render_plan/plan_builder.rs` (lines 1205-1220)

- **Missing Table Prefix Fix** (Nov 5, 2025)
  - Fixed: `FROM users` → `FROM test_integration.users` (full qualified names)
  - Root cause: SchemaInference only used table_name, ignored database field
  - Solution: Use `format!("{}.{}", node_schema.database, node_schema.table_name)`
  - File: `brahmand/src/query_planner/analyzer/schema_inference.rs` (lines 75-92)

- **OPTIONAL MATCH Parser Fix** (Nov 4, 2025)
  - Fixed: Parser wasn't recognizing OPTIONAL MATCH clauses at all (optional_match_clauses.len() = 0)
  - Root cause: Parser tried OPTIONAL MATCH before WHERE, but queries have WHERE between MATCH and OPTIONAL MATCH
  - Solution: Reordered parser: MATCH → WHERE → OPTIONAL MATCH → RETURN
  - File: `brahmand/src/open_cypher_parser/mod.rs`

- **DuplicateScansRemoving Fix** (Nov 4, 2025)
  - Fixed: Analyzer removing GraphRel nodes needed for OPTIONAL MATCH LEFT JOINs
  - Solution: Check `plan_ctx.is_optional(alias)` before removing duplicate scans
  - File: `brahmand/src/query_planner/analyzer/duplicate_scans_removing.rs`

### 🧪 Testing
- **Integration Test Data Setup**: Added setup script for test_integration database
  - Script: `scripts/setup/setup_integration_test_data.sql`
  - Creates: users, follows, products, purchases, friendships tables (Memory engine for Windows)
  - Run: `Get-Content scripts\setup\setup_integration_test_data.sql | docker exec -i clickhouse clickhouse-client --user test_user --password test_pass --multiquery`

### Breaking Changes
- **HTTP API Response Format**: Changed from bare array to object wrapper
  - Old: `[{"name": "Alice"}]` → New: `{"results": [{"name": "Alice"}]}`
  - Columns use simple property names (e.g., `"name"`) without alias prefixes
  - Update client code to access `response.results` or `response["results"]`

### � Features
- **WHERE Clause Support for Simple MATCH Queries**: Implemented WHERE clause filtering for ViewScan nodes
  - Enables filtering on simple MATCH queries: `MATCH (u:User) WHERE u.name = 'Alice' RETURN u`
  - Previously only worked for variable-length paths (GraphRel nodes)
  - Two-phase fix: Filter injection (optimizer) + SQL subquery generation
  - Modified `FilterIntoGraphRel` optimizer to handle ViewScan patterns
  - Enhanced SQL generator to wrap filtered ViewScans in subqueries
  - Test Status: ✅ 318/318 unit tests passing (100%)
  - See `notes/where-viewscan.md` for implementation details

### �🐛 Bug Fixes
- **CRITICAL FIX**: Removed hardcoded property mappings from `to_sql_query.rs` that were overriding schema-based property resolution
  - Fixed: `("u", "name") → "full_name"` hardcoded mapping causing 95% of test failures
  - Impact: Test pass rate improved from 0.4% (1/272) to 26% (5/19 in basic_queries)
  - Removed unused `map_property_to_column()` function from `cte_generation.rs`
  - All property mappings now correctly use schema configuration

### 🧪 Testing
- Created comprehensive integration test suite: 272 tests across 11 test files
- Test infrastructure validated: ClickGraph + ClickHouse connectivity working
- All basic MATCH tests now passing (test_match_all_nodes, test_match_with_label, test_match_with_alias)
- Test database: test_integration schema with users/follows tables

### 📚 Documentation
- Added schema loading architecture documentation
- Created dual-key schema registration implementation docs
- Documented property mapping bug investigation and fix
- Updated STATUS.md with critical bug fix details

## [0.1.0] - 2025-01-XX

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
