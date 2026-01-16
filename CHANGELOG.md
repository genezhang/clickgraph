## [Unreleased]

### ⚙️ Refactoring

- **Schema Consolidation Phase 1**: Eliminated scattered `is_denormalized` conditionals from analyzer passes
  - Migrated property resolution logic to use `NodeAccessStrategy` enum pattern
  - Refactored `projection_tagging.rs` to use strategy matching with schema fallback
  - Updated `filter_tagging.rs` with hybrid strategy + schema approach
  - Maintained `projected_columns_resolver.rs` NodeAccessStrategy usage
  - All 766 library tests + integration tests passing

### 🚀 Features

- **CTE Unification Phase 3 Complete**: Unified recursive CTE generation across all schema patterns with comprehensive test coverage
  - TraditionalCteStrategy: Standard node/edge table patterns
  - DenormalizedCteStrategy: Single-table denormalized schemas
  - FkEdgeCteStrategy: Hierarchical FK relationships  
  - MixedAccessCteStrategy: Hybrid embedded/JOIN access patterns
  - EdgeToEdgeCteStrategy: Multi-hop denormalized edge-to-edge patterns
  - CoupledCteStrategy: Coupled edges in same physical row
- **Parameter Extraction Complete**: All CTE strategies now properly extract parameters from WHERE clause filters for SQL parameterization

## [0.6.1] - 2026-01-13

### 🚀 Features

- **Neo4j-compatible field aliases**: RETURN clause now preserves exact expression text as field names when AS alias not specified (matches Neo4j behavior)

- Integrate data_security schema, remove benchmark schemas from unified tests
- Auto-load all test schemas at session start
- Add PatternGraphMetadata POC for cleaner join inference evolution
- Phase 1 - Use cached node references from PatternGraphMetadata
- *(graph_join_inference)* Phase 2 - Simplified cross-branch detection using metadata
- *(graph_join_inference)* Phase 4 - Add relationship uniqueness constraints
- Complete fixed-length path inline JOIN optimization
- Property pruning optimization with unified test infrastructure
- Edge constraints for cross-node validation (8/8 tests passing)
- Pattern Comprehensions and Multiple UNWIND support
- Add multi-schema YAML support for loading multiple graph schemas
- Add multi-schema database setup and test scripts
- Add array subscript syntax support and complete multi-type VLP path functions
- Make MAX_INFERRED_TYPES configurable via query parameter

### 🐛 Bug Fixes

- Support anonymous nodes in graph patterns
- Use node ID columns for VLP CTE generation
- Optimize JOIN generation based on property usage, not node naming
- Optimize JOIN generation based on property usage, not node naming
- Permanently fix test infrastructure issues
- Add filesystem and group membership test data to setup script
- Add small-scale benchmark test data and cleanup obsolete scripts
- Migrate from schema_name='default' to USE clause convention
- Add missing matrix test schemas and USE clause support
- Add USE clause to multi-hop pattern tests
- Update social_polymorphic schema to use actual table names
- Resolve ontime schema name conflict, add benchmark schemas back for matrix tests
- Add flights to default db for ontime_benchmark - Copy flights to default database - Comprehensive matrix: +256 tests - Overall: +186 tests to 2947 - Session total: +1047 tests (+55 percent)
- Restore ontime_flights schema name for pattern matrix tests - Revert ontime_denormalized back to ontime_flights - Remove ontime_benchmark from unified test loading - Update matrix conftest to use ontime_flights - Pattern schema matrix: 0/51 to 9/51 recovery - Overall: 2758 to 2958 (+200 tests) - Session: 1900 to 2958 (+1058 tests, +55.7 percent, 85.2 percent pass rate)
- Add property_expressions schema to test loading - Fix database to default where tables actually exist - Replace CASE WHEN with if() for parsing compatibility - Add to load_test_schemas.py - Property expressions tests: 0/28 to 13/28 recovery - Overall: 2958 to 2976 (+18 tests) - Session: 1900 to 2976 (+1076 tests, +56.6 percent, 85.7 percent pass rate)
- Add schema_name to role-based query tests - Role tests now use unified_test_schema - All 5 role-based tests now pass
- Add missing property aliases to property_expressions schema
- VLP cross-branch JOIN uses node alias instead of relationship alias
- VLP transitivity check handles polymorphic relationships
- All integration tests now passing or properly marked xfail
- Add relationship labels to edge list test GraphRel structures
- Update edge list test assertions for SingleTableScan optimization
- Add proper GraphSchema to failing tests
- Thread schema through single-hop query pipeline for edge constraints
- *(vlp)* Fix denormalized VLP node ID selection (Dec 22 regression)
- *(vlp)* Complete denormalized VLP with comprehensive fixes
- VLP path functions in WITH clauses + CTE body rewriting
- Remove escaped quotes and multi_schema loader entry from conftest
- Load denormalized_flights_test schema with proper data
- VLP WHERE clause alias resolution for denormalized schemas
- Correct AUTHORED relationship schema in unified_test_multi_schema.yaml
- Multi-type VLP architectural fix - FROM alias solves all mapping issues
- Multi-type VLP JSON extraction - skip alias mapping for multi-type CTEs
- FK-edge zero-length VLP edge tuple generation
- Unify MAX_INFERRED_TYPES default to 5 for consistency
- Parameterized views apply to both node and edge tables in VLP queries
- Add anyLast() wrapping for CTE references in GROUP BY aggregations
- Rewrite CTE column references in JOINs
- VLP+WITH+MATCH pattern (ic9) - delegate to input.extract_joins() for CTE references
- Add VLP endpoint detection in find_id_column_for_alias
- Correct ontime_denormalized schema to use default database
- Skip JOINs for fully denormalized VLP patterns
- Map denormalized VLP endpoint aliases to CTE alias for rewriting
- Consecutive MATCH with per-MATCH WHERE, comment support, scalar aggregate investigation
- WITH expression scope - rewrite CASE expressions to use CTE columns

### 💼 Other

- Comprehensive test failure categorization (507 failures)
- V0.6.1 - WITH clause fixes, GraphRAG enhancements, LDBC progress
- Update Cargo.lock for v0.6.1 release

### 🚜 Refactor

- *(graph_join_inference)* Phase 3 - Break up infer_graph_join() god method
- [**breaking**] Migrate all integration tests to multi-schema format
- [**breaking**] Remove obsolete unified_test_schema and cleanup
- Consolidate denormalized_flights schema references

### 📚 Documentation

- Update README.md with v0.6.0 and accumulated features
- Update KNOWN_ISSUES.md with v0.6.0 fixes
- Archive wiki for v0.6.0 release
- Add release notes for v0.6.0
- Fix ClickHouse function prefix (ch./chagg. not clickhouse.)
- Fix composite node ID example (use nodes not edges)
- Update STATUS and investigation plan with anonymous node fix
- Update STATUS with property usage optimization and current test status
- Complete test infrastructure documentation
- Update STATUS with schema loading fix
- Update STATUS - ALL INTEGRATION TESTS PASSING! 🎉
- Add comprehensive architecture analysis for Scan/ViewScan/GraphNode relationships
- Update gap analysis - Gap #2 already implemented
- Add schema testing requirements (VLP multi-schema mandate)
- Add VLP denormalized property handling TODO
- Add session findings and feature analysis
- Clean up KNOWN_ISSUES.md and add path function limitation
- Update CHANGELOG and test infrastructure for VLP fixes
- Add multi-schema configuration documentation
- Add multi-schema setup guide
- Update TESTING.md for multi-schema architecture
- Update STATUS.md - remove load_test_schemas.py reference
- Add VS Code terminal freeze prevention to TESTING.md
- Document VLP WHERE clause bug discovery
- Update Cypher-Subgraph-Extraction.md with verified pattern support matrix
- Document max_inferred_types feature and update default to 5
- Update STATUS with LDBC progress and IC-9 CTE naming issue
- Systematic documentation cleanup and reorganization
- Streamline STATUS.md to focus on current state (2822 → 322 lines)
- LDBC benchmark baseline testing and analysis
- Update README test coverage to 3000+ tests and reorganize features
- Archive wiki documentation for v0.6.1 release

### 🧪 Testing

- Update test expectations for known limitations
- Add error message verification for known limitations
- *(graph_join_inference)* Add comprehensive unit tests for Phase 4 uniqueness constraints
- Add comprehensive VLP cross-functional testing
- Add comprehensive GraphRAG schema variation tests
- Add zero-length VLP tests for [*0..] and [*0..N] patterns

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Add lineage test schema and cleanup temporary files
- Move SCHEMA_THREADING_ARCHITECTURE.md to docs/development/
- Ignore docs1 directory in gitignore
- Clean up docs
- More doc cleanup
- More docs clean up, README
- Remove unused Flight node from unified_test_schema.yaml
- Update CHANGELOG.md [skip ci]
## [0.6.0] - 2025-12-22

### 🚀 Features

- *(functions)* Add 18 new Neo4j function mappings for v0.5.5
- *(functions)* Add 30 more Neo4j function mappings for v0.5.5
- *(functions)* Add ClickHouse function pass-through via ch:: prefix
- *(functions)* Add ClickHouse aggregate function pass-through via ch. prefix
- *(functions)* Add chagg. prefix for explicit aggregates, expand aggregate registry to ~150 functions
- *(benchmark)* Add LDBC SNB Interactive v1 benchmark
- *(benchmark)* Add ClickGraph schema matching datagen format
- *(benchmark)* Add LDBC query test script
- *(ldbc)* Achieve 100% LDBC BI benchmark (26/26 queries)
- Implement chained WITH clause support with CTE generation
- Support ORDER BY, SKIP, LIMIT after WITH clause
- Implement size() on patterns with schema-aware ID lookup
- Add composite node ID infrastructure for multi-column primary keys
- Add CTE reference validation
- CTE-aware variable resolution for WITH clauses
- Fix CTE column filtering and JOIN condition rewriting for WITH clauses
- CTE-aware variable resolution + WITH validation + documentation improvements
- Add lambda expression support for ClickHouse passthrough functions
- Add comprehensive LDBC benchmark suite with loading, query, and concurrency tests
- Implement scope-based variable resolution in analyzer (Phase 1)
- Remove dead CTE validation functions
- Implement CTE column resolution across all join strategies
- Remove obsolete JOIN rewriting code from renderer (Phase 3D-A)
- Move CTE column resolution to analyzer (Phase 3D-B)
- Pre-compute projected columns in analyzer (Phase 3E)
- Add CTE schema registry for analyzer (Phase 3F)
- Use pre-computed projected_columns in renderer (Phase 3E-B)
- Implement cross-branch shared node JOIN detection
- Allow disconnected comma patterns with WHERE clause predicates
- Support multiple sequential MATCH clauses
- Implement generic CTE JOIN generation using correlation predicates
- Complete LDBC SNB schema and data loading infrastructure
- Improve relationship validation error messages
- Clarify node_id semantics as property names with auto-identity mappings
- Complete composite node_id support (Phase 2)
- Add polymorphic relationship resolution architecture
- Complete polymorphic relationship resolution data flow
- Fix polymorphic relationship resolution in CTE generation
- Add Comment REPLY_OF Message schema definition
- Add schema entity collection in VariableResolver for Projection scope
- Add dedicated LabelInference analyzer pass
- Enhance TypeInference to infer both node labels and edge types
- Reduce MAX_INFERRED_TYPES from 20 to 5
- *(parser)* Add clear error messages for unsupported pattern comprehensions
- *(parser)* Add clear error messages for bidirectional relationship patterns
- *(parser)* Convert temporal property accessors to function calls
- *(analyzer)* Add UNWIND variable scope handling to variable_resolver
- *(analyzer)* Add type inference for UNWIND elements from collect() expressions
- Support path variables in comma-separated MATCH patterns
- Add polymorphic relationship resolution with node types
- Complete collect(node) + UNWIND tuple mapping & metadata preservation architecture
- Make CLICKHOUSE_DATABASE optional with 'default' fallback
- Add parser support for != (NotEqual) operator
- Add unified test schema for streamlined testing
- Add unified test data setup and fix matrix test schema issues
- Complete multi-tenant parameterized view support
- Add denormalized flights schema to unified test schema
- Add VLP transitivity check to prevent invalid recursive patterns

### 🐛 Bug Fixes

- *(benchmark)* Use Docker-based LDBC data generation
- *(benchmark)* Align DDL with actual datagen output format
- *(benchmark)* Add ClickHouse credentials support
- *(benchmark)* Align DDL and schema with actual datagen output
- *(ldbc)* Fix CTE pattern for WITH + table alias pass-through
- *(ldbc)* Fix ic3 relationship name POST_IS_LOCATED_IN -> POST_LOCATED_IN
- WITH+MATCH CTE generation for correct SQL context
- Replace all silent defaults with explicit errors in render_expr.rs
- Eliminate ViewScan silent defaults - require explicit relationship columns
- Expand WITH TableAlias to all columns for aggregation queries
- Track CTE schemas to build proper property_mapping for references
- Remove CTE validation to enable nested WITH clauses
- Prevent duplicate CTE generation in multi-level WITH queries
- Three-level WITH nesting with correct CTE scope resolution
- Add proper schemas to WITH/HAVING tests
- Correct CTE naming convention to use all exported aliases
- Coupled edge alias resolution for multiple edges in same table
- Rewrite expressions in intermediate CTEs to fix 4-level WITH queries
- Add GROUP BY and ORDER BY expression rewriting for final queries
- Issue #6 - Fix Comma Pattern and NOT operator bugs
- Resolve 3 critical LDBC query blocking issues
- *(ldbc)* Inline property matching & semantic relationship expansion
- *(ldbc)* Handle IS NULL checks on relationship wildcards (IS7)
- *(ldbc)* Fix size() pattern comprehensions - handle internal variables correctly (BI8)
- *(ldbc)* Rewrite path functions in WITH clause (IC1)
- Strip database prefixes from CTE names for ClickHouse compatibility
- Cartesian Product WITH clause missing JOIN ON
- Operator precedence in expression parser
- VLP endpoint JOINs with alias rewriting for chained patterns
- Correct NOT operator precedence and remove hardcoded table fallbacks
- Three critical shortestPath and query execution bugs
- Extend VLP alias rewriting to WHERE clauses for IC1 support
- Use correct CTE names for multi-variant relationship JOINs
- Remove database prefix from CTE table names in cross-branch JOINs
- Hoist trailing non-recursive CTEs to prevent nesting scope issues
- VLP + WITH label corruption bug - use node labels in RelationshipSchema
- Resolve compilation errors from AST and GraphRel changes
- Add fallback to lookup table names from relationship schema
- Complete RelationshipSchema refactoring - all 646 tests passing
- Add database prefixes to base table JOINs
- Use underscore convention for CTE column aliases
- Thread node labels through relationship lookup pipeline for polymorphic relationships
- Support filtered node views in relationship validation
- Add JOIN dependency sorting to CTE generation path
- Use existing TableCtx labels in multi-pattern MATCH label inference
- TypeInference creates ViewScan for inferred node labels
- QueryValidation respects parser normalization
- Populate from_id/to_id columns during JOIN creation for correct NULL checks
- *(ldbc)* Align BI queries with LDBC schema definitions
- Prevent RefCell panic in populate_relationship_columns_from_plan
- UNWIND after WITH now uses CTE as FROM table instead of system.one
- Replace all panic!() with log::error!() - PREVENT SERVER CRASHES
- Clean up unit tests - fix 21 compilation errors
- Complete unit test cleanup - fix assertions and mark unimplemented features
- Replace non-standard LIKE syntax with proper OpenCypher string predicates
- Add != operator support to comparison expression parser
- Preserve database prefix in ViewTableRef SQL generation
- Relationship variable expansion + consolidate property helpers
- Use relationship alias for denormalized edge FROM clause
- Re-enable selective cross-branch JOIN for comma-separated patterns
- Rel_type_index to prefer composite keys over simple keys
- WITH...MATCH pattern using wrong table for FROM clause
- Update test labels to match unified_test_schema
- Test_multi_database.py - use schema_name instead of database for USE clause
- Unify aggregation logic and fix multi-schema support
- Multi-table label bug fixes and error handling improvements

### 💼 Other

- Fix dependency vulnerabilities for v0.5.5
- Partial fix for nested WITH clauses - add recursive handling
- Multi-variant CTE column name resolution in JOIN conditions
- SchemaInference using table names instead of node labels

### 🚜 Refactor

- Fix compiler warnings and clean up unused variables
- *(functions)* Change ch:: to ch. prefix for Neo4j ecosystem compatibility
- Extract TableAlias expansion into helper functions
- Replace wildcard expansion in build_with_aggregation_match_cte_plan with helper
- Remove deprecated v1 graph pattern handler (1,568 lines)
- Extract CTE hoisting helper function
- Remove unused ProjectionKind::With enum variant
- Remove 676 lines of dead WITH clause handling code
- Remove 47 lines of dead GraphNode branch with empty property_mapping
- Remove redundant variable resolution from renderer (Phase 3A)
- Remove unused bidirectional and FK-edge functions
- Remove dead code function find_cte_in_plan
- Consolidate duplicate property extraction code (-23 lines)
- Remove dead extract_ctes() function (-301 lines)
- Separate graph labels from table names in RelationshipSchema
- Remove redundant WithScopeSplitter analyzer pass
- Remove old parsing-time label inference
- Consolidate inference logic into TypeInference with polymorphic support
- Replace hardcoded fallbacks with descriptive errors
- Add strict validation for system.one usage in UNWIND
- ELIMINATE ALL HARDCODED FALLBACKS - fail fast instead
- Consolidate test data setup - use MergeTree, remove duplicates

### 📚 Documentation

- Update wiki documentation for v0.5.4 release
- Archive wiki for v0.5.4 release
- Add UNWIND clause documentation to wiki
- Update v0.5.4 wiki snapshot with UNWIND documentation
- Update Known-Limitations with recently implemented features
- Update v0.5.4 wiki snapshot with corrected feature status
- Add 30 new functions to Cypher-Functions.md reference
- Expand vector similarity section with RAG usage
- Clarify scalar vs aggregate function categories in ch.* docs
- Add lambda expression limitation to ch.* pass-through documentation
- Split ClickHouse pass-through into dedicated doc for better discoverability
- Add comparison with PuppyGraph, TigerGraph, NebulaGraph
- Fix PuppyGraph architecture description
- Fix license - Apache 2.0, not MIT
- *(benchmark)* Update README with correct workflow and files
- Update KNOWN_ISSUES with accurate LDBC benchmark status
- Update STATUS.md and KNOWN_ISSUES.md for WITH clause improvements
- Add size() documentation and replace silent defaults with errors
- Document composite node ID feature
- Update STATUS.md with IC-1 fix and 100% LDBC benchmark
- Document WITH handler refactoring (120 lines eliminated)
- Identify remaining code quality hotspots after WITH refactoring
- Update STATUS and code quality analysis with v1 removal
- Add quality improvement plan and clarify parameter limitation
- Add comprehensive lambda expression documentation to Cypher Language Reference
- Reorganize lambda expressions as subsection of ClickHouse Function Passthrough
- Move lambda expressions details to ClickHouse-Functions.md
- Update LDBC benchmark analysis with accurate coverage (94% actionable)
- Add comprehensive LDBC data loading and persistence guide
- Add benchmark infrastructure completion summary
- Add benchmark quick reference card
- Update STATUS and CHANGELOG with predicate correlation
- Update STATUS and CHANGELOG for sequential MATCH support
- Update CHANGELOG and KNOWN_ISSUES for Issue #2 fix
- Update KNOWN_ISSUES - mark Issues #1, #3, #4 as FIXED
- Verify and update KNOWN_ISSUES - mark #5, #7 FIXED, detail #6 bugs
- Update KNOWN_ISSUES.md - Mark Issue #6 as FIXED
- Add LDBC benchmark audit tools and issue tracking
- Update STATUS.md with WHERE clause rewriting completion
- Document CTE database prefix fix in STATUS.md
- Add AI Assistant Integration via MCP Protocol
- Update STATUS.md with RelationshipSchema refactoring progress
- Update STATUS.md - RelationshipSchema refactoring complete (646/646 tests)
- Update STATUS and planning docs for node_id semantic clarification
- Update STATUS.md and KNOWN_ISSUES.md for database prefix fix
- Add database prefix fix to CHANGELOG.md
- Update QUERY_FIX_TRACKER with Dec 19 fixes
- Update STATUS, CHANGELOG, KNOWN_ISSUES for polymorphic relationship fix
- Update STATUS with polymorphic resolution progress
- Update STATUS.md with session summary
- Update STATUS with TypeInference ViewScan fix
- Update STATUS with QueryValidation fix - 70% LDBC passing
- Update CHANGELOG with Dec 19 achievements and cleanup root directory
- Analyze LDBC failures - 70% pass rate, identify 3 root causes
- Add LDBC benchmark configuration guide
- Correct bi-8/bi-14 root cause - pattern comprehensions not implemented
- Update KNOWN_ISSUES with parser improvements for pattern comprehensions
- Clarify CASE expression status - fully implemented
- Update all documentation with correct schema paths
- Add systematic test failure investigation plan
- Update STATUS and CHANGELOG with test infrastructure progress
- Mark relationship variable return bug as fixed
- Update STATUS and CHANGELOG for 24/24 zeek tests
- Update STATUS and CHANGELOG with test label fixes
- Document path function VLP alias bug in KNOWN_ISSUES

### ⚡ Performance

- Replace UUID-based CTE names with sequential counters

### 🎨 Styling

- Apply rustfmt formatting to entire codebase

### 🧪 Testing

- Update standalone relationship test for v2 behavior
- Add comprehensive WITH + advanced features test suite
- Add parameter tests for WITH clause combinations
- Add LDBC benchmark test scripts
- Add missing LDBC query parameters to audit script

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Remove dead code and fix all compiler warnings
- Hide internal documentation from public repo
- Keep wiki, images, and features subdirs external
- Remove internal documentation from repo
- Remove copilot instructions from public repo
- Remove debug output after nested CTE fix
- Add *.log to gitignore to prevent log file commits
- Comprehensive cleanup - standardize schemas and reorganize tests
- Remove duplicate setup_all_test_data.sh in scripts/setup/
- Release v0.6.0 - VLP transitivity check and bug fixes
## [0.5.4] - 2025-12-08

### 🚀 Features

- Add native support for self-referencing FK pattern
- Add relationship uniqueness enforcement for undirected patterns
- *(schema)* Add fixed-endpoint polymorphic edge support
- *(union)* Add UNION and UNION ALL query support
- Multi-table label support and denormalized schema improvements
- *(pattern_schema)* Add unified PatternSchemaContext abstraction - Phase 1
- *(graph_join_inference)* Integrate PatternSchemaContext - Phase 2
- *(graph_join_inference)* Add handle_graph_pattern_v2 - Phase 3
- *(pattern_schema)* Add FkEdgeJoin strategy for FK-edge patterns
- *(graph_join)* Wire up handle_graph_pattern_v2 with USE_PATTERN_SCHEMA_V2 env toggle

### 🐛 Bug Fixes

- GROUP BY expansion and count(DISTINCT r) for denormalized schemas
- Undirected multi-hop patterns generate correct SQL
- Support fixed-endpoint polymorphic edges without type_column
- Correct polymorphic filter condition in graph_join_inference
- Normalize GraphRel left/right semantics for consistent JOIN generation
- Recurse into nested GraphRels for VLP detection
- *(render_plan)* Add WHERE filters for VLP chained pattern endpoints (Issue #5)
- *(parser)* Reject binary operators (AND/OR/XOR) as variable names
- Multi-hop anonymous patterns, OPTIONAL MATCH polymorphic, string operators
- Aggregation and UNWIND bugs
- Denormalized schema query pattern fixes (TODO-1, TODO-2, TODO-4)
- Cross-table WITH correlation now generates proper JOINs (TODO-3)
- WITH clause alias propagation through GraphJoins wrapper (TODO-8)
- Multi-hop denormalized edge JOIN generation
- Update schema files to match test data columns
- *(pattern_schema)* Pass prev_edge_info for multi-hop detection in v2 path
- *(filter_tagging)* Correct owning edge detection for multi-hop intermediate nodes
- FK-edge JOIN direction bug - use join_side instead of fk_on_right
- Add polymorphic label filter generation for edges

### 🚜 Refactor

- Unify FK-edge pattern for self-ref and non-self-ref cases
- Minor code cleanup in bidirectional_union and plan_builder_helpers
- Make PatternSchemaContext (v2) the default join inference path
- Reorganize benchmarks into individual directories
- Replace NodeIdSchema.column with Identifier-based id field
- Change YAML field id_column to node_id for consistency
- Extract predicate analysis helpers to plan_builder_helpers.rs
- Extract JOIN and filter helpers to plan_builder_helpers.rs

### 📚 Documentation

- Update README for v0.5.3 release
- Add fixed-endpoint polymorphic edge documentation
- Add VLP+chained patterns docs and private security tests
- Document Issue #5 (WHERE filter on VLP chained endpoints)
- *(readme)* Minor wording improvements
- Update PLANNING_v0.5.3 and CHANGELOG with bug fix status
- Add unified schema abstraction proposal and test scripts
- Add unified schema abstraction Phase 4 completion to STATUS
- Update unified schema abstraction progress - Phase 4 fully complete
- *(benchmarks)* Add ClickHouse env vars and fix paths in README
- *(benchmarks)* Streamline README to be a concise index
- Archive PLANNING_v0.5.3.md - all bugs resolved

### 🧪 Testing

- Add multi-hop pattern integration tests
- Fix Zeek integration tests - response format and skip cross-table tests
- Add v1 vs v2 comparison test script
- Add unit tests for predicate analysis helpers

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Make test files use CLICKGRAPH_URL env var for port flexibility
- *(benchmarks)* Move social_network-specific files to subdirectory
## [0.5.3] - 2025-12-02

### 🚀 Features

- Add regex match (=~) operator and fix collect() function
- Add EXISTS subquery and WITH+MATCH chaining support
- Add label() function for scalar label return

### 🐛 Bug Fixes

- Remove unused schemas volume from docker-compose
- Parser now rejects invalid syntax with unparsed input
- Column alias for type(), id(), labels() graph introspection functions
- Update release workflow to use clickgraph binary name
- Update release workflow to use clickgraph-client binary name
- Build entire workspace in release workflow

### 📚 Documentation

- Archive wiki for v0.5.2 release
- Fix schema documentation and shorten README
- Fix Quick Start to include required GRAPH_CONFIG_PATH
- Add 3 new known issues from ontime schema testing
- Update KNOWN_ISSUES.md - WHERE AND now caught
- Clean up KNOWN_ISSUES.md - remove resolved issues
- Remove false known limitations - all verified working

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Release v0.5.3
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock for v0.5.3
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
## [0.5.2] - 2025-11-30

### 🚀 Features

- Add docker-compose.dev.yaml for development
- [**breaking**] Phase 1 - Fixed-length paths use inline JOINs instead of CTEs
- Add cycle prevention for fixed-length paths
- Restore PropertyValue and denormalized support from stash, integrate with anchor_table
- Complete denormalized query support with alias remapping and WHERE clause filtering
- Implement denormalized node-only queries with UNION ALL
- Support RETURN DISTINCT for denormalized node-only queries
- Support ORDER BY for denormalized UNION queries
- Fix UNION ALL aggregation semantics for denormalized node queries
- Variable-length paths for denormalized edge tables
- Add schema-level filter field with SQL predicate parsing
- Schema-level filters and OPTIONAL MATCH LEFT JOIN fix
- Add VLP + UNWIND support with ARRAY JOIN generation
- Implement coupled edge alias unification for denormalized patterns
- Implement polymorphic edge query support
- *(polymorphic)* Add VLP polymorphic edge filter support
- *(polymorphic)* Add IN clause support for multiple relationship types in single-hop
- Complete polymorphic edge support for wildcard relationship patterns
- Add edge inline property filter tests and update documentation
- Implement bidirectional pattern UNION ALL transformation

### 🐛 Bug Fixes

- ORDER BY rewrite bug for chained JOIN CTEs
- Zero-hop variable-length path support
- Remove ChainedJoinGenerator CTE for fixed-length paths
- Complete PropertyValue type conversions in plan_builder.rs
- Revert table alias remapping in filter_tagging to preserve filter context
- Eliminate duplicate WHERE filters by optimizing FilterIntoGraphRel
- Correct JOIN order and FROM table selection for mixed property expressions
- Ensure variable-length and shortest path queries use CTE path
- Destination node properties now map to correct columns in denormalized edge tables
- Multi-hop denormalized edge patterns and duplicate WHERE filters
- Variable-length path schema resolution for denormalized edges
- Add edge_id support to RelationshipDefinition for cycle prevention
- Fixed-length VLP (*1, *2, *3) now generates inline JOINs
- Fixed-length VLP (*2, *3) now works correctly
- Denormalized schema VLP property alias resolution
- VLP recursive CTE min_hops filtering and aggregation handling
- OPTIONAL MATCH + VLP returns anchor when no path exists
- RETURN r and graph functions (type, id, labels)
- Support inline property filters with numeric literals
- Push projections into Union branches for bidirectional patterns
- Polymorphic multi-type JOIN filter now uses IN clause

### 💼 Other

- Manual addition of denormalized fields (incomplete)

### 🚜 Refactor

- Simplify ORDER BY logic for inline JOINs
- Simplify GraphJoins FROM clause logic - use relationship table when no joins exist
- Store anchor table in GraphJoins, eliminate redundant find_anchor_node() calls
- Set is_denormalized flag directly in analyzer, remove redundant optimizer pass
- Move helper functions from plan_builder.rs to plan_builder_helpers.rs
- Rename co-located → coupled edges terminology
- Consolidate schema loading with shared helpers
- Consolidated VLP handling with VlpSchemaType

### 📚 Documentation

- Prioritize Docker Hub image in getting-started guide
- Update README with v0.5.1 Docker Hub release
- Add v0.5.2 planning document
- Update wiki Quick Start to use Docker Hub image with credentials
- Add Zeek network log examples and denormalized edge table guide
- Update STATUS.md with denormalized single-hop fix
- Update denormalized blocker notes with current status
- Update denormalized edge status to COMPLETE
- Add graph algorithm support to denormalized edge docs
- Add 0-hop pattern support to denormalized edge docs
- *(wiki)* Update denormalized properties with all supported patterns
- Add coupled edges documentation
- *(wiki)* Add Coupled Edges section to denormalized properties
- Add v0.5.2 TODO list for polymorphic edges and code consolidation
- Mark schema loading consolidation complete in TODO
- Update STATUS.md with polymorphic edge filter completion
- Add Schema-Basics.md and wiki versioning workflow
- Update documentation for v0.5.2 schema variations
- Update KNOWN_ISSUES.md with v0.5.2 status
- Update KNOWN_ISSUES.md with fixed-length VLP resolution
- Update KNOWN_ISSUES with VLP fixes and *0 pattern limitation
- Add Cypher Subgraph Extraction wiki with Nebula GET SUBGRAPH comparison
- Update README with v0.5.2 features

### 🎨 Styling

- Use UNION instead of UNION DISTINCT

### 🧪 Testing

- Add comprehensive Docker image validation suite
- Add comprehensive schema variation test suite (73 tests)

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
- Clean up root directory - remove temp files and organize Python tests
- Release v0.5.2
- Update CHANGELOG.md [skip ci]
- Update Cargo.lock for v0.5.2
## [0.5.1] - 2025-11-21

### 🚀 Features

- Add SQL Generation API (v0.5.1)
- Implement RETURN DISTINCT for de-duplication
- Add role-based connection pool for ClickHouse RBAC

### 🐛 Bug Fixes

- Eliminate flaky cache LRU eviction test with millisecond timestamps
- Replace docker_publish.yaml with docker-publish.yml
- Add missing distinct field to all Projection initializations

### 📚 Documentation

- Fix getting-started guide issues
- Update STATUS.md with fixed flaky test achievement (423/423 passing)
- Add /query/sql endpoint and RETURN DISTINCT documentation
- Add /query/sql endpoint and RETURN DISTINCT to wiki

### 🧪 Testing

- Add role-based connection pool integration tests

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Release v0.5.1
- Update CHANGELOG.md [skip ci]
## [0.5.0] - 2025-11-19

### 🚀 Features

- *(phase2)* Add tenant_id and view_parameters to request context
- *(phase2)* Thread tenant_id through HTTP/Bolt to query planner
- Implement SET ROLE RBAC support for single-tenant deployments
- *(multi-tenancy)* Add view_parameters field to schema config
- *(multi-tenancy)* Implement parameterized view SQL generation
- *(multi-tenancy)* Add Bolt protocol view_parameters extraction
- *(phase2)* Add engine detection for FINAL keyword support
- *(phase2)* Add use_final field to schema configuration
- *(phase2)* Add FINAL keyword support to SQL generation
- *(phase2)* Auto-schema discovery with column auto-detection
- *(auto-discovery)* Add camelCase naming convention support
- Add PowerShell scripts for wiki validation workflow
- Add Helm chart for Kubernetes deployment

### 🐛 Bug Fixes

- *(phase2)* Correct FINAL keyword placement - after alias
- *(tests)* Add missing engine and use_final fields to test schemas
- Implement property expansion for RETURN whole node queries
- Update clickgraph-client and add documentation

### 🚜 Refactor

- Minor code improvements in parser and planner

### 📚 Documentation

- Phase 2 minimal RBAC - parameterized views with multi-parameter support
- Fix Pattern 2 RBAC examples to use SET ROLE approach
- Add Phase 2 progress to STATUS.md
- Add comprehensive Phase 2 multi-tenancy status report
- *(multi-tenancy)* Complete parameterized views documentation + cleanup
- Update parameterized views note with cache optimization details
- *(phase2)* Complete Phase 2 multi-tenancy documentation and tests
- Correct Phase 2 status - 2/5 complete, not fully done
- Update ROADMAP.md Phase 2 progress - 2/5 complete
- *(phase2)* Update STATUS and CHANGELOG for FINAL syntax fix
- *(phase2)* Update STATUS and CHANGELOG for auto-schema discovery
- Align wiki examples with benchmark schema and add validation
- Add session documentation and planning notes
- Update STATUS, CHANGELOG, and KNOWN_ISSUES
- Update ROADMAP with wiki documentation and bug fix progress
- Mark Phase 2 complete - v0.5.0 release ready!

### ⚡ Performance

- *(cache)* Optimize multi-tenant caching with SQL placeholders

### 🧪 Testing

- Add comprehensive SET ROLE RBAC test suite
- *(multi-tenancy)* Add parameterized views test infrastructure
- *(multi-tenancy)* Add unit tests for view_parameters
- Add integration test utilities and schema

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Clean up temporary test output and debug files
## [0.4.0] - 2025-11-15

### 🚀 Features

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
