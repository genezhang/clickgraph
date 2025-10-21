# Changelog

## [Unreleased] - 2025-10-18

### 🚀 Features

- **Path Variables & Functions** (Oct 21): Complete implementation for path variables and functions
  - Parser: `MATCH p = (a)-[:TYPE*]-(b)` syntax parsing ✅
  - Path functions: `length(p)`, `nodes(p)`, `relationships(p)` ✅
  - CTE integration: `hop_count` and `path_nodes` columns in recursive CTEs ✅
  - SQL mapping: `length(p)` → `hop_count`, `nodes(p)` → `path_nodes` ✅
  - End-to-end testing: Path queries execute successfully ✅
  - See `notes/path-variables.md` for implementation details

- **Alternate Relationship Types** (Oct 21): Complete implementation for `[:TYPE1|TYPE2]` multiple relationship patterns
  - Parser: Extended relationship pattern parsing to handle multiple labels separated by `|`
  - Logical planning: GraphRel.labels stores Vec<String> for multiple relationship types
  - SQL generation: UNION ALL CTEs for multiple relationship types in render plan
  - Query execution: `MATCH (a)-[:FOLLOWS|FRIENDS_WITH]->(b)` generates UNION SQL
  - Backward compatibility: Single relationship types work unchanged
  - Tests: 44/44 render plan tests passing, new comprehensive test coverage
  - See `notes/alternate-relationship-types.md` for implementation details

- **Shortest Path Queries** (Oct 18-20): Complete implementation for `shortestPath()` and `allShortestPaths()`
  - Parser: Case-insensitive matching for `shortestPath((a)-[:TYPE*]-(b))`
  - Query planner: ShortestPath/AllShortestPaths pattern handling
  - SQL generation: Nested CTE structure with hop count tracking
  - `shortestPath()`: `ORDER BY hop_count ASC LIMIT 1` (single shortest)
  - `allShortestPaths()`: `WHERE hop_count = MIN(hop_count)` (all shortest)
  - Cycle detection with `NOT has(path_nodes, node.id)`
  - **WHERE clause filtering**: Applied in base case of recursive CTEs ✅ **[FIXED: Oct 20]**
  - Integration: Queries execute successfully against ClickHouse
  - Tests: 18/18 WHERE filter tests passing, allShortestPaths SQL generation verified
  - 267/268 tests passing (99.6% coverage)
  - See `notes/shortest-path.md` for implementation details and debugging story

- **ViewScan Implementation**: View-based SQL translation for Cypher node queries
  - Label-to-table resolution via YAML schema (GLOBAL_GRAPH_SCHEMA)
  - Table alias propagation through ViewTableRef
  - Graceful fallback to regular Scan operations
  - Simple node queries fully working: `MATCH (u:User) RETURN u.name` ✅
  - 261/262 tests passing (99.6% coverage)

- **OPTIONAL MATCH Support**: Full implementation of LEFT JOIN semantics for optional graph patterns
  - Two-word keyword parsing (`OPTIONAL MATCH`)
  - Optional alias tracking in query planner
  - Automatic LEFT JOIN generation in SQL
  - 11/11 tests passing (100% coverage)
  - Complete documentation in `docs/optional-match-guide.md`

- **Variable-Length Path Queries**: Complete implementation with recursive CTEs (Oct 15)
  - All path patterns supported: `*1`, `*1..3`, `*..5`, `*2..`
  - Recursive CTE generation with `WITH RECURSIVE` keyword
  - Property selection in paths (two-pass architecture)
  - Cycle detection with array-based path tracking
  - Multi-hop traversals tested up to *1..3
  - Configurable recursion depth (10-1000 via CLI/env)
  - Schema integration with YAML column mapping
  - **Chained JOIN optimization** (Oct 17): 2-5x faster for exact hop counts (`*2`, `*3`)
  - 374/374 tests passing (100%)

- **YAML Schema Improvements**: Fixed label and type_name field handling
  - Server now uses `node_mapping.label` instead of HashMap keys
  - Relationship `from_node_type`/`to_node_type` properly loaded from YAML
  - Schema loads correctly with User nodes and FRIENDS_WITH relationships
  - Fixed RelationshipSchema column name vs node type separation (Oct 15)
  - Added `from_column`/`to_column` fields to RelationshipSchema


### 📚 Documentation

- Added `notes/shortest-path.md` - Implementation details, debugging story, known limitations (Oct 18)
- Updated `STATUS.md` - Added shortest path to working features with limitations note (Oct 18)
- Updated `CHANGELOG.md` - Documented shortest path implementation (Oct 18)
- Added `STATUS.md` - Single source of truth for current project state
- Added `notes/viewscan.md` - ViewScan implementation details
- Simplified documentation structure (3 core docs + feature notes)
- Archived historical session summaries to `archive/`
- Added `KNOWN_ISSUES.md` - Issue tracking with workarounds (Oct 15)
- Added `docs/optional-match-guide.md` - Comprehensive OPTIONAL MATCH feature guide
- Updated `README.md` - Added OPTIONAL MATCH examples and Windows warnings
- Updated `.github/copilot-instructions.md` - Windows constraints and OPTIONAL MATCH status

### 🐛 Bug Fixes

- **Shortest Path Nested CTE Bug (Oct 18)**: Fixed malformed SQL generation
  - Issue: CTE name wrapper applied twice, creating `cte_inner AS (cte AS (...))`
  - Fix: Generate query body without wrapper, apply appropriate wrapper based on mode
  - Result: Clean nested CTE structure that ClickHouse accepts
  - Commit: 53b4852
- **Shortest Path Parser Whitespace Bug (Oct 18)**: Fixed integration parsing failure
  - Issue: Parser expected no leading space, but `MATCH` consumed left space after keyword
  - Fix: Added `multispace0` at start of parser tuples to consume leading whitespace
  - Unit tests passed but integration failed - lesson in testing full pipeline
  - Commit: d7ebe6d
- **Windows Server Crash (Oct 17)**: Fixed critical crash on HTTP requests
  - Server now runs reliably on native Windows
  - Verified with 20+ consecutive request stress tests
  - Root cause: State initialization order during config refactor
- **GROUP BY with Variable-Length Paths (Oct 17)**: Fixed SQL generation for aggregations
  - GROUP BY/ORDER BY expressions now correctly reference CTE columns
  - Resolved "Unknown expression identifier" errors
- **Schema Integration Bug (Oct 15)**: Fixed RelationshipSchema column name confusion
  - Separated column names (`user1_id`) from node types (`User`)
  - Fixed variable-length path queries with properties
  - Resolved "Unknown identifier 'rel.node'" ClickHouse error
- Fixed YAML schema loading to use proper label/type_name fields
- Fixed relationship from/to node type mapping in graph_catalog.rs
- Fixed test_traverse_node_pattern_new_node to accept ViewScan or Scan

### 🧪 Testing

- Test suite: 261/262 passing (99.6%)
- End-to-end validation with real ClickHouse queries
- Variable-length path queries verified with 3 users, 3 friendships (Oct 15)
- Test data creation with Windows Memory engine constraint
- 11/11 OPTIONAL MATCH-specific tests (100%)
- Only failure: test_version_string_formatting (Bolt protocol, cosmetic)

### ⚙️ Infrastructure

- **HTTP Bind Error Handling**: Added descriptive error messages for port conflicts
- **Logging Framework**: Integrated env_logger for structured logging (RUST_LOG support)
- **Development Tools**: Batch files and PowerShell scripts for server startup
- **Environment Documentation**: DEV_ENVIRONMENT_CHECKLIST.md with Docker cleanup procedures
- Documented Windows environment constraints (Docker volume permissions, curl alternatives)
- Created `setup_test_data.sql` for test data with Memory engine


## [0.0.4] - 2025-09-18

### 🚀 Features

- Query planner rewrite (#11)

### 🐛 Bug Fixes

- Count start issue (#6)

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.0.3] - 2025-06-29

### 🚀 Features

- :sparkles: support for multi node conditions
- Support for multi node conditions

### 🐛 Bug Fixes

- :bug: relation direction when same node types
- :bug: Property tagging to node name
- :bug: node name in return clause related issues

### 💼 Other

- Node name in return clause related issues

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
## [0.0.2] - 2025-06-27

### 🚀 Features

- :sparkles: Added basic schema inferenc

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.0.1] - 2025-05-28

### ⚙️ Miscellaneous Tasks

- Fixed docker pipeline mac issue
- Fixed docker mac issue
- Fixed docker image mac issue
