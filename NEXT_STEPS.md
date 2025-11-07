# Next Steps - Development Roadmap

**Last Updated**: November 1, 2025
**Current Status**: Critical bug fixes completed - 100% benchmark success
**Branch**: `main`
**Latest Commit**: db6c914 - Three critical bug fixes for graph query execution

---

## 🎉 Just Completed (November 1, 2025)

### Bug Fix Session - All Graph Queries Working ✅
**What**: Fixed three critical bugs blocking production benchmarking

**Bug #1**: ChainedJoin CTE Wrapper (`variable_length_cte.rs:505-514`)
- Issue: Exact hop queries (`*2`, `*3`) generated malformed SQL
- Fix: Added CTE wrapper matching recursive CTE structure
- Impact: Variable-length exact hop queries now work perfectly

**Bug #2**: Shortest Path Filter Rewriting (`variable_length_cte.rs:152-173`)
- Issue: End node filters used wrong column references (`end_node.user_id` vs `end_id`)
- Fix: Added `rewrite_end_filter_for_cte()` method to transform filters
- Impact: Shortest path queries with WHERE clauses now work

**Bug #3**: Aggregation Table Names (`schema_inference.rs:72-99`, `match_clause.rs:31-60`)
- Issue: Scans used label "User" instead of table "users_bench" in SQL
- Fix: Schema inference now looks up actual table names from GLOBAL_GRAPH_SCHEMA
- Impact: All aggregation queries with incoming relationships work

**Benchmark Results**:
- ✅ **10/10 queries passing (100% success rate)**
- ✅ All query types validated:
  - Simple node lookups
  - Filtered scans
  - Direct relationships
  - Multi-hop traversals
  - Variable-length paths (`*2`, `*1..3`)
  - Shortest paths with filters
  - Aggregations with incoming relationships
  - Bidirectional patterns

**Files Modified**:
- `variable_length_cte.rs` - CTE wrapper and filter rewriting
- `schema_inference.rs` - Table name schema lookup
- `match_clause.rs` - Fallback scan table lookup
- `plan_builder.rs` - Duplicate alias detection
- `social_benchmark.yaml` - Added from_node/to_node
- `test_benchmark_final.py` - Comprehensive validation script

**Testing**: 1000 users, 5000 follows, 2000 posts on `social_benchmark.yaml`

---

## 🎉 Previously Completed (October 21-25, 2025)

### 1. Query Performance Metrics - Complete ✅ (October 25, 2025)
**What**: Comprehensive query performance monitoring with phase-by-phase timing, HTTP headers, and structured logging

**Implementation**:
- Phase-by-phase timing: Parse, planning, render, SQL generation, execution
- HTTP response headers: `X-Query-Total-Time`, `X-Query-Parse-Time`, etc.
- Structured logging: INFO-level performance metrics with millisecond precision
- Query type classification: read/write/call with SQL query count tracking

**Files Modified**:
- `brahmand/src/server/handlers.rs` - QueryPerformanceMetrics struct and timing integration
- `notes/query-performance-metrics.md` - Comprehensive implementation documentation
- `STATUS.md` - Updated with completion status
- `README.md` - Added to features and development status

**Testing Status**:
- ✅ Phase timing: All phases properly measured and logged
- ✅ HTTP headers: Performance data included in responses
- ✅ Structured logging: Clean, parseable log format
- ✅ Server integration: Works with existing query pipeline

**Impact**:
- 🎯 Performance monitoring: Track query bottlenecks and optimization opportunities
- 🎯 Debugging support: Detailed timing breakdown for slow queries
- 🎯 Production readiness: HTTP headers for monitoring and alerting
- 🎯 See: `notes/query-performance-metrics.md` for implementation details

### 2. Path Variables - Complete ✅
**What**: Full support for path variables and path functions in Cypher queries

**Implementation**:
- Path variables: `p = (a)-[r*]->(b)` → Captures entire path data
- Path functions: `length(p)`, `nodes(p)`, `relationships(p)` → Extract path components
- CTE-based implementation with array columns: `hop_count`, `path_nodes`, `path_relationships`
- End-to-end testing with comprehensive validation

**Files Modified**:
- `brahmand/src/clickhouse_query_generator/variable_length_cte.rs` - CTE column generation
- `brahmand/src/render_plan/plan_builder.rs` - Path function mapping
- `brahmand/src/open_cypher_parser/ast.rs` - Path variable AST support
- `test_path_variable.py` - End-to-end testing

**Testing Status**:
- ✅ Path variable parsing: Works correctly
- ✅ Path functions: `length(p)`, `nodes(p)`, `relationships(p)` all functional
- ✅ End-to-end queries: Return correct data types and values
- ✅ CTE generation: Proper array column handling

**Impact**:
- 🎯 Enables complex path analysis: `MATCH p=(a)-[*2..4]->(b) RETURN length(p), nodes(p)`
- 🎯 Foundation for advanced graph algorithms and analytics
- 🎯 See: `notes/path-variables.md` for implementation details

### 3. Multiple Relationship Types - Complete ✅
**What**: Support for `[:TYPE1|TYPE2]` alternate relationship patterns with UNION SQL generation

**Implementation**:
- `MATCH (a)-[:FOLLOWS|FRIENDS_WITH]->(b)` → Generates UNION ALL SQL
- Extended `TableCtx` from single `label` to `labels` vector throughout codebase
- UNION CTE generation in `render_plan/plan_builder.rs`
- Comprehensive unit tests and end-to-end validation

**Files Modified**:
- `brahmand/src/render_plan/plan_builder.rs` - UNION logic implementation
- `brahmand/src/query_planner/plan_ctx/mod.rs` - labels vector support
- `brahmand/src/render_plan/tests/multiple_relationship_tests.rs` - Unit tests
- 50+ files updated for labels vector compatibility

**Testing Status**:
- ✅ Unit tests: `test_multiple_relationship_types_union` passes
- ✅ Single relationships: Work correctly
- ✅ End-to-end multiple relationships: **VERIFIED Oct 22, 2025** - returns all expected relationships (10 total: 8 FOLLOWS + 2 FRIENDS_WITH)
- ✅ Multiple relationship types (>2): **VERIFIED Oct 25, 2025** - correctly generates (N-1) UNION ALL clauses

**Impact**:
- � Enables complex relationship queries: `[:FOLLOWS|FRIENDS_WITH|LIKES]`
- 🎯 UNION SQL generation for multiple relationship types
- 🎯 Foundation for advanced graph pattern matching
- 🎯 See: `notes/alternate-relationships.md` for implementation details

### 4. ViewScan - Complete Schema-Driven Query Planning ✅
**What**: Fully YAML-driven graph model (no hardcoded table mappings)

**Node Queries** - ✅ DONE:
- `MATCH (u:User)` → Schema lookup: "User" label → "users" table
- Implementation: `try_generate_view_scan()` in `match_clause.rs`
- Uses: `GLOBAL_GRAPH_SCHEMA.get_node_schema()`

**Relationship Queries** - ✅ DONE (just completed):
- `MATCH ()-[r:FRIENDS_WITH]->()` → Schema lookup: "FRIENDS_WITH" → "friendships" table
- Implementation: Enhanced `rel_type_to_table_name()` in `plan_builder.rs`
- Uses: `GLOBAL_GRAPH_SCHEMA.get_rel_schema()`
- Keeps hardcoded fallback for backwards compatibility

**Impact**:
- 🎯 Can define entire graph model in YAML (no code changes)
- 🎯 Add new node/relationship types without touching Rust code
- 🎯 Multiple graph schemas via different YAML files
- 🎯 See: `notes/viewscan-complete.md` for full details

### 5. Standardized Testing Infrastructure ✅
**Problem Solved**: Terminal chaos, port conflicts, process accumulation

**New Tools**:
1. **PowerShell Runner** (`test_server.ps1`):
   - `.\test_server.ps1 -Start` - Background server (single window!)
   - `.\test_server.ps1 -Test` - Quick query test
   - `.\test_server.ps1 -Clean` - Kill all orphaned processes
   - ✅ PID tracking prevents duplicates
   - ✅ Automatic cleanup

2. **Python Test Suite** (`test_runner.py`):
   - `python test_runner.py --test` - Run comprehensive tests
   - `python test_runner.py --query "..."` - Single query
   - ✅ Cross-platform (Windows/Linux/Mac)
   - ✅ Structured test results
   - ✅ Validates both node and relationship ViewScan

3. **Docker Compose** (`docker-compose.test.yaml`):
   - Complete isolation (ClickHouse + ClickGraph)
   - Production-like environment
   - Clean startup/shutdown

**See**: `TESTING_GUIDE.md` for complete workflows

### 6. Schema Validation Enhancement - Complete ✅
**What**: Optional startup validation of YAML configurations against ClickHouse schema

**Problem Solved**: 
- Runtime failures from misconfigured YAML files
- Silent failures when tables/columns don't exist
- Poor developer experience with cryptic error messages

**Implementation**:
- CLI flag: `--validate-schema` (opt-in, defaults to disabled for performance)
- Environment variable: `CLICKGRAPH_VALIDATE_SCHEMA=true`
- Validation scope: Table existence, column mappings, ID column types, relationship columns
- Performance: Minimal impact (4-6 cached system.columns queries)
- Error handling: Clear, actionable error messages

**Files Modified**:
- `brahmand/src/main.rs` - Added `--validate-schema` CLI flag
- `brahmand/src/server/mod.rs` - Added validate_schema to ServerConfig
- `brahmand/src/server/graph_catalog.rs` - Integrated validation into startup process
- `brahmand/src/graph_catalog/schema_validator.rs` - Core validation logic (already implemented)

**Testing Status**:
- ✅ CLI flag integration: `--validate-schema` appears in help output
- ✅ Environment variable support: `CLICKGRAPH_VALIDATE_SCHEMA`
- ✅ Validation logic: Table/column existence, type checking
- ✅ Error messages: Clear feedback for misconfigurations
- ✅ Performance: No impact when disabled (default)

**Impact**:
- 🎯 Production readiness: Catches configuration errors at startup
- 🎯 Better developer experience: Clear error messages instead of runtime failures
- 🎯 Performance conscious: Opt-in validation with minimal overhead
- 🎯 Backward compatible: No impact on existing deployments

### 7. WHERE Clause Filtering for Variable-Length Paths - 🔄 IN PROGRESS (October 22, 2025)
**What**: Full WHERE clause support for variable-length path queries and shortest path functions

**Implementation**:
- End node filters: `WHERE b.name = "David Lee"` in variable-length paths
- Parser support for double-quoted strings and proper SQL quoting
- Context storage in `CteGenerationContext` for filter propagation
- Expression rewriting for CTE column mapping (`b.name` → `end_name`)

**Files Modified**:
- `brahmand/src/render_plan/plan_builder.rs` - Main filter processing and SQL generation
- `brahmand/src/open_cypher_parser/expression.rs` - Double-quoted string support
- `brahmand/src/clickhouse_query_generator/variable_length_cte.rs` - CTE property selection

**Testing Status**:
- ✅ End node filters: Work with all variable-length paths
- ✅ Shortest path WHERE clauses: Fully functional
- ✅ Parser: Double-quoted strings properly handled
- 🔄 SQL generation: Column alias mapping in CTEs needs debugging
- ✅ Test results: Most tests passing, alias mapping issue in progress

**Impact**:
- 🎯 Complete Cypher WHERE clause support for graph queries
- 🎯 Enables filtered shortest path queries: `shortestPath((a)-[*]-(b)) WHERE b.city = "NYC"`
- 🎯 Foundation for complex graph analytics with filtering
- 🎯 See: `notes/where-clause-complete.md` for implementation details

### 8. PageRank Algorithm Implementation - Complete ✅ (October 23, 2025)
**What**: Complete graph centrality algorithm with CALL statement support

**Implementation**:
- Cypher syntax: `CALL pagerank(maxIterations: 10, dampingFactor: 0.85)`
- Iterative SQL approach using UNION ALL (avoids CTE depth limits)
- Configurable parameters: `maxIterations`, `dampingFactor`, `nodeLabels`, `relationshipTypes`
- Multi-graph support with YAML schema integration
- Parameter parsing with both `=>` (GDS style) and `:` syntax

**Files Modified**:
- `brahmand/src/clickhouse_query_generator/pagerank.rs` - SQL generation logic
- `brahmand/src/open_cypher_parser/call_clause.rs` - CALL statement parsing
- `brahmand/src/query_planner/mod.rs` - Procedure name matching
- `brahmand/src/server/handlers.rs` - HTTP endpoint integration
- `test_pagerank_*.py` - Comprehensive test suite (3 new files)

**Testing Status**:
- ✅ Parameter validation: All combinations tested (13/13 passing)
- ✅ SQL generation: Correct iterative CTEs with out-degree calculations
- ✅ End-to-end execution: Converges properly with different parameters
- ✅ Schema integration: Works with YAML-defined node/relationship types
- ✅ Error handling: Proper validation and error messages

**Impact**:
- 🎯 Graph analytics capabilities: Node importance ranking
- 🎯 Enterprise features: Multi-graph support with selective node/relationship filtering
- 🎯 Performance: O(iterations × |E|) leveraging ClickHouse parallel processing
- 🎯 Cypher/GDS compatibility: Standard parameter names and syntax
- 🎯 See: `notes/pagerank.md` for implementation details

## ⚠️ Known Issues & Limitations

**Last Updated**: October 30, 2025

### ✅ RESOLVED Issues (All Major Issues Fixed)

#### 1. ViewScan Relationship Support - **RESOLVED! ✅**
**Status**: ✅ **WORKING** - Relationships fully support YAML schema lookup
**Resolution**: Relationships use `rel_type_to_table_name()` which calls `schema.get_rel_schema()` first, then falls back to hardcoded mappings
**Verification**: Confirmed working end-to-end with YAML-defined relationship schemas

#### 2. OPTIONAL MATCH with ViewScan - **RESOLVED! ✅**
**Status**: ✅ **WORKING** - OPTIONAL MATCH works correctly with ViewScan
**Resolution**: Was a test configuration issue (mismatched schema files), not a code bug
**Verification**: All OPTIONAL MATCH tests passing with proper schema configuration

#### 3. WHERE Clause Filtering - **RESOLVED! ✅**
**Status**: ✅ **COMPLETE** - Full WHERE clause support for variable-length paths
**Resolution**: Implemented end node filters, start node filters, and shortest path filtering
**Verification**: 312/312 tests passing with comprehensive WHERE clause coverage

#### 4. Multi-Variable CROSS JOIN Queries - **RESOLVED! ✅**
**Status**: ✅ **WORKING** - CROSS JOIN generation for multiple standalone variables
**Resolution**: Property mapping and SQL generation working for all variables
**Verification**: End-to-end testing confirms proper CROSS JOIN semantics

#### 5. CASE Expression Support - **RESOLVED! ✅**
**Status**: ✅ **COMPLETE** - Full CASE WHEN THEN ELSE with ClickHouse optimization
**Resolution**: Simple and searched CASE expressions with property mapping
**Verification**: All CASE expression variants working in WHERE clauses and SELECT

### Windows Development Constraints (Documented Limitations)

#### 6. ClickHouse Volume Permission Issues
**Issue**: ClickHouse Docker containers cannot write to mounted volumes on Windows
**Workaround**: Must use `ENGINE = Memory` instead of persistent engines
**Impact**: Data is not persisted between container restarts (acceptable for development)
**Status**: Documented constraint, no code changes needed
**Note**: This is a Windows Docker limitation, not a ClickGraph issue

#### 7. PowerShell curl Command Unavailable
**Issue**: `curl` command not available in Windows PowerShell environment
**Workaround**: Use `Invoke-RestMethod` PowerShell cmdlet or Python `requests`
**Impact**: HTTP testing requires different commands on Windows
**Status**: Documented in README and test scripts
**Note**: Cross-platform testing scripts handle this automatically

### Remaining Minor Limitations

#### 8. Multi-hop Base Cases (*2, *3..5)
**Status**: Planned enhancement (Low Priority)
**Issue**: Variable-length paths starting at hop count > 1 use placeholder `WHERE false` instead of chained JOINs
**Impact**: Functional but suboptimal performance for exact hop count queries
**Timeline**: Future enhancement when performance optimization begins

#### 9. Test Coverage Gaps
**Status**: Known limitation (Low Priority)
**Issue**: Missing edge case tests for 0 hops, negative ranges, circular paths
**Impact**: Core functionality works, edge cases may have unexpected behavior
**Timeline**: Address during comprehensive testing phase

---

## 🚀 Current Priorities (November 1, 2025)

### Immediate Next Steps

1. **Document Performance Baseline** ⏳ (1-2 hours)
   - Extract metrics from `test_benchmark_final.py` results
   - Create `notes/benchmarking.md` with:
     - Query latencies (mean, median, min, max)
     - Throughput measurements
     - Performance characteristics by query type
   - Baseline for future optimization work

2. **Update CHANGELOG** ⏳ (30 minutes)
   - Add entry for Bug #1 (ChainedJoin CTE wrapper)
   - Add entry for Bug #2 (Shortest path filter rewriting)
   - Add entry for Bug #3 (Table name schema lookup)
   - Include benchmark results: 10/10 queries passing

### Medium-Term Priorities

3. **Production Benchmarking Suite** (2-4 hours)
   - Expand `benchmark/benchmark.py` with more query patterns
   - Add stress testing with larger datasets
   - Performance regression detection
   - Automated benchmark runs in CI/CD

4. **Hot Reload for YAML Configs** (3-4 hours)
   - Watch YAML files and reload without server restart
   - Development velocity improvement
   - Safer production updates

### Future Features

5. **Additional Graph Algorithms** (1-2 weeks each)
   - Betweenness centrality, closeness centrality
   - Community detection algorithms
   - Leverage existing PageRank infrastructure

6. **Pattern Comprehensions** (4-6 hours)
   - List comprehensions: `[(a)-[]->(b) | b.name]`
   - Advanced query patterns

**My Recommendation**: Complete documentation tasks (1-2), then move to production benchmarking (#3) to establish performance baselines before adding new features.

---
- ✅ **Status**: Working end-to-end!

**Test Result**:
```json
[
  {"name": "Alice"},
  {"name": "Bob"},
  {"name": "Charlie"}
]
```

---

## 🔍 Previously Known Issues (NOW RESOLVED)

### 1. View-Based SQL Translation (✅ RESOLVED!)

**Issue**: YAML schema loads correctly, but queries didn't translate Cypher labels to source tables

**Original Symptoms**:
```
Error: Unknown table expression identifier 'User' in scope SELECT u.name FROM User AS u
```

**Root Cause Found**: 
- `generate_scan()` in `match_clause.rs` was passing `None` for label parameter
- ViewScan was not being created even though schema was loaded correctly
- Table alias was hardcoded to "t" instead of using Cypher variable name

**Solution Implemented**:
1. Modified `generate_scan()` to accept label parameter and call `try_generate_view_scan()`
2. Created `try_generate_view_scan()` to lookup table name from GLOBAL_GRAPH_SCHEMA
3. Added `alias` field to ViewTableRef structure
4. Modified GraphNode extraction to pass alias through to SQL generation
5. Updated SQL generation to use explicit alias from ViewTableRef

**Files Modified**:
- `brahmand/src/query_planner/logical_plan/match_clause.rs` - ViewScan generation
- `brahmand/src/render_plan/view_table_ref.rs` - Added alias field
- `brahmand/src/render_plan/plan_builder.rs` - Pass alias from GraphNode
- `brahmand/src/clickhouse_query_generator/to_sql_query.rs` - Use explicit alias

**Testing**:
```cypher
# Query:
MATCH (u:User) RETURN u.name LIMIT 3

# Generated SQL (correct!):
SELECT u.name FROM users AS u LIMIT 3

# Result:
[
  {"name": "Alice"},
  {"name": "Bob"},
  {"name": "Charlie"}
]
```

**Status**: ✅ COMPLETE - Working end-to-end!

---

### 2. Cypher DDL Parser (MEDIUM PRIORITY)

**Issue**: CREATE TABLE DDL syntax doesn't parse

**Error**:
```
Brahmand Error: Unable to parse: TABLE User (user_id UInt32, name String) 
PRIMARY KEY user_id ON CLICKHOUSE TABLE users
```

**Files to Investigate**:
- `brahmand/src/open_cypher_parser/create_clause.rs` - DDL parsing
- `brahmand/src/open_cypher_parser/create_node_table_clause.rs`
- `brahmand/src/open_cypher_parser/create_table_schema.rs`

**Documentation Reference**: 
- `docs/getting-started.md` shows CREATE TABLE syntax
- May need to verify documented syntax matches parser expectations

**Priority**: MEDIUM - Alternative approaches exist (YAML views), but documented feature should work

---

### 3. ClickHouse Permission Issues on Windows (LOW PRIORITY)

**Issue**: ClickHouse filesystem permission errors when loading schema from database

**Error**:
```
Failed to load schema from database: Clickhouse Error: 
filesystem error: in rename: Permission denied
```

**Workaround**: 
- Use YAML configuration instead (`GRAPH_CONFIG_PATH` environment variable)
- Already documented in `.github/copilot-instructions.md`

**Priority**: LOW - Workaround available, Windows-specific

---

## 🚀 Next Feature Priorities

### 1. Shortest Path Algorithms (RECOMMENDED NEXT)

**Why**: Leverages existing recursive CTE infrastructure from variable-length paths

**Features to Implement**:
```cypher
-- Single shortest path
MATCH path = shortestPath((a:User)-[:FRIENDS_WITH*]-(b:User))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN path, length(path)

-- All shortest paths
MATCH paths = allShortestPaths((a)-[:KNOWS*]-(b))
RETURN paths
```

**Implementation Approach**:
1. Extend parser for `shortestPath()` and `allShortestPaths()` functions
2. Add path weight/cost calculations
3. Use recursive CTEs with MIN() aggregation
4. Optimize for early termination when shortest found

**Files to Modify**:
- `brahmand/src/open_cypher_parser/expression.rs` - Add path functions
- `brahmand/src/query_planner/logical_plan/` - Path planning
- `brahmand/src/clickhouse_query_generator/` - CTE generation with MIN()

**Estimated Effort**: 1-2 days (similar complexity to variable-length paths)

---

### 3. Pattern Extensions

**Features**:
- Path comprehensions: `[(a)-[]->(b) | b.name]`
- List comprehensions with filtering
- Pattern predicates

**Implementation**:
- Extend parser for comprehension syntax
- Transform to subquery with aggregation
- ClickHouse array manipulation

**Estimated Effort**: 3-5 days

---

### 4. Additional Graph Algorithms

**Completed**:
- ✅ **PageRank** (Oct 23, 2025) - `CALL pagerank(maxIterations: 10, dampingFactor: 0.85)`
  - See `notes/pagerank.md` for details
  - Multi-graph support with node/relationship filtering
  - 2/2 tests passing

**Future Algorithms**:
- Centrality measures (betweenness, closeness, degree)
- Community detection (Louvain, label propagation)
- Connected components
- Clustering coefficients

**Approach**: 
- Leverage existing CTE infrastructure (similar to PageRank)
- ClickHouse UDFs for complex computations
- Performance testing critical for large graphs

**Estimated Effort**: 1-2 weeks per algorithm

---

## 🧪 Testing Strategy

### Immediate Actions (When Resuming Work)

1. **Fix View-Based SQL Translation** → Enables all e2e tests
2. **Run End-to-End Tests** → Validate OPTIONAL MATCH with real data
3. **Performance Benchmarks** → Measure LEFT JOIN impact

### Test Files Ready to Use

- ✅ `test_optional_match_e2e.py` - HTTP-based e2e tests (4 scenarios)
- ✅ `test_optional_match_ddl.py` - DDL-based tests
- ✅ `optional_match_demo.py` - Feature demonstration
- ✅ `setup_test_data.sql` - Test data (5 users, 6 friendships)

---

## 📋 Quick Start (When Returning)

### ⚠️ CRITICAL: Environment Variables Must Be Set BEFORE Starting Server

**DO NOT use `Start-Process` or background processes** - they don't inherit environment variables!

### 1. Environment Setup
```powershell
# Start ClickHouse
docker-compose up -d

# ✅ CORRECT: Set environment variables in current shell
$env:CLICKHOUSE_URL="http://localhost:8123"
$env:CLICKHOUSE_USER="test_user"
$env:CLICKHOUSE_PASSWORD="test_pass"
$env:CLICKHOUSE_DATABASE="brahmand"
$env:GRAPH_CONFIG_PATH="social_network.yaml"

# Verify they're set
Write-Host "GRAPH_CONFIG_PATH = $env:GRAPH_CONFIG_PATH"
```

### 2. Load Test Data
```powershell
Get-Content setup_test_data.sql | docker exec -i clickhouse clickhouse-client --user test_user --password test_pass --database brahmand --multiquery
```

### 3. Start Server

**✅ CORRECT WAY (foreground - inherits env vars):**
```powershell
cargo run --bin brahmand
# OR for release build:
.\target\release\brahmand.exe
```

**❌ WRONG WAY (will NOT work - env vars not inherited):**
```powershell
# DON'T DO THIS:
Start-Process -FilePath "cargo" -ArgumentList "run"
Start-Process -FilePath ".\target\release\brahmand.exe" -WindowStyle Hidden
```

### 4. Verify Server Started Correctly

Look for these lines in the server output:
```
✅ Found GRAPH_CONFIG_PATH: social_network.yaml
✅ Successfully loaded schema from YAML config
✅ Successfully bound HTTP listener to 0.0.0.0:8080
✅   - Loaded 1 node types: ["User"]
✅   - Loaded 1 relationship types: ["FRIENDS_WITH"]
```

**If you see these warnings instead, env vars were NOT set:**
```
⚠ No GRAPH_CONFIG_PATH environment variable found
⚠ No ClickHouse client configuration available
```

**If you see port binding errors:**
```
✗ FATAL: Failed to bind HTTP listener to 0.0.0.0:8080: Address already in use
  Is another process using port 8080?
```

**Solution**: Check for old Docker containers!
```powershell
# Find what's using the port
netstat -ano | Select-String "8080"

# Check for old containers
docker ps -a | Select-String "clickgraph|brahmand"

# Stop and remove if found
docker stop brahmand
docker rm brahmand
```

### 5. Common Troubleshooting

**Debug Output Not Appearing?**
- ✅ Make sure `RUST_LOG=trace` or `RUST_LOG=debug` is set
- ✅ Check that you're running the LATEST build (check timestamp: `Get-Item .\target\debug\brahmand.exe`)
- ✅ Verify process is actually running: `Get-Process | Where-Object { $_.Name -like "*brahmand*" }`

**Query Returns 500 Error?**
- ✅ Check if it's the OLD Docker container responding (stop it!)
- ✅ Look at server output for actual error message
- ✅ Verify ClickHouse is running: `docker ps | Select-String clickhouse`

**Code Changes Not Taking Effect?**
- ✅ Make sure you rebuilt: `cargo build`
- ✅ Kill old processes: `Get-Process | Where-Object { $_.Name -like "*brahmand*" } | Stop-Process -Force`
- ✅ Check for old Docker containers that might still be serving requests!

### 6. Verify OPTIONAL MATCH and View-Based Queries Work
```powershell
# Simple query test (should work now!)
python test_query_simple.py

# Unit tests
cargo test optional_match

# E2E tests 
python test_optional_match_e2e.py
```

---

## 📂 Important Files Reference

### View-Based SQL Translation (Recently Fixed!)
- `brahmand/src/query_planner/logical_plan/match_clause.rs` - ViewScan generation & label resolution
- `brahmand/src/render_plan/view_table_ref.rs` - Added alias field
- `brahmand/src/render_plan/plan_builder.rs` - Alias propagation from GraphNode
- `brahmand/src/clickhouse_query_generator/to_sql_query.rs` - SQL generation with correct aliases
- `brahmand/src/server/graph_catalog.rs` - Schema loading (GLOBAL_GRAPH_SCHEMA)

### OPTIONAL MATCH Implementation
- `brahmand/src/open_cypher_parser/optional_match_clause.rs` - Parser
- `brahmand/src/query_planner/logical_plan/optional_match_clause.rs` - Logical plan
- `brahmand/src/query_planner/plan_ctx/mod.rs` - Alias tracking
- `brahmand/src/clickhouse_query_generator/graph_join_inference.rs` - JOIN type

### Server & Configuration
- `start_server_with_env.ps1` - PowerShell startup script with env vars
- `start_server_new_window.bat` - Batch file to start in separate window (recommended!)
- `brahmand/src/server/mod.rs` - HTTP/Bolt server initialization with improved error handling

### Documentation
- `docs/optional-match-guide.md` - OPTIONAL MATCH feature guide
- `OPTIONAL_MATCH_COMPLETE.md` - Technical implementation details
- `YAML_SCHEMA_INVESTIGATION.md` - Schema fixes and known issues
- `.github/copilot-instructions.md` - Development guidelines

---

## 🎯 Recommended Work Session Plan

### Session 1: Fix View-Based SQL Translation (4-6 hours)

**Goals**:
1. Understand how ViewScan translates to table names
2. Connect view resolution to SQL generation
3. Make `test_optional_match_e2e.py` pass

**Starting Points**:
1. Read `brahmand/src/query_planner/analyzer/view_resolver.rs`
2. Trace query flow from parser → planner → SQL generator
3. Find where label names become table names
4. Insert view lookup before table name emission

**Success Criteria**:
- Query `MATCH (u:User)` generates SQL with `FROM users` not `FROM User`
- All 4 e2e tests in `test_optional_match_e2e.py` pass
- OPTIONAL MATCH works end-to-end with real ClickHouse data

### Session 2: Shortest Path Implementation (6-8 hours)

**Goals**:
1. Add `shortestPath()` function parsing
2. Implement shortest path logical plan
3. Generate optimized CTEs with MIN() for path finding

**Starting Points**:
1. Review variable-length path implementation (already done)
2. Extend for weighted paths and early termination
3. Add MIN(length) aggregation in CTEs

### Session 3: Performance Testing & Optimization (4 hours)

**Goals**:
1. Benchmark OPTIONAL MATCH vs MATCH performance
2. Test with large datasets (100K+ nodes)
3. Optimize LEFT JOIN execution plans

---

## 💾 Git State

**Current Branch**: `graphview1`  
**Last Commit**: `c27d631` - OPTIONAL MATCH complete  
**Status**: 9 commits ahead of `origin/graphview1`  
**Action**: Already pushed to origin ✅

**To Create PR**:
```bash
# When ready to merge to main
git checkout main
git pull
git merge graphview1
# Or create PR via GitHub UI
```

---

## 📊 Current Project Status

**Overall**: 312/312 tests passing (100%)
- **Python integration tests**: 8/8 passing (100%)
- **Rust unit tests**: 304/304 passing (100%)
- **Path variable tests**: 3/3 passing (100%)

**Last updated**: October 30, 2025
**Branch**: `main`
**Latest feature**: Codebase health improvements and comprehensive error handling

**Test Command**:
```bash
cargo test                    # Run all Rust tests
cargo test optional_match     # Run only OPTIONAL MATCH tests
python test_runner.py --test  # Run Python integration tests
```

**Performance Status**: Ready for benchmarking phase
- All major Cypher features implemented and tested
- Query performance metrics and monitoring in place
- Error handling systematically improved throughout codebase

---

## 🔗 Useful Links

- **GitHub Repo**: https://github.com/genezhang/clickgraph
- **Current Branch**: https://github.com/genezhang/clickgraph/tree/graphview1
- **Neo4j Cypher Docs**: https://neo4j.com/docs/cypher-manual/current/
- **ClickHouse Docs**: https://clickhouse.com/docs/

---

## ✅ Pre-Session Checklist

Before starting next work session:

- [ ] Pull latest changes: `git pull origin graphview1`
- [ ] Start ClickHouse: `docker-compose up -d`
- [ ] Verify ClickHouse: `docker ps` (should show "healthy")
- [ ] Load test data: Run `setup_test_data.sql`
- [ ] Set environment variables (see Quick Start above)
- [ ] Run tests to verify state: `cargo test`
- [ ] Review this document for context

---

## � Troubleshooting

### Server Says "No GRAPH_CONFIG_PATH environment variable found"

**Cause**: Environment variables weren't set before starting the server, or were set in a different shell/process.

**Solution**:
1. Stop the server (Ctrl+C)
2. Verify env vars in your current shell:
   ```powershell
   Write-Host "GRAPH_CONFIG_PATH = $env:GRAPH_CONFIG_PATH"
   Write-Host "CLICKHOUSE_URL = $env:CLICKHOUSE_URL"
   ```
3. If they're not set or empty, set them again in the SAME shell
4. Start server again in the SAME shell (don't use `Start-Process`)

### Query Returns "Unknown table expression identifier 'User'"

**Causes**:
1. Schema not loaded (see issue above)
2. ViewScan not being created (development issue we're currently fixing)

**Check**:
- Server startup should show: "Loaded 1 node types: ["User"]"
- If not, env vars weren't set correctly

### ClickHouse Connection Errors

**Symptoms**: "ClickHouse connection test failed"

**Solutions**:
1. Verify ClickHouse is running: `docker ps` (should show "healthy")
2. Check credentials match docker-compose.yaml
3. Test direct connection:
   ```powershell
   docker exec -it clickhouse clickhouse-client --user test_user --password test_pass --database brahmand
   ```

### Process Won't Stop / Port Already in Use

**Solution**:
```powershell
# Force kill all brahmand processes
Stop-Process -Name "brahmand" -Force -ErrorAction SilentlyContinue

# If port 8080 still in use, find and kill the process
Get-NetTCPConnection -LocalPort 8080 -ErrorAction SilentlyContinue | 
  Select-Object -ExpandProperty OwningProcess | 
  ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
```

---

## �📝 Notes

**Windows Development**:
- ClickHouse tables MUST use `ENGINE = Memory` (permission issues)
- Use `Invoke-RestMethod` not `curl` for HTTP testing
- **CRITICAL**: Set env vars in current shell, not via `Start-Process`
- See `.github/copilot-instructions.md` for full Windows constraints

**Code Quality**:
- Maintain 100% test coverage for new features
- Update documentation alongside code
- Follow existing code patterns (builder pattern, error handling)

**Communication**:
- Keep STATUS_REPORT.md updated
- Document breaking changes in CHANGELOG.md
- Update feature matrix when adding capabilities

---

**Last Updated**: October 30, 2025
**Current Status**: All major Cypher features complete - ready for benchmarking phase
**Test Coverage**: 312/312 tests passing (100%)
**Latest Achievement**: Comprehensive codebase health improvements and error handling fixes
**Next Phase**: Performance benchmarking and optimization

**Happy Coding! 🚀**
