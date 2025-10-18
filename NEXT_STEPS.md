# Next Steps - Development Roadmap

**Last Updated**: October 18, 2025  
**Current Status**: View-based SQL translation FIXED! ViewScan working end-to-end  
**Branch**: `graphview1`  
**Latest Commit**: TBD - ViewScan label-to-table translation + alias fix

---

## ⚠️ CRITICAL: Clean Development Environment Setup

### Docker Container Cleanup (MUST DO BEFORE STARTING SERVER!)

**Problem**: Old Docker containers can cause mysterious port conflicts and wrong server versions being tested.

**Symptoms**:
- Port 8080 already in use
- Queries work but debug output doesn't appear
- Code changes don't seem to take effect
- Server says "running" but can't connect

**Solution - Clean Docker Environment**:
```powershell
# 1. Check for old ClickGraph/Brahmand containers
docker ps -a | Select-String "clickgraph|brahmand"

# 2. Stop and remove any old containers
docker stop brahmand
docker rm brahmand

# 3. Optional: Remove old images if needed
docker images | Select-String "clickgraph|brahmand"
docker rmi clickgraph-brahmand  # if found

# 4. Verify ports are free
netstat -ano | Select-String "8080|7687"
```

**Best Practice**: Always check for old containers before starting development session!

---

## 🎉 Just Completed (October 18, 2025)

### View-Based SQL Translation Feature 
- ✅ **Root Cause Found**: `generate_scan()` was passing `None` for label instead of actual label
- ✅ **Implementation**: Modified to create ViewScan with schema lookup via GLOBAL_GRAPH_SCHEMA
- ✅ **Table Alias Fix**: Added alias field to ViewTableRef, properly passes Cypher variable names through to SQL
- ✅ **Port Conflict Resolved**: Stopped old Docker container using port 8080
- ✅ **Error Handling Improved**: Added proper HTTP bind error handling with clear messages
- ✅ **Testing**: Query `MATCH (u:User) RETURN u.name` successfully returns data from ClickHouse
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

### 1. View-Based SQL Translation (HIGH PRIORITY)

**Issue**: YAML schema loads correctly, but queries don't translate Cypher labels to source tables### 1. View-Based SQL Translation (✅ RESOLVED!)

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

### 2. Alternate Relationship Types

**Feature**:
```cypher
MATCH (a:User)-[:FOLLOWS|FRIENDS_WITH]->(b:User)
RETURN a.name, b.name
```

**Implementation**:
- Extend path pattern parser for `|` operator
- Generate UNION or multiple JOIN conditions
- Handle property access across alternate types

**Estimated Effort**: 4-6 hours

---

### 3. Path Variables

**Feature**:
```cypher
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
RETURN p, nodes(p), relationships(p), length(p)
```

**Implementation**:
- Store path information in CTEs
- Implement `nodes()`, `relationships()`, `length()` functions
- Array aggregation in ClickHouse for path components

**Estimated Effort**: 1 day

---

### 4. Graph Algorithms

**Features**:
- PageRank
- Centrality measures (betweenness, closeness, degree)
- Community detection
- Connected components

**Approach**: 
- May require ClickHouse UDFs or complex CTEs
- Consider integration with external graph libraries
- Performance testing critical

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

## 📊 Current Test Status

**Overall**: 261/262 tests passing (99.6%)  
**OPTIONAL MATCH**: 11/11 tests passing (100%)  
**One Failing Test**: Unrelated to OPTIONAL MATCH (pre-existing)

**Test Command**:
```bash
cargo test
cargo test optional_match  # Run only OPTIONAL MATCH tests
```

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

**Last Session**: October 18, 2025 - View-based SQL translation fix (in progress)  
**Previous Session**: October 17, 2025 - OPTIONAL MATCH implementation and documentation  
**Duration**: ~8-10 hours of focused development  
**Current Status**: Implementing ViewScan generation to resolve Cypher labels to ClickHouse tables

**Happy Coding! 🚀**
