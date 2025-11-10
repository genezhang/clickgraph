# NEXT_SESSION_PRIORITIES.md

**Date**: November 8, 2025  
**Current Status**: WITH CLAUSE 100% COMPLETE! 🎉
**Latest Success**: 12/12 WITH clause tests passing (100%)

---

## 🎯 IMMEDIATE PRIORITY: Complete Integration Test Coverage

**Goal**: Fix remaining 11 integration test failures (24/35 → 35/35 = 100%)

**Status**: Ready for systematic debugging

**Estimated Time**: 1-2 hours

### Why This Matters
- Many tests likely trivial fixes (similar to today's success)
- Validates all core functionality end-to-end
- Prepares for production benchmarking
- Quick momentum after today's victory

### Approach
1. Run full integration test suite: `python run_tests.py`
2. Identify failing tests and categorize issues
3. Fix systematically (like today's WITH clause fixes)
4. Validate no regressions (ensure 325/325 unit tests still pass)

### Expected Issues (Based on Past Patterns)
- Property mapping bugs
- Table prefix inconsistencies
- WHERE clause edge cases
- Relationship traversal corner cases

**Target**: 35/35 integration tests (100%) by end of next session

---

## 🏃 SHORT-TERM: Documentation & Performance

### 1. Production Benchmarking Suite (2-3 hours)
**What**: Establish performance baseline with realistic workloads

**Why**: Need baseline before adding new features

**Tasks**:
- Create realistic test data (1M nodes, 10M relationships)
- Benchmark all query patterns (scans, traversals, aggregations, WITH clauses)
- Document baseline metrics
- Set up automated regression testing

**Deliverable**: `notes/benchmarking.md` with baseline metrics

### 2. Hot Reload for YAML Configs (2-3 hours)
**What**: Watch for schema YAML changes and reload automatically

**Why**: Developer productivity (no server restarts during schema iteration)

**Estimated Complexity**: Medium (file watching + atomic reload)

---

## 🚀 MEDIUM-TERM: Graph Algorithms & Pattern Extensions

### 1. Additional Graph Algorithms (1-2 weeks per algorithm)
**Completed**: PageRank ✅

**Next Algorithms** (using PageRank infrastructure):
- Betweenness centrality
- Closeness centrality  
- Degree centrality
- Community detection (label propagation)
- Connected components

### 2. Pattern Comprehensions (3-5 days)
**What**: `[(a)-[]->(b) | b.name]` syntax

**Why**: More expressive queries, common in Neo4j

**Estimated Complexity**: Medium (AST extension + list handling)

---

## 📋 Completed Priorities (Reference)

### ✅ November 8, 2025: WITH CLAUSE 100% COMPLETE
- Fixed multi-hop pattern JOIN extraction (recursive GraphRel)
- Fixed ORDER BY + LIMIT with CTE
- Fixed WITH alias resolution for non-aggregation
- Result: 12/12 tests (100%), up from 9/12 (75%)
- Duration: ~2.5 hours
- Documentation: `notes/with-clause-complete.md`

### ✅ November 5, 2025: OPTIONAL MATCH Complete
- 11/11 parser tests passing
- Clean LEFT JOIN SQL generation
- Full documentation
- Integration test coverage: 24/35 (68.6%)

### ✅ November 1, 2025: Critical Bug Fixes
- ChainedJoin CTE wrapper
- Shortest path filter rewriting
- Aggregation table names
- Benchmark: 10/10 queries (100%)

### ✅ October 25, 2025: Query Performance Metrics
- Phase-by-phase timing
- HTTP response headers
- Structured logging

### ✅ October 21, 2025: Variable-Length Paths & PageRank
- Complete `*`, `*2`, `*1..3` patterns
- PageRank algorithm implementation
- Path variables and functions

---

## 🎯 Session Planning Template

**For Next Session**:
1. **Warm-up** (5 min): Review STATUS.md, run test suite
2. **Main Task** (60-90 min): Fix integration test failures
3. **Validation** (15 min): Run full test suite, check for regressions
4. **Document** (15 min): Update STATUS.md, CHANGELOG.md if needed

**Success Criteria**:
- All integration tests passing (35/35 = 100%)
- No unit test regressions (maintain 325/325)
- Clean commit with descriptive message
- Updated documentation

---

## 💡 Development Philosophy

**Lessons from Today's Success**:
1. **Quick wins over big refactors** ("I want to success quickly!")
2. **Systematic debugging** (isolate each test failure)
3. **Surgical fixes** (minimal code changes, maximum impact)
4. **Document technical debt** (deprecation comments, TODO notes)
5. **Celebrate victories** (100% success rates deserve recognition!)

**What Works**:
- Focus on test coverage (validates all functionality)
- Fix bugs as they're found (don't let them accumulate)
- Document immediately (while fresh in memory)
- Quick iteration (2-3 hour sessions with clear goals)

**Avoid**:
- Big refactors when quick fixes work
- Premature optimization (establish baseline first)
- Feature creep (finish current work before starting new)
- Skipping documentation (future self will thank you)

---

## 📊 Current Project Health

**Test Coverage**:
- Unit Tests: 325/325 (100%) ✅
- WITH Clause: 12/12 (100%) ✅
- Integration Tests: 24/35 (68.6%) ← **NEXT TARGET**
- OPTIONAL MATCH: 11/11 (100%) ✅

**Core Features Status**:
- ✅ Basic MATCH queries
- ✅ WHERE clause filters
- ✅ OPTIONAL MATCH (LEFT JOINs)
- ✅ Variable-length paths (`*`, `*2`, `*1..3`)
- ✅ Shortest path algorithms
- ✅ Multiple relationship types (`[:TYPE1|TYPE2]`)
- ✅ WITH clauses (all patterns)
- ✅ Path variables and functions
- ✅ ORDER BY, LIMIT, SKIP
- ✅ Aggregations (COUNT, SUM, AVG, etc.)
- ✅ PageRank algorithm

**Known Limitations**:
- Write operations (out of scope - read-only engine)
- Some edge cases in integration tests (to be fixed)
- Performance baseline not yet established
- Schema hot reload not implemented

**Momentum**: 🚀 **STRONG** - Three major victories in past week!
 

RETURN a, b- **Impact**: Consistent table naming**NEW (Simplified)**:

# SQL: FROM a JOIN node1 ON ... JOIN b ON ...

- Server loads `test_integration.yaml` at startup → ONE schema registered as "default"

# 3. Mixed direction chain

MATCH (a)<-[r1]-()-[r2]->(b) ### 3. Run Full Test Suite (5 min)- Tests use `schema_name="default"` (or omit parameter to use default)

RETURN a, b

# SQL: FROM a JOIN node1 ON ... JOIN b ON ...```powershell- NO API loading in basic tests



# 4. With OPTIONAL MATCH variantscargo test --lib- Multi-schema tests isolated to `test_multi_database.py` only

MATCH (a:User) 

OPTIONAL MATCH (a)-[:FOLLOWS]->()-[:FOLLOWS]->(b) python run_tests.py

RETURN a.name, b.name

# SQL: FROM a LEFT JOIN ... LEFT JOIN b```**Change Made**: Modified `conftest.py` to NOT call `/api/schemas/load`



# 5. Variable-length on intermediate nodes

MATCH (a)-[r*1..2]->()<-[]-(b) 

RETURN a, b## Status Summary## Quick Start Commands

# SQL: Recursive CTE with multiple paths



# 6. Named intermediate nodes

MATCH (a)-[r1]->(mid)<-[r2]-(b) ### What Works Now ✅### 1. Test the Fix (5 min)

WHERE mid.type = 'hub'

RETURN a, mid, b- **OPTIONAL MATCH parser**: Correctly recognizes clauses (was completely broken!)```powershell

```

- **LEFT JOIN generation**: Working correctly# Terminal 1: Start server (if not running)

### Why Important

- **Real-world queries**: Social networks, recommendation engines use multi-hop traversals- **DuplicateScansRemoving**: Preserves optional relationshipscd c:\Users\GenZ\clickgraph

- **Edge cases**: Tests JOIN chain generation, intermediate node handling

- **Direction handling**: Validates bidirectional pattern support- **Parser unit tests**: 11/11 passing (100%)$env:CLICKHOUSE_URL="http://localhost:8123"

- **OPTIONAL semantics**: Ensures LEFT JOINs work across multiple hops

- **Core functionality**: LEFT JOIN semantics working$env:CLICKHOUSE_USER="test_user"

### Implementation Notes

$env:CLICKHOUSE_PASSWORD="test_pass"

**Parser Status**: ✅ Already supports these via `path_pattern.rs`  

**Query Planner**: Need to verify multi-hop GraphRel generation  ### Known Issues 🔧$env:CLICKHOUSE_DATABASE="default"

**SQL Generator**: Multiple JOIN chains (should work, needs testing)  

**Test Coverage**: Currently minimal1. WHERE clause duplication (cosmetic)$env:GRAPH_CONFIG_PATH="tests/integration/test_integration.yaml"



**Files to Check**:2. Missing table prefix (minor)cargo run --bin clickgraph

- `brahmand/src/open_cypher_parser/path_pattern.rs` - Pattern parsing

- `brahmand/src/query_planner/logical_plan/match_clause.rs` - Multi-hop planning3. `is_optional` field not set (doesn't break current functionality)

- `brahmand/src/query_planner/analyzer/graph_join_inference.rs` - JOIN chain generation

# Terminal 2: Run tests with simplified schema

**Test Approach**:

1. Create `tests/python/test_multi_hop_patterns.py` for integration tests### Test Statuscd tests\integration

2. Add unit tests in `match_clause.rs` for multi-hop pattern planning

3. Test with both MATCH and OPTIONAL MATCH variants- Unit Tests: 330/331 (99.7%) ✅$env:CLICKHOUSE_USER="test_user"

4. Verify SQL generation for each pattern type

- OPTIONAL MATCH Parser: 11/11 (100%) ✅$env:CLICKHOUSE_PASSWORD="test_pass"

**Example Test Structure**:

```python- Integration Tests: Rerun neededpython -m pytest -v test_basic_queries.py::TestBasicMatch

def test_diamond_pattern():

    query = "MATCH (a:User)-[:FOLLOWS]->(m:User)<-[:FOLLOWS]-(b:User) RETURN a.name, b.name"

    # Expected: 3 JOINs (a->follows->m, b->follows->m)

    ## Key Breakthrough# Should see 3/3 passing if fix works!

def test_chain_pattern():

    query = "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(b:User) RETURN a.name, b.name"```

    # Expected: 3 JOINs (a->follows->intermediate->follows->b)

```**Problem**: Parser tried to parse OPTIONAL MATCH BEFORE WHERE, but real queries have:



---```cypher### 2. Add Debug Logging (10 min)



## Current Status SummaryMATCH (a:User)                    ← 1. MATCH clause**File**: `brahmand/src/query_planner/analyzer/view_resolver.rs`  



### What Works ✅WHERE a.name = 'Alice'            ← 2. WHERE (filters MATCH)**Location**: Line 51-60 in `resolve_node_property()`

- OPTIONAL MATCH parser (100% - fixed last night!)

- LEFT JOIN generationOPTIONAL MATCH (a)-[:FOLLOWS]->(b) ← 3. OPTIONAL MATCH

- Basic relationship patterns

- WHERE clause filtering (minor duplication issue)RETURN a.name, b.name**Add before the lookup**:



### Known Issues 🔧``````rust

1. WHERE duplication (cosmetic)

2. Missing table prefix (minor)pub fn resolve_node_property(&self, label: &str, property: &str) -> Result<String, AnalyzerError> {

3. Multi-hop patterns (untested)

**Solution**: Reordered parser to match actual query structure:    let node_schema = self.schema.get_node_schema(label)?;

### Test Status

- Unit Tests: 330/331 (99.7%) ✅1. Parse MATCH    

- OPTIONAL MATCH Parser: 11/11 (100%) ✅

- Integration Tests: Rerun needed after fixes2. Parse WHERE (filters the MATCH above)    // DEBUG: Print entire property_mappings HashMap


3. Parse OPTIONAL MATCH (now positioned correctly)    eprintln!("🔍 DEBUG resolve_node_property:");

    eprintln!("   Label: {}", label);

**Result**: Parser now recognizes OPTIONAL MATCH → generates LEFT JOINs! 🎉    eprintln!("   Property requested: {}", property);

    eprintln!("   Available mappings: {:?}", node_schema.property_mappings);

## Quick Validation    

```powershell    let result = node_schema.property_mappings.get(property).cloned();

docker-compose down; docker-compose up -d    eprintln!("   Mapped to: {:?}", result);

Start-Sleep -Seconds 3    

python test_simple.py    result.ok_or_else(|| AnalyzerError::PropertyNotFound {

```        entity_type: "node".to_string(),

        entity_name: label.to_string(),

Expected output for OPTIONAL MATCH query:        property: property.to_string(),

```sql    })

SELECT a.name, b.name }

FROM test_integration.users AS a ```

LEFT JOIN test_integration.follows AS r ON r.follower_id = a.user_id 

LEFT JOIN test_integration.users AS b ON b.user_id = r.followed_id ### 3. Capture Debug Output (5 min)

WHERE a.name = 'Alice'```powershell

-- ✅ Has LEFT JOINs (was completely missing before!)# Re-run test and capture output

```python -m pytest -v -s test_basic_queries.py::TestBasicMatch::test_match_with_label 2>&1 | Tee-Object debug_output.txt


# Look for the debug lines
Get-Content debug_output.txt | Select-String "DEBUG resolve_node_property" -Context 5,5
```

### 4. Analyze & Fix (30-60 min)

**Scenario A**: Schema has `{"name": "full_name"}` (corrupted)
→ Find where schema gets corrupted during load/merge  
→ Fix schema loading in `graph_catalog.rs`

**Scenario B**: Schema has `{"name": "name"}` but resolution returns wrong value
→ Check for caching or stale schema reference  
→ Fix schema retrieval in query handler

**Scenario C**: Different schemas for different properties
→ Check if projection items use different schema lookups  
→ Ensure consistent schema resolution

### 5. Verify Fix (10 min)
```powershell
# Run all basic_queries tests
python -m pytest -v test_basic_queries.py

# Should see ~15-18/19 passing after fix

# Run full suite
python -m pytest -v --maxfail=50

# Target: >220/272 passing
```

### 6. Release v0.2 (30 min)
```powershell
# Update CHANGELOG.md
# Update STATUS.md
# Tag release
git tag v0.2.0
git push origin v0.2.0

# Create GitHub release with test results
```

## Expected Root Causes (In Order of Likelihood)

### 1. Schema Merge/Overwrite Issue (60% probability)
- API `load_schema_by_name()` merges instead of replaces
- Old schema from examples (with `name: full_name`) persists
- New schema partially overwrites, causing mixed mappings

**Fix**: Ensure `load_schema_by_name()` does full replacement

### 2. Schema Caching (25% probability)
- `get_graph_schema_by_name()` returns cached/stale schema
- Schema loaded at startup different from test schema
- RwLock not properly updated

**Fix**: Verify RwLock write updates propagate correctly

### 3. Hardcoded Fallback Being Used (10% probability)
- Despite warnings, `map_property_to_column()` somehow gets called
- Fallback logic kicks in for `name` property specifically

**Fix**: Remove or audit all hardcoded property mappings

### 4. Projection Item Processing Bug (5% probability)
- First projection item vs second item use different code paths
- Order-dependent schema resolution

**Fix**: Ensure consistent schema lookup for all projection items

## Success Metrics

- ✅ Debug output shows correct property_mappings HashMap
- ✅ `test_match_with_label` passes
- ✅ All TestBasicMatch tests pass (3/3)
- ✅ >80% of integration tests pass
- ✅ CHANGELOG and STATUS updated
- ✅ v0.2.0 tagged and released

## Fallback Plan

If fix takes >2 hours:
1. Document exact root cause
2. Create minimal reproduction test
3. File detailed GitHub issue
4. Release v0.1.1 with known issues documented
5. Schedule dedicated debugging session

## Reference Documents

- **Bug Investigation**: [PROPERTY_MAPPING_BUG_INVESTIGATION.md](PROPERTY_MAPPING_BUG_INVESTIGATION.md)
- **Test Run Summary**: [TEST_RUN_SUMMARY.md](TEST_RUN_SUMMARY.md)
- **Current Status**: [STATUS.md](STATUS.md)

---

**Estimated Total Time**: 1-2 hours  
**Confidence**: HIGH (clear reproduction, good hypotheses)  
**Impact**: CRITICAL (unlocks 250+ tests)
