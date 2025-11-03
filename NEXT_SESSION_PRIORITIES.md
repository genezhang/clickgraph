# NEXT_SESSION_PRIORITIES.md
**Priority**: Fix property mapping bug for v0.2 release  
**Date**: November 2, 2025  
**Status**: Ready for rapid fix

## 🎯 Goal: Fix Property Mapping Bug & Release v0.2

**Current**: 1/272 tests passing (0.4%)  
**Target**: >220/272 tests passing (>80%) after fix  
**Timeline**: 1-2 hour fix session

## Quick Start Commands

### 1. Start Debug Session (5 min)
```powershell
# Terminal 1: Start ClickHouse (if not running)
docker start clickhouse

# Terminal 2: Start server with debug logging
cd c:\Users\GenZ\clickgraph
$env:CLICKHOUSE_URL="http://localhost:8123"
$env:CLICKHOUSE_USER="test_user"
$env:CLICKHOUSE_PASSWORD="test_pass"
$env:CLICKHOUSE_DATABASE="default"
$env:GRAPH_CONFIG_PATH="tests/integration/test_integration.yaml"
$env:RUST_LOG="debug"
cargo run --bin clickgraph

# Terminal 3: Run failing test
cd tests\integration
$env:CLICKHOUSE_USER="test_user"
$env:CLICKHOUSE_PASSWORD="test_pass"
python -m pytest -v -s test_basic_queries.py::TestBasicMatch::test_match_with_label
```

### 2. Add Debug Logging (10 min)
**File**: `brahmand/src/query_planner/analyzer/view_resolver.rs`  
**Location**: Line 51-60 in `resolve_node_property()`

**Add before the lookup**:
```rust
pub fn resolve_node_property(&self, label: &str, property: &str) -> Result<String, AnalyzerError> {
    let node_schema = self.schema.get_node_schema(label)?;
    
    // DEBUG: Print entire property_mappings HashMap
    eprintln!("🔍 DEBUG resolve_node_property:");
    eprintln!("   Label: {}", label);
    eprintln!("   Property requested: {}", property);
    eprintln!("   Available mappings: {:?}", node_schema.property_mappings);
    
    let result = node_schema.property_mappings.get(property).cloned();
    eprintln!("   Mapped to: {:?}", result);
    
    result.ok_or_else(|| AnalyzerError::PropertyNotFound {
        entity_type: "node".to_string(),
        entity_name: label.to_string(),
        property: property.to_string(),
    })
}
```

### 3. Capture Debug Output (5 min)
```powershell
# Re-run test and capture output
python -m pytest -v -s test_basic_queries.py::TestBasicMatch::test_match_with_label 2>&1 | Tee-Object debug_output.txt

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
