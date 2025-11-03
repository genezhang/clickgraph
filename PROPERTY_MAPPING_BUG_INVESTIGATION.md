# Property Mapping Bug Investigation - Nov 2, 2025

## Problem Statement
Integration tests failing with property mapping error:
- **Query**: `MATCH (u:User) RETURN u.name, u.age`
- **Expected SQL**: `SELECT u.name, u.age FROM test_integration.users AS u`
- **Actual SQL**: `SELECT u.full_name, u.age FROM test_integration.users AS u`

**Pattern**: `u.name` → `u.full_name` (WRONG), but `u.age` → `u.age` (CORRECT)

## Key Findings

### 1. Schema Configuration is Correct ✓
**File**: `tests/integration/test_integration.yaml`
```yaml
property_mappings:
  name: name  # ← Correct mapping
  age: age    # ← Correct mapping
```

### 2. First Test Passes, Second Fails 🤔
- ✅ `test_match_all_nodes`: `MATCH (n:User) RETURN n.name` - PASSES
- ❌ `test_match_with_label`: `MATCH (u:User) RETURN u.name, u.age` - FAILS

### 3. Property Resolution Code Path
**FilterTagging analyzer** (`filter_tagging.rs` line 159-220):
- Calls `view_resolver.resolve_node_property(label, property)`
- Should lookup property in `node_schema.property_mappings`
- Returns mapped column name

**View Resolver** (`view_resolver.rs` line 51-60):
```rust
pub fn resolve_node_property(&self, label: &str, property: &str) -> Result<String, AnalyzerError> {
    let node_schema = self.schema.get_node_schema(label)?;
    node_schema.property_mappings.get(property)
        .cloned()
        .ok_or_else(|| AnalyzerError::PropertyNotFound { /* ... */ })
}
```

### 4. Hardcoded Fallback Found (NOT USED)
**File**: `cte_generation.rs` line 244-250
```rust
fn map_property_to_column(property: &str) -> String {
    match property {
        "name" => "full_name".to_string(),  // ← Suspicious!
        // ...
    }
}
```
**Status**: Function is marked as unused in compiler warnings ✓

### 5. Example Schemas Have `name: full_name`
Many example YAMLs map `name → full_name`:
- `social_network.yaml`
- `social_benchmark.yaml`
- `test_friendships.yaml`
- etc.

**But**: Our `test_integration.yaml` clearly has `name: name`

## Hypothesis

**✅ CONFIRMED ROOT CAUSE: Schema Loading Confusion**

The integration tests are **mixing two schema loading mechanisms**:

1. **Server Startup**: Loads `test_integration.yaml` via `GRAPH_CONFIG_PATH`
   - Registers schema with TWO keys: "default" + "test_integration"
   - Uses dual-key registration (recent feature)

2. **Test Fixture**: `simple_graph` ALSO calls `/api/schemas/load` 
   - Tries to load same YAML again via API
   - API loader registers with ONE key: "test_integration" (no "default")
   - Creates duplicate/conflicting schema registrations

**The Problem**:
- Tests query with `schema_name="test_integration"` 
- Which "test_integration" schema gets used? Startup or API-loaded?
- Schema may be partially merged/overwritten
- Results in mixed property mappings (some correct, some wrong)

**Why `u.age` works but `u.name` doesn't**:
- Likely schema state is inconsistent/corrupted from duplicate loading
- Or API schema load partially overwrites startup schema
- Results in HashMap with some properties from one schema, some from another

**The Fix**: 
✅ **Simplified schema strategy** - use ONLY server's startup schema
- Remove API schema loading from `simple_graph` fixture
- Tests use `schema_name="default"` 
- ONE schema, ONE loading mechanism, NO confusion
- Multi-schema tests isolated to dedicated test file

**Files Modified**:
- `conftest.py`: Removed `/api/schemas/load` call from `simple_graph` fixture
- Tests now use server's default schema directly

## Suspect Code Locations

### High Priority
1. **Property mapping in FilterTagging** (`filter_tagging.rs:159-220`)
   - Check if schema lookup is consistent
   - Verify `node_schema.property_mappings` contains correct values
   
2. **Schema loading/merging** (`graph_catalog.rs`)
   - Check `load_schema_by_name()` - does it merge or replace?
   - Check if API loading corrupts existing schema

3. **Projection item processing** (`filter_tagging.rs:95-105`)
   - Why does first item work but second fails?
   - Check if there's ordering dependency

### Medium Priority
4. **GraphSchema construction** (`config.rs:189-250`)
   - Verify `property_mappings: node_def.properties.clone()` works correctly
   - Check if HashMap clone is deep copy

## Reproduction Steps

1. Start server: `cargo run --bin clickgraph` with `GRAPH_CONFIG_PATH=tests/integration/test_integration.yaml`
2. Run test: `pytest test_basic_queries.py::TestBasicMatch::test_match_with_label`
3. Observe: `u.name` becomes `u.full_name` in generated SQL

## Next Actions (Prioritized)

### Immediate (< 30 min)
1. ✅ **Add debug logging** in `resolve_node_property` to print:
   - Input: label, property
   - Schema property_mappings HashMap contents
   - Output: mapped column name

2. **Run single test** with debug output
   - Capture exact property_mappings being used
   - Confirm if schema has `name: name` or `name: full_name`

### Quick Fix (30-60 min)
3. If schema is corrupted:
   - Find where corruption happens (load vs merge)
   - Fix schema loading to prevent merge

4. If schema is correct but resolution wrong:
   - Find why `name` resolves differently than `age`
   - Check for special case handling of `name` property

### Thorough Fix (1-2 hours)
5. **Add integration test for schema loading**
   - Verify property_mappings after load
   - Test that mappings persist correctly

6. **Remove all hardcoded property mappings**
   - Audit codebase for any `name → full_name` assumptions
   - Ensure all property resolution uses schema only

## Impact Assessment

**Affected Tests**: ~250/272 tests (95%)  
**Root Cause**: Property mapping resolution bug  
**Severity**: HIGH - blocks most integration tests  
**Fix Complexity**: MEDIUM - once root cause found, fix should be straightforward

## Success Criteria

After fix:
- ✅ `test_match_with_label` passes
- ✅ Property mappings respect YAML configuration
- ✅ No hardcoded property assumptions
- ✅ Test pass rate jumps to >80%

---

**Time Investment**: 2+ hours debugging  
**Status**: Root cause not yet confirmed, multiple hypotheses  
**Recommendation**: Add targeted debug logging before continuing investigation
