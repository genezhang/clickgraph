# WSL Migration & Denormalized Edge Architecture Review

**Date**: November 24, 2025  
**Status**: Development Environment Migrated, Architecture Analysis Complete

---

## What We Just Did

### 1. ✅ Fixed Git Line Ending Issues
- **Problem**: All files showed as modified after WSL migration
- **Solution**: Configured `git config core.autocrlf input`
- **Result**: Clean working tree

### 2. ✅ Set Up Rust Build System on WSL
- **Installed**: Rust 1.91.1 (stable, released Nov 10, 2025)
- **Configured**: Added to `.bashrc` for persistence
- **Tested**: 
  - `cargo build --release` ✅ Success
  - `cargo test --lib` ✅ 466/477 tests passing (97.7%)
  - 11 known failures (shortest path filters - pre-existing)

### 3. ✅ Created Linux Server Start Script
- **File**: `scripts/server/start_server_background.sh`
- **Features**: Same functionality as PowerShell version
- **Usage**: `./scripts/server/start_server_background.sh -c ontime_denormalized.yaml`

### 4. ✅ Architecture Analysis Complete
Created three comprehensive documents:

---

## Key Documents Created

### 1. `docs/GRAPH_TO_SQL_ARCHITECTURE.md` (Main Analysis)

**Key Findings**:
- **Problem**: Graph concepts (nodes/edges) leak into SQL generation layer
- **Root Cause**: No clear boundary between graph and SQL translation
- **Impact**: Denormalized edge tables fail because alias mapping happens too late

**Proposed Solution**: `AliasResolutionContext` in analyzer layer
- Maps Cypher aliases → SQL table aliases **early**
- Resolves property mappings during analysis phase
- RenderPlan becomes pure SQL (no graph concepts)

### 2. `docs/DENORMALIZED_EDGE_FIX_PLAN.md` (Detailed Fix)

**The LAX Query Problem**:
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX'
```

**Should generate**:
```sql
SELECT * FROM flights AS f WHERE f.Origin = 'LAX'
```

**Currently generates** (BROKEN):
```sql
SELECT * FROM flights AS f WHERE a.origin = 'LAX'  -- ❌ 'a' doesn't exist!
```

**Implementation Plan**: 6 phases, ~23 hours

### 3. Architecture Flow Diagrams

Visual representation of:
- Current 5-layer translation (Parser → Analyzer → RenderPlan → SQL)
- Where graph concepts leak into SQL layer
- Proposed early resolution approach

---

## What We Were Working On (Before WSL Switch)

From git log and existing documents:

### Last Commits (Nov 23, 2025)
1. `5a61360` - Reverted table alias remapping (it wasn't working)
2. `6d3b56b` - Attempted denormalized query support (incomplete)
3. `a99a4aa` - Fixed PropertyValue type conversions
4. `c0061cd` - Refactored is_denormalized flag setting

### Known Issues
From `DENORMALIZED_EDGE_IMPLEMENTATION_GAP.md`:

**What Works**:
- ✅ Schema loading (from_node_properties/to_node_properties)
- ✅ Property mapping function (for CTEs only)

**What's Broken**:
- ❌ Query planner doesn't detect denormalized patterns
- ❌ JOIN optimizer always creates JOINs (even when single table)
- ❌ Filter alias mapping uses graph aliases in SQL context
- ❌ Test: `MATCH (a)-[f]->(b) WHERE a.origin='LAX'` fails

---

## Root Cause Analysis

### The Conceptual Problem

**Current Architecture** has unclear boundaries:
```
Parser (Graph) 
  ↓
LogicalPlan (Graph + partial SQL) 
  ↓
Analyzer (Mixed concepts)  ⚠️ PROBLEM HERE
  ↓
RenderPlan (Should be SQL, but has graph concepts)  ⚠️ AND HERE
  ↓
SQL Generation
```

**Key Issues**:
1. **`ViewScan.is_denormalized` flag** - Graph concept in LogicalPlan
2. **`get_denormalized_aliases()`** - Called during SQL generation (too late!)
3. **Filter tagging** - Uses Cypher aliases (`"a"`) instead of SQL aliases (`"f"`)
4. **Property mapping** - Applied inconsistently

### Why It's Hard to Fix Currently

**Scattered Information**:
- Schema knows: Airport table = flights table
- LogicalPlan knows: ViewScan for `a` has alias `"a"`
- Filter knows: Filter on `"a".origin`
- RenderPlan knows: Only `"f"` exists in FROM clause

**Information Loss**:
By the time we generate SQL, we've lost:
- Which edge `"a"` belongs to (`"f"`)
- Whether `"a"` is from/to position (affects property mapping)
- Original pattern structure

---

## Proposed Solution: Early Alias Resolution

### New Component: `AliasResolutionContext`

```rust
// In query_planner/analyzer/alias_resolution.rs (NEW FILE)

pub struct AliasResolutionContext {
    /// Maps Cypher alias → SQL table alias
    /// Example: "a" → "f", "b" → "f", "f" → "f"
    alias_map: HashMap<String, String>,
    
    /// Maps (Cypher alias, property) → (SQL alias, column)
    /// Example: ("a", "origin") → ("f", "Origin")
    property_map: HashMap<(String, String), (String, String)>,
}
```

### When It Runs

**New Analyzer Pass** (after schema_inference, before filter_tagging):

```
1. schema_inference    ← Identifies tables
2. alias_resolution    ← NEW: Resolves graph→SQL aliases  
3. filter_tagging      ← Uses resolved aliases
4. filter_push_down    ← etc.
```

### How It Works

For pattern: `(a:Airport)-[f:Flight]->(b:Airport)`

**Step 1: Detect Denormalized**
```rust
let airport_table = schema.get_node("Airport").table;  // "flights"
let flight_table = schema.get_relationship("FLIGHT").table;  // "flights"

if airport_table == flight_table {
    // Denormalized!
}
```

**Step 2: Build Mappings**
```rust
alias_map.insert("a", "f");  // Node a maps to edge f
alias_map.insert("b", "f");  // Node b maps to edge f
alias_map.insert("f", "f");  // Edge f maps to itself

property_map.insert(("a", "origin"), ("f", "Origin"));  // from_node_properties
property_map.insert(("b", "origin"), ("f", "Dest"));    // to_node_properties
```

**Step 3: Use in Filter Tagging**
```rust
// When tagging: WHERE a.origin = 'LAX'
let (sql_alias, sql_column) = resolution_ctx.resolve_property("a", "origin");
// Returns: ("f", "Origin")

PropertyAccess {
    table_alias: "f",      // ✅ Correct!
    column: "Origin",      // ✅ Correct!
}
```

### Result

**RenderPlan** now has clean SQL:
```rust
RenderPlan {
    from: ViewTableRef { table: "flights", alias: "f" },
    joins: vec![],  // No joins needed!
    filters: PropertyAccess("f", "Origin"),  // ✅ Valid SQL
}
```

**Generated SQL**:
```sql
SELECT * FROM flights AS f WHERE f.Origin = 'LAX'
```

✅ **Correct!**

---

## Implementation Plan

### Phase 1: Core Infrastructure (3 hours)
- [ ] Create `src/query_planner/analyzer/alias_resolution.rs`
- [ ] Define `AliasResolutionContext` struct
- [ ] Implement `resolve()` and `resolve_property()` methods
- [ ] Add unit tests for context

### Phase 2: Pattern Detection (6 hours)
- [ ] Implement `resolve_aliases()` function
- [ ] Handle GraphJoins pattern detection
- [ ] Detect denormalized nodes via schema comparison
- [ ] Build alias_map and property_map
- [ ] Handle edge cases (mixed scenarios, variable-length paths)

### Phase 3: Integration (4 hours)
- [ ] Add `AliasResolutionContext` to `PlanCtx`
- [ ] Modify `filter_tagging.rs` to use resolution context
- [ ] Update `PropertyAccess` creation throughout analyzer
- [ ] Pass resolution context through analyzer passes

### Phase 4: Cleanup (2 hours)
- [ ] Remove `is_denormalized` flag from `ViewScan`
- [ ] Delete `get_denormalized_aliases()` from helpers
- [ ] Remove denormalized-specific logic from `plan_builder.rs`
- [ ] Simplify RenderPlan building

### Phase 5: Testing (6 hours)
- [ ] Create test for LAX query
- [ ] Test with ontime_denormalized.yaml
- [ ] Test mixed scenarios (some nodes denorm, some not)
- [ ] Test RETURN with denormalized properties
- [ ] Update existing denormalized tests
- [ ] Verify all 477 tests still pass

### Phase 6: Documentation (2 hours)
- [ ] Update STATUS.md
- [ ] Add to CHANGELOG.md
- [ ] Update architecture docs with solution
- [ ] Add inline code comments

**Total Estimate**: ~23 hours (3 working days)

---

## Test Cases to Validate Fix

### 1. Simple Filter (The LAX Query)
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX'
```
**Expected SQL**: `SELECT * FROM flights AS f WHERE f.Origin = 'LAX'`

### 2. Both Nodes Filtered
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX' AND b.origin = 'JFK'
```
**Expected SQL**: `WHERE f.Origin = 'LAX' AND f.Dest = 'JFK'`

### 3. Edge Property
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE f.distance > 1000
```
**Expected SQL**: `WHERE f.Distance > 1000`

### 4. Mixed Properties
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.city = 'Los Angeles' AND f.carrier = 'AA'
```
**Expected SQL**: `WHERE f.OriginCityName = 'Los Angeles' AND f.Carrier = 'AA'`

### 5. RETURN Denormalized Properties
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
RETURN a.city, f.flight_num, b.airport
```
**Expected SQL**: `SELECT f.OriginCityName, f.FlightNum, f.DestAirportName FROM flights AS f`

---

## Why This Refactoring Matters

### 1. **Correctness**
- Fixes broken denormalized edge queries
- Eliminates invalid SQL generation

### 2. **Clarity**
- Clean separation: Graph (Parser/LogicalPlan) ↔ SQL (RenderPlan)
- Single source of truth for alias mappings

### 3. **Maintainability**
- Easier to debug (print resolution context)
- Easier to extend (polymorphic edges, multi-hop patterns)

### 4. **Performance**
- Eliminate redundant tree walks
- One-time resolution in analyzer
- Optimal SQL for denormalized patterns (no unnecessary JOINs)

### 5. **Extensibility**
- Template for handling other special cases
- Clear pattern for future features

---

## Current Development Status

### What's Ready
- ✅ WSL environment fully set up
- ✅ Rust toolchain working
- ✅ Docker/ClickHouse running
- ✅ Comprehensive architecture analysis complete
- ✅ Implementation plan detailed
- ✅ Test cases defined

### What's Next
**Option A: Start Implementation**
Begin Phase 1 (Core Infrastructure) - create `AliasResolutionContext`

**Option B: Validate Approach**
Review architecture documents, discuss any concerns, refine plan

**Option C: Quick Test**
Set up ontime schema and manually test current behavior to confirm issue

---

## Files to Reference

### Architecture Documents (NEW)
- `docs/GRAPH_TO_SQL_ARCHITECTURE.md` - Full analysis
- `docs/DENORMALIZED_EDGE_FIX_PLAN.md` - Detailed implementation
- `scripts/server/start_server_background.sh` - Linux server script

### Existing Context
- `DENORMALIZED_EDGE_IMPLEMENTATION_GAP.md` - What's broken
- `DENORMALIZED_EDGE_FINDINGS.md` - Investigation notes
- `schemas/examples/ontime_denormalized.yaml` - Test schema

### Key Source Files
- `src/query_planner/analyzer/filter_tagging.rs` - Where fix needed
- `src/query_planner/logical_plan/view_scan.rs` - Has is_denormalized flag
- `src/render_plan/plan_builder_helpers.rs` - Has get_denormalized_aliases()
- `src/graph_catalog/graph_schema.rs` - Schema structure

---

## Recommended Next Steps

1. **Review Architecture Documents** (30 min)
   - Read `GRAPH_TO_SQL_ARCHITECTURE.md`
   - Validate proposed approach
   - Ask questions/raise concerns

2. **Quick Validation Test** (15 min)
   - Start server with ontime schema
   - Run LAX query manually
   - Confirm it produces invalid SQL

3. **Begin Implementation** (if approach approved)
   - Start with Phase 1: Create `AliasResolutionContext`
   - Implement unit tests first (TDD)
   - Build incrementally

---

## Questions to Consider

1. **Scope**: Should we fix denormalized edges first, or refactor the entire graph→SQL boundary?
2. **Testing**: Should we write integration tests before implementation, or after?
3. **Backwards Compatibility**: Will this change break any existing (non-denormalized) queries?
4. **Performance**: Should we benchmark alias resolution overhead?
5. **Variable-Length Paths**: How do CTEs interact with denormalized patterns?

---

## Resources

**Documentation**:
- Architecture analysis: 3 comprehensive docs created
- Implementation plan: 6 phases with time estimates
- Test cases: 5 validation queries defined

**Environment**:
- Rust 1.91.1 installed and working
- Docker/ClickHouse ready
- Git configured for Linux

**Codebase State**:
- 466/477 tests passing (97.7%)
- 11 known failures (unrelated to denormalized edges)
- Clean git status (ready for new branch)

---

**Ready to proceed with implementation? Let me know which option you'd like to pursue!**
