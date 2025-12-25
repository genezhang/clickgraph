# Schema Threading Architecture: Making Schema Explicit Everywhere

**Date**: December 27, 2025  
**Context**: Edge constraints VLP gap investigation  
**Root Cause**: Schema flows explicitly through planning but implicitly (via global lookups) through SQL generation

---

## Current Architecture (Partially Explicit)

```
User Request
    ↓
handlers.rs: Schema Selection
    ├─ USE clause → extract schema name
    ├─ schema_name parameter → use explicit name  
    ├─ No schema specified → explicit "default" with logging
    └─ ✅ `let schema = GLOBAL_SCHEMAS.get(schema_name)` (ONE LOOKUP)
         ↓
handlers.rs: Query Planning
    ├─ `logical_plan.to_render_plan(&schema)` ✅ EXPLICIT
    └─ Schema flows through planning layer
         ↓
plan_builder.rs: SQL Generation Entry
    ├─ `impl RenderPlanBuilder for LogicalPlan`
    ├─ `fn to_render_plan(&self, schema: &GraphSchema)` ✅ EXPLICIT
    └─ Schema available for JOIN extraction
         ↓
plan_builder.rs: Constraint Compilation
    ├─ `extract_joins(&self, schema: &GraphSchema)` ✅ EXPLICIT (fixed Dec 27)
    └─ `compile_constraint(schema, ...)` ✅ Works for single-hop
         ↓
cte_extraction.rs: CTE Generation  ⚠️ SCHEMA LOST HERE!
    ├─ `extract_ctes_with_context(..., schema: &GraphSchema)` ✅ Has schema
    ├─ Creates `VariableLengthCteGenerator::new_*(...)`
    └─ ❌ Doesn't pass schema to generator!
         ↓
variable_length_cte.rs: VLP SQL Generation  ❌ NO SCHEMA!
    ├─ `generate_cte()` → calls `generate_base_case()`
    ├─ Needs schema for constraint compilation
    └─ ❌ Falls back to: `for schema_name in ["default", ""].iter()`
         ↓
    ❌ BREAKS: Named schemas → constraint lookup fails → constraint skipped silently
```

---

## The Gap: SQL Generation Layer Loses Schema

### Why This Happens

**Logical Planning** (✅ Schema-aware):
- `query_planner/` receives schema from handlers
- Type inference uses schema for node/edge resolution  
- Property mapping uses schema for Cypher→SQL translation

**SQL Generation** (❌ Schema-agnostic):
- `clickhouse_query_generator/variable_length_cte.rs` → **no schema parameter**
- `render_plan/cte_generation.rs` → **hardcoded "default" lookups**
- Property wildcard expansion → **`GLOBAL_SCHEMAS.get("default")`** (line 219)
- Constraint compilation → **schema loop `["default", ""]`** (line 465)

### Symptoms

1. **Edge Constraints Don't Work in VLP** (test_edge_constraint_vlp fails):
   ```cypher
   // Schema: lineage (NOT "default")
   MATCH (f:DataFile)-[c:COPIED_BY*1..3]->(t:DataFile)
   WHERE f.file_id = 1 AND c.created_timestamp <= t.created_timestamp
   RETURN f.path, t.path
   
   // Generated SQL: NO constraint `created_timestamp <=` appears!
   // Why? variable_length_cte.rs:465 loops over ["default", ""] → misses "lineage" schema
   ```

2. **Property Wildcard in VLP CTEs** (edge case - not critical):
   ```cypher
   MATCH p = (a)-[*]->(b) RETURN nodes(p)
   // cte_generation.rs:219 → GLOBAL_SCHEMAS.get("default") → might miss schema
   ```

3. **Silent Failures**: No error, just missing SQL clauses → mysterious bugs later

---

## Target Architecture (Fully Explicit)

```
User Request
    ↓
handlers.rs: Schema Selection ✅
    └─ `let schema = ...` (ONE GLOBAL LOOKUP)
         ↓
handlers.rs → plan_builder.rs ✅  
    └─ `logical_plan.to_render_plan(&schema)`
         ↓
plan_builder.rs → cte_extraction.rs ✅
    └─ `extract_ctes_with_context(..., schema)` 
         ↓
cte_extraction.rs → variable_length_cte.rs 🔧 FIX NEEDED
    ├─ Create generator: `VariableLengthCteGenerator::new_*(..., schema)` 
    └─ Pass schema to constructor
         ↓
variable_length_cte.rs: VLP SQL Generation 🔧 FIX NEEDED
    ├─ Store: `schema: &GraphSchema` field
    ├─ `generate_base_case(...)` → compile_constraint(self.schema, ...)
    ├─ `generate_recursive_case(...)` → compile_constraint(self.schema, ...)
    └─ ✅ No more hardcoded lookups!
         ↓
    ✅ WORKS: All schemas → constraint lookup succeeds → SQL includes constraints
```

### Key Principle

**"Schema flows like blood through the codebase"** - Selected once, threaded everywhere, never looked up again.

---

## Implementation Plan

### Phase 1: VLP Constraint Support (Critical - 12.5% test gap)

**File**: `src/clickhouse_query_generator/variable_length_cte.rs`

**Changes**:
1. Add `schema: &'a GraphSchema` field to `VariableLengthCteGenerator`
2. Update all constructors (`new`, `new_denormalized`, `new_mixed`, `new_with_fk_edge`)
3. Thread schema to `compile_constraint()` in:
   - `generate_base_case()` (base CTE part)
   - `generate_recursive_case()` (recursive CTE part)
4. Remove hardcoded loop: `for schema_name in ["default", ""].iter()`

**Call Sites** (cte_extraction.rs lines 1250, 1268, 1291):
```rust
// BEFORE
VariableLengthCteGenerator::new_denormalized(
    spec.clone(),
    &rel_table,
    &from_col,
    ...
)

// AFTER
VariableLengthCteGenerator::new_denormalized(
    spec.clone(),
    &rel_table,
    &from_col,
    ...,
    schema  // 🔧 ADD THIS
)
```

**Estimated**: 2-3 hours (straightforward parameter threading)

---

### Phase 2: Property Wildcard Expansion (Low Priority - Edge Case)

**File**: `src/render_plan/cte_generation.rs`

**Changes**:
1. Line 219: `extract_node_label_from_viewscan(plan)` → add `schema` parameter
2. Remove: `GLOBAL_SCHEMAS.get("default")`
3. Use: Passed-in schema for node label resolution

**Impact**: Only affects VLP queries with `RETURN n.*` (rare)

**Estimated**: 1 hour

---

### Phase 3: Audit & Document (Complete Schema Flow)

**Goal**: Verify NO remaining hardcoded schema lookups after handlers.rs

**Audit checklist**:
- [x] handlers.rs: Schema selection (✅ explicit with loud failures)
- [x] plan_builder.rs: JOIN extraction (✅ fixed Dec 27 - `extract_joins(schema)`)
- [ ] variable_length_cte.rs: VLP constraint compilation (⏸️ TODO Phase 1)
- [ ] cte_generation.rs: Property wildcard expansion (⏸️ TODO Phase 2)
- [ ] constraint_compiler.rs: Already takes schema parameter (✅ ready)

**Documentation**:
- Update `EDGE_CONSTRAINTS_FIX_SUMMARY.md` with Phase 1 completion
- Add architecture diagram to `docs/architecture/schema-flow.md`
- Update `STATUS.md`: VLP constraints from ⏸️ to ✅

---

## Why This Matters

### Technical Benefits

1. **Correctness**: Schema always matches query context (no hidden "default" assumptions)
2. **Debuggability**: Clear flow from selection → usage (no mysterious global lookups)
3. **Multi-tenancy**: Named schemas work correctly everywhere
4. **Performance**: One lookup instead of repeated global map access

### Philosophical Benefits

**"Always do the right thing"** - User expectations:
- If I specify schema "lineage" → ALL code paths should use "lineage"
- No surprises where feature X works but feature Y silently uses "default"
- Explicit failures better than silent incorrect behavior

**Principle**: *"Use default explicitly, fail loudly, never fall back luckily"*

---

## Current Status

**Edge Constraints**:
- ✅ Single-hop queries: 7/7 tests passing (100%)
- ✅ Standard, FK-edge, Denormalized, Polymorphic schemas
- ⏸️ VLP queries: 0/1 test passing (documented TODO)

**Schema Threading**:
- ✅ handlers.rs → plan_builder.rs (explicit)
- ✅ plan_builder.rs → cte_extraction.rs (explicit)
- ❌ cte_extraction.rs → variable_length_cte.rs (implicit - global lookup)
- ❌ variable_length_cte.rs → constraint compilation (missing schema)

**Next Action**: Implement Phase 1 (VLP constraint support) to achieve 8/8 tests passing (100%)

---

## References

- **Edge Constraints Issue**: variable_length_cte.rs line 465
- **Property Wildcard Issue**: cte_generation.rs line 219
- **Constraint Compiler**: `src/render_plan/constraint_compiler.rs` (ready to use)
- **Test Gap**: `tests/integration/test_edge_constraints.py::test_edge_constraint_vlp`

**Key Files**:
1. `src/server/handlers.rs` - Schema selection (✅ solid)
2. `src/render_plan/plan_builder.rs` - JOIN extraction (✅ fixed)
3. `src/render_plan/cte_extraction.rs` - CTE generation (needs update)
4. `src/clickhouse_query_generator/variable_length_cte.rs` - VLP SQL (needs schema field)
5. `src/render_plan/cte_generation.rs` - Property expansion (needs schema param)
