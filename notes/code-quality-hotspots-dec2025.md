# Code Quality Hotspots - December 2025

**Analysis Date**: December 11, 2025  
**Purpose**: Identify remaining "disaster areas" after WITH handler refactoring

---

## Executive Summary

✅ **RECENTLY FIXED**: WITH clause handlers - eliminated ~120 lines duplication (Dec 11, 2025)

🚨 **REMAINING HOTSPOTS** (by priority):

1. **graph_join_inference.rs** - 5,778 lines, monolithic impl block
2. **plan_builder.rs** - 9,129 lines, god object anti-pattern
3. **Nested CTE hoisting** - Still manual in 2 locations
4. **GROUP BY expansion** - 3 different implementations
5. **Filter scope splitting** - Logic duplicated

---

## Hotspot #1: graph_join_inference.rs (5,778 lines) 🔥🔥🔥

### The Problem

**Second largest file** in codebase with monolithic structure:

```rust
impl GraphJoinInference {
    // 5,700+ lines in a SINGLE impl block!
    // Handles ALL graph pattern types:
    // - Standard nodes/edges
    // - Denormalized edges
    // - FK edges
    // - Polymorphic edges
    // - Undirected patterns
    // - Multi-hop patterns
    // - OPTIONAL MATCH
}
```

### Metrics

- **Lines**: 5,778
- **Functions**: Likely 50-100 methods in single impl
- **Cyclomatic Complexity**: Estimated very high
- **Test Coverage**: Unknown (likely implicit via integration tests)

### Why It's Fragile

1. **God Object**: One struct responsible for all pattern types
2. **Long Methods**: Some methods likely 200+ lines
3. **Deep Nesting**: Match statements within match statements
4. **Hard to Test**: Integration tests only, no unit tests for individual strategies
5. **Hard to Extend**: Adding new pattern type requires modifying 1000+ line methods

### Recent Improvements

✅ **Pattern Schema Context** (Dec 7, 2025):
- Added `PatternSchemaContext` abstraction
- `USE_PATTERN_SCHEMA_V2=1` toggle for new code path
- Identical SQL output between v1 and v2
- **Status**: v2 implemented but not yet default

### Remaining Issues

❌ **V1 Still Default**: Original 4,800+ line logic still active
❌ **No v1 Removal Plan**: When to deprecate old code path?
❌ **Limited v2 Testing**: Only tested Traditional pattern so far

### Recommended Refactoring

**Phase 1** (1 week): Make v2 default, remove v1
```bash
# Test all schema variations with v2
USE_PATTERN_SCHEMA_V2=1 cargo test
# Remove v1 code path
# Estimated reduction: 1,500-2,000 lines
```

**Phase 2** (2 weeks): Extract strategy pattern
```rust
trait PatternStrategy {
    fn generate_joins(&self, ctx: &PatternSchemaContext) -> Vec<Join>;
}

struct TraditionalPatternStrategy;
struct FkEdgePatternStrategy;
struct DenormalizedPatternStrategy;
// etc.
```

**Phase 3** (1 week): Split into modules
```
query_planner/analyzer/
  graph_join_inference/
    mod.rs              (100 lines - dispatcher)
    traditional.rs      (500 lines)
    fk_edge.rs          (400 lines)
    denormalized.rs     (600 lines)
    polymorphic.rs      (300 lines)
```

**Expected Benefit**: 
- Reduce from 5,778 lines to ~2,000 lines (core logic)
- Move 3,000+ lines to strategy modules
- Enable unit testing of individual strategies

---

## Hotspot #2: plan_builder.rs (9,129 lines) 🔥🔥🔥

### The Problem

**Largest file** in codebase, god object for all rendering:

```rust
impl LogicalPlan {
    fn to_render_plan(&self, schema: &GraphSchema) -> Result<RenderPlan> {
        // 9,000+ lines handling:
        // - All plan types (Match, With, Return, Union, GroupBy, etc.)
        // - All CTE generation
        // - All JOIN generation
        // - All filter extraction
        // - All projection handling
    }
}
```

### Metrics

- **Lines**: 9,129
- **WITH Handlers**: 3 (recently refactored ✅)
- **Plan Types**: 15+ different LogicalPlan variants
- **Helper Functions**: ~50 functions
- **Test Coverage**: Via integration tests only

### Recent Improvements

✅ **WITH Handler Refactoring** (Dec 11, 2025):
- Extracted 3 helper functions
- Eliminated ~120 lines duplication
- Still have 3 separate WITH handlers

### Remaining Issues

❌ **Still 9,100+ lines**: Even after refactoring
❌ **Monolithic Structure**: All plan types in one file
❌ **Three WITH Handlers**: Could be unified further
❌ **Implicit State**: CTEs, filters, projections all threaded manually

### Code Organization Issues

**Current Structure** (anti-pattern):
```rust
// plan_builder.rs - 9,129 lines
fn to_render_plan() {
    match self {
        Match => build_match_plan(),           // 500 lines
        With => build_with_plan(),             // 800 lines
        Return => build_return_plan(),         // 300 lines
        Union => build_union_plan(),           // 600 lines
        GroupBy => build_group_by_plan(),      // 400 lines
        GraphJoins => build_graph_joins(),     // 1,500 lines
        GraphRel => build_graph_rel(),         // 800 lines
        // ... 8 more plan types
    }
}

// Three duplicate WITH handlers still exist!
fn build_with_match_cte_plan()          // 150 lines
fn build_chained_with_match_cte_plan()  // 400 lines  
fn build_with_aggregation_match_cte_plan() // 180 lines
```

### Recommended Refactoring

**Phase 1** (3 days): Extract plan type modules
```
render_plan/
  plan_builder/
    mod.rs                  (500 lines - dispatcher)
    match_plan.rs          (500 lines)
    with_plan.rs           (800 lines)
    return_plan.rs         (300 lines)
    union_plan.rs          (600 lines)
    group_by_plan.rs       (400 lines)
    graph_joins_plan.rs    (1,500 lines)
    graph_rel_plan.rs      (800 lines)
```

**Phase 2** (2 days): Unify WITH handlers
```rust
// with_plan.rs
fn build_with_cte_plan(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    config: WithConfig,  // Flags for: chained, aggregation, etc.
) -> Result<RenderPlan>
```

**Phase 3** (1 week): Builder pattern for RenderPlan
```rust
struct RenderPlanBuilder {
    cte_registry: CteRegistry,
    filter_tracker: FilterTracker,
    projection_tracker: ProjectionTracker,
}

impl RenderPlanBuilder {
    fn build_match(&mut self, plan: &MatchPlan) -> Result<RenderPlan>;
    fn add_cte(&mut self, cte: Cte) -> String;  // Returns CTE name
    fn hoist_nested_ctes(&mut self, plan: &mut RenderPlan);
}
```

**Expected Benefit**:
- Reduce main file from 9,129 to ~500 lines
- Extract 8,000+ lines to plan-specific modules
- Enable unit testing per plan type
- Centralize CTE/filter/projection management

---

## Hotspot #3: Nested CTE Hoisting (Manual, 2 locations) 🔥🔥

### The Problem

**From `architectural-fragility-analysis.md`**:

CTE hoisting still manual in 2 places:
- `build_with_match_cte_plan` (line ~505)
- `build_chained_with_match_cte_plan` (line ~771)

```rust
// Pattern repeated 2x:
let nested_ctes = std::mem::take(&mut with_cte_render.ctes.0);
if !nested_ctes.is_empty() {
    log::info!("🔧 Hoisting {} nested CTEs", nested_ctes.len());
    all_ctes.extend(nested_ctes);
}
```

### Why It's Fragile

1. **Easy to forget**: No compiler enforcement
2. **Silent failure**: Generates invalid SQL if missed
3. **No validation**: No check that all CTEs are hoisted

### Recommended Fix

**Option A** (Quick, 1 hour): Extract helper function
```rust
fn hoist_nested_ctes(render_plan: &mut RenderPlan) -> Vec<Cte> {
    std::mem::take(&mut render_plan.ctes.0)
}

// Usage:
let nested = hoist_nested_ctes(&mut with_cte_render);
all_ctes.extend(nested);
```

**Option B** (Better, 1 day): CTE Registry
```rust
struct CteRegistry {
    ctes: Vec<Cte>,
}

impl CteRegistry {
    fn register(&mut self, cte: Cte) -> String;
    fn merge(&mut self, other: CteRegistry);  // Auto-hoisting
    fn validate(&self, sql: &str) -> Result<()>;
}
```

**Recommendation**: Do **Option A now** (1 hour), **Option B later** (part of plan_builder.rs refactoring)

---

## Hotspot #4: GROUP BY Expansion (3 implementations) 🔥

### The Problem

**From `architectural-fragility-analysis.md`**:

Three different ways to expand `TableAlias` in GROUP BY:

1. **extract_group_by** (lines 5878-6050) - ✅ Correct, expands to ID column
2. **build_chained_with_match_cte_plan** (recently fixed) - ✅ Now uses helpers
3. **build_with_aggregation_match_cte_plan** (recently fixed) - ✅ Now uses helpers

### Current Status

✅ **Partially Fixed** (Dec 11, 2025):
- Created 2 helper functions for LogicalExpr expansion
- Created 1 helper function for RenderExpr expansion
- All 3 implementations now consolidated!

### Remaining Issue

❓ **Are helpers actually used everywhere?**

Let me check if `extract_group_by` uses the new helpers or still has inline logic:

**Action Item**: Verify `extract_group_by` could also use the new helper functions

---

## Hotspot #5: Filter Scope Splitting (2 implementations?) 🔥

### The Problem

**From `architectural-fragility-analysis.md`**:

> Filter scope splitting: Implemented 1x

**Location**: `build_with_match_cte_plan` (lines ~520-542):
```rust
if let Some(outer_filter) = render_plan.filters.0.take() {
    let (internal_filter, external_filter) = split_filter_by_scope(&outer_filter, &exposed_aliases);
    // Move internal filters to CTE
    // Keep external filters in outer query
}
```

### Questions

1. Is `split_filter_by_scope` a shared helper or inline logic?
2. Do other WITH handlers need the same logic?
3. Is this duplicated anywhere else?

**Action Item**: Investigate if filter scope splitting is properly centralized

---

## Hotspot #6: Relationship Type Handling 🔥

### The Problem

**From KNOWN_ISSUES.md** (referenced in architectural-fragility-analysis.md):

Similar duplication patterns exist in relationship type handling.

### Symptoms

- Multiple relationship types (`[:TYPE1|TYPE2]`) generate UNION SQL
- Logic scattered across:
  - Parser (`path_pattern.rs`)
  - Analyzer (`graph_join_inference.rs`)
  - Renderer (`plan_builder.rs`)
  - CTE generation (`variable_length_cte.rs`)

### Potential Issues

❓ **Is UNION generation duplicated?**
❓ **Do all code paths handle multiple types consistently?**
❓ **Are there edge cases with VLP + multiple types?**

**Action Item**: Audit relationship type handling for duplication

---

## Summary of Remaining Work

### Immediate (1-2 days)
1. ✅ **WITH handlers** - DONE (Dec 11, 2025)
2. 🔲 **CTE hoisting helper** - Extract `hoist_nested_ctes()` (1 hour)
3. 🔲 **Verify GROUP BY** - Check if `extract_group_by` can use new helpers (30 min)
4. 🔲 **Filter scope audit** - Verify `split_filter_by_scope` is centralized (30 min)

### Short-term (1 week)
1. 🔲 **Pattern Schema v2 Default** - Remove v1 code path from graph_join_inference.rs
2. 🔲 **Unify WITH handlers** - Single function with config flags
3. 🔲 **Add CTE validation** - Assert all references exist in SQL

### Medium-term (2-4 weeks)
1. 🔲 **Extract plan_builder modules** - Split 9,129 lines into 8 modules
2. 🔲 **Strategy pattern for patterns** - Extract 5,778 lines into strategies
3. 🔲 **CTE Registry** - Centralized CTE management
4. 🔲 **RenderPlanBuilder** - Builder pattern for render plans

### Long-term (1-2 months)
1. 🔲 **Comprehensive unit tests** - Test individual strategies
2. 🔲 **Property-based tests** - Test CTE hoisting correctness
3. 🔲 **Architectural documentation** - ADRs for key decisions

---

## Code Quality Metrics

### Current State
| Metric | Value | Target | Status |
|--------|-------|--------|--------|
| Largest file | 9,129 lines | < 2,000 | 🔴 |
| Second largest | 5,778 lines | < 2,000 | 🔴 |
| WITH duplication | 0 lines | 0 | ✅ |
| CTE hoisting | Manual (2x) | Automated | 🔴 |
| GROUP BY expansion | 3 helpers | 1 unified | 🟡 |
| Unit test coverage | Low | High | 🔴 |
| Integration tests | 8/8 LDBC | 8/8 | ✅ |

### After Quick Wins (1 week)
| Metric | Value | Status |
|--------|-------|--------|
| Largest file | 9,129 lines | 🔴 (no change) |
| CTE hoisting | Automated | ✅ |
| Pattern v2 | Default | ✅ |
| WITH handlers | Unified | ✅ |

### After Major Refactoring (1 month)
| Metric | Value | Status |
|--------|-------|--------|
| Largest file | < 2,000 lines | ✅ |
| Second largest | < 1,500 lines | ✅ |
| Unit test coverage | High | ✅ |
| Code duplication | Minimal | ✅ |

---

## Prioritization Criteria

**High Priority** (Do Now):
- ✅ Blocks new features
- ✅ High bug risk
- ✅ Easy to fix (< 1 day)

**Medium Priority** (Do This Sprint):
- ✅ Reduces technical debt
- ✅ Improves maintainability
- ⚠️ Takes 1-2 weeks

**Low Priority** (Do Later):
- ⚠️ Nice to have
- ⚠️ Requires major refactoring
- ⚠️ Takes 1+ months

### Prioritized List

1. 🔥 **CTE hoisting helper** - High (1 hour, prevents bugs)
2. 🔥 **Pattern Schema v2 default** - High (1 week, removes 1,500+ lines)
3. 🔥 **Unify WITH handlers** - High (2 days, completes refactoring)
4. 🟡 **Extract plan_builder modules** - Medium (3 days, improves structure)
5. 🟡 **Strategy pattern for patterns** - Medium (2 weeks, testability)
6. 🔵 **CTE Registry** - Low (part of larger refactoring)
7. 🔵 **Property-based tests** - Low (long-term quality)

---

## Lessons from WITH Handler Success

### What Worked

1. ✅ **Incremental approach** - Two separate refactoring commits
2. ✅ **Test after each change** - 8/8 LDBC benchmark after each commit
3. ✅ **Clear metrics** - "120 lines eliminated" is tangible
4. ✅ **Documentation** - Created comprehensive summary note

### Apply to Next Refactoring

1. **Pattern Schema v2 Default**:
   - Step 1: Test all schema variations with v2
   - Step 2: Make v2 default, keep v1 as fallback
   - Step 3: Remove v1 code path
   - Test after each step!

2. **Extract plan_builder modules**:
   - Step 1: Extract Match plan (500 lines)
   - Step 2: Extract Union plan (600 lines)
   - Step 3: Extract remaining plans one by one
   - Test after each extraction!

3. **Document progress**:
   - Update this file with completion status
   - Track lines eliminated per commit
   - Celebrate wins! 🎉

---

## References

- **WITH handler refactoring**: `notes/with-clause-refactoring-dec2025.md`
- **Architectural fragility**: `notes/architectural-fragility-analysis.md`
- **Pattern Schema v2**: `STATUS.md` (Dec 7, 2025 section)
- **LDBC benchmark**: 8/8 queries passing (100%)
- **Known issues**: `KNOWN_ISSUES.md`
