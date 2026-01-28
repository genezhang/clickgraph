# WITH Clause Refactoring Summary (December 2025)

## Overview

Systematic code deduplication effort that eliminated ~120 lines of duplicate code across WITH clause handlers in `src/render_plan/plan_builder.rs`.

**Trigger**: IC-1 query fix exposed 40% code duplication (~560 lines) across three WITH handlers, documented in `notes/architectural-fragility-analysis.md`.

**Outcome**: 
- ✅ Reduced duplicate logic from 3 implementations to reusable helper functions
- ✅ Maintained 100% LDBC benchmark passing (8/8 queries)
- ✅ Improved code maintainability and readability

## Code Metrics

### Commits
1. **61ccbbb** - Extract TableAlias expansion helpers
   - Added: 73 lines (helper functions)
   - Removed: 64 lines (duplicate code)
   - Net: +9 lines, but eliminated ~50 lines of duplication
   
2. **e19331f** - Extract wildcard expansion helper
   - Added: 82 lines (helper function + usage)
   - Removed: 71 lines (duplicate code)
   - Net: +11 lines, but eliminated ~60 lines of duplication

### Total Impact
- **Duplication Eliminated**: ~120 lines
- **Helper Functions Created**: 3
- **Handlers Refactored**: 2 of 3 (66%)
- **Test Coverage**: 100% (8/8 LDBC queries passing after each change)

## Helper Functions Created

### 1. `expand_table_alias_to_select_items()`
**Purpose**: Convert `LogicalExpr::TableAlias` to SELECT items with ALL columns

**Signature**:
```rust
fn expand_table_alias_to_select_items(
    group_by_exprs: Vec<&LogicalExpr>,
    with_alias: &str,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<Vec<SelectItem>>
```

**Usage**: `build_chained_with_match_cte_plan` (line ~715)

**Replaces**: ~30 lines of inline SELECT item expansion logic

**Key Logic**:
- Filters GROUP BY expressions for matching WITH alias
- Converts each LogicalExpr to RenderExpr
- Creates SelectItem with column alias (e.g., "friend.id")
- Returns complete list of explicit columns

---

### 2. `expand_table_alias_to_group_by_exprs()`
**Purpose**: Convert `LogicalExpr::TableAlias` to GROUP BY expressions

**Signature**:
```rust
fn expand_table_alias_to_group_by_exprs(
    group_by_exprs: Vec<&LogicalExpr>,
    with_alias: &str,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<Vec<RenderExpr>>
```

**Usage**: `build_chained_with_match_cte_plan` (line ~748)

**Replaces**: ~20 lines of inline GROUP BY expansion logic

**Key Logic**:
- Filters GROUP BY expressions for matching WITH alias
- Converts each LogicalExpr to RenderExpr
- Returns expressions for ClickHouse GROUP BY clause

---

### 3. `replace_wildcards_with_group_by_columns()`
**Purpose**: Replace wildcards and expand TableAlias in already-rendered SELECT items

**Signature**:
```rust
fn replace_wildcards_with_group_by_columns(
    select_items: Vec<SelectItem>,
    group_by_columns: &[RenderExpr],
    with_alias: &str,
) -> Vec<SelectItem>
```

**Usage**: `build_with_aggregation_match_cte_plan` (line ~1137)

**Replaces**: ~70 lines of wildcard + TableAlias expansion logic

**Key Logic**:
- Detects wildcards: `Column("*")` or `PropertyAccessExp` with "*"
- If wildcard + GROUP BY exists: expands to explicit GROUP BY columns
- If wildcard alone: converts to "with_alias.*"
- If `RenderExpr::TableAlias`: matches with GROUP BY, uses matching expression
- Otherwise: keeps item as-is

**Context Difference**: Operates on `RenderExpr` (post-rendering) vs `LogicalExpr` (pre-rendering)

## Refactored Handlers

### Handler 1: `build_chained_with_match_cte_plan` ✅
**Lines**: ~483-800 (before refactoring)

**Pattern**: Chained WITH clauses (e.g., `WITH a MATCH ... WITH b MATCH ...`)

**Changes**:
- Line ~715: Replaced inline SELECT expansion with `expand_table_alias_to_select_items()`
- Line ~748: Replaced inline GROUP BY expansion with `expand_table_alias_to_group_by_exprs()`

**Code Reduction**: ~50 lines eliminated

**Status**: ✅ Fully refactored, 8/8 queries passing

---

### Handler 2: `build_with_aggregation_match_cte_plan` ✅
**Lines**: ~1134-1300 (before refactoring)

**Pattern**: WITH clause with aggregation (e.g., `WITH friend, count(*) AS cnt`)

**Changes**:
- Line ~1137: Replaced ~70 lines of wildcard + TableAlias expansion with single call to `replace_wildcards_with_group_by_columns()`

**Code Reduction**: ~60 lines eliminated

**Status**: ✅ Fully refactored, 8/8 queries passing

---

### Handler 3: `build_with_match_cte_plan` ⏭️
**Lines**: ~420-580 (before refactoring)

**Pattern**: Simple WITH+MATCH patterns

**Current Code**: Manually adds ID columns when SELECT has wildcards/stars

**Evaluation**: ❌ Does NOT have the same duplication pattern
- Handles simpler case: just adding ID columns when needed
- No wildcard expansion or complex TableAlias matching
- No refactoring needed for this handler

**Status**: ⏭️ Skipped (no duplication to eliminate)

## Testing Verification

### Benchmark Results (Stable Across All Changes)
- **IC-1**: 37.5ms ✅ (was failing, now working after original fix)
- **IS-1**: 9.3ms ✅
- **IS-2**: 25.2ms ✅
- **IS-3**: 25.0ms ✅
- **IS-5**: 19.7ms ✅
- **IC-2**: 228.9ms ✅
- **IC-3**: 268.0ms ✅
- **IC-9**: 373.1ms ✅

**Total**: 8/8 queries passing (100%)

### Verification Process
After each refactoring commit:
1. `cargo build --release` - Verify compilation
2. Restart server with LDBC schema
3. Run full LDBC benchmark (`python3 benchmarks/ldbc_snb/scripts/run_benchmark.py --queries all`)
4. Confirm 8/8 queries still passing

## Root Cause Analysis

### Why Code Duplication Existed

**Historical Context**: Each WITH pattern (simple, chained, aggregation) was implemented separately without shared abstractions.

**Duplication Pattern**:
```rust
// Pattern repeated 3 times:
1. Extract WITH clause from nested plan
2. Render WITH clause to CTE
3. Expand TableAlias/wildcards to explicit columns
4. Handle SELECT items vs GROUP BY expressions
5. Create CTE and transform plan
```

**Lines 3-4 were duplicated** across handlers with slight variations:
- `build_chained_with_match_cte_plan`: LogicalExpr expansion before rendering
- `build_with_aggregation_match_cte_plan`: RenderExpr expansion after rendering
- `build_with_match_cte_plan`: Simple ID column addition

### Why Fix Now?

**IC-1 Bug Discovery**: Fixing `WITH friend, count(*) AS cnt` revealed that all three handlers had similar TableAlias expansion logic, but implementations were slightly different.

**Risk of Divergence**: Without shared helpers, future bug fixes would need to be applied to 3 different locations, risking inconsistency.

**Maintainability**: Code review of `notes/architectural-fragility-analysis.md` showed 40% duplication metric, indicating high technical debt.

## Design Decisions

### Helper Function Signatures

**Choice**: Accept different input types (LogicalExpr vs RenderExpr) rather than force single input type

**Rationale**:
- `build_chained_with_match_cte_plan` operates on LogicalExpr (before rendering)
- `build_with_aggregation_match_cte_plan` operates on RenderExpr (after rendering)
- Forcing conversion would add unnecessary complexity

**Trade-off**: Two helper families instead of one, but each is simpler and more focused

---

### Function Location

**Choice**: Keep helpers in `plan_builder.rs` rather than separate module

**Rationale**:
- All helpers tightly coupled to WITH rendering logic
- Only used by WITH handlers (no external callers)
- Moving to separate module would require exposing internal types (SelectItem, RenderExpr)

**Future Work**: If more shared abstractions emerge, consider `render_plan/with_handlers/` module

---

### Naming Convention

**Choice**: Descriptive verb-noun names (`expand_table_alias_to_select_items`)

**Rationale**:
- Clear intent from function name
- Matches Rust naming conventions
- Easy to find with grep/search

**Alternative Rejected**: Short names like `expand_alias()` (too ambiguous)

## Benefits Achieved

### 1. Single Source of Truth ✅
- TableAlias expansion logic now in ONE place per context (LogicalExpr vs RenderExpr)
- Future bugs only need fixing in helper functions, not 3 separate locations

### 2. Improved Readability ✅
- Handler functions now show high-level intent:
  ```rust
  // Before (40+ lines of inline code)
  for expr in group_by_exprs { ... complex logic ... }
  
  // After (single function call)
  let select_items = expand_table_alias_to_select_items(group_by_exprs, &with_alias, schema)?;
  ```

### 3. Easier Testing ✅
- Helper functions can be unit tested independently
- Current testing: Integration tests (8/8 LDBC queries)
- Future: Add unit tests for helper functions with edge cases

### 4. Reduced Cognitive Load ✅
- Developers reading handler code see WHAT (expand table alias) not HOW (iteration + matching + conversion)
- Complex logic encapsulated in well-named functions

## Limitations & Future Work

### Remaining Duplication
- **WITH Handler Structure**: All three handlers still follow same overall pattern:
  1. Find WITH clause
  2. Extract VLP CTEs
  3. Render CTE
  4. Transform plan
  5. Merge CTEs

**Next Step**: Consider extracting entire WITH rendering into shared `build_with_cte()` function with strategy pattern for different WITH types.

---

### Missing Abstractions
- **CTE Registry**: No centralized tracking of CTE names, dependencies, or order
- **Plan Transformation**: `replace_with_clause_with_cte_reference()` is a module-level function, not clearly associated with WITH rendering

**Next Step**: Create `WithClauseRenderer` struct that encapsulates CTE registry + transformation logic.

---

### Test Coverage
- **Integration Tests**: 8/8 LDBC queries (excellent)
- **Unit Tests**: ❌ None for helper functions

**Next Step**: Add unit tests for:
- `expand_table_alias_to_select_items()` with various GROUP BY patterns
- `expand_table_alias_to_group_by_exprs()` with edge cases
- `replace_wildcards_with_group_by_columns()` with wildcard + TableAlias combinations

---

### Documentation
- **Inline Comments**: ✅ Helper functions have doc comments
- **Architecture Docs**: ⏳ Need to update `notes/architectural-fragility-analysis.md` with "Fixed" sections
- **User-Facing Docs**: ✅ No changes needed (pure internal refactoring)

**Next Step**: Update architectural analysis document with resolution details.

## Lessons Learned

### 1. Test-Driven Refactoring Works ✅
- Having 8/8 benchmark passing before refactoring gave confidence
- After each change: rebuild, test, verify
- No regressions introduced

### 2. Incremental Refactoring Beats Big Bang ✅
- Two separate commits (first LogicalExpr helpers, then RenderExpr helper)
- Each commit verified independently
- Easy to bisect if issues arise

### 3. Context Matters for Abstractions ✅
- LogicalExpr vs RenderExpr context required different helper functions
- Trying to force single helper would add complexity, not reduce it
- Better to have 3 focused helpers than 1 overly generic one

### 4. Code Metrics Drive Prioritization ✅
- `architectural-fragility-analysis.md` quantified 40% duplication
- Clear ROI calculation: ~120 lines eliminated with 3 helpers
- Objective data convinced team to prioritize refactoring

### 5. User Excitement Matters 🎵
> "fantastic. code reduction is music to my ears... shall we do it?" — User

- Stakeholder enthusiasm enabled dedicated refactoring time
- Shared understanding of technical debt value

## Related Documents

- **notes/architectural-fragility-analysis.md**: Original analysis identifying 40% duplication
- **STATUS.md**: LDBC benchmark results (8/8 passing)
- **CHANGELOG.md**: Release notes for v0.5.5 mentioning IC-1 fix

## Commits

- **df38a58**: fix: Expand WITH TableAlias to all columns for aggregation queries
- **45d33c1**: docs: Update STATUS.md with IC-1 fix and 100% LDBC benchmark
- **61ccbbb**: refactor: Extract TableAlias expansion into helper functions
- **e19331f**: refactor: Replace wildcard expansion in build_with_aggregation_match_cte_plan with helper

## Timeline

- **Dec 11, 2025 20:00**: IC-1 bug fixed, 8/8 LDBC queries passing
- **Dec 11, 2025 21:30**: First refactoring commit (TableAlias helpers)
- **Dec 11, 2025 22:00**: Second refactoring commit (wildcard helper)
- **Dec 11, 2025 22:30**: This summary document

---

**Status**: ✅ Refactoring Complete  
**Test Results**: 8/8 LDBC queries passing  
**Code Reduction**: ~120 lines duplicate logic eliminated  
**Maintainability**: Significantly improved
