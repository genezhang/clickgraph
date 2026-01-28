# Property Pruning Implementation - Task Breakdown

**Date**: December 24, 2025  
**Status**: In Progress  
**Approach**: Consolidate RETURN/WITH expansion FIRST, then add property pruning

---

## Phase 1: Foundation + Consolidation (Week 1)

### Task 1.1: PropertyRequirements Data Structure ✅ READY
**Estimated**: 4 hours  
**Files**: New file `src/query_planner/analyzer/property_requirements.rs`

- [ ] Create `PropertyRequirements` struct with HashMap<String, HashSet<String>>
- [ ] Add `wildcard_aliases` tracking
- [ ] Implement `require_property()`, `require_all()`, `get_requirements()`
- [ ] Implement `merge()` for combining requirements
- [ ] Add unit tests (15+ test cases)
- [ ] Add to module exports in `src/query_planner/analyzer/mod.rs`

**Output**: `PropertyRequirements` data structure ready for use

---

### Task 1.2: Add PropertyRequirements to PlanCtx ✅ READY
**Estimated**: 2 hours  
**Files**: `src/query_planner/plan_ctx/mod.rs`

- [ ] Add `property_requirements: Option<PropertyRequirements>` field to `PlanCtx`
- [ ] Add getter: `pub fn get_property_requirements(&self) -> Option<&PropertyRequirements>`
- [ ] Add setter: `pub fn set_property_requirements(&mut self, reqs: PropertyRequirements)`
- [ ] Update all `PlanCtx` constructors to initialize with `None`
- [ ] Add documentation comments

**Output**: PlanCtx can store and retrieve property requirements

---

### Task 1.3: Research Current Expansion Code Paths 🔍 SUB-AGENT
**Estimated**: 2 hours  
**Purpose**: Map all locations where TableAlias expansion happens

**Research Questions**:
1. Where does RETURN path expand TableAlias? (should be ~line 5450 in plan_builder.rs)
2. Where does WITH path expand TableAlias? (should be ~line 1741 in plan_builder.rs)
3. What helper functions exist in property_expansion.rs today?
4. Are there other expansion sites we need to consolidate?
5. What parameters do expansion functions take (for interface design)?

**Output**: Document with all expansion call sites and their signatures

---

### Task 1.4: Create Unified Expansion Helper (No Pruning Yet) ⚙️ IMPLEMENT
**Estimated**: 4 hours  
**Files**: `src/render_plan/property_expansion.rs`

**Step 1**: Add new function (backward compatible):
```rust
pub fn expand_alias_to_select_items_unified(
    alias: &str,
    plan: &LogicalPlan,
    actual_table_alias: Option<String>,
    alias_format: PropertyAliasFormat,
) -> Result<Vec<SelectItem>, String>
```

**Step 2**: Implementation without pruning:
- Get all properties from plan
- Call existing `expand_alias_to_select_items()`
- Return SelectItems

**Step 3**: Add documentation and examples

**Output**: New unified helper that works for both RETURN and WITH

---

### Task 1.5: Update RETURN Path to Use Unified Helper ⚙️ IMPLEMENT
**Estimated**: 3 hours  
**Files**: `src/render_plan/plan_builder.rs` (~line 5450)

- [ ] Find TableAlias expansion in `extract_select_items()`
- [ ] Replace with call to `expand_alias_to_select_items_unified()`
- [ ] Pass `PropertyAliasFormat::Dot` for RETURN
- [ ] Handle Result type properly
- [ ] Verify existing tests still pass

**Output**: RETURN uses unified helper

---

### Task 1.6: Update WITH Path to Use Unified Helper ⚙️ IMPLEMENT
**Estimated**: 3 hours  
**Files**: `src/render_plan/plan_builder.rs` (~line 1741)

- [ ] Find TableAlias expansion in `build_chained_with_match_cte_plan()`
- [ ] Replace with call to `expand_alias_to_select_items_unified()`
- [ ] Pass `PropertyAliasFormat::Underscore` for WITH
- [ ] Handle Result type properly
- [ ] Verify existing tests still pass

**Output**: WITH uses unified helper

---

### Task 1.7: Remove Duplicate Expansion Code ⚙️ IMPLEMENT
**Estimated**: 2 hours  
**Files**: `src/render_plan/plan_builder.rs`

- [ ] Remove old manual expansion code from RETURN path
- [ ] Remove old manual expansion code from WITH path
- [ ] Clean up unused helper functions
- [ ] Update comments

**Output**: ~150 lines of duplicate code removed

---

### Task 1.8: Consolidation Testing ✅ TEST
**Estimated**: 3 hours

- [ ] Run all existing unit tests
- [ ] Run all integration tests  
- [ ] Verify SQL generation unchanged for RETURN queries
- [ ] Verify SQL generation unchanged for WITH queries
- [ ] Add regression tests for consolidation

**Output**: All tests pass, no regressions

---

## Phase 2: Property Requirements Analyzer (Week 2)

### Task 2.1: Create PropertyRequirementsAnalyzer Skeleton 🔍 SUB-AGENT
**Estimated**: 3 hours  
**Files**: New file `src/query_planner/analyzer/property_requirements_analyzer.rs`

**Research First**:
- How do other analyzer passes structure their code?
- What's the AnalyzerPass trait interface?
- How do passes access PlanCtx?

**Then Create**:
- [ ] Struct `PropertyRequirementsAnalyzer`
- [ ] Implement `AnalyzerPass` trait
- [ ] Add skeleton `analyze()` method
- [ ] Add to analyzer module exports

**Output**: Skeleton analyzer pass ready to implement

---

### Task 2.2: Implement Expression Property Extraction ⚙️ IMPLEMENT
**Estimated**: 4 hours  
**Files**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

- [ ] Add `extract_from_expr()` recursive function
- [ ] Handle PropertyAccessExp → require property
- [ ] Handle TableAlias → require all (wildcard)
- [ ] Handle Operator → recurse into operands
- [ ] Handle AggregateFnCall → recurse into args
- [ ] Handle ScalarFnCall → recurse into args
- [ ] Handle CaseExpr → recurse into branches
- [ ] Add unit tests for extraction

**Output**: Can extract property requirements from any expression

---

### Task 2.3: Implement Root-to-Leaf Traversal ⚙️ IMPLEMENT
**Estimated**: 4 hours  
**Files**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

- [ ] Add `collect_requirements_recursive()` function
- [ ] Handle Projection (RETURN) → extract from items
- [ ] Handle Filter (WHERE) → extract from predicate
- [ ] Handle OrderBy → extract from order expressions
- [ ] Recurse down tree (root to leaves)
- [ ] Add unit tests for traversal

**Output**: Basic traversal working for simple queries

---

### Task 2.4: Implement WITH Scope Propagation ⚙️ IMPLEMENT
**Estimated**: 6 hours (CRITICAL/COMPLEX)  
**Files**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

- [ ] Handle WithClause in traversal
- [ ] Detect collect(alias) patterns
- [ ] Map downstream requirements to source alias
- [ ] Handle simple alias passthrough (WITH node)
- [ ] Handle expression aliases (WITH node.property AS alias)
- [ ] Add comprehensive tests for multi-scope queries

**Output**: Property requirements propagate through WITH boundaries

---

### Task 2.5: Implement UNWIND Tracking ⚙️ IMPLEMENT
**Estimated**: 3 hours  
**Files**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

- [ ] Handle Unwind in traversal
- [ ] Track properties accessed on unwound alias
- [ ] Propagate to array source in WITH
- [ ] Add tests with UNWIND patterns

**Output**: UNWIND requirements tracked correctly

---

### Task 2.6: Add ID Column Enforcement ⚙️ IMPLEMENT
**Estimated**: 2 hours  
**Files**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

- [ ] Add `ensure_id_property()` helper
- [ ] Call for every alias with requirements
- [ ] Use `plan.find_id_column_for_alias()`
- [ ] Add tests verifying ID always included

**Output**: ID columns always included for correctness

---

### Task 2.7: Integrate into Analyzer Pipeline ⚙️ IMPLEMENT
**Estimated**: 2 hours  
**Files**: `src/query_planner/mod.rs`

- [ ] Add to analyzer sequence after TypeInference
- [ ] Before CteColumnResolver
- [ ] Update pipeline documentation
- [ ] Test integration

**Output**: Analyzer runs as part of pipeline

---

### Task 2.8: Multi-Scope Testing ✅ TEST
**Estimated**: 4 hours

- [ ] Single-scope queries (MATCH...RETURN)
- [ ] Two-level WITH queries
- [ ] Three-level WITH queries  
- [ ] UNWIND after WITH
- [ ] Complex LDBC-style queries
- [ ] Verify requirements correctness

**Output**: Analyzer correctly handles all query patterns

---

## Phase 3: Property Pruning in Renderer (Week 3)

### Task 3.1: Add Pruning to Unified Expansion Helper ⚙️ IMPLEMENT
**Estimated**: 4 hours  
**Files**: `src/render_plan/property_expansion.rs`

- [ ] Add `plan_ctx: &PlanCtx` parameter to `expand_alias_to_select_items_unified()`
- [ ] Query `plan_ctx.get_property_requirements()`
- [ ] Add `filter_properties()` helper function
- [ ] Apply filtering: all_properties → required_properties
- [ ] Handle wildcards (requires_all)
- [ ] Handle missing requirements (fallback to all)
- [ ] Add unit tests

**Output**: Expansion helper applies pruning

---

### Task 3.2: Update RETURN Call Sites ⚙️ IMPLEMENT
**Estimated**: 2 hours  
**Files**: `src/render_plan/plan_builder.rs`

- [ ] Pass `plan_ctx` to unified helper calls
- [ ] Update error handling if needed
- [ ] Test RETURN queries with pruning

**Output**: RETURN queries benefit from pruning

---

### Task 3.3: Update WITH Call Sites ⚙️ IMPLEMENT
**Estimated**: 2 hours  
**Files**: `src/render_plan/plan_builder.rs`

- [ ] Pass `plan_ctx` to unified helper calls
- [ ] Update error handling if needed
- [ ] Test WITH queries with pruning

**Output**: WITH queries benefit from pruning

---

### Task 3.4: Update collect() Expansion ⚙️ IMPLEMENT
**Estimated**: 3 hours  
**Files**: `src/render_plan/property_expansion.rs`

- [ ] Add `requirements: Option<&PropertyRequirements>` param to `expand_collect_to_group_array()`
- [ ] Filter properties before creating tuple
- [ ] Update call sites
- [ ] Test collect() with pruning

**Output**: collect() only collects needed properties

---

### Task 3.5: Update anyLast() Wrapping Logic ⚙️ IMPLEMENT
**Estimated**: 3 hours  
**Files**: `src/render_plan/cte_extraction.rs` (~line 1691)

- [ ] Query property requirements in CTE extraction
- [ ] Only wrap required properties with anyLast()
- [ ] Update tests

**Output**: anyLast() only wraps needed properties

---

### Task 3.6: Integration Testing ✅ TEST
**Estimated**: 4 hours

- [ ] Query SQL generation - verify only required columns
- [ ] RETURN with pruning
- [ ] WITH with pruning
- [ ] collect() with pruning
- [ ] Multi-scope with pruning
- [ ] Verify correctness (same results, less data)

**Output**: All pruning working end-to-end

---

## Phase 4: Edge Cases & Polish (Week 4)

### Task 4.1: Handle Nested Properties ⚙️ IMPLEMENT
**Estimated**: 2 hours

- [ ] Test `friend.address.city` patterns
- [ ] Ensure parent property required
- [ ] Add tests

**Output**: Nested properties work correctly

---

### Task 4.2: Handle Wildcards ⚙️ IMPLEMENT
**Estimated**: 2 hours

- [ ] Test `RETURN friend.*` patterns
- [ ] Ensure all properties collected
- [ ] Add tests

**Output**: Wildcards work correctly

---

### Task 4.3: Handle Multiple UNWIND Sites ⚙️ IMPLEMENT
**Estimated**: 2 hours

- [ ] Test multiple UNWIND of same collection
- [ ] Merge requirements correctly
- [ ] Add tests

**Output**: Multiple UNWIND sites work

---

### Task 4.4: Performance Benchmarking ✅ TEST
**Estimated**: 4 hours

- [ ] Create benchmark suite
- [ ] Test with 10, 50, 100, 200 column tables
- [ ] Measure memory usage
- [ ] Measure execution time
- [ ] Document results

**Output**: Performance improvements quantified

---

### Task 4.5: Comprehensive Test Suite ✅ TEST
**Estimated**: 6 hours

- [ ] All edge cases covered
- [ ] All query patterns tested
- [ ] 90%+ code coverage
- [ ] Regression tests
- [ ] Performance tests

**Output**: Comprehensive test coverage

---

### Task 4.6: Documentation Updates 📚 DOCUMENT
**Estimated**: 4 hours

- [ ] Update STATUS.md
- [ ] Update CHANGELOG.md
- [ ] Add user-facing docs in docs/wiki/
- [ ] Update implementation notes
- [ ] Code documentation complete

**Output**: Complete documentation

---

## Task Legend

- 🔍 **SUB-AGENT**: Research/analysis task (delegate to sub-agent)
- ⚙️ **IMPLEMENT**: Direct implementation task
- ✅ **TEST**: Testing and verification
- 📚 **DOCUMENT**: Documentation
- **READY**: Design complete, ready to implement

---

## Progress Tracking

**Phase 1 (Week 1)**: 0/8 tasks complete (0%)  
**Phase 2 (Week 2)**: 0/8 tasks complete (0%)  
**Phase 3 (Week 3)**: 0/6 tasks complete (0%)  
**Phase 4 (Week 4)**: 0/6 tasks complete (0%)

**Overall**: 0/28 tasks complete (0%)

---

## Next Steps

1. ✅ Start with Task 1.1 (PropertyRequirements data structure)
2. Then Task 1.2 (Add to PlanCtx)
3. Use sub-agent for Task 1.3 (research expansion paths)
4. Continue with systematic implementation

**Let's begin!** 🚀
