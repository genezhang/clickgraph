# ClickGraph Quality Improvement Plan
**Date**: December 12, 2025  
**Focus**: Architectural cleanup and robustness improvements  
**Context**: Post-WITH clause implementation, focus on code quality over new features

## Status Overview

**Current State**:
- ✅ 642/642 unit tests passing (100%)
- ✅ 32/35 integration tests passing (91.4%)
- ✅ Rich feature set: VLP, OPTIONAL MATCH, WITH, multiple rel types, shortest path, PageRank
- ⚠️ Code quality issues: 3 WITH handlers with duplication, fragile CTE management
- ⚠️ Known bugs: Three-level WITH nesting, composite aliases

**Quality Metrics**:
- Code duplication: ~560 lines across 3 WITH handlers
- Test coverage: Strong unit tests, good integration coverage
- Documentation: Complete for features, needs architecture docs
- Technical debt: Medium (manageable with focused effort)

---

## Phase 1: Critical Bug Fixes (2-3 days)
**Priority**: HIGH  
**Goal**: Fix breaking issues discovered during comprehensive testing

### 1.1 Three-Level WITH Nesting Fix (4-6 hours)
**Issue**: Query fails with "Cannot render plan with remaining WITH clauses"

**Example Query**:
```cypher
MATCH (a:User)
WITH a, a.name as name
WITH a, name, upper(name) as upper_name
WITH a, upper_name
WHERE length(upper_name) > 5
RETURN a.user_id, upper_name
```

**Root Cause**: `build_chained_with_match_cte_plan()` doesn't recursively process deeply nested WITH clauses

**Solution Approach**:
1. Add recursive WITH processing in chained handler
2. Ensure CTE hoisting works at all nesting levels
3. Validate each WITH clause is fully rendered before next

**Files**:
- `src/render_plan/plan_builder.rs` (~lines 850-1000)

**Validation**:
- Test case already exists in `test_with_advanced_combinations.py`
- Add unit test for 3-4-5 level nesting
- Verify all CTEs properly hoisted

**Success Criteria**:
- [ ] Three-level WITH nesting test passes
- [ ] Four-level nesting works (add test)
- [ ] No regression in existing WITH tests

---

### 1.2 Composite Alias Resolution in GROUP BY (2-3 hours)
**Issue**: Aliases like `a_connections` not resolved in GROUP BY expansion

**Example Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, b
WITH a, COUNT(b) as connections
RETURN a.user_id, connections
```

**Root Cause**: Alias composition logic doesn't handle underscore-joined TableAlias names

**Solution Approach**:
1. Trace how `a_connections` alias is generated
2. Fix alias resolution in GROUP BY expansion logic
3. Ensure consistent alias naming across WITH boundaries

**Files**:
- `src/render_plan/plan_builder.rs` (GROUP BY expansion)
- `src/render_plan/alias_resolver.rs` (alias tracking)

**Validation**:
- Test case exists: "TableAlias GROUP BY expansion"
- Add unit tests for various alias patterns
- Test with multiple underscores: `a_b_c_connections`

**Success Criteria**:
- [ ] TableAlias GROUP BY test passes
- [ ] Complex alias patterns work (a_b_c, x_y_z)
- [ ] No regression in existing aggregation tests

---

## Phase 2: Code Consolidation (1 week)
**Priority**: HIGH  
**Goal**: Reduce duplication, improve maintainability

### 2.1 Extract Common WITH Rendering Logic (2 days)
**Current State**: ~560 lines duplicated across 3 WITH handlers

**Three Handlers**:
1. `build_with_match_cte_plan()` - Standard WITH + MATCH (~377 lines)
2. `build_chained_with_match_cte_plan()` - Multiple WITH clauses (~771 lines)
3. `build_with_optional_match_plan()` - WITH + OPTIONAL MATCH (~412 lines)

**Shared Logic to Extract**:
- CTE generation from WITH clause
- Alias mapping and tracking
- Property resolution
- WHERE clause handling
- SELECT item processing
- CTE hoisting (already done ✅)
- CTE validation (already done ✅)

**Implementation Plan**:

#### Step 1: Create Helper Module (4 hours)
Create `src/render_plan/with_helpers.rs`:

```rust
/// Generate CTE from WITH clause items and WHERE
pub fn generate_with_cte(
    with_clause: &ast::WithClause,
    plan_ctx: &PlanCtx,
    prev_plan: &RenderPlan,
) -> RenderPlanBuilderResult<Cte> { ... }

/// Map WITH aliases to underlying columns
pub fn create_with_alias_mapping(
    with_items: &[ast::ReturnItem],
    plan_ctx: &PlanCtx,
) -> HashMap<String, RenderExpr> { ... }

/// Process WHERE clause for WITH
pub fn process_with_where_clause(
    where_clause: Option<&ast::WhereClause>,
    plan_ctx: &PlanCtx,
    available_aliases: &HashMap<String, RenderExpr>,
) -> RenderPlanBuilderResult<Option<RenderExpr>> { ... }
```

**Files to Create**:
- `src/render_plan/with_helpers.rs` (new)
- Update `src/render_plan/mod.rs` to include module

**Success Metrics**:
- [ ] Helper module compiles independently
- [ ] Clear function contracts with documentation
- [ ] Unit tests for each helper function

#### Step 2: Refactor Standard WITH Handler (4 hours)
Update `build_with_match_cte_plan()` to use helpers:

**Before**: 377 lines of inline logic  
**After**: ~150 lines calling helpers

**Approach**:
1. Replace CTE generation with `generate_with_cte()`
2. Replace alias mapping with `create_with_alias_mapping()`
3. Replace WHERE processing with `process_with_where_clause()`
4. Keep handler-specific logic (MATCH integration)

**Validation**:
- All existing WITH tests pass
- Performance unchanged (helpers should inline)

#### Step 3: Refactor Chained WITH Handler (6 hours)
Update `build_chained_with_match_cte_plan()` to use helpers:

**Before**: 771 lines (most complex handler)  
**After**: ~300 lines

**Special Cases**:
- Multiple WITH clauses need loop
- Recursive nesting handling (from Phase 1.1)
- Proper CTE ordering

**Validation**:
- All chained WITH tests pass
- Three-level nesting works (Phase 1.1 fix)

#### Step 4: Refactor OPTIONAL MATCH Handler (4 hours)
Update `build_with_optional_match_plan()` to use helpers:

**Before**: 412 lines  
**After**: ~200 lines

**Special Cases**:
- LEFT JOIN semantics
- Optional alias tracking

**Validation**:
- All OPTIONAL MATCH + WITH tests pass

**Total Reduction**: 1560 → ~650 lines (~58% reduction)

**Success Criteria**:
- [ ] All 642 unit tests pass
- [ ] All 32 integration tests pass
- [ ] New 22 comprehensive tests pass (when DB set up)
- [ ] Code is more maintainable and readable
- [ ] Performance unchanged or improved

---

### 2.2 Implement CteRegistry (3 days)
**Goal**: Centralize CTE management and automatic validation

**Current Problems**:
- CTEs tracked in multiple places (Vec<Cte>, HashMap aliases)
- Manual validation needed after each operation
- Dependency tracking is implicit
- Easy to create invalid SQL with missing CTEs

**Design**:

```rust
/// Centralized CTE management with automatic validation
pub struct CteRegistry {
    /// All CTEs in order of definition
    ctes: Vec<Cte>,
    
    /// Quick lookup by name
    cte_index: HashMap<String, usize>,
    
    /// Dependency tracking: CTE name -> referenced table names
    dependencies: HashMap<String, Vec<String>>,
    
    /// Reverse lookup: table name -> dependent CTE names
    dependents: HashMap<String, Vec<String>>,
}

impl CteRegistry {
    /// Add CTE with automatic dependency extraction
    pub fn add_cte(&mut self, cte: Cte) -> Result<(), String> { ... }
    
    /// Validate all CTEs have dependencies satisfied
    pub fn validate(&self) -> Result<(), String> { ... }
    
    /// Get CTEs in dependency order
    pub fn get_ordered_ctes(&self) -> Vec<&Cte> { ... }
    
    /// Merge another registry (for nested plans)
    pub fn merge(&mut self, other: CteRegistry) { ... }
    
    /// Hoist nested CTEs automatically
    pub fn hoist_from_plan(&mut self, plan: &mut RenderPlan) { ... }
}
```

**Implementation Steps**:

1. **Create CteRegistry module** (4 hours)
   - File: `src/render_plan/cte_registry.rs`
   - Core data structures
   - Basic add/get operations
   - Unit tests

2. **Add dependency tracking** (6 hours)
   - Extract table references from CTE SQL
   - Build dependency graph
   - Detect circular dependencies
   - Topological sort for ordering
   - Comprehensive unit tests

3. **Integrate with RenderPlan** (8 hours)
   - Replace `Vec<Cte>` with `CteRegistry`
   - Update all CTE creation sites
   - Use automatic validation
   - Update plan builders
   - Fix compilation errors
   - Run all tests

4. **Add validation checks** (4 hours)
   - Validate on CTE addition
   - Validate before SQL generation
   - Clear error messages
   - Integration tests

**Files Affected**:
- `src/render_plan/cte_registry.rs` (new)
- `src/render_plan/render_plan.rs` (RenderPlan struct)
- `src/render_plan/plan_builder.rs` (all CTE operations)
- `src/render_plan/with_helpers.rs` (use CteRegistry)

**Success Criteria**:
- [ ] CteRegistry tracks all CTEs correctly
- [ ] Automatic validation catches missing dependencies
- [ ] Dependency ordering correct
- [ ] All existing tests pass
- [ ] Clearer error messages for CTE issues

---

### 2.3 Consolidate Three WITH Handlers (2 days)
**Goal**: Single unified handler with behavior flags

**Current**: 3 separate handlers, ~1560 lines total  
**Target**: 1 unified handler, ~600-800 lines

**After Phases 2.1 and 2.2**:
- Common logic extracted to helpers
- CTE management centralized in CteRegistry
- Handlers are already much simpler

**Unified Design**:

```rust
/// Unified WITH clause handler with configurable behavior
fn build_with_cte_plan(
    config: WithHandlerConfig,
    with_clause: &ast::WithClause,
    next_clause: NextClause,
    plan_ctx: &mut PlanCtx,
    prev_plan: RenderPlan,
) -> RenderPlanBuilderResult<RenderPlan> {
    // Common initialization
    let mut cte_registry = CteRegistry::new();
    cte_registry.hoist_from_plan(&mut prev_plan);
    
    // Generate WITH CTE using helpers
    let with_cte = with_helpers::generate_with_cte(
        with_clause,
        plan_ctx,
        &prev_plan,
    )?;
    
    cte_registry.add_cte(with_cte)?;
    
    // Handle next clause based on config
    match next_clause {
        NextClause::Match(match_clause) => {
            // Standard WITH + MATCH path
            if config.allow_chaining && has_another_with() {
                // Recursive handling for chained WITH
                return build_with_cte_plan(config, next_with, ...);
            }
            // Normal MATCH processing
        }
        NextClause::OptionalMatch(opt_match) => {
            // WITH + OPTIONAL MATCH path
            // Special LEFT JOIN handling
        }
        NextClause::Return(return_clause) => {
            // WITH + RETURN path
        }
    }
    
    // Common finalization
    cte_registry.validate()?;
    Ok(build_final_plan(cte_registry, ...))
}

struct WithHandlerConfig {
    allow_chaining: bool,
    track_optional_aliases: bool,
}

enum NextClause<'a> {
    Match(&'a ast::MatchClause<'a>),
    OptionalMatch(&'a ast::OptionalMatchClause<'a>),
    Return(&'a ast::ReturnClause<'a>),
}
```

**Migration Strategy** (to avoid breaking changes):

1. **Phase A**: Create unified handler alongside existing ones (1 day)
   - Implement `build_with_cte_plan()` with all features
   - Add feature flag: `use_unified_with_handler`
   - Test with feature flag enabled

2. **Phase B**: Switch over gradually (0.5 day)
   - Enable unified handler for one pattern type
   - Run tests, fix issues
   - Enable for remaining patterns
   - Delete old handlers

3. **Phase C**: Cleanup (0.5 day)
   - Remove old handler code
   - Remove feature flag
   - Final test run

**Success Criteria**:
- [ ] Single unified WITH handler works for all cases
- [ ] All 642 unit tests pass
- [ ] All integration tests pass
- [ ] Code is clearer and easier to maintain
- [ ] Total WITH handler code: ~600-800 lines (vs 1560)

---

## Phase 3: Architecture Improvements (2 weeks)
**Priority**: MEDIUM  
**Goal**: Long-term maintainability and extensibility

### 3.1 Visitor Pattern for Plan Transformation (1 week)
**Current**: Plan transformations scattered across codebase

**Goal**: Systematic plan traversal and transformation

**Design**:
```rust
trait PlanVisitor {
    fn visit_render_plan(&mut self, plan: &mut RenderPlan) -> Result<()>;
    fn visit_cte(&mut self, cte: &mut Cte) -> Result<()>;
    fn visit_select(&mut self, select: &mut SelectPlan) -> Result<()>;
    fn visit_join(&mut self, join: &mut JoinPlan) -> Result<()>;
    // ... other node types
}

// Example visitors:
struct CteHoistingVisitor;
struct AliasResolutionVisitor;
struct OptimizationVisitor;
```

**Use Cases**:
- CTE hoisting (replace current manual hoisting)
- Alias resolution passes
- Query optimization passes
- Query validation passes

**Implementation**: 4-5 days

---

### 3.2 Builder Pattern for RenderPlan (3 days)
**Current**: Direct struct construction, error-prone

**Goal**: Fluent, validated plan construction

**Design**:
```rust
let plan = RenderPlan::builder()
    .with_cte(cte1)
    .with_cte(cte2)
    .select(select_items)
    .from(table)
    .where_clause(filter)
    .validate()?  // Automatic validation
    .build()?;
```

**Benefits**:
- Impossible to create invalid plans
- Clear construction flow
- Automatic validation
- Better error messages

**Implementation**: 3 days

---

### 3.3 Property-Based Testing (1 week)
**Goal**: Automated test generation and invariant checking

**Framework**: Use `proptest` crate

**Test Properties**:
1. **CTE Hoisting Invariant**: CTEs always at top level in final SQL
2. **Alias Resolution**: All aliases in GROUP BY resolve to columns
3. **Join Validity**: All JOIN tables exist in FROM or CTEs
4. **Parameter Substitution**: Parameters always replaced in final SQL
5. **WITH Nesting**: Any depth of WITH nesting produces valid SQL

**Example**:
```rust
proptest! {
    #[test]
    fn test_with_nesting_invariant(depth in 1..10) {
        let query = generate_nested_with_query(depth);
        let result = execute_query(&query);
        
        // Invariants:
        assert!(all_ctes_at_top_level(&result.sql));
        assert!(no_nested_with_in_sql(&result.sql));
        assert!(valid_clickhouse_sql(&result.sql));
    }
}
```

**Implementation**: 5-7 days

---

## Phase 4: Documentation & Knowledge Capture (3 days)
**Priority**: MEDIUM  
**Goal**: Preserve architectural knowledge

### 4.1 Architecture Decision Records (1 day)
Document key decisions:
- Why three WITH handlers initially?
- Why CTE hoisting approach?
- Why view-based model?
- Parameter substitution strategy

### 4.2 Code Architecture Guide (1 day)
Create `docs/architecture/`:
- `query-pipeline.md` - End-to-end query flow
- `cte-management.md` - How CTEs work
- `with-clause-design.md` - WITH clause architecture
- `testing-strategy.md` - Testing approach

### 4.3 Contributing Guide (1 day)
- How to add new Cypher features
- Testing requirements
- Code review checklist
- Common pitfalls

---

## Execution Strategy

### Recommended Order:
1. **Week 1**: Phase 1 (Critical Bugs) + Start Phase 2.1
2. **Week 2**: Complete Phase 2.1 + Phase 2.2
3. **Week 3**: Phase 2.3 + Phase 4
4. **Week 4-5**: Phase 3 (architecture improvements)

### Resource Allocation:
- **1 developer, full-time**: 3-4 weeks
- **1 developer, part-time (50%)**: 6-8 weeks
- **2 developers, coordinated**: 2-3 weeks

### Risk Mitigation:
- Keep old code during refactoring (feature flags)
- Run full test suite after each phase
- Incremental integration (don't refactor everything at once)
- Document as you go

### Success Metrics:
- [ ] All known bugs fixed
- [ ] Code duplication reduced by >50%
- [ ] Test coverage maintained at 100% (unit tests)
- [ ] No performance regression
- [ ] Clearer, more maintainable codebase
- [ ] Architecture documented

---

## Priority Justification

**Why This Order?**

1. **Phase 1 first**: Bugs are blocking users, quick wins build momentum
2. **Phase 2 before Phase 3**: Consolidation makes architecture changes easier
3. **Phase 3 optional**: Nice-to-have, but not critical for stability
4. **Phase 4 alongside work**: Document decisions as they're made

**Quality vs Features Trade-off**:
- Current feature set is rich enough for most use cases
- Code quality debt is manageable but growing
- 3-4 weeks of quality focus now prevents months of pain later
- Easier to add features on solid foundation

**What We're NOT Doing** (deliberately deferred):
- ❌ New Cypher features (sufficient for now)
- ❌ Performance optimization (no known bottlenecks)
- ❌ New algorithms (PageRank, shortest path are enough)
- ❌ Write operations (out of scope)

---

## Next Steps

**Immediate Actions** (today):
1. ✅ Review this plan
2. ✅ Agree on priorities
3. 🔄 Create GitHub issues for Phase 1 bugs
4. 🔄 Set up project board for tracking

**This Week**:
1. Fix three-level WITH nesting bug
2. Fix composite alias bug
3. Start extracting WITH helper functions

**This Month**:
1. Complete Phase 1 and Phase 2
2. Document architecture decisions
3. Measure code quality improvement

---

## Success Criteria

**After Phase 1**:
- [ ] All 22 comprehensive tests pass
- [ ] No known critical bugs
- [ ] User confidence restored

**After Phase 2**:
- [ ] Code duplication < 50% of current
- [ ] Single WITH handler
- [ ] CteRegistry in use
- [ ] All tests passing

**After Phase 3** (optional):
- [ ] Visitor pattern implemented
- [ ] Builder pattern for RenderPlan
- [ ] Property-based tests running in CI

**Overall**:
- [ ] Codebase more maintainable
- [ ] Faster feature development
- [ ] Fewer bugs introduced
- [ ] Better onboarding for new developers

---

*This plan prioritizes quality and maintainability over new features, focusing on making the codebase robust and extensible for long-term success.*
