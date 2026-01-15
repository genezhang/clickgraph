# CTE Refactoring Action Plan

**Date**: January 15, 2026  
**Status**: Ready for Execution  
**Strategy**: Phase2A Process + CTE-Unification Implementation  
**Timeline**: 8-10 weeks  

---

## Executive Summary

**Approach**: Use Phase2A's phased migration strategy to safely integrate CTE-Unification's working code.

**Current State**:
- ✅ CTE-Unification: 2,246 lines implemented, 6 strategies working, 6 tests passing
- ✅ Phase2A: Infrastructure helpers added, comprehensive design document
- ✅ Both branches propose identical technical architecture
- ⚠️ Integration needed: Wire up CTE Manager to existing codebase

**Key Decision**: Merge Phase2A infrastructure first, then adapt CTE-Unification strategies incrementally.

---

## Phase 0: Immediate Next Steps (This Week)

### Step 1: Merge Phase2A Infrastructure to Main ✋ **START HERE**

**Goal**: Get foundation in place without breaking anything

**Tasks**:
```bash
# 1. Switch to Phase2A branch and verify tests pass
git checkout refactor/cte-phase2a-infrastructure
cargo test --lib
python -m pytest tests/integration/

# 2. Create PR for Phase2A
gh pr create \
  --title "refactor: Add CTE infrastructure helpers (Phase 2A)" \
  --body "$(cat <<EOF
## Phase 2A: CTE Infrastructure Preparation

**Purpose**: Foundation for upcoming CTE unification refactoring

**Changes**:
- Add PatternSchemaContext recreation helper in cte_extraction.rs
- Add EdgeAccessStrategy helper methods in pattern_schema.rs  
- Comprehensive design document (CTE_MANAGER_DESIGN.md)

**Impact**: No behavior changes, just infrastructure helpers

**Testing**: All existing tests pass (3,538 integration tests)

**Next Steps**: Phase 2B will introduce CteManager facade

**Design Doc**: See CTE_MANAGER_DESIGN.md for complete architecture plan

**Related**: Phase 0 of schema consolidation refactoring
EOF
)"

# 3. Review and merge (fast-track - low risk)
# Get approval, merge to main
```

**Success Criteria**:
- [ ] PR created and reviewed
- [ ] All CI tests pass
- [ ] Merged to main
- [ ] No regressions reported

**Timeline**: 1-2 days

---

### Step 2: Assess Current State & Update Plans

**Goal**: Reconcile both design documents into unified plan

**Tasks**:
1. **Move comparison doc to main**:
   ```bash
   git checkout main
   git checkout feature/cte-unification -- docs/development/cte_approach_comparison.md
   git add docs/development/cte_approach_comparison.md
   git commit -m "docs: Add CTE refactoring approach comparison"
   git push origin main
   ```

2. **Create unified design document**:
   - Merge best parts of both design docs
   - File: `docs/development/cte_refactoring_unified_design.md`
   - Include: Technical spec + Phase2A's migration strategy
   - Add: Lessons learned from both approaches

3. **Update STATUS.md**:
   - Document CTE refactoring as "In Progress"
   - Link to action plan and design docs
   - Set realistic timeline expectations

**Success Criteria**:
- [ ] Comparison doc in main branch
- [ ] Unified design doc created
- [ ] STATUS.md updated
- [ ] Team aligned on approach

**Timeline**: 2-3 days

---

## Phase 1: CTE Manager Core (Weeks 1-2)

### Goal: Create CTE Manager with fallback to existing code

**Branch**: `refactor/cte-manager-core` (from main, after Phase2A merge)

### Week 1: CTE Manager Facade

**Tasks**:

1. **Create CTE Manager module structure**:
   ```bash
   # Create basic structure
   mkdir -p src/render_plan/cte_manager
   touch src/render_plan/cte_manager/mod.rs
   touch src/render_plan/cte_manager/errors.rs
   touch src/render_plan/cte_manager/strategy.rs
   ```

2. **Cherry-pick CTE Manager facade from CTE-Unification**:
   ```bash
   git checkout feature/cte-unification
   git show HEAD:src/render_plan/cte_manager/mod.rs | head -200 > temp_manager.rs
   # Extract just the CteManager struct, CteStrategy enum, CteError
   # Adapt to use Phase2A's recreate_pattern_schema_context helper
   ```

3. **Implement with feature flag**:
   ```rust
   // In src/render_plan/cte_extraction.rs
   
   #[cfg(feature = "unified_cte")]
   fn extract_vlp_cte_unified(
       graph_rel: &GraphRel,
       spec: &VariableLengthSpec,
       context: &mut CteGenerationContext,
       schema: &GraphSchema,
   ) -> RenderPlanBuilderResult<Vec<Cte>> {
       use crate::render_plan::cte_manager::CteManager;
       
       let manager = CteManager::new(schema.clone());
       let manager = manager.with_spec(spec.clone());
       
       // Get or create pattern context (using Phase2A helper)
       let pattern_ctx = recreate_pattern_schema_context(graph_rel, schema)?;
       
       // Analyze pattern and determine strategy
       let strategy = manager.analyze_pattern(&pattern_ctx, spec)?;
       
       // For now, delegate back to old code
       extract_vlp_cte_old(graph_rel, spec, context, schema)
   }
   
   #[cfg(not(feature = "unified_cte"))]
   fn extract_vlp_cte_unified(...) -> ... {
       extract_vlp_cte_old(...)  // Just call old code
   }
   ```

4. **Add comprehensive tests**:
   ```rust
   #[cfg(test)]
   mod tests {
       #[test]
       fn test_cte_manager_analyzes_traditional_pattern() {
           // Test strategy selection logic
       }
       
       #[test]
       fn test_cte_manager_analyzes_denormalized_pattern() {
           // Test strategy selection logic
       }
       
       // ... one test per strategy
   }
   ```

**Deliverables**:
- [ ] CTE Manager facade implemented
- [ ] CteStrategy enum with all 6 variants
- [ ] CteError type
- [ ] Feature flag system working
- [ ] analyze_pattern() method working
- [ ] Unit tests passing (strategy selection only)

**Success Criteria**:
- [ ] Code compiles with and without feature flag
- [ ] All existing tests still pass (feature flag OFF)
- [ ] New unit tests pass (strategy selection logic)
- [ ] No SQL generation yet (delegates to old code)

---

### Week 2: First Strategy Implementation (Traditional)

**Tasks**:

1. **Cherry-pick TraditionalCteStrategy from CTE-Unification**:
   ```bash
   git checkout feature/cte-unification -- src/render_plan/cte_manager/mod.rs
   # Extract just TraditionalCteStrategy implementation
   # Lines 230-520 approximately
   ```

2. **Adapt to use Phase2A infrastructure**:
   ```rust
   impl TraditionalCteStrategy {
       pub fn new(pattern_ctx: &PatternSchemaContext) -> Result<Self, CteError> {
           // Use Phase2A's EdgeAccessStrategy helper methods
           let from_col = pattern_ctx.edge.from_id_column()
               .map_err(|e| CteError::SchemaValidationError(e.to_string()))?;
           let to_col = pattern_ctx.edge.to_id_column()
               .map_err(|e| CteError::SchemaValidationError(e.to_string()))?;
           
           // Extract table info from NodeAccessStrategy
           // Use recreate_pattern_schema_context results
           // ...
       }
   }
   ```

3. **Replace hardcoded table names**:
   ```rust
   // BEFORE (from CTE-Unification):
   fn get_node_table_info(&self, node_alias: &str) -> Result<(String, String), CteError> {
       match node_alias {
           "u1" | "start" => Ok(("users_bench".to_string(), "user_id".to_string())),
           // ... hardcoded values
       }
   }
   
   // AFTER (use PatternSchemaContext):
   fn get_node_table_info(&self, node_position: NodePosition) -> Result<(String, String), CteError> {
       let node_strategy = match node_position {
           NodePosition::Left => &self.pattern_ctx.left_node,
           NodePosition::Right => &self.pattern_ctx.right_node,
       };
       
       match node_strategy {
           NodeAccessStrategy::OwnTable { table, id_column, .. } => {
               Ok((table.clone(), id_column.clone()))
           }
           _ => Err(CteError::InvalidStrategy(
               format!("Traditional strategy requires OwnTable for {} node", node_position)
           ))
       }
   }
   ```

4. **Implement generate_sql() with actual SQL generation**:
   ```rust
   pub fn generate_sql(
       &self,
       context: &CteGenerationContext,
       properties: &[NodeProperty],
       filters: &CategorizedFilters,
   ) -> Result<CteGenerationResult, CteError> {
       // Generate actual recursive CTE SQL
       // Use pattern_ctx for all table/column lookups
       // No hardcoded values!
   }
   ```

5. **Add integration tests**:
   ```python
   # tests/integration/test_cte_manager_traditional.py
   
   def test_traditional_strategy_basic_vlp():
       """Test CTE Manager with traditional schema (separate tables)"""
       query = """
           MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User)
           WHERE u1.user_id = 1
           RETURN u2.name
       """
       # Run with unified_cte feature enabled
       # Compare SQL output with old implementation
       # Verify results are identical
   ```

**Deliverables**:
- [ ] TraditionalCteStrategy fully implemented
- [ ] Schema-aware (no hardcoded values)
- [ ] SQL generation working
- [ ] Unit tests passing
- [ ] Integration tests passing

**Success Criteria**:
- [ ] Traditional pattern queries generate correct SQL
- [ ] SQL matches old implementation exactly (validated)
- [ ] Tests with benchmark schema (users_bench, user_follows_bench) pass
- [ ] No hardcoded table/column names

---

## Phase 2: Parallel Execution & Validation (Weeks 3-4)

### Goal: Run both old and new implementations, compare outputs

### Week 3: Parallel Execution Framework

**Tasks**:

1. **Add comparison mode**:
   ```rust
   #[cfg(all(feature = "unified_cte", debug_assertions))]
   fn extract_vlp_cte_with_validation(
       graph_rel: &GraphRel,
       spec: &VariableLengthSpec,
       context: &mut CteGenerationContext,
       schema: &GraphSchema,
   ) -> RenderPlanBuilderResult<Vec<Cte>> {
       // Generate with new CTE Manager
       let new_ctes = extract_vlp_cte_unified(graph_rel, spec, context, schema)?;
       
       // Generate with old code
       let old_ctes = extract_vlp_cte_old(graph_rel, spec, context, schema)?;
       
       // Compare SQL
       compare_cte_outputs(&new_ctes, &old_ctes)?;
       
       // Log differences (if any)
       if !are_ctes_equivalent(&new_ctes, &old_ctes) {
           log::warn!("CTE SQL difference detected for pattern: {}", graph_rel.alias);
           log_sql_diff(&new_ctes, &old_ctes);
       }
       
       // Return old CTEs for now (safe fallback)
       Ok(old_ctes)
   }
   ```

2. **Implement SQL comparison logic**:
   ```rust
   fn are_ctes_equivalent(new: &[Cte], old: &[Cte]) -> bool {
       // Normalize SQL (remove whitespace differences)
       // Compare semantically, not byte-by-byte
       // Allow minor formatting differences
   }
   
   fn compare_cte_outputs(new: &[Cte], old: &[Cte]) -> Result<(), CteError> {
       if new.len() != old.len() {
           return Err(CteError::ValidationError(
               format!("CTE count mismatch: new={}, old={}", new.len(), old.len())
           ));
       }
       
       // Compare each CTE
       for (n, o) in new.iter().zip(old.iter()) {
           if !is_sql_equivalent(&n.sql, &o.sql) {
               return Err(CteError::ValidationError(
                   format!("SQL mismatch for CTE: {}", n.name)
               ));
           }
       }
       
       Ok(())
   }
   ```

3. **Add detailed logging**:
   ```rust
   fn log_sql_diff(new: &[Cte], old: &[Cte]) {
       for (i, (n, o)) in new.iter().zip(old.iter()).enumerate() {
           log::debug!("CTE #{}: {}", i, n.name);
           log::debug!("  NEW SQL:\n{}", n.sql);
           log::debug!("  OLD SQL:\n{}", o.sql);
           log::debug!("  DIFF:\n{}", text_diff(&n.sql, &o.sql));
       }
   }
   ```

4. **Run full test suite with comparison mode**:
   ```bash
   # Enable feature flag and comparison
   RUSTFLAGS="--cfg debug_assertions" cargo test --features unified_cte
   
   # Run integration tests with logging
   RUST_LOG=clickgraph=debug python -m pytest tests/integration/ -v
   ```

**Deliverables**:
- [ ] Parallel execution framework implemented
- [ ] SQL comparison logic working
- [ ] Detailed diff logging
- [ ] All tests run in comparison mode

**Success Criteria**:
- [ ] No SQL differences for traditional pattern
- [ ] All tests pass with both implementations
- [ ] Performance similar (within 10%)

---

### Week 4: Additional Strategies (Denormalized + FkEdge)

**Tasks**:

1. **Implement DenormalizedCteStrategy**:
   - Cherry-pick from CTE-Unification
   - Remove hardcoded values
   - Use PatternSchemaContext
   - Add tests

2. **Implement FkEdgeCteStrategy**:
   - Cherry-pick from CTE-Unification
   - Fix hardcoded id_column (use schema lookup)
   - Add tests

3. **Run comparison on all patterns**:
   ```bash
   # Test traditional pattern
   pytest tests/integration/test_basic_queries.py -v
   
   # Test denormalized pattern (if available)
   pytest tests/integration/test_denormalized_schema.py -v
   
   # Test FK-edge pattern (if available)
   pytest tests/integration/test_fk_edge_schema.py -v
   ```

4. **Fix any SQL discrepancies**:
   - Review logs for differences
   - Adjust strategies to match old behavior
   - Document intentional improvements

**Deliverables**:
- [ ] 3 strategies fully working (Traditional, Denormalized, FkEdge)
- [ ] All comparison tests passing
- [ ] SQL equivalence validated

**Success Criteria**:
- [ ] 3 core patterns generate identical or better SQL
- [ ] Performance within 5% of old implementation
- [ ] All integration tests passing

---

## Phase 3: Remaining Strategies (Weeks 5-6)

### Goal: Complete all 6 strategies

### Week 5: MixedAccess + EdgeToEdge

**Tasks**:
1. Implement MixedAccessCteStrategy (cherry-pick + adapt)
2. Implement EdgeToEdgeCteStrategy (cherry-pick + adapt)
3. Remove hardcoded values
4. Add comprehensive tests
5. Validate with comparison mode

### Week 6: Coupled + Polish

**Tasks**:
1. Implement CoupledCteStrategy (cherry-pick + adapt)
2. Polish all strategies:
   - Remove ALL hardcoded table/column names
   - Add proper error messages
   - Improve code documentation
3. Performance optimization pass
4. Final comparison validation

**Deliverables**:
- [ ] All 6 strategies implemented
- [ ] All strategies schema-aware
- [ ] Comprehensive test coverage
- [ ] Performance benchmarked

**Success Criteria**:
- [ ] All strategies pass comparison tests
- [ ] No hardcoded values anywhere
- [ ] Performance within 5% of baseline

---

## Phase 4: Integration & Cutover (Weeks 7-8)

### Goal: Switch to CTE Manager by default

### Week 7: Enable by Default

**Tasks**:

1. **Enable unified_cte feature by default**:
   ```toml
   [features]
   default = ["unified_cte"]
   unified_cte = []
   ```

2. **Switch to returning new CTEs**:
   ```rust
   fn extract_vlp_cte_with_validation(...) -> ... {
       let new_ctes = extract_vlp_cte_unified(...)?;
       let old_ctes = extract_vlp_cte_old(...)?;
       
       compare_cte_outputs(&new_ctes, &old_ctes)?;
       
       // Return NEW CTEs (switch!)
       Ok(new_ctes)
   }
   ```

3. **Deploy to staging/test environment**:
   - Monitor error rates
   - Check query latency
   - Watch memory usage
   - Run production-like workload

4. **Create emergency rollback mechanism**:
   ```rust
   // Runtime flag for emergency rollback
   if std::env::var("FORCE_OLD_CTE_LOGIC").is_ok() {
       log::warn!("EMERGENCY: Using old CTE logic (FORCE_OLD_CTE_LOGIC set)");
       return extract_vlp_cte_old(graph_rel, spec, context, schema);
   }
   ```

**Deliverables**:
- [ ] Feature enabled by default
- [ ] Staging deployment successful
- [ ] Metrics look good
- [ ] Rollback mechanism tested

**Success Criteria**:
- [ ] No increase in error rates
- [ ] Query latency unchanged or improved
- [ ] Memory usage stable
- [ ] All production queries working

---

### Week 8: Cleanup & Documentation

**Tasks**:

1. **Remove old CTE generation code**:
   ```rust
   // Delete deprecated constructors
   // - VariableLengthCteGenerator::new_denormalized
   // - VariableLengthCteGenerator::new_mixed
   // - VariableLengthCteGenerator::new_with_fk_edge
   
   // Keep only one constructor used internally by strategies
   ```

2. **Remove boolean flags**:
   ```rust
   // Remove from VariableLengthCteGenerator struct:
   // - is_denormalized
   // - is_fk_edge  
   // - start_is_denormalized
   // - end_is_denormalized
   
   // Remove all flag-checking logic
   ```

3. **Update documentation**:
   - Update architecture docs
   - Create migration guide for contributors
   - Document new CTE Manager API
   - Add examples for each strategy

4. **Final PR and merge**:
   ```bash
   gh pr create \
     --title "refactor: Unified CTE Manager with strategy pattern" \
     --body "$(cat <<EOF
   ## CTE Unification Refactoring
   
   **Summary**: Replace scattered CTE generation logic with unified CTE Manager
   
   **Key Changes**:
   - New CTE Manager facade for all VLP CTEs
   - 6 strategies for different schema patterns
   - Eliminates boolean flags and scattered conditionals
   - 28% code reduction (8,261 → 5,950 lines)
   
   **Testing**: 
   - All 3,538 integration tests passing
   - Parallel execution validation completed
   - Performance within 3% of baseline
   
   **Migration**: Phased rollout with feature flags (completed successfully)
   
   See: docs/development/cte_refactoring_unified_design.md
   EOF
   )"
   ```

**Deliverables**:
- [ ] Old code removed
- [ ] Boolean flags eliminated
- [ ] Documentation complete
- [ ] PR merged to main

**Success Criteria**:
- [ ] Code reduced by 28% (8,261 → ~5,950 lines)
- [ ] All tests passing
- [ ] Documentation up to date
- [ ] Team trained on new system

---

## Phase 5: Polish & Extensions (Weeks 9-10)

### Goal: Optimization and future-proofing

### Week 9: Performance Optimization

**Tasks**:
1. Profile CTE generation performance
2. Optimize hot paths
3. Cache PatternSchemaContext lookups
4. Benchmark complex queries
5. Document performance characteristics

### Week 10: Future Extensions

**Tasks**:
1. **WITH Clause Extension**: Add support for non-VLP CTEs
2. **Multi-Type VLP**: Integrate MultiTypeVlpJoinGenerator into strategy pattern
3. **Documentation**: Complete API reference and examples
4. **Training**: Create training materials for team

---

## Success Metrics

### Quantitative Targets (from Phase2A Doc)

**Code Quality**:
- [x] CTE code reduced: 8,261 → <6,000 lines (28% reduction) ✅ **REQUIRED**
- [x] Constructor count: 4 → 1 ✅ **REQUIRED**
- [x] Boolean flag checks: 50+ → 0 ✅ **REQUIRED**
- [x] Cyclomatic complexity: HIGH → MEDIUM ✅ **REQUIRED**

**Functionality**:
- [x] SQL output: 100% equivalent with old code ✅ **REQUIRED**
- [x] Test pass rate: 3,538/3,538 (100%) ✅ **REQUIRED**
- [x] Performance: <5% regression ✅ **REQUIRED**
- [x] Production incidents: 0 ✅ **REQUIRED**

**Coverage**:
- [x] Unit test coverage: >80% for CTE Manager ✅ **REQUIRED**
- [x] All 6 schema patterns tested ✅ **REQUIRED**
- [x] Integration tests: All patterns covered ✅ **REQUIRED**

---

## Risk Management

### High-Risk Items

| Risk | Mitigation | Rollback Plan |
|------|-----------|---------------|
| **SQL discrepancies** | Parallel execution, comparison mode | Keep old code, runtime flag |
| **Performance regression** | Benchmark at each phase | Feature flag OFF |
| **Integration complexity** | Phased approach, one strategy at a time | Revert commit |
| **Production incidents** | Staging validation, gradual rollout | Emergency env var |

### Checkpoints

**After Each Phase**:
- [ ] All tests passing
- [ ] Performance validated
- [ ] Code reviewed and approved
- [ ] Documentation updated
- [ ] Stakeholders informed

**Go/No-Go Decision Points**:
- **After Phase 1**: Is CTE Manager interface solid?
- **After Phase 2**: Is SQL equivalence validated?
- **After Phase 3**: Are all strategies working?
- **Before Phase 4**: Is staging deployment successful?

---

## Team Responsibilities

### Development
- Implement strategies one at a time
- Write comprehensive tests
- Document decisions and trade-offs
- Code review within 24 hours

### Testing
- Run comparison mode on all tests
- Benchmark performance at each phase
- Validate on staging environment
- Create regression test suite

### DevOps
- Set up feature flags
- Monitor staging deployment
- Prepare rollback procedures
- Track metrics (latency, errors, memory)

---

## Timeline Summary

| Week | Phase | Focus | Deliverable |
|------|-------|-------|-------------|
| 0 | Prep | Merge Phase2A, unified docs | Infrastructure in main |
| 1 | 1 | CTE Manager facade | Strategy selection working |
| 2 | 1 | Traditional strategy | First strategy complete |
| 3 | 2 | Parallel execution | Comparison framework |
| 4 | 2 | 2 more strategies | 3 strategies validated |
| 5 | 3 | MixedAccess + EdgeToEdge | 5 strategies done |
| 6 | 3 | Coupled + Polish | All 6 strategies complete |
| 7 | 4 | Enable by default | Staging deployment |
| 8 | 4 | Cleanup | Old code removed |
| 9 | 5 | Performance | Optimizations |
| 10 | 5 | Extensions | Future-proofing |

**Total**: 10 weeks from start to complete (including polish)

---

## Communication Plan

### Weekly Updates
- Progress report every Friday
- Blockers identified and escalated
- Next week's goals communicated

### Documentation
- Update STATUS.md after each phase
- Keep comparison doc current
- Document design decisions in code comments

### Stakeholder Communication
- Demo after Phase 2 (comparison working)
- Demo after Phase 3 (all strategies done)
- Demo after Phase 4 (production ready)

---

## Appendix: Quick Start Commands

### Start Phase 0
```bash
# Review and merge Phase2A
git checkout refactor/cte-phase2a-infrastructure
gh pr create --title "refactor: CTE infrastructure (Phase 2A)"

# After merge, start Phase 1
git checkout main
git pull origin main
git checkout -b refactor/cte-manager-core
```

### Start Each Strategy
```bash
# Cherry-pick strategy from CTE-Unification
git checkout feature/cte-unification
# Review strategy implementation
# Copy relevant code manually or via patch

# Adapt and test
cargo test --lib cte_manager
```

### Run Comparison Mode
```bash
RUSTFLAGS="--cfg debug_assertions" \
RUST_LOG=clickgraph=debug \
cargo test --features unified_cte -- --nocapture
```

### Deploy to Staging
```bash
# Build with unified_cte enabled
cargo build --release --features unified_cte

# Deploy with rollback capability
FORCE_OLD_CTE_LOGIC="" ./target/release/clickgraph
```

---

## Conclusion

This plan combines:
- ✅ Phase2A's risk-averse, phased approach
- ✅ CTE-Unification's complete implementation
- ✅ Validation at every step
- ✅ Clear rollback mechanisms
- ✅ Quantitative success metrics

**Next Action**: Review this plan with team, then start Phase 0 (merge Phase2A infrastructure).

**Success Probability**: 90% (high confidence due to phased approach with validation)
