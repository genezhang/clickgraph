# CTE Refactoring - Next Steps Summary

**Date**: January 15, 2026  
**Current Branch**: `feature/cte-unification`  
**Recommended Action**: Merge Phase2A first, then integrate strategies incrementally

---

## TL;DR - What to Do Right Now

### ✋ IMMEDIATE NEXT STEP (This Week)

**Merge Phase2A infrastructure to main:**

```bash
# 1. Switch to Phase2A branch
git checkout refactor/cte-phase2a-infrastructure

# 2. Verify tests pass
cargo test --lib
python -m pytest tests/integration/ -v

# 3. Create PR
gh pr create \
  --title "refactor: Add CTE infrastructure helpers (Phase 2A)" \
  --body "Foundation for CTE unification. Adds PatternSchemaContext helpers. No behavior changes."

# 4. Get approval and merge
```

**Why**: Gets low-risk infrastructure in place without breaking anything.

---

## Key Findings from Analysis

### 🎯 Both Branches Propose IDENTICAL Architecture
- Same CTE Manager design
- Same 6 strategies (Traditional, Denormalized, FkEdge, MixedAccess, EdgeToEdge, Coupled)
- Same use of PatternSchemaContext
- **Only difference**: Implementation strategy

### 📊 Current State
- **CTE-Unification**: 85% complete (2,246 lines of working code, 6 tests passing)
- **Phase2A**: 15% complete (design doc + infrastructure helpers)
- Both have excellent documentation

### ✅ Recommended Approach
**Use Phase2A's phased process to integrate CTE-Unification's code**

**Why this works**:
1. CTE-Unification code already works (proven by tests)
2. Phase2A provides safer integration strategy with validation gates
3. Combines complete implementation + risk mitigation
4. 90% success probability vs 60-70% for other approaches

---

## 10-Week Timeline

| Week | Phase | Key Milestone |
|------|-------|---------------|
| 0 | Prep | ✋ **Merge Phase2A to main** |
| 1-2 | 1 | CTE Manager facade + Traditional strategy |
| 3-4 | 2 | Parallel execution + 2 more strategies |
| 5-6 | 3 | Complete all 6 strategies |
| 7-8 | 4 | Enable by default + cleanup |
| 9-10 | 5 | Polish + extensions |

---

## Success Criteria (Quantitative)

From Phase2A design document:

- ✅ **Code reduction**: 8,261 → <6,000 lines (28% reduction)
- ✅ **Constructor consolidation**: 4 constructors → 1
- ✅ **Boolean flags eliminated**: 50+ checks → 0
- ✅ **SQL equivalence**: 100% match with old implementation
- ✅ **Performance**: <5% regression
- ✅ **Test pass rate**: 3,538/3,538 (100%)

---

## Risk Mitigation

**Multiple rollback mechanisms**:
1. **Feature flags** - Can disable at compile time
2. **Runtime flag** - Emergency env var `FORCE_OLD_CTE_LOGIC`
3. **Parallel execution** - Compare old vs new SQL before committing
4. **Phased approach** - Stop at any phase if issues found

**Validation at every step**:
- Comparison mode runs both implementations
- SQL diff logging for discrepancies
- Performance benchmarking at each phase
- Integration tests with real queries

---

## Documents Created

1. **[cte_approach_comparison.md](cte_approach_comparison.md)** - Detailed comparison of both designs
2. **[cte_refactoring_action_plan.md](cte_refactoring_action_plan.md)** - Complete 10-week plan
3. **This file** - Quick summary for immediate action

---

## Team Communication

**Before Starting**:
- [ ] Review comparison document with team
- [ ] Get alignment on phased approach
- [ ] Assign responsibilities
- [ ] Set up weekly check-ins

**After Each Phase**:
- [ ] Update STATUS.md
- [ ] Demo progress
- [ ] Review metrics
- [ ] Go/no-go decision for next phase

---

## Questions?

**Q: Why not just merge CTE-Unification directly?**
A: Higher risk. The code is 85% complete but needs integration work. Phase2A's phased approach with validation gates reduces risk of breaking production.

**Q: Can we do this faster?**
A: Yes, could compress to 6-8 weeks, but that reduces validation time and increases risk. The 10-week timeline includes buffer for unexpected issues.

**Q: What if we find major issues in Phase 2?**
A: Feature flag OFF, rollback, investigate. That's why we have validation gates at each phase.

**Q: Will this break existing queries?**
A: No. Parallel execution ensures SQL equivalence. Old code stays until fully validated.

**Q: What about the hardcoded table names in CTE-Unification?**
A: That's why we adapt strategies during integration (Phase 1-3). We replace hardcoded values with PatternSchemaContext lookups using Phase2A infrastructure.

---

## References

- **Design Comparison**: [cte_approach_comparison.md](cte_approach_comparison.md)
- **Detailed Action Plan**: [cte_refactoring_action_plan.md](cte_refactoring_action_plan.md)
- **Phase2A Design**: `CTE_MANAGER_DESIGN.md` (in refactor/cte-phase2a-infrastructure branch)
- **CTE-Unification Design**: [cte_unification_design.md](cte_unification_design.md)
- **Current Implementation**: `src/render_plan/cte_manager/mod.rs` (in feature/cte-unification branch)

---

## Start Here

```bash
# Step 1: Review this summary with team
# Step 2: Read the action plan (cte_refactoring_action_plan.md)
# Step 3: Switch to Phase2A branch and create PR
git checkout refactor/cte-phase2a-infrastructure
gh pr create --title "refactor: CTE infrastructure (Phase 2A)"

# Step 4: After Phase2A merges, start Phase 1
git checkout main
git pull origin main
git checkout -b refactor/cte-manager-core
```

**Next Review**: After Phase2A merges, review Phase 1 tasks in detail before starting implementation.
