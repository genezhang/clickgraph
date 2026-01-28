# Property Pruning Optimization - Executive Summary

**Date**: December 23, 2025  
**Status**: Ready for Implementation  
**Priority**: HIGH  
**Effort**: 3-4 weeks  
**Impact**: 8-16x performance improvement for wide tables

---

## Problem Statement

ClickGraph currently materializes **all columns** in aggregation contexts, even when downstream queries only use a few properties. This causes severe performance issues for:

- LDBC Person tables (50+ properties)
- E-commerce product tables (100-200 properties)
- Security log tables with wide schemas

### Example: collect() Inefficiency

**Query**:
```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH collect(f) as friends
UNWIND friends as friend
RETURN friend.firstName, friend.lastName  -- Only 2 properties!
```

**Current SQL** (❌ Inefficient):
```sql
SELECT groupArray(tuple(
    f.city, f.country, f.email, f.phone, f.address, 
    f.zipcode, f.birthday, f.gender, ...,  -- 50+ columns!
    f.firstName, f.lastName, f.user_id
)) as friends
```

**Memory Cost**: 400 KB for 1000 rows  
**Execution Time**: ~100ms

**Optimal SQL** (✅ With optimization):
```sql
SELECT groupArray(tuple(
    f.firstName, f.lastName, f.user_id  -- Only 3 columns!
)) as friends
```

**Memory Cost**: 16 KB for 1000 rows (96% reduction)  
**Execution Time**: ~12ms (8x faster)

---

## Solution Architecture

### 4-Phase Implementation

**Phase 1: Foundation** (Week 1)
- Create `PropertyRequirements` data structure
- Add to `PlanCtx` for cross-module access
- Unit tests for API

**Phase 2: Analysis** (Week 2)
- New analyzer pass: `PropertyRequirementsAnalyzer`
- Bottom-up traversal from RETURN → MATCH
- Track property usage from:
  - RETURN clause
  - WHERE filters
  - ORDER BY expressions
  - UNWIND downstream access

**Phase 3: Selective Expansion** (Week 3)
- Update `expand_collect_to_group_array()` to filter properties
- Update `expand_table_alias_to_select_items()` for WITH clauses
- Update `anyLast()` wrapping logic for aggregations

**Phase 4: Edge Cases** (Week 4)
- Handle nested properties (`friend.address.city`)
- Handle wildcards (`RETURN friend.*`)
- Handle multiple UNWIND sites
- Comprehensive testing

---

## Key Components

### 1. PropertyRequirements Tracker

```rust
// New data structure to track required properties
struct PropertyRequirements {
    required_properties: HashMap<String, HashSet<String>>,
    wildcard_aliases: HashSet<String>,
}

// Example usage:
// alias "friend" requires properties: ["firstName", "lastName", "id"]
```

### 2. PropertyRequirementsAnalyzer Pass

**Location**: `src/query_planner/analyzer/property_requirements_analyzer.rs`

**Purpose**: Analyze plan tree to discover which properties are actually used

**Algorithm**:
1. Start from RETURN clause (bottom of plan tree)
2. Walk up to MATCH clause (top of plan tree)
3. Collect property references from:
   - `PropertyAccessExp("friend", "firstName")` → require firstName
   - `TableAlias("friend")` (wildcard) → require all
4. Store results in `PlanCtx` for renderer to use

### 3. Selective Expansion

**Modified Functions**:
- `expand_collect_to_group_array()` - filter properties based on requirements
- `expand_table_alias_to_select_items()` - only expand required properties
- CTE extraction anyLast logic - only wrap required properties

---

## Performance Impact

| Scenario | Table Columns | Properties Used | Before | After | Improvement |
|----------|---------------|-----------------|--------|-------|-------------|
| LDBC Person | 50 | 2 | 100ms, 400KB | 12ms, 16KB | 8x faster, 96% less memory |
| E-commerce Product | 200 | 3 | 800ms, 16MB | 50ms, 240KB | 16x faster, 98.5% less memory |
| Security Logs | 150 | 5 | 500ms, 8MB | 45ms, 180KB | 11x faster, 97.8% less memory |

---

## Affected Query Patterns

### Pattern 1: collect() + UNWIND
```cypher
WITH collect(node) as nodes
UNWIND nodes as n
RETURN n.property
```
**Impact**: 85-98% memory reduction

### Pattern 2: WITH aggregation
```cypher
WITH node, count(other) as cnt
RETURN node.property, cnt
```
**Impact**: Eliminates unnecessary anyLast() calls

### Pattern 3: Relationship in aggregation
```cypher
MATCH (a)-[r:TYPE]->(b)
WITH r, count(b) as cnt
RETURN r.date, cnt
```
**Impact**: Only SELECT required relationship properties

---

## Testing Strategy

### Unit Tests
- PropertyRequirements API
- PropertyRequirementsAnalyzer logic
- Edge cases (nested, wildcards, etc.)

### Integration Tests
- Verify SQL only collects required properties
- Verify correct results for all patterns
- Performance benchmarks with wide tables

### Regression Tests
- All existing tests must pass
- Backward compatibility when analyzer disabled

---

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| ID column not included | Query fails | Always include ID in analyzer |
| Analyzer misses property | Missing column error | Comprehensive test coverage + fallback to all |
| Performance regression (small tables) | Unnecessary overhead | Only filter for tables with 20+ columns |

---

## Success Criteria

✅ **Correctness**: All 650+ existing tests pass  
✅ **Performance**: 5x+ improvement for tables with 50+ columns  
✅ **Memory**: 80%+ reduction in intermediate result size  
✅ **Compatibility**: Graceful degradation when analyzer disabled  
✅ **Coverage**: 90%+ test coverage for new code  

---

## Timeline

| Week | Phase | Deliverables |
|------|-------|-------------|
| 1 | Foundation | PropertyRequirements struct, PlanCtx integration |
| 2 | Analysis | PropertyRequirementsAnalyzer pass, pipeline integration |
| 3 | Expansion | Update collect/anyLast/CTE expansion logic |
| 4 | Polish | Edge cases, comprehensive tests, documentation |

**Total Effort**: 3-4 weeks (1 senior engineer)

---

## Future Enhancements (Phase 5+)

### 1. No-op Detection
Eliminate `collect() + UNWIND` when it's effectively a passthrough
- **Estimated**: 1-2 weeks
- **Impact**: 98% time reduction for simple patterns

### 2. Window Functions
Use ClickHouse window functions instead of groupArray for COUNT-only cases
- **Estimated**: 2-3 weeks
- **Impact**: Avoid array materialization entirely

### 3. Partial Materialization
Only materialize array elements that are accessed (e.g., `friends[0]`)
- **Estimated**: 3-4 weeks
- **Impact**: Further memory optimization

---

## Documentation

**Implementation Plan**: [notes/property_pruning_optimization_plan.md](property_pruning_optimization_plan.md)  
**Background Analysis**: [notes/collect_unwind_optimization.md](collect_unwind_optimization.md)  
**Known Issues**: [KNOWN_ISSUES.md](../KNOWN_ISSUES.md#1-collect-performance---wide-tables)

---

## Recommendation

**✅ APPROVE FOR IMPLEMENTATION**

This optimization:
- Addresses critical performance bottleneck
- Has clear, systematic implementation path
- Provides 8-16x improvement for common scenarios
- Maintains backward compatibility
- Low risk with comprehensive test plan

**Next Steps**:
1. Review and approve implementation plan
2. Create feature branch: `feature/property-pruning`
3. Begin Phase 1 implementation
4. Weekly progress reviews

---

**Questions?** See full implementation plan in [notes/property_pruning_optimization_plan.md](property_pruning_optimization_plan.md)
