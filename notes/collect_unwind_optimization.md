# collect() + UNWIND Optimization Opportunities

**Status**: Future optimization (not implemented)  
**Impact**: HIGH - Real-world scenarios with hundreds of columns  
**Date**: December 20, 2025

## Current Implementation

### collect(node) Expansion
Currently, when we encounter `collect(node)`, we expand it to collect **ALL** properties:

```cypher
WITH collect(f) as friends
```

Generates:
```sql
WITH ... SELECT groupArray(tuple(
    f.firstName, f.lastName, f.email, f.age, f.city, f.country, 
    f.phone, f.address, f.zipcode, ... /* hundreds more */
)) as friends
```

### Problem

**For tables with hundreds of columns**, this is extremely expensive:
- Memory consumption: Storing all columns in arrays
- Network I/O: Transferring unnecessary data
- ClickHouse overhead: Processing unused columns
- Query performance: Significantly slower for wide tables

**Real-world impact**: LDBC SNB Person table has 50+ columns. E-commerce product tables can have 100-200 columns.

## Optimization 1: Column Projection Analysis

### Strategy
Analyze downstream usage to collect only referenced properties.

### Example

**Query**:
```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, collect(f) as friends
UNWIND friends as friend
RETURN p.firstName, friend.firstName, friend.lastName
```

**Current**: Collects ALL properties of `f` (50+ columns)

**Optimized**: Should collect ONLY `firstName` and `lastName`:
```sql
WITH ... SELECT 
    groupArray(tuple(f.firstName, f.lastName)) as friends
```

### Implementation Approach

**Phase 1: Downstream Analysis**
1. During analyzer phase, track property references in UNWIND and subsequent clauses
2. Build a "required columns" map: `{ alias -> Set<property_names> }`
3. Pass this map through query planning

**Phase 2: Selective Expansion**
Modify `expand_collect_to_group_array()` to accept optional filter:
```rust
pub fn expand_collect_to_group_array(
    alias: &str,
    all_properties: Vec<(String, String)>,
    required_properties: Option<&HashSet<String>>,
) -> LogicalExpr {
    let properties = if let Some(required) = required_properties {
        all_properties.into_iter()
            .filter(|(prop, _)| required.contains(prop))
            .collect()
    } else {
        all_properties // Fallback: collect all
    };
    // ... rest of expansion
}
```

**Phase 3: Wildcard Handling**
If downstream uses `friend.*`, must collect all properties:
```cypher
UNWIND friends as friend
RETURN friend.*  -- Need all properties
```

### Edge Cases

1. **Nested property access**: `friend.address.city` → Need `address` column
2. **Function calls**: `toUpper(friend.firstName)` → Need `firstName`
3. **WHERE clauses**: `WHERE friend.age > 30` → Need `age`
4. **Multiple UNWINDs**: Track requirements from all usage sites

## Optimization 2: collect + UNWIND No-op Detection

### Pattern Recognition

Detect when collect/UNWIND effectively cancels out:

```cypher
-- Pattern 1: Direct passthrough
WITH collect(f) as friends
UNWIND friends as f
RETURN f.firstName

-- Equivalent to:
RETURN f.firstName  -- No grouping needed!
```

**Conditions for no-op**:
1. ✅ Single alias collected
2. ✅ No other aggregations in same WITH
3. ✅ UNWIND immediately follows
4. ✅ No WHERE/ORDER BY between WITH and UNWIND
5. ✅ Unwound alias not used in aggregations later

### When It's NOT a No-op

```cypher
-- Example 1: Multiple aggregations
WITH collect(f) as friends, count(f) as cnt
-- Need grouping for count()

-- Example 2: Filtering after collection
WITH collect(f) as friends
WHERE size(friends) > 5
-- Need array to check size

-- Example 3: Sorting collected items
WITH collect(f) as friends
UNWIND friends as f
ORDER BY f.age
-- May need array if ORDER BY is on collection
```

### Implementation

**Analyzer pass**: `detect_collect_unwind_noop()`
```rust
struct NoopPattern {
    with_clause_idx: usize,
    collected_alias: String,
    unwound_alias: String,
    can_eliminate: bool,
}

fn detect_collect_unwind_noop(plan: &LogicalPlan) -> Vec<NoopPattern> {
    // 1. Find WITH + UNWIND pairs
    // 2. Check no-op conditions
    // 3. Return elimination candidates
}
```

**Optimizer pass**: `eliminate_collect_unwind_noop()`
- Remove collect() from WITH
- Remove UNWIND clause
- Preserve original alias mapping

## Optimization 3: Partial Array Materialization

For cases where we must collect but don't need full materialization:

```cypher
WITH collect(f) as friends
WHERE size(friends) > 5
UNWIND friends as f
RETURN f.firstName
```

**Strategy**: Use ClickHouse's `arrayReduce` or window functions instead:
```sql
-- Instead of: groupArray(tuple(...))
-- Use: If only checking count/size
HAVING count(*) > 5
```

## Implementation Priority

**Phase 1** (High Impact):
- Column projection analysis (Optimization 1)
- Most impactful for wide tables
- Estimated effort: 2-3 days

**Phase 2** (Medium Impact):
- No-op detection (Optimization 2)
- Simpler queries benefit most
- Estimated effort: 1-2 days

**Phase 3** (Advanced):
- Partial materialization (Optimization 3)
- Complex cases only
- Estimated effort: 3-4 days

## Performance Impact Estimation

**Scenario**: LDBC Person table (50 properties), query collecting 1000 persons

### Current Implementation
```
Memory: 50 * 8 bytes * 1000 = ~400 KB per query
Time: ~100ms (empirical)
```

### With Column Projection (only 3 properties needed)
```
Memory: 3 * 8 bytes * 1000 = ~24 KB per query
Time: ~15ms (estimated 7x faster)
Savings: 94% memory, 85% time
```

### With No-op Elimination
```
Memory: 0 (no array materialization)
Time: ~2ms (no grouping overhead)
Savings: 100% memory, 98% time
```

## Related Code

**Current expansion logic**:
- `src/render_plan/property_expansion.rs` - `expand_collect_to_group_array()`
- `src/render_plan/plan_builder.rs` - Line ~5500 (collect expansion call site)

**Where to add analysis**:
- `src/query_planner/analyzer/` - New pass: `collect_property_analyzer.rs`
- Track in `AnalyzerContext` or new `CollectContext` structure

**Optimizer location**:
- `src/query_planner/optimizer/` - New pass: `collect_unwind_optimizer.rs`

## Testing Plan

1. **Benchmark queries** with varying column counts (10, 50, 100, 200)
2. **Memory profiling** with `cargo bench` or flamegraph
3. **Correctness tests** ensuring optimized queries return same results
4. **Edge case coverage** for all no-op conditions

## References

- ClickHouse optimization docs: https://clickhouse.com/docs/en/operations/optimizing-performance
- Column pruning in SQL optimizers: Common database optimization technique
- OpenCypher spec on UNWIND: https://opencypher.org/resources/

---

**Note**: This optimization is particularly important for production workloads where:
- Tables have many columns (100+)
- Queries collect large datasets (1000+ rows)
- Performance is critical (latency-sensitive applications)
