# Query Rewrite Optimizations for ClickGraph

## Implemented

### 1. Collect+UNWIND Elimination (Partial)
**Pattern**: `WITH collect(x) as xs UNWIND xs as x`  
**Optimization**: Eliminate the collect+UNWIND no-op  
**Status**: ✅ Pattern detection working, ⚠️ needs alias rewriting refinement  
**Impact**: 2-5x performance improvement for applicable queries

### 2. Trivial WITH Elimination  
**Pattern**: `WITH a, b RETURN a, b` (no aggregation, ordering, filtering)  
**Optimization**: Remove unnecessary materialization boundary  
**Status**: ✅ Implemented, integration complete  
**Impact**: Eliminates unnecessary CTE overhead  
**Note**: May not trigger if WITH has already been processed for CTE generation

## Recommended Future Optimizations

### 3. Constant Folding
**Pattern**: `WHERE 1 + 2 = x` or `RETURN 5 * 10`  
**Optimization**: Evaluate constant expressions at compile time  
**Complexity**: Low  
**Impact**: Small but helps readability of generated SQL

### 4. Identity Operation Elimination
**Patterns**:
- `x * 1` → `x`
- `x + 0` → `x`  
- `x / 1` → `x`
- `x - 0` → `x`

**Complexity**: Low  
**Impact**: Cleaner SQL generation

### 5. Double Negation Elimination
**Pattern**: `NOT (NOT expression)`  
**Optimization**: `expression`  
**Complexity**: Low  
**Impact**: Cleaner predicates

### 6. De Morgan's Law Application
**Patterns**:
- `NOT (a AND b)` → `NOT a OR NOT b`
- `NOT (a OR b)` → `NOT a AND NOT b`

**Complexity**: Medium  
**Impact**: Can enable further predicate push-down

### 7. Redundant DISTINCT Removal
**Pattern**: `MATCH (n) WITH DISTINCT n RETURN DISTINCT n`  
**Optimization**: Remove inner DISTINCT when outer guarantees uniqueness  
**Complexity**: Medium (requires uniqueness analysis)  
**Impact**: Eliminates unnecessary deduplication

### 8. Empty Result Short-Circuit
**Patterns**:
- `WHERE false`
- `WHERE 1 = 2`  
- `LIMIT 0`

**Optimization**: Replace entire query with empty result  
**Complexity**: Low  
**Impact**: Prevents unnecessary query execution

### 9. Predicate Simplification
**Patterns**:
- `WHERE true AND x` → `WHERE x`
- `WHERE false OR x` → `WHERE x`
- `WHERE x OR true` → `WHERE true` (always matches)
- `WHERE x AND false` → `WHERE false` (never matches)

**Complexity**: Low  
**Impact**: Cleaner predicates, enables short-circuiting

### 10. Subquery Flattening
**Pattern**: Nested subqueries that can be merged  
**Complexity**: High  
**Impact**: Major performance improvement for nested queries

### 11. Common Subexpression Elimination (CSE)
**Pattern**: Same expression computed multiple times  
**Example**: `RETURN a.x + a.y, a.x + a.y * 2` (a.x + a.y computed twice)  
**Complexity**: Medium  
**Impact**: Reduces redundant computation

### 12. NULL Propagation
**Patterns**:
- `NULL + x` → `NULL`
- `NULL AND x` → `NULL`  
- `NULL = x` → `NULL`

**Complexity**: Low  
**Impact**: Early NULL detection, cleaner SQL

### 13. Range Condition Merging
**Pattern**: `WHERE x > 5 AND x > 3` → `WHERE x > 5`  
**Complexity**: Medium (requires constraint analysis)  
**Impact**: Simpler predicates, better index utilization

### 14. Pattern Match Simplification
**Pattern**: `MATCH (a)-[r]->(b) WHERE type(r) = 'FOLLOWS'`  
**Optimization**: `MATCH (a)-[:FOLLOWS]->(b)` (move to pattern)  
**Complexity**: Low  
**Impact**: Better query planning

## Priority Ranking

**High Priority (Low-hanging fruit)**:
1. Empty result short-circuit (LIMIT 0, WHERE false)
2. Constant folding
3. Identity operation elimination
4. Predicate simplification (true/false constants)
5. Double negation elimination

**Medium Priority**:
6. NULL propagation
7. Pattern match simplification
8. Common subexpression elimination
9. De Morgan's laws

**Lower Priority (Complex but valuable)**:
10. Redundant DISTINCT removal
11. Range condition merging
12. Subquery flattening

## Implementation Strategy

1. **Start Simple**: Implement high-priority optimizations first (1-5)
2. **Composable Passes**: Each optimization is a separate optimizer pass
3. **Test Driven**: Add tests for each pattern before implementation
4. **Incremental**: One optimizer at a time, validate before moving forward
5. **Logging**: Add debug logging to show when optimizations trigger

## Testing Approach

For each optimization:
1. Create unit tests with before/after LogicalPlan comparisons
2. Add integration tests with actual Cypher queries
3. Verify generated SQL is simpler/faster
4. Benchmark impact on real queries

## Example: Constant Folding Implementation

```rust
// src/query_planner/optimizer/constant_folding.rs
match expr {
    LogicalExpr::Operator(op) => {
        // If all operands are literals, evaluate
        if op.operands.iter().all(|e| matches!(e, LogicalExpr::Literal(_))) {
            return evaluate_constant_expr(op);
        }
    }
    _ => {}
}
```

This provides a roadmap for expanding ClickGraph's query optimization capabilities systematically.
