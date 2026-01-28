# Intermediate Node Elimination Optimization

**Created**: November 22, 2025  
**Status**: Planned for future optimization  
**Priority**: High (performance win for common queries)

## Problem Statement

In multi-hop path patterns, intermediate nodes are currently JOINed even when their properties are never referenced. This adds unnecessary table joins that hurt query performance.

## Example

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS*2]->(c:User)
RETURN a.name, c.name
```

**Current SQL** (Suboptimal):
```sql
SELECT a.name, c.name
FROM users a
JOIN follows r1 ON a.user_id = r1.follower_id
JOIN users b ON r1.followed_id = b.user_id      -- ❌ UNNECESSARY!
JOIN follows r2 ON b.user_id = r2.follower_id
JOIN users c ON r2.followed_id = c.user_id
```

**Optimized SQL** (Should be):
```sql
SELECT a.name, c.name
FROM users a
JOIN follows r1 ON a.user_id = r1.follower_id
JOIN follows r2 ON r1.followed_id = r2.follower_id  -- ✅ Direct bridge!
JOIN users c ON r2.followed_id = c.user_id
```

**Performance Impact**:
- Eliminates 1 table JOIN per intermediate hop
- Reduces I/O, memory usage, and query time
- For `*N` patterns, saves N-1 node JOINs!

## When Intermediate Nodes ARE Needed

**Example 1**: Properties referenced in RETURN
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
RETURN a.name, b.name, c.name  -- b.name is used!
```
✅ Must JOIN `users b` - properties needed

**Example 2**: Properties referenced in WHERE
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
WHERE b.country = 'USA'  -- b.country is used!
RETURN a.name, c.name
```
✅ Must JOIN `users b` - filter needs properties

**Example 3**: Node alias referenced in aggregation
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
RETURN a.name, COUNT(DISTINCT b) as intermediate_count
```
✅ Must JOIN `users b` - COUNT(DISTINCT) needs node identity

## When Intermediate Nodes Can Be Eliminated

**Pattern**: Node appears in path pattern but:
- ❌ NOT referenced in RETURN clause
- ❌ NOT referenced in WHERE clause  
- ❌ NOT referenced in ORDER BY
- ❌ NOT referenced in WITH clause
- ❌ NOT used in aggregations or functions

**Result**: Bridge directly through relationship ID columns!

## Implementation Strategy

### 1. Analysis Phase (in `query_planner/analyzer/`)

Add a new optimizer pass: **UnusedNodeElimination**

```rust
pub struct UnusedNodeElimination;

impl OptimizerPass for UnusedNodeElimination {
    fn optimize(&self, plan: Arc<LogicalPlan>, ctx: &mut PlanCtx) 
        -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        
        // Collect all node aliases that are actually referenced
        let referenced_nodes = collect_referenced_nodes(&plan)?;
        
        // Mark nodes in path patterns as "needed" or "bridge-only"
        mark_intermediate_nodes(&plan, &referenced_nodes, ctx)?;
        
        Ok(Transformed::No(plan))
    }
}
```

### 2. Add Metadata to TableCtx

In `query_planner/plan_ctx/mod.rs`:

```rust
pub struct TableCtx {
    // ... existing fields ...
    
    /// True if this node is only needed as a bridge in a path
    /// (its properties are never referenced)
    pub is_bridge_only: bool,
}
```

### 3. SQL Generation (in `render_plan/`)

When generating JOINs for multi-hop patterns:

```rust
fn generate_multi_hop_joins(hops: usize, rel_info: &RelInfo) -> Vec<Join> {
    let mut joins = Vec::new();
    
    for i in 0..hops {
        // Add relationship JOIN
        joins.push(generate_rel_join(i, rel_info));
        
        // Check if intermediate node is needed
        if i < hops - 1 {  // Not the final node
            let node_alias = format!("n{}", i + 1);
            if ctx.is_node_bridge_only(&node_alias) {
                // Skip node JOIN - bridge directly through relationship IDs
                continue;
            } else {
                // Node properties needed - add the JOIN
                joins.push(generate_node_join(i + 1, rel_info));
            }
        }
    }
    
    // Always JOIN the final node (endpoint)
    joins.push(generate_final_node_join(hops, rel_info));
    
    joins
}
```

### 4. Testing Strategy

**Unit Tests**:
- Node properties referenced → node JOIN generated
- Node properties NOT referenced → node JOIN skipped
- Edge cases: RETURN *, COUNT(DISTINCT n), etc.

**Integration Tests**:
- Verify optimized SQL correctness
- Performance benchmarks (before/after)
- Ensure results identical with/without optimization

## Historical Context

**Earlier Implementation**: Intermediate nodes were eliminated (this optimization existed!)

**Why Added Back**: Fixed **cycle prevention bugs** - intermediate nodes needed for uniqueness constraints!

### The Cycle Prevention Constraint

In `ChainedJoinGenerator::generate_chained_join_query()`, intermediate nodes are JOINed to enforce:

```sql
WHERE s.user_id != e.user_id          -- Start != End
  AND s.user_id != m1.user_id         -- Start != Middle1
  AND e.user_id != m1.user_id         -- End != Middle1
  AND m1.user_id != m2.user_id        -- Middle1 != Middle2
  ...
```

**Problem**: Can't check `m1.user_id != m2.user_id` without JOINing the node tables!

### The Optimization Opportunity

**Key Insight**: Cycle prevention can use **relationship target IDs** instead of node table JOINs!

```sql
-- Current (requires node JOINs):
WHERE m1.user_id != m2.user_id

-- Optimized (use relationship columns directly):
WHERE r1.followed_id != r2.followed_id
```

**Implementation Strategy**:
1. Keep cycle prevention logic
2. Rewrite constraints to use relationship ID columns
3. Only JOIN node tables if properties actually referenced

**Example - Optimized with Cycle Prevention**:
```sql
SELECT a.name, c.name
FROM users a
JOIN follows r1 ON a.user_id = r1.follower_id
JOIN follows r2 ON r1.followed_id = r2.follower_id
JOIN users c ON r2.followed_id = c.user_id
WHERE a.user_id != c.user_id              -- Start != End
  AND a.user_id != r1.followed_id         -- Start != r1.target
  AND c.user_id != r1.followed_id         -- End != r1.target
  AND r1.followed_id != r2.followed_id    -- r1.target != r2.target
```

No intermediate node JOINs needed! ✅

**Action Items**:
1. ✅ Understand why nodes added: Cycle prevention
2. 📋 Rewrite cycle prevention to use relationship ID columns
3. 📋 Ensure correctness with comprehensive tests
4. 📋 Add flag to enable/disable optimization for safety

## Search Commands for Investigation

```bash
# Find commits that modified JOIN generation
git log --all --oneline --grep="intermediate" -- src/render_plan/
git log --all --oneline --grep="node.*join" -- src/clickhouse_query_generator/

# Find when ChainedJoinGenerator was created/modified
git log --all --oneline -- src/clickhouse_query_generator/variable_length_cte.rs

# Search for related comments in code
rg -i "intermediate.*node" --type rust
rg -i "bridge.*join" --type rust
```

## Dependencies

This optimization should be implemented AFTER:
- ✅ Fixed-length path refactoring (remove unnecessary CTE wrapper)
- ✅ Clean separation of recursive vs non-recursive patterns

**Reason**: Cleaner codebase makes this optimization easier to implement correctly.

## Performance Benefits (Estimated)

**Benchmark Query**: `MATCH (a)-[:REL*5]->(f) RETURN a.name, f.name`

| Metric | Current | Optimized | Improvement |
|--------|---------|-----------|-------------|
| JOINs | 11 (5 rel + 6 node) | 7 (5 rel + 2 node) | -36% JOINs |
| Tables Scanned | 6 | 2 | -67% tables |
| Memory | High | Low | Significant |
| Query Time | Baseline | 30-50% faster | Expected |

**Note**: Actual performance gains depend on:
- Table sizes
- Index coverage on relationship ID columns
- ClickHouse query planner optimizations

## Next Steps

1. ✅ Document optimization (this note)
2. 🔄 Complete CTE architecture refactoring first
3. 📋 Research git history for why intermediate nodes added
4. 📋 Implement `UnusedNodeElimination` optimizer pass
5. 📋 Add comprehensive tests
6. 📋 Benchmark performance improvements
7. 📋 Document in user-facing docs

## References

- **Related**: `notes/viewscan.md` - Similar property-access optimization
- **Code**: `src/clickhouse_query_generator/variable_length_cte.rs` - ChainedJoinGenerator
- **Code**: `src/render_plan/cte_extraction.rs` - Current multi-hop JOIN generation
- **Code**: `src/query_planner/analyzer/` - Where optimizer pass would go
