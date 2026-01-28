# CRITICAL: Relationship Uniqueness Implementation Required

**Date**: November 22, 2025  
**Status**: 🚨 **INCOMPLETE - SQL structure alone is NOT sufficient**

## The Discovery

Initial assumption: "SQL structure automatically prevents relationship reuse because different table aliases (r1, r2) ensure different rows."

**This is WRONG for undirected patterns!**

## The Problem

### Directed Patterns ✅ (Work Correctly)

```cypher
MATCH (a)-[r1:FOLLOWS]->(b)-[r2:FOLLOWS]->(c)
```

Generated SQL:
```sql
FROM users a
JOIN follows r1 ON r1.follower_id = a.user_id
JOIN users b ON b.user_id = r1.followed_id  
JOIN follows r2 ON r2.follower_id = b.user_id  -- MUST have follower_id = b.user_id
JOIN users c ON c.user_id = r2.followed_id
```

**Why it works**: If `r1` matches row `(1, 2)` (Alice→Bob), then:
- `b.user_id = 2`
- `r2` must have `follower_id = 2`
- Row `(1, 2)` has `follower_id = 1`, so `r2` CANNOT use it
- Different join conditions automatically enforce uniqueness ✅

### Undirected Patterns ❌ (BROKEN!)

```cypher
MATCH (a)-[r1]-(b)-[r2]-(c)
```

Generated SQL (current):
```sql
WITH r1_bidirectional AS (
  SELECT follower_id AS from_id, followed_id AS to_id FROM follows
  UNION ALL
  SELECT followed_id AS from_id, follower_id AS to_id FROM follows
),
r2_bidirectional AS (
  SELECT follower_id AS from_id, followed_id AS to_id FROM follows
  UNION ALL
  SELECT followed_id AS from_id, follower_id AS to_id FROM follows
)
SELECT *
FROM users a
JOIN r1_bidirectional r1 ON r1.from_id = a.user_id
JOIN users b ON b.user_id = r1.to_id
JOIN r2_bidirectional r2 ON r2.from_id = b.user_id
JOIN users c ON c.user_id = r2.to_id
```

**The Problem**: Same physical row `(1, 2)` from `follows` table can appear as:
- In `r1`: `(from_id=1, to_id=2)` - forward direction
- In `r2`: `(from_id=2, to_id=1)` - reverse direction **of the SAME row!**

This would match: Alice(1) - Bob(2) - Alice(1) using only ONE edge traversed twice!

## Neo4j Verification

**Test Setup**: Graph with single edge: `(Alice)-[:FOLLOWS]->(Bob)`

**Test Query**:
```cypher
MATCH (a)-[r1]-(b)-[r2]-(c)
WHERE a.user_id = 1
RETURN a, b, c, id(r1), id(r2)
```

**Result**: 0 matches ✅

**Interpretation**: Neo4j prevents the same relationship from being used twice, even in opposite directions!

**Test File**: `scripts/test/test_undirected_relationship_uniqueness.py`

## The Solution

We need **explicit relationship uniqueness filters** using composite keys.

### For Directed Multi-Hop Patterns

**Current**: No filter needed (join conditions sufficient)  
**Status**: ✅ **CORRECT**

### For Undirected Multi-Hop Patterns  

**Required Filter** (for each pair r1, r2):
```sql
WHERE NOT (
    -- Prevent same row, same direction
    (r1.follower_id = r2.follower_id AND r1.followed_id = r2.followed_id)
    OR
    -- Prevent same row, opposite direction
    (r1.follower_id = r2.followed_id AND r1.followed_id = r2.follower_id)
)
```

**For N relationships in pattern**, need O(N²) pairwise filters:
- `r1` vs `r2`
- `r1` vs `r3`
- `r2` vs `r3`
- etc.

### Implementation Approach

**Option A: Add filters in SQL generation** (`clickhouse_query_generator/`)
- Generate filters when emitting WHERE clause
- Check if relationship uses bidirectional UNION
- Add pairwise filters for all relationship pairs

**Option B: Add filters during logical planning** (`query_planner/`)
- Track relationship aliases during planning
- Generate Filter nodes in logical plan
- Translate to SQL during generation

**Recommendation**: Option A (simpler, matches our architecture)

## Implementation Details

### 1. Track Relationship Information

In `plan_builder.rs`, when building relationship joins, track:
```rust
struct RelationshipInfo {
    alias: String,
    is_undirected: bool,
    from_node_id: String,  // e.g., "r1.follower_id"
    to_node_id: String,    // e.g., "r1.followed_id"
}
```

### 2. Generate Uniqueness Filters

After building all joins, for each pair of undirected relationships:
```rust
fn generate_relationship_uniqueness_filter(
    r1: &RelationshipInfo,
    r2: &RelationshipInfo,
) -> String {
    if !r1.is_undirected && !r2.is_undirected {
        return String::new();  // No filter needed for directed
    }
    
    format!(
        "NOT (({r1_from} = {r2_from} AND {r1_to} = {r2_to}) OR \
              ({r1_from} = {r2_to} AND {r1_to} = {r2_from}))",
        r1_from = r1.from_id_column,
        r1_to = r1.to_id_column,
        r2_from = r2.from_id_column,
        r2_to = r2.to_id_column,
    )
}
```

### 3. Apply Filters in WHERE Clause

When generating final SQL:
```rust
let rel_uniqueness_filters = generate_all_pairwise_filters(&relationships);
if !rel_uniqueness_filters.is_empty() {
    sql.push_str(" WHERE ");
    sql.push_str(&rel_uniqueness_filters.join(" AND "));
}
```

## Files to Modify

1. **`src/render_plan/plan_builder.rs`** (~line 1460)
   - Track relationship info when building joins
   - Generate uniqueness filters before returning SQL

2. **`src/query_planner/analyzer/graph_traversal_planning.rs`** (~line 475)
   - Already handles bidirectional CTEs
   - May need to mark relationships as undirected in plan

3. **Tests to Add**:
   - `tests/integration/test_undirected_relationship_uniqueness.rs`
   - Test case: Single edge, undirected 2-hop → should return 0 results
   - Test case: Two edges (cycle), undirected 2-hop → should return valid paths only

## Impact Assessment

### Performance
- **O(N²) filters** for N relationships in pattern
- For typical queries (2-3 hops): minimal impact
- For deep patterns (5+ hops): could add overhead
- **Mitigation**: Only generate filters for undirected relationships

### Correctness
- **Currently BROKEN** for undirected multi-hop patterns
- **After fix**: Will match Neo4j behavior ✅

### Complexity
- Adds ~100-150 lines of code
- Relatively straightforward logic
- Well-defined problem space

## Related Documents

- `notes/CRITICAL_relationship_vs_node_uniqueness.md` - Original discovery about relationship vs node uniqueness
- `scripts/test/test_relationship_vs_node_uniqueness.py` - Proof that cycles are allowed (different relationships)
- `scripts/test/test_undirected_relationship_uniqueness.py` - Proof that same edge reuse is NOT allowed

## Next Steps

1. ✅ Document the problem (this file)
2. ⬜ Implement relationship tracking in `plan_builder.rs`
3. ⬜ Add filter generation logic
4. ⬜ Write integration tests
5. ⬜ Verify against Neo4j test cases
6. ⬜ Update `STATUS.md` and `KNOWN_ISSUES.md`

## Key Takeaways

- **SQL structure alone is NOT sufficient for relationship uniqueness in undirected patterns**
- **We MUST add explicit composite key filters**
- **This is required for Neo4j compatibility**
- **The fix is well-defined and tractable**

**Thank you for catching this critical issue!** 🙏
