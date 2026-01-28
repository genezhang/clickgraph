# Cycle Prevention vs Node Uniqueness Analysis

**Date**: November 22, 2025  
**Context**: Clarifying semantics after CTE refactoring

## Key Distinctions

### 1. Cycle Prevention (Implemented ✅)
**What it prevents**: Immediate backtracking in paths
```cypher
MATCH (a)-[:FOLLOWS*2]->(c)
-- Prevents: (a)->(b)->(a)  ❌
```

**Implementation**: Relationship-level filters
```sql
WHERE a.user_id <> c.user_id              -- Start != End
  AND r1.followed_id <> r2.follower_id    -- No backtracking
```

**Cost**: O(N) filters - **very cheap!**

**Current Status**: 
- ✅ Variable-length paths (`*2`, `*3`)
- ❌ Explicit multi-hop patterns (not yet implemented)

---

### 2. Node Uniqueness (NOT Implemented)
**What it prevents**: Any node appearing twice in pattern
```cypher
MATCH (a)-[r1:FOLLOWS]-(b)-[r2:FOLLOWS]-(c)
-- Must ensure: a, b, c are ALL different
```

**Implementation**: Node-level filters
```sql
WHERE a.user_id <> b.user_id    -- All pairs
  AND a.user_id <> c.user_id
  AND b.user_id <> c.user_id
  AND r1.to_id <> r2.from_id    -- Plus backtracking
```

**Cost**: O(N²) filters - **expensive for long paths!**

**Current Status**: ❌ Not implemented

---

## OpenCypher Spec Requirements

### Undirected Friend-of-Friends Pattern ⭐ **MUST FIX**
From OpenCypher spec:
> "Looking for a user's friends of friends should not return said user"

```cypher
MATCH (user:User {name: 'Adam'})-[r1:FRIEND]-()-[r2:FRIEND]-(fof)
RETURN fof.name
```

**Required behavior**: `user` should NOT appear in `fof` results

**Solution**: Add single filter
```sql
WHERE user.user_id <> fof.user_id
```

**Complexity**: EASY - just one filter! ✅

---

## Neo4j Behavior Questions ⚠️

We need to **test in Neo4j** to verify actual semantics:

### Q1: Does Neo4j prevent cycles in directed variable-length?
```cypher
MATCH (a)-[:FOLLOWS*2]->(c)
-- Does Neo4j allow (a)->(b)->(a)?
```

**Our current assumption**: NO (we prevent it)  
**Need to verify**: Test in Neo4j

---

### Q2: Does Neo4j prevent cycles in explicit patterns?
```cypher
MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
-- Does Neo4j allow (a)->(b)->(a)?
```

**Our current implementation**: No filters (allows cycles)  
**Need to verify**: Test in Neo4j

---

### Q3: Does Neo4j enforce full node uniqueness?
```cypher
MATCH (a)-[:FOLLOWS]-(b)-[:FOLLOWS]-(c)
-- Must a, b, c be different?
```

**OpenCypher spec**: Relationship uniqueness guaranteed  
**Node uniqueness**: Only for undirected patterns?  
**Need to verify**: Test comprehensive patterns in Neo4j

---

## Current Implementation Status

| Pattern Type | Cycle Prevention | Node Uniqueness |
|-------------|------------------|-----------------|
| Variable-length `*2` | ✅ YES | ❌ NO |
| Variable-length `*1..` | ✅ YES | ❌ NO |
| Explicit 2-hop | ❌ NO | ❌ NO |
| Undirected pattern | ❌ **BUG** | ❌ NO |

---

## Recommendations

### Immediate (Easy Wins) ✅

**1. Fix undirected patterns** - PRIORITY 1
```rust
// In extract_filters() or cycle prevention logic
if pattern.is_undirected() {
    // Add: start.id <> end.id
}
```

**Effort**: 30 minutes  
**Impact**: Fixes OpenCypher spec violation

---

**2. Add cycle prevention to explicit patterns** - PRIORITY 2
```cypher
MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
-- Add: WHERE a.user_id <> c.user_id
```

**Effort**: 1-2 hours (detect explicit multi-hop chains)  
**Impact**: Consistency with variable-length behavior

---

### Research Needed ⚠️

**3. Test Neo4j semantics**
- Create test database with same schema
- Run all our test queries
- Document actual Neo4j behavior
- Update our implementation to match

**Effort**: 2-3 hours  
**Impact**: Ensures compatibility

---

### Future Enhancements 📋

**4. Configurable node uniqueness**
```yaml
# Schema config
query_optimization:
  node_uniqueness: "auto"  # auto | always | never
  # auto: Only for undirected and short patterns
  # always: Full O(N²) uniqueness checks
  # never: Only cycle prevention
```

**Effort**: 2-3 days  
**Impact**: Advanced use cases, performance tuning

---

## Recursion Depth Configuration ✅

**Already implemented!** 🎉

```bash
# CLI flag
cargo run --bin clickgraph -- --max-cte-depth 1000

# Environment variable
export CLICKGRAPH_MAX_CTE_DEPTH=1000

# Docker compose
environment:
  CLICKGRAPH_MAX_CTE_DEPTH: "1000"
```

**Default**: 100  
**Range**: 10-1000  
**Config location**: `src/config.rs`

---

## Implementation Plan

### Phase 1: Fix Undirected Patterns (30 min)
1. Detect undirected relationships in pattern
2. Add `start_id <> end_id` filter
3. Test with friends-of-friends query
4. Update test suite

### Phase 2: Test Neo4j Semantics (2-3 hours)
1. Set up Neo4j test instance
2. Load benchmark schema
3. Run comparison tests:
   - Variable-length directed
   - Variable-length undirected
   - Explicit multi-hop
   - Named intermediate nodes
4. Document findings

### Phase 3: Implement Cycle Prevention for Explicit Patterns (2-3 hours)
1. Detect explicit multi-hop chains
2. Generate cycle prevention filters
3. Test all pattern variations
4. Update documentation

### Phase 4: Document and Release (1 hour)
1. Update KNOWN_ISSUES.md
2. Update STATUS.md
3. Add to CHANGELOG.md
4. Create release notes

---

## Key Takeaways

1. **Cycle prevention ≠ Node uniqueness**
   - Cycle: O(N) cost, prevents backtracking
   - Uniqueness: O(N²) cost, prevents any reuse

2. **Undirected patterns MUST exclude start node**
   - OpenCypher spec requirement
   - Easy fix (single filter)
   - PRIORITY 1

3. **Test Neo4j before implementing more**
   - Don't assume behavior
   - Verify actual semantics
   - Ensure compatibility

4. **Recursion depth already configurable ✅**
   - CLI: `--max-cte-depth`
   - ENV: `CLICKGRAPH_MAX_CTE_DEPTH`
   - Default: 100

5. **User can add filters manually**
   - Current implementation allows explicit WHERE clauses
   - Power users can optimize themselves
   - Maybe automatic prevention only for common patterns?
