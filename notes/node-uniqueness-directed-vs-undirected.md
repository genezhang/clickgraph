# Node Uniqueness: Directed vs Undirected Patterns

**Date**: November 22, 2025  
**Question**: Are node uniqueness rules only for undirected relationships?  
**Answer**: **NO** - Neo4j prevents cycles in BOTH directed and undirected patterns!

## Summary

Based on actual Neo4j 5.x testing, **cycle prevention (start != end) applies to ALL patterns**, regardless of direction. However, the **full pairwise uniqueness** behavior differs slightly.

## Test Results from Neo4j

### Directed Patterns ✅

**Test 1: Directed Variable-Length (*2)**
```cypher
MATCH (a:User)-[:FOLLOWS*2]->(c:User)
WHERE a.user_id = 1
RETURN a.name AS start, c.name AS end
```

**Result**: `Alice -> Charlie`  
**Key Finding**: Does **NOT** return `Alice -> Alice` (cycle prevented!)

**Test 2: Explicit 2-Hop Directed**
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
WHERE a.user_id = 1
RETURN a.name AS start, b.name AS intermediate, c.name AS end
```

**Result**: `Alice -> Bob -> Charlie`  
**Key Finding**: Does **NOT** return `Alice -> Bob -> Alice` (cycle prevented!)

### Undirected Patterns ✅

**Test 3: Undirected 1-Hop**
```cypher
MATCH (a:User)-[:FOLLOWS]-(b:User)
WHERE a.user_id = 1
RETURN a.name AS node_a, b.name AS node_b
```

**Results**: `Alice - Bob`, `Alice - David`  
**Key Finding**: Does **NOT** return `Alice - Alice` (self-match prevented!)

**Test 4: Friends-of-Friends (Undirected 2-Hop)**
```cypher
MATCH (user:User)-[:FOLLOWS]-(friend:User)-[:FOLLOWS]-(fof:User)
WHERE user.user_id = 1
RETURN user.name, friend.name, fof.name
```

**Results**: 
- `Alice - David - Charlie`
- `Alice - Bob - Charlie`

**Key Finding**: Does **NOT** return `Alice - Bob - Alice` (start node excluded!)

**Test 7: Named 3-Node Undirected Chain**
```cypher
MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
WHERE a.user_id = 1
RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c
```

**Analysis**:
- ✅ `a != b` for all results
- ✅ `b != c` for all results  
- ✅ `a != c` for all results (FULL pairwise uniqueness!)

## Neo4j Behavior Summary

### Cycle Prevention (start != end)

| Pattern Type | Direction | Prevents Cycles? | Example |
|-------------|-----------|------------------|---------|
| Variable-length | Directed `->` | ✅ YES | `(a)-[:F*2]->(c)` no `a->a` |
| Variable-length | Undirected `-` | ✅ YES | `(a)-[:F*2]-(c)` no `a-a` |
| Explicit multi-hop | Directed `->` | ✅ YES | `(a)->(b)->(c)` no `a->b->a` |
| Explicit multi-hop | Undirected `-` | ✅ YES | `(a)-(b)-(c)` no `a-b-a` |

**Conclusion**: Cycle prevention applies to **BOTH directed and undirected** patterns!

### Pairwise Node Uniqueness (all nodes different)

| Pattern Type | Direction | Full Uniqueness? | Details |
|-------------|-----------|------------------|---------|
| Single-hop | Directed `->` | ✅ YES (trivial) | `(a)->(b)` always has `a != b` |
| Single-hop | Undirected `-` | ✅ YES | `(a)-(b)` prevents `a-a` |
| Multi-hop explicit | Directed `->` | ✅ YES | `(a)->(b)->(c)` enforces all pairs |
| Multi-hop explicit | Undirected `-` | ✅ YES | `(a)-(b)-(c)` enforces all pairs |

**Conclusion**: Full pairwise uniqueness applies to **BOTH directed and undirected** named patterns!

## Why the Confusion?

The OpenCypher spec example specifically mentions **undirected** friends-of-friends:

> "Looking for a user's friends of friends should not return said user"

This quote uses undirected relationships because:
1. Social networks often use undirected FRIEND relationships
2. The cycle problem is more obvious with undirected (bidirectional traversal)
3. It's the canonical example in graph databases

**But the rule applies to directed patterns too!**

## Implications for ClickGraph

### What We Got Right ✅

1. **Cycle prevention for directed patterns** - Already working!
   - Variable-length: `(a)-[:F*2]->(c)` adds `a != c` ✅
   - This works for both directed and undirected

2. **Single-hop undirected** - Already working!
   - `(a)-(b)` adds `a != b` filter ✅

### What We Need to Fix ⚠️

**The issue is NOT about directed vs undirected** - it's about **multi-hop explicit patterns**!

**Problem**: Multi-hop explicit patterns (both directed and undirected) need full pairwise uniqueness:

1. **Undirected multi-hop**:
   ```cypher
   MATCH (a)-(b)-(c)
   ```
   - Currently: Only `a != b` and `b != c`
   - Needed: Also `a != c`

2. **Directed multi-hop** (probably same issue):
   ```cypher
   MATCH (a)->(b)->(c)
   ```
   - Should enforce: `a != b`, `b != c`, AND `a != c`
   - Need to test ClickGraph's current behavior!

### Action Items

1. **Test ClickGraph's directed multi-hop behavior** (5 min)
   ```cypher
   MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
   WHERE a.user_id = 1
   RETURN a.name, b.name, c.name
   ```
   - Does it enforce `a != c`?
   - Or does it allow `a = c`?

2. **Implement full pairwise uniqueness** (4-6 hours)
   - Apply to **ALL multi-hop explicit patterns** (both directed and undirected)
   - Generate O(N²) filters for N-node chains
   - For 3 nodes: 3 filters (`a!=b`, `b!=c`, `a!=c`)

3. **Update documentation** (30 min)
   - Clarify that cycle prevention applies to all patterns
   - Explain that undirected is just the canonical example
   - Document pairwise uniqueness for multi-hop patterns

## Key Takeaway

**Node uniqueness rules apply to BOTH directed and undirected patterns!**

The difference is:
- **Variable-length paths** (`*2`, `*3`, etc.): Already handle cycle prevention correctly ✅
- **Explicit multi-hop paths** (`(a)-(b)-(c)`): Need full pairwise uniqueness for all directions ⚠️

The fix we need is **pattern type agnostic** - it should work the same for:
- `(a)->(b)->(c)` (directed)
- `(a)-(b)-(c)` (undirected)  
- `(a)<-(b)->(c)` (mixed)

All should enforce: `a!=b AND b!=c AND a!=c`

## References

- **Test Script**: `scripts/test/neo4j_semantics_test_ascii.py`
- **Full Results**: `notes/neo4j-verified-semantics.md`
- **Test Data**: 4-node cycle (Alice->Bob->Charlie->David->Alice)
- **Neo4j Version**: 5.x latest
