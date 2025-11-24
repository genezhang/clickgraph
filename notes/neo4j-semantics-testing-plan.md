# Neo4j Semantics Testing Plan

**Purpose**: Verify actual Neo4j behavior for cycle prevention and node uniqueness to ensure ClickGraph compatibility.

**Date**: November 22, 2025

## Setup

### 1. Install Neo4j
```bash
# Docker
docker run -d \
  --name neo4j-test \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/testpassword \
  neo4j:latest

# Or use Neo4j Desktop / AuraDB free tier
```

### 2. Load Test Data
Use the same benchmark schema as ClickGraph:

```cypher
// Create Users
CREATE (:User {user_id: 1, full_name: 'Alice'});
CREATE (:User {user_id: 2, full_name: 'Bob'});
CREATE (:User {user_id: 3, full_name: 'Charlie'});
CREATE (:User {user_id: 4, full_name: 'David'});

// Create FOLLOWS relationships (directed)
MATCH (a:User {user_id: 1}), (b:User {user_id: 2})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {user_id: 2}), (b:User {user_id: 3})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {user_id: 3}), (b:User {user_id: 1})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {user_id: 1}), (b:User {user_id: 3})
CREATE (a)-[:FOLLOWS]->(b);

// Create cycle: 1 -> 2 -> 3 -> 1
// Also: 1 -> 3 (direct)
```

---

## Test Cases

### Test 1: Directed Variable-Length (*2) - Cycle Behavior

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS*2]->(c:User)
WHERE a.user_id = 1
RETURN a.user_id, c.user_id
ORDER BY c.user_id
```

**Question**: Does Neo4j allow `(a)-[:FOLLOWS*2]->(a)` (returning to start)?

**Expected Scenarios**:
- **If Neo4j prevents cycles**: Results = [(1, 1)] or empty (depends on cycle)
- **If Neo4j allows cycles**: Results = [(1, 1), (1, 3)]

**ClickGraph Current**: Prevents cycles (adds `a <> c` filter)

---

### Test 2: Directed Explicit 2-Hop - Cycle Behavior

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
WHERE a.user_id = 1
RETURN a.user_id, b.user_id, c.user_id
ORDER BY b.user_id, c.user_id
```

**Question**: Does Neo4j allow `(a)->(b)->(a)` in explicit patterns?

**Expected Results**:
- Path 1: (1, 2, 3)
- Path 2: (1, 2, 1) ← **Does this appear?**
- Path 3: (1, 3, 1) ← **Does this appear?**

**ClickGraph Current**: No filters - allows all paths

---

### Test 3: Undirected Single-Hop - Node Uniqueness

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]-(b:User)
WHERE a.user_id = 1
RETURN a.user_id, b.user_id
ORDER BY b.user_id
```

**Question**: Does Neo4j return `(1, 1)` if user 1 follows itself?

**Expected**: Should NOT return (1, 1) - undirected requires `a != b`

**ClickGraph Current**: ✅ Prevents with `a <> b` filter

---

### Test 4: Undirected Two-Hop - Friends-of-Friends

**Query** (OpenCypher spec example):
```cypher
MATCH (user:User)-[r1:FOLLOWS]-(friend)-[r2:FOLLOWS]-(fof:User)
WHERE user.user_id = 1
RETURN DISTINCT fof.user_id
ORDER BY fof.user_id
```

**Question**: Does user_id=1 appear in the results?

**Expected** (per spec): Should NOT return user_id=1

**ClickGraph Current**: ⚠️ Partially works (only filters adjacent relationships)

---

### Test 5: Undirected Variable-Length (*2)

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS*2]-(c:User)
WHERE a.user_id = 1
RETURN a.user_id, c.user_id
ORDER BY c.user_id
```

**Question**: Does Neo4j return (1, 1)?

**Expected**: Should NOT return (1, 1) for undirected

**ClickGraph Current**: ✅ Prevents with `a <> c` filter

---

### Test 6: Mixed Directed/Undirected

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]-(c:User)
WHERE a.user_id = 1
RETURN a.user_id, b.user_id, c.user_id
ORDER BY b.user_id, c.user_id
```

**Question**: What filters apply when mixing directions?

**ClickGraph Current**: No special handling

---

### Test 7: Named Intermediate Nodes

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
WHERE a.user_id = 1
RETURN a.user_id, b.user_id, c.user_id
ORDER BY b.user_id, c.user_id
```

**Question**: Must a, b, c all be different nodes?

**Scenarios**:
- Can `a == c`? (probably NO for undirected)
- Can `a == b`? (probably NO for undirected)
- Can `b == c`? (probably NO for undirected)

**ClickGraph Current**: Only filters adjacent endpoints

---

### Test 8: Multiple MATCH Clauses - No Uniqueness

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]-(b:User)
MATCH (b)-[:FOLLOWS]-(c:User)
WHERE a.user_id = 1
RETURN DISTINCT c.user_id
ORDER BY c.user_id
```

**Question**: Can `a == c`? (should be YES - no cross-MATCH uniqueness)

**Expected**: Should allow user_id=1 in results

**ClickGraph Current**: ✅ No cross-MATCH filters (correct)

---

### Test 9: Unbounded Variable-Length (*1..)

**Query**:
```cypher
MATCH (a:User)-[:FOLLOWS*1..]->(c:User)
WHERE a.user_id = 1
RETURN a.user_id, c.user_id
ORDER BY c.user_id
LIMIT 10
```

**Question**: 
- Does Neo4j prevent infinite loops?
- What's the default max depth?
- Does it allow returning to start node?

**ClickGraph Current**: Uses recursive CTE with configurable max depth (100)

---

### Test 10: Relationship Uniqueness

**Query**:
```cypher
MATCH (a:User)-[r1:FOLLOWS]-(b:User)-[r2:FOLLOWS]-(c:User)
WHERE a.user_id = 1 AND r1 = r2
RETURN a.user_id, b.user_id, c.user_id
```

**Question**: Should this return empty? (r1 and r2 must be different)

**Expected**: Empty results - relationship uniqueness guaranteed

**ClickGraph Current**: ✅ Each relationship JOIN uses different table alias

---

## Testing Procedure

### Option A: Neo4j Browser
1. Open http://localhost:7474
2. Run each query manually
3. Document results in spreadsheet

### Option B: Python Script
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", 
                              auth=("neo4j", "testpassword"))

def test_query(tx, query):
    result = tx.run(query)
    return [record.data() for record in result]

with driver.session() as session:
    results = session.execute_read(test_query, "MATCH (a)-[:FOLLOWS*2]->(c) RETURN a, c")
    print(results)
```

### Option C: Cypher-Shell
```bash
cypher-shell -u neo4j -p testpassword < test_queries.cypher
```

---

## Results Template

| Test | Query Pattern | Neo4j Result | ClickGraph Current | Match? | Notes |
|------|--------------|--------------|-------------------|--------|-------|
| 1 | Directed *2 cycle | ? | Prevents | ? | |
| 2 | Explicit 2-hop cycle | ? | Allows | ? | |
| 3 | Undirected 1-hop | ? | Prevents | ? | |
| 4 | Undirected 2-hop (FOF) | ? | Partial | ❌ | Need fix |
| 5 | Undirected *2 | ? | Prevents | ? | |
| 6 | Mixed direction | ? | No filter | ? | |
| 7 | Named intermediates | ? | Partial | ? | |
| 8 | Multi-MATCH | ? | Allows | ✅ | Correct |
| 9 | Unbounded *1.. | ? | Max depth | ? | |
| 10 | Rel uniqueness | Empty | Different aliases | ✅ | Correct |

---

## Decision Matrix

Based on test results, decide for each case:

### If Neo4j prevents cycles in directed *2:
- ✅ Keep ClickGraph current behavior
- 📝 Document as Neo4j-compatible

### If Neo4j allows cycles in directed *2:
- ❌ Remove cycle prevention filter
- 📝 Let users add manually: `WHERE a <> c`
- 💡 Or: Add config option `prevent_cycles: bool`

### If Neo4j prevents cycles in explicit 2-hop:
- ✅ Add filter to ClickGraph
- 📝 Document as Neo4j-compatible

### If Neo4j allows cycles in explicit 2-hop:
- ✅ Keep ClickGraph current behavior (no filter)
- 📝 Document as Neo4j-compatible

### For undirected patterns:
- ✅ Fix ClickGraph to match Neo4j exactly
- Focus on friends-of-friends case (Test 4)
- Ensure `user != fof` filter propagates correctly

---

## Implementation Priorities (After Testing)

1. **Highest**: Fix undirected friends-of-friends (Test 4) - OpenCypher spec violation
2. **High**: Match Neo4j behavior for variable-length (Tests 1, 5, 9)
3. **Medium**: Match Neo4j behavior for explicit patterns (Tests 2, 6, 7)
4. **Low**: Document where ClickGraph differs (if any)

---

## Configuration Options (Future)

Consider adding:
```yaml
# Schema config
query_semantics:
  prevent_cycles_in_variable_length: true  # Default: match Neo4j
  prevent_cycles_in_explicit_patterns: false  # Default: match Neo4j
  enforce_node_uniqueness_undirected: true  # Default: true (spec)
  max_recursion_depth: 100  # Already implemented ✅
```

---

## Expected Timeline

- **Setup**: 30 min
- **Run all tests**: 1-2 hours
- **Document results**: 30 min
- **Implement fixes**: 2-4 hours (depending on findings)

**Total**: 4-7 hours for complete Neo4j compatibility verification

---

## Open Questions

1. Does Neo4j use a different strategy for directed vs undirected cycle prevention?
2. Are there performance implications to always preventing cycles?
3. Should cycle prevention be user-configurable or always match Neo4j?
4. What's Neo4j's default max recursion depth for *1..?
5. Do named intermediate nodes enforce full node uniqueness (O(N²) filters)?
