# Future Features - Commented Examples Tracker

This document tracks examples that are commented out in the wiki documentation until features are implemented.

## How to Use

Examples for unsupported features are wrapped in HTML comments like this:

```markdown
<!-- 
⚠️ FUTURE FEATURE - Brief description

Explanation of why it's not supported and what's needed to implement.

```cypher
-- Example query that doesn't work yet
MATCH (n) RETURN n
```

Optional: Link to GitHub issue
-->
```

These examples:
- ✅ Don't appear in rendered markdown (GitHub wiki)
- ✅ Don't get tested by validation scripts
- ✅ Are preserved in source for future reference
- ✅ Easy to uncomment when feature is ready

## Tracking Future Features

### 1. Labelless Node Matching

**Status**: 🔴 Not Supported  
**Location**: `Cypher-Basic-Patterns.md` - "Match All Nodes" section  
**Syntax**: `MATCH (n) RETURN n`  

**Why**: Architectural limitation - columnar storage requires knowing node type upfront.

**What's Needed**:
- UNION ALL across all node types, OR
- Type inference system, OR
- Metadata table approach

**Workaround**: Use labeled nodes: `MATCH (u:User)` or `MATCH (p:Post)`

---

### 2. ~~Inline Property Filters~~ ✅ IMPLEMENTED

**Status**: ✅ Supported (as of v0.5.2)  
**Syntax**: `MATCH (u:User {name: 'Alice'}) RETURN u`  

Both node and relationship inline property filters are now fully supported!

---

### 3. ~~Inline Relationship Property Filters~~ ✅ IMPLEMENTED

**Status**: ✅ Supported (as of v0.5.2)  
**Syntax**: `MATCH (a)-[:FOLLOWS {since: '2024-01-01'}]->(b) RETURN a, b`  

Both single and multiple inline properties work on relationships.

---

### 4. List Comprehensions

**Status**: 🔴 Not Supported  
**Location**: Various (not yet documented)  
**Syntax**: `[node IN nodes(path) | node.name]`  

**Why**: Complex expression type not yet implemented.

**What's Needed**:
- Parser support for list comprehension syntax
- AST representation
- SQL generation (likely using array functions)

**Workaround**: Use explicit path traversal and collect results

---

## Process for Uncommenting Examples

When a feature is implemented:

1. **Verify it works**
   ```bash
   # Test the query manually
   curl -X POST http://localhost:8080/query \
     -d '{"query": "MATCH (n) RETURN n LIMIT 1"}'
   ```

2. **Add integration tests**
   - Add test case to appropriate test file
   - Ensure it passes consistently

3. **Uncomment in documentation**
   - Remove HTML comment wrapper
   - Update status in this tracker
   - Move from "Future Feature" to regular example

4. **Update validation**
   - Re-run validation script
   - Verify success rate improved

5. **Update CHANGELOG.md**
   - Document the new feature
   - Include examples

## Validation Status

Current state of commented examples:

| Feature | Examples Hidden | Tests Skipped | Reason |
|---------|----------------|---------------|---------|
| Labelless nodes | 1 | N/A | Architecture |
| Inline properties | 2 | N/A | Parser |
| Inline rel props | 1 | N/A | Parser |
| List comprehensions | 0 | N/A | Not documented yet |

**Total queries commented out**: ~4 examples

**Impact on validation**:
- Before: 211 queries, 0% success (includes non-working examples)
- After commenting: ~207 queries, higher success rate expected

## Future Enhancements

Patterns we might want to support in the future:

- **Path comprehensions**: `[p = (a)-[*]->(b) | length(p)]`
- **Pattern comprehensions**: `[(a)-[:KNOWS]->(b) | b.name]`
- **Map projections**: `person{.name, .age}`
- **CASE in pattern matching**: Complex conditional patterns
- **Subqueries**: `CALL { MATCH ... RETURN ... }`

## References

- OpenCypher specification: https://opencypher.org/
- Neo4j Cypher manual: https://neo4j.com/docs/cypher-manual/
- ClickGraph issues: https://github.com/genezhang/clickgraph/issues
