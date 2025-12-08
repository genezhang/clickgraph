# Known Issues

**Active Issues**: 2  
**Last Updated**: December 7, 2025

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Active Issues

### 1. CTE Column Aliasing for Mixed RETURN (WITH alias + node property)

**Status**: 🔴 Active  
**Severity**: MEDIUM

**Symptom**: When RETURN references both WITH aliases AND node properties, the JOIN condition may use incorrect column names.

**Example**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Root Cause**: CTE column aliases include the table prefix (e.g., `"a.age"`) but the outer query JOIN tries to reference `grouped_data.age` (without prefix).

**Workaround**: For queries that only need WITH aliases in RETURN (no additional node properties), the optimization correctly skips the JOIN and selects directly from CTE. Ensure RETURN only references WITH clause output:
```cypher
-- ✅ Works: RETURN only references WITH output
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a.name as name, COUNT(b) as follows
WHERE follows > 1
RETURN name, follows
```

---

### 2. Anonymous Nodes Without Labels (Partial Support)

**Status**: 🟡 Partial Support  
**Severity**: LOW

**What Works** ✅:
- Label inference from relationship type: `()-[r:FLIGHT]->()` infers Airport
- Relationship type inference from typed nodes: `(a:Airport)-[r]->()` infers r:FLIGHT  
- Single-schema inference: `()-[r]->()` when only one relationship defined
- Single-node-schema inference: `MATCH (n) RETURN n` when only one node type
- Multi-hop anonymous patterns with single relationship type

**Limitations**:
- `MATCH (n)` with multiple node types requires explicit label
- Safety limit: max 4 types inferred before requiring explicit specification

**Workaround**: Specify at least one label when multiple types exist:
```cypher
MATCH (a:User)-[r]->(b:User) RETURN r  -- ✅ Works
```

---

## Test Statistics

| Category | Passing | Total | Rate |
|----------|---------|-------|------|
| Unit Tests | 596 | 596 | 100% |
| Integration (social_benchmark) | 391 | 391 | 100% |
| Integration (security_graph) | 391 | 391 | 100% |
| **Total** | **1,378** | **1,378** | **100%** |
