# UNWIND Specification for ClickGraph

**Status**: Draft  
**Date**: November 28, 2025  
**Purpose**: Map Cypher UNWIND to ClickHouse ARRAY JOIN

## Standards Comparison

| Standard | Syntax | Notes |
|----------|--------|-------|
| **OpenCypher** | `UNWIND list AS x` | Neo4j, ClickGraph |
| **ISO GQL** (Spanner) | `FOR x IN array_expression` | ISO standard |
| **ClickHouse** | `ARRAY JOIN arr AS x` | Target SQL |

**Decision**: ClickGraph uses **OpenCypher `UNWIND` syntax** for Neo4j compatibility. 
Future enhancement could add `FOR...IN` as GQL alias.

## Overview

UNWIND transforms array/list values into individual rows. This is essential for:
1. Zeek DNS logs with `answers` array columns
2. Any denormalized data with array-valued properties
3. Graph patterns where relationships are stored in arrays

## Cypher UNWIND Syntax

```cypher
UNWIND <expression> AS <variable>
```

### Examples

```cypher
-- Literal list
UNWIND [1, 2, 3] AS x RETURN x

-- Property that is an array
MATCH (d:Domain)-[r:RESOLVED_TO]->(ip:ResolvedIP)
UNWIND r.answers AS answer
RETURN d.name, answer

-- With filtering
MATCH (d:Domain)
UNWIND d.resolved_ips AS ip
WHERE ip STARTS WITH '192.168.'
RETURN d.name, ip
```

## ClickHouse ARRAY JOIN Mapping

### Syntax Comparison

| Cypher | ClickHouse |
|--------|------------|
| `UNWIND arr AS x` | `ARRAY JOIN arr AS x` |
| `UNWIND arr AS x` (keep empty) | `LEFT ARRAY JOIN arr AS x` |

### Key Differences

1. **Position**: ARRAY JOIN goes after FROM, before WHERE
2. **Empty arrays**: 
   - `ARRAY JOIN` excludes rows with empty arrays (like INNER JOIN)
   - `LEFT ARRAY JOIN` keeps rows, sets default value (like LEFT JOIN)
3. **Alias required**: Both require an alias for the unwound element

## Translation Rules

### Rule 1: Basic UNWIND → ARRAY JOIN

```cypher
MATCH (n:Node)
UNWIND n.items AS item
RETURN n.id, item
```

Translates to:

```sql
SELECT n.id, item
FROM nodes AS n
ARRAY JOIN n.items AS item
```

### Rule 2: UNWIND with WHERE → ARRAY JOIN + WHERE

```cypher
MATCH (n:Node)
UNWIND n.items AS item
WHERE item > 10
RETURN n.id, item
```

Translates to:

```sql
SELECT n.id, item
FROM nodes AS n
ARRAY JOIN n.items AS item
WHERE item > 10
```

### Rule 3: Multiple UNWINDs → Multiple ARRAY JOINs

```cypher
MATCH (n:Node)
UNWIND n.list1 AS x
UNWIND n.list2 AS y
RETURN x, y
```

Translates to:

```sql
SELECT x, y
FROM nodes AS n
ARRAY JOIN n.list1 AS x, n.list2 AS y
-- Note: This is a "zip" join, not cartesian product!
-- For cartesian product, would need subqueries
```

**Important**: ClickHouse ARRAY JOIN with multiple arrays zips them (requires same length), while Cypher UNWIND chains create cartesian products. We may need to handle this differently.

### Rule 4: UNWIND on Literal List

```cypher
UNWIND [1, 2, 3] AS x
RETURN x
```

Translates to:

```sql
SELECT x
FROM (SELECT 1) -- dummy row
ARRAY JOIN [1, 2, 3] AS x
```

Or using `arrayJoin` function:

```sql
SELECT arrayJoin([1, 2, 3]) AS x
```

### Rule 5: UNWIND Preserving Empty (COALESCE pattern)

```cypher
-- Neo4j: UNWIND coalesce(list, [null]) AS x
MATCH (n:Node)
UNWIND coalesce(n.items, [null]) AS item
RETURN n.id, item
```

Translates to:

```sql
SELECT n.id, item
FROM nodes AS n
LEFT ARRAY JOIN n.items AS item
```

## Zeek DNS Log Use Case

### Schema (No special array handling needed)

```yaml
edges:
  - type: RESOLVED_TO
    table: dns_log
    from_id: query
    to_id: answers  # This is an Array(String) column
    from_node: Domain
    to_node: ResolvedIP
```

### Query Example

```cypher
-- Find all IPs that a domain resolved to
MATCH (d:Domain)-[r:RESOLVED_TO]->(ip:ResolvedIP)
WHERE d.name = 'testmyids.com'
UNWIND r.answers AS resolved_ip
RETURN d.name, resolved_ip
```

Generates:

```sql
SELECT d.query AS "d.name", resolved_ip
FROM zeek.dns_log AS d
ARRAY JOIN d.answers AS resolved_ip
WHERE d.query = 'testmyids.com'
```

### Without UNWIND (Returns Array)

```cypher
MATCH (d:Domain)-[r:RESOLVED_TO]->(ip:ResolvedIP)
WHERE d.name = 'testmyids.com'
RETURN d.name, r.answers
```

Generates:

```sql
SELECT d.query AS "d.name", d.answers AS "r.answers"
FROM zeek.dns_log AS d
WHERE d.query = 'testmyids.com'
```

## Implementation Plan

### MVP Scope (v1)

**In Scope:**
- Single UNWIND per query
- UNWIND on property expressions (`r.answers`, `n.items`)
- UNWIND on literal arrays (`[1, 2, 3]`)
- ARRAY JOIN generation (excludes empty arrays - matches Neo4j semantics)
- WHERE filtering after UNWIND

**Deferred:**
- Chained UNWIND (cartesian product semantics)
- UNWIND with WITH clause (intermediate materialization)
- LEFT ARRAY JOIN (use `coalesce(arr, [null])` pattern instead)

### Phase 1: Parser Changes

1. Add UNWIND to AST:
```rust
pub struct Unwind {
    pub expression: Box<LogicalExpr>,
    pub alias: String,
}

// Add to QueryPart enum
QueryPart::Unwind(Unwind)
```

2. Parse UNWIND clause:
```
UNWIND <expr> AS <identifier>
```

### Phase 2: Logical Plan

Add `UnwindPlan` to LogicalPlan:
```rust
pub struct UnwindPlan {
    pub input: Arc<LogicalPlan>,
    pub expression: LogicalExpr,
    pub alias: String,
}
```

### Phase 3: SQL Generation

1. Detect UNWIND in plan
2. Generate ARRAY JOIN clause in SQL
3. Handle alias properly in subsequent clauses

### Phase 4: Edge Cases

1. **Empty arrays**: Default to `ARRAY JOIN` (exclude empty). Add `LEFT` variant if needed.
2. **Chained UNWINDs**: May need subqueries for correct cartesian product semantics
3. **UNWIND in WITH**: Handle intermediate result materialization
4. **NULL handling**: `UNWIND null` returns no rows (matches Neo4j)

## Open Questions

### Resolved

1. **Should we support LEFT ARRAY JOIN?** 
   - **Decision**: Not initially. Use `UNWIND coalesce(list, [null]) AS x` pattern.
   - GQL `FOR` statement also excludes empty/NULL arrays (no OPTIONAL variant found)
   - Can add `OPTIONAL UNWIND` later if needed, but it's non-standard

2. **UNWIND on non-array property?**
   - **Decision**: Error at compile time (type checking)
   - ClickHouse has strong typing, so array columns are known from schema
   - Don't need `isArray()` runtime checks

### Still Open

1. **Cartesian product for chained UNWINDs?**
   - Neo4j creates cartesian product
   - ClickHouse ARRAY JOIN zips (requires same-length arrays)
   - May need: `CROSS JOIN (SELECT arrayJoin(...))` for true Neo4j semantics
   - **For MVP**: Support single UNWIND only, defer chained UNWINDs

## References

- [Neo4j UNWIND](https://neo4j.com/docs/cypher-manual/current/clauses/unwind/)
- [ClickHouse ARRAY JOIN](https://clickhouse.com/docs/en/sql-reference/statements/select/array-join)
- [OpenCypher UNWIND spec](https://opencypher.org/resources/)
- [Spanner GQL FOR statement](https://cloud.google.com/spanner/docs/reference/standard-sql/graph-for-statement) (ISO GQL equivalent)
