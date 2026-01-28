# Relationship Variable Return Bug

**Status**: Root cause identified (Dec 21, 2025)  
**Impact**: ~200 matrix test failures  
**Severity**: High - blocks major testing scenarios

## Problem

Cypher queries that return relationship variables fail with ClickHouse errors:

```cypher
MATCH (a:Object)-[r:PARENT]->(b) RETURN a, r, b LIMIT 10
```

**Error**: `Duplicate aliases r` or `Unknown expression identifier 'r'`

## Root Cause

The SQL generator produces:
```sql
SELECT 
      a.object_id AS "a_object_id",
      a.name AS "a_name",
      r AS "r",  -- ❌ INVALID! Can't SELECT entire table alias
      b.object_id AS "b_object_id"
FROM test_integration.fs_objects AS a
INNER JOIN test_integration.fs_parent AS r ON r.child_id = a.object_id
INNER JOIN test_integration.fs_objects AS b ON b.object_id = r.parent_id
```

**ClickHouse Limitation**: You cannot `SELECT tablealias` in ClickHouse. You must select specific columns.

## What Works vs What Fails

### ✅ Works: Queries WITHOUT relationship variable return
```cypher
MATCH (a)-[r:PARENT]->(b) RETURN a.name, b.name  
-- SQL: SELECT a.name, b.name FROM ... (no 'r' selected)
```

### ❌ Fails: Queries WITH relationship variable return
```cypher
MATCH (a)-[r:PARENT]->(b) RETURN a, r, b
MATCH (a)-[r:PARENT]->(b) RETURN r
MATCH (a)-[r:PARENT]->(b) RETURN r.child_id
-- All try to SELECT from 'r' alias
```

## Solution Required

When returning a relationship variable, expand it to its columns:

```sql
-- Instead of: r AS "r"
-- Generate:
r.child_id AS "r_child_id",
r.parent_id AS "r_parent_id",
r.follow_date AS "r_follow_date"  -- if relationship has properties
```

## Implementation Location

Need to fix in `clickhouse_query_generator/`:
1. Identify when a `RETURN` item is a relationship variable
2. Look up the relationship's columns from schema
3. Expand to `rel_alias.col1, rel_alias.col2, ...`
4. Format output as nested map or tuple in results

## Test Scenarios Affected

- Matrix tests: `test_simple_edge` (~40 failures across schemas)
- Matrix tests: Some `test_filtered_edge` cases
- Any integration test returning relationship data
- Variable-length path tests returning relationship arrays

## Verification

After fix, these should all work:
```cypher
MATCH (a)-[r:FOLLOWS]->(b) RETURN r
MATCH (a)-[r:FOLLOWS]->(b) RETURN r.follow_date
MATCH (a)-[r:FOLLOWS]->(b) RETURN a, r, b
MATCH (a)-[*1..3]->(b) RETURN relationships(p)
```

## Related Issues

- Property access on relationships (r.property_name) likely also affected
- Aggregations over relationships: `collect(r)`, `count(r)`
- Path functions: `relationships(p)` in variable-length paths
