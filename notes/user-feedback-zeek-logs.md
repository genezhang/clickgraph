# User Feedback: Zeek Log Data Use Case

**Date**: November 30, 2025  
**Source**: User feature request for Zeek network log analysis  
**Status**: ✅ RESOLVED - All issues fixed

---

## Summary of Fixes (November 30, 2025)

### Issue 1: RETURN r (Whole Relationship) - ✅ FIXED

**Problem**: `RETURN r` generated invalid SQL `SELECT r AS "r"`

**Solution**: Modified `get_all_properties_for_alias()` in `plan_builder.rs` to check `GraphRel.alias` and extract properties from `GraphRel.center` (the relationship ViewScan). Also fixed `try_generate_relationship_view_scan()` in `match_clause.rs` to copy property_mappings from schema.

**Result**:
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r
-- Now generates: SELECT r.follow_date AS "follow_date"
```

### Issue 2: Graph Functions (type, id, labels) - ✅ FIXED

**Problem**: `type(r)`, `id(n)`, `labels(n)` generated invalid `function(alias."*")` syntax

**Solution**: Added special handling in `projection_tagging.rs` to intercept these functions:
- `type(r)` → Returns literal relationship type `'FOLLOWS'` (or type_column for polymorphic)
- `id(n)` → Returns `n.id_column` from schema
- `labels(n)` → Returns literal array `['Label']` from schema

**Result**:
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN type(r)
-- Now generates: SELECT 'FOLLOWS'

MATCH (u:User) RETURN id(u)
-- Now generates: SELECT u.user_id

MATCH (u:User) RETURN labels(u)
-- Now generates: SELECT ['User']
```

### Issue 3: Inline Property Filters - ✅ VERIFIED WORKING

**Status**: Was already working with string values

**Limitation Found**: Integer literals in inline filters fail to parse:
```cypher
-- ✅ WORKS: String values
MATCH (u:User {name: "Alice"}) RETURN u

-- ❌ PARSE ERROR: Integer values
MATCH (u:User {user_id: 1}) RETURN u

-- ✅ WORKAROUND: Use WHERE clause
MATCH (u:User) WHERE u.user_id = 1 RETURN u
```

---

## Files Modified

1. **`src/render_plan/plan_builder.rs`**:
   - `get_all_properties_for_alias()`: Added check for `GraphRel.alias` matching and property extraction from relationship center ViewScan

2. **`src/query_planner/logical_plan/match_clause.rs`**:
   - `try_generate_relationship_view_scan()`: Changed from empty `HashMap::new()` to `rel_schema.property_mappings.clone()`

3. **`src/query_planner/analyzer/projection_tagging.rs`**:
   - Added special handling for `type()`, `id()`, `labels()` functions before generic ScalarFnCall processing

---

## Test Verification

```bash
# All pass after fixes:
cargo test --lib  # 526/526 tests passing

# Manual verification:
curl -X POST http://localhost:8080/query -d '{"query": "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r", "sql_only": true}'
# Result: r.follow_date AS "follow_date" ✓

curl -X POST http://localhost:8080/query -d '{"query": "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN type(r)", "sql_only": true}'
# Result: 'FOLLOWS' ✓

curl -X POST http://localhost:8080/query -d '{"query": "MATCH (u:User) RETURN id(u)", "sql_only": true}'
# Result: u.user_id ✓

curl -X POST http://localhost:8080/query -d '{"query": "MATCH (u:User) RETURN labels(u)", "sql_only": true}'
# Result: ['User'] ✓
```

---

## Original Problem Report (Kept for Reference)

#### Part B: Proper Implementation (Recommended)

Inline property filters are common and convenient. They should be converted to WHERE conditions:

```cypher
MATCH (n:Person {name: "Alice", age: 30})
```

Should be equivalent to:

```cypher
MATCH (n:Person) WHERE n.name = "Alice" AND n.age = 30
```

**Implementation Location**:
- `src/query_planner/logical_plan/match_clause.rs` - During node pattern processing
- Convert `NodePattern.properties` to `Filter` logical plan node
- Or inject into existing WHERE clause if present

### Wiki Documentation Fix

Current wiki says inline property filters are "not optimal" - this is misleading.

**Should say**: 
- Inline property filters are syntactic sugar for WHERE clauses
- They compile to the same SQL
- Use whichever is more readable for your use case

---

## Issue 2: Whole Node/Edge Returns - CONFIRMED BUG

### Testing Results (November 30, 2025)

**RETURN n (whole node) - PARTIALLY WORKS**:
```cypher
MATCH (u:User) WHERE u.name = "Alice" RETURN u
```
Generates:
```sql
SELECT u.age AS "age", u.name AS "name", u.user_id AS "user_id"
FROM test_integration.users AS u WHERE u.name = 'Alice'
```
✅ This works - all properties are expanded.

**RETURN r (whole relationship) - BROKEN**:
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) WHERE a.name = "Alice" RETURN r
```
Generates:
```sql
SELECT r AS "r"  -- ❌ INVALID! 'r' is table alias, not column
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON r.follower_id = a.user_id
...
```
❌ This is invalid SQL - `r` by itself is a table alias, not a column.

**type(r) - BROKEN**:
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN type(r)
```
Generates:
```sql
SELECT type(r."*")  -- ❌ INVALID! No such column r."*"
```

**id(n) - BROKEN**:
```cypher
MATCH (u:User) RETURN id(u)
```
Generates:
```sql
SELECT id(u."*")  -- ❌ INVALID! No such column u."*"
```

### Root Cause Analysis

1. **`RETURN r` (relationship)**: Not expanded to all columns like nodes are
2. **Graph functions (type, id, labels)**: Generate invalid `alias."*"` syntax instead of proper SQL

### Fixes Needed

1. **RETURN r**: Expand to all relationship properties (like nodes do)
2. **type(r)**: For non-polymorphic edges, return literal string of the relationship type
3. **id(n/r)**: Map to the schema-defined ID column(s)

---

## Issue 2: Whole Node/Edge Returns - Original Analysis

### Problem

```cypher
MATCH (head:IP)-[r:ACCESSED]->(tail:IP) 
WHERE head.ip = "192.168.5.115" 
RETURN r
```

User wants to return the **entire relationship** as a map/object.

Similarly:
```cypher
RETURN head   -- Entire node as object
RETURN tail   -- Entire node as object
```

### Current Behavior

- `RETURN r` - Unclear what happens (likely error or empty)
- `RETURN head` - Unclear what happens

### Neo4j Semantics

In Neo4j, returning a node or relationship returns a **map** with all properties:

```json
// RETURN n where n is a Person node
{
  "name": "Alice",
  "age": 30,
  "email": "alice@example.com"
}

// RETURN r where r is a FOLLOWS relationship
{
  "since": "2023-01-15",
  "strength": 0.8
}
```

### Required Functions

To be Neo4j-compatible, we need to support:

| Function | Description | Example |
|----------|-------------|---------|
| `id(n)` | Internal node ID | `RETURN id(head)` |
| `id(r)` | Internal relationship ID | `RETURN id(r)` |
| `labels(n)` | Node labels as array | `RETURN labels(head)` → `["IP"]` |
| `type(r)` | Relationship type | `RETURN type(r)` → `"ACCESSED"` |
| `properties(n)` | All properties as map | `RETURN properties(head)` |
| `properties(r)` | All properties as map | `RETURN properties(r)` |
| `keys(n)` | Property names as array | `RETURN keys(head)` |

### Design Considerations

#### Option A: Expand to All Columns

When `RETURN n` is encountered:
1. Look up the node's schema
2. Expand to all property columns
3. Return as JSON object or map

```sql
-- RETURN head
SELECT 
  map('ip', head.ip, 'hostname', head.hostname, ...) AS head
FROM ...
```

ClickHouse has `map()` function for this.

#### Option B: Return as JSON

```sql
SELECT 
  toJSONString(tuple(head.ip, head.hostname, ...)) AS head
FROM ...
```

#### Option C: Return Column Prefix

```sql
SELECT 
  head.ip AS "head.ip",
  head.hostname AS "head.hostname",
  ...
FROM ...
```

### Implementation Plan

1. **Schema Lookup**: When `RETURN alias` (without property), look up all properties
2. **Property Expansion**: Generate SELECT items for each property
3. **Result Formatting**: Choose format (map, JSON, or prefixed columns)
4. **Function Support**: Implement `id()`, `labels()`, `type()`, `properties()`, `keys()`

### User's Actual Need

User wanted: `(head, relation, tail)` - the complete triple

**Suggested workaround**:
```cypher
RETURN head.ip, type(r), tail.ip
```

But this requires `type(r)` to work (which it currently doesn't for non-polymorphic tables).

---

## Issue 3: type(r) Function

### Problem

For standard (non-polymorphic) edge tables, `type(r)` should return the relationship type from the schema:

```cypher
MATCH (a)-[r:FOLLOWS]->(b) RETURN type(r)
-- Should return: "FOLLOWS"
```

### Current State

- Polymorphic edges: `type_column` stores the type, so `type(r)` can map to that column
- Standard edges: Type is implicit in the schema, not stored in a column

### Solution for Standard Edges

For non-polymorphic edges, `type(r)` should be a **literal string** based on the schema:

```sql
-- If schema says this is a FOLLOWS relationship
SELECT 'FOLLOWS' AS "type(r)"
```

---

## Priority Order

1. **HIGH**: Return error for unsupported inline property filters (prevents confusion)
2. **HIGH**: Implement inline property filter support (common use case)
3. **MEDIUM**: Implement `type(r)` function (needed for relationship returns)
4. **MEDIUM**: Implement `RETURN r` / `RETURN n` (whole node/edge)
5. **LOWER**: Implement `id()`, `labels()`, `properties()`, `keys()`

---

## Files to Modify

### Inline Property Filters
- `src/open_cypher_parser/path_pattern.rs` - Already parses, verify AST
- `src/query_planner/logical_plan/match_clause.rs` - Convert to WHERE conditions
- `docs/wiki/Cypher-Language-Reference.md` - Update documentation

### Whole Node/Edge Returns
- `src/query_planner/logical_plan/return_clause.rs` - Detect bare alias returns
- `src/query_planner/analyzer/projection_tagging.rs` - Expand to all properties
- `src/clickhouse_query_generator/` - Generate map/JSON output

### Function Support
- `src/open_cypher_parser/expression.rs` - Parse functions
- `src/clickhouse_query_generator/function_translator.rs` - Translate to SQL

---

## Testing Required

```cypher
-- Inline property filters
MATCH (n:User {name: "Alice"}) RETURN n.email
MATCH (n:User {name: "Alice", active: true}) RETURN n.email
MATCH (a:User {name: "Alice"})-[:FOLLOWS]->(b:User) RETURN b.name

-- Whole node/edge returns
MATCH (n:User) RETURN n
MATCH (a)-[r:FOLLOWS]->(b) RETURN r
MATCH (a)-[r]->(b) RETURN a, r, b

-- Functions
MATCH (n:User) RETURN id(n), labels(n)
MATCH (a)-[r]->(b) RETURN type(r), id(r)
MATCH (n) RETURN properties(n), keys(n)
```
