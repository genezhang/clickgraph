# Known Issues

**Active Issues**: 3  
**Test Results**: 542/542 unit tests passing (100%)  
**Last Updated**: December 4, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Active Issues

### 1. Anonymous Nodes Without Labels Not Supported

**Status**: 📋 Limitation  
**Severity**: LOW  
**Identified**: December 2, 2025

**Problem**: Anonymous nodes without labels cannot be resolved to tables:
```cypher
MATCH ()-[r:FOLLOWS]->() RETURN r LIMIT 5  -- ❌ Broken SQL
MATCH ()-[r]->() RETURN r LIMIT 5          -- ❌ Also broken
```

**Root Cause**: Without a label, the query planner cannot determine which node table to use. The anonymous node gets a generated alias (e.g., `aeba9f1d7f`) but no `table_name`, causing invalid SQL with dangling references.

**Workaround**: Always specify node labels:
```cypher
MATCH (:User)-[r:FOLLOWS]->(:User) RETURN r LIMIT 5  -- ✅ Works
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r LIMIT 5  -- ✅ Works
```

**Future Enhancement**: For schemas with a single relationship type or polymorphic edge table, the system could infer node types from the relationship's `from_node_label`/`to_node_label` configuration. Deferred for now.

---

### 2. Disconnected Patterns Generate Invalid SQL

**Status**: 🐛 Bug  
**Severity**: MEDIUM  
**Identified**: November 20, 2025

**Problem**: Comma-separated patterns without shared nodes generate invalid SQL:
```cypher
MATCH (user:User), (other:User) WHERE user.user_id = 1 RETURN other.user_id
```

**Current**: Generates SQL referencing `user` not in FROM clause → ClickHouse error  
**Expected**: Either throw `DisconnectedPatternFound` error OR generate CROSS JOIN

**Location**: `src/query_planner/logical_plan/match_clause.rs` - disconnection check not triggering

---

### 3. ✅ FIXED: Variable-Length Paths + Chained Patterns Missing Property JOINs

**Status**: ✅ Fixed (December 4, 2025)  
**Severity**: HIGH  
**Identified**: December 4, 2025

**Problem**: When a variable-length path (VLP) is chained with additional graph patterns, the generated SQL failed because the start/end nodes of the VLP were not JOINed back to fetch their properties.

**Fix Applied**:
1. `src/render_plan/plan_builder.rs` (lines ~5257-5277): Added logic to preserve subsequent pattern JOINs before clearing, then re-add them after VLP endpoint JOINs
2. `src/render_plan/cte_extraction.rs`: Fixed nested GraphRel recursion in `has_variable_length_rel()`, `is_variable_length_denormalized()`, and `get_variable_length_denorm_info()` to properly detect VLP when nested inside other patterns

**Example Query Now Working**:
```cypher
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File)
RETURN u.name, g.name AS group_name, f.name AS file_name LIMIT 20
```

**Generated SQL (Now Correct)**:
```sql
WITH RECURSIVE variable_path_xxx AS (...)
SELECT u.name, g.name AS group_name, f.name AS file_name
FROM variable_path_xxx AS t
JOIN brahmand.sec_users AS u ON t.start_id = u.user_id        -- ✅ VLP start node
JOIN brahmand.sec_groups AS g ON t.end_id = g.group_id        -- ✅ VLP end node
INNER JOIN brahmand.sec_permissions AS ... ON ... = g.group_id -- ✅ Subsequent pattern
INNER JOIN brahmand.sec_fs_objects AS f ON ...                 -- ✅ Subsequent pattern
LIMIT 20
```

---

### 4. ✅ FIXED: VLP + Chained Patterns with Aggregation Missing CTE Path

**Status**: ✅ Fixed (December 4, 2025)  
**Severity**: MEDIUM  
**Identified**: December 4, 2025

**Problem**: Aggregation queries combining VLP with chained patterns didn't use the CTE path, resulting in missing table JOINs.

**Fix Applied**: `src/render_plan/cte_extraction.rs` - `get_variable_length_spec()` now recursively checks `rel.left` and `rel.right` in nested GraphRels. This ensures VLP detection works for chained patterns like `(a)-[*]->(b)-[:R]->(c)` where the VLP is nested inside an outer GraphRel.

**Example Query Now Working**:
```cypher
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File) 
RETURN u.name, COUNT(DISTINCT f) AS total_files, SUM(f.sensitive_data) AS sensitive_files
```

**Generated SQL (Now Correct)**:
```sql
WITH RECURSIVE variable_path_xxx AS (...)
SELECT u.name AS "user_name", 
       COUNT(DISTINCT f.fs_id) AS "file_count", 
       SUM(f.sensitive_data) AS "total_sensitive"
FROM variable_path_xxx AS t
JOIN brahmand.sec_users AS u ON t.start_id = u.user_id
JOIN brahmand.sec_groups AS g ON t.end_id = g.group_id
INNER JOIN brahmand.sec_permissions AS ... ON ...
INNER JOIN brahmand.sec_fs_objects AS f ON ...
GROUP BY u.name
```

---

### 5. WHERE Filters on VLP Chained Pattern Endpoints Not Applied

**Status**: 🐛 Bug  
**Severity**: MEDIUM  
**Identified**: December 4, 2025

**Problem**: When using VLP + chained patterns, WHERE clause filters on the chained endpoint node are not applied to the generated SQL.

**Example Query**:
```cypher
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File)
WHERE f.sensitive_data = 1 AND u.exposure = 'external'
RETURN f.name, COUNT(DISTINCT u) AS external_users
```

**Current Behavior**:
- `u.exposure = 'external'` ✅ Applied (pushed into CTE base case)
- `f.sensitive_data = 1` ❌ **Missing** from generated SQL

**Generated SQL** (incorrect):
```sql
WITH RECURSIVE variable_path_xxx AS (
    ...
    WHERE rel.member_type = 'User' AND start_node.exposure = 'external'  -- ✅ User filter applied
    ...
)
SELECT f.name, COUNT(DISTINCT u.user_id)
FROM variable_path_xxx AS t
JOIN sec_users AS u ON t.start_id = u.user_id
JOIN sec_groups AS g ON t.end_id = g.group_id
JOIN sec_permissions AS p ON p.subject_id = g.group_id
JOIN sec_fs_objects AS f ON f.fs_id = p.object_id
GROUP BY f.name
-- ❌ Missing: WHERE f.sensitive_data = 1
```

**Workaround**: Use HAVING with conditional aggregation or filter in application layer:
```cypher
-- Workaround 1: Use SUM with conditional and filter
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File)
WHERE u.exposure = 'external'
RETURN f.name, SUM(f.sensitive_data) AS is_sensitive, COUNT(DISTINCT u) AS external_users

-- Then filter where is_sensitive > 0 in application
```

**Root Cause**: Filter extraction in `extract_filters()` doesn't propagate filters from WHERE clause to chained pattern endpoints when VLP is present.

**Location**: `src/render_plan/plan_builder.rs` - `extract_filters()` or `build_variable_length_cte_plan()`

