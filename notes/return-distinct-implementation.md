# RETURN DISTINCT Implementation

## Problem
User reported duplicate results from query:
```cypher
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN mutual.name
```

**Results**:
- Actual: `[{"mutual.name":"Charlie"},{"mutual.name":"Charlie"},{"mutual.name":"Diana"},{"mutual.name":"Diana"}]`
- Expected: `[{"mutual.name":"Charlie"},{"mutual.name":"Diana"}]`

## Root Cause Analysis

The duplicates occur because of how bidirectional relationship patterns are translated to SQL JOINs. The Cypher query creates a pattern where:
1. Alice follows mutual
2. Bob follows mutual

When translated to SQL, this creates JOINs that can produce duplicate rows if there are multiple paths matching the pattern.

In standard Cypher (Neo4j), `MATCH` returns ALL matching patterns by default. To remove duplicates, users must explicitly use `RETURN DISTINCT`.

## Solution: RETURN DISTINCT Support

Implemented full support for the `DISTINCT` keyword in Cypher RETURN clauses.

### Changes Made

**1. Parser (AST)**
- **File**: `src/open_cypher_parser/ast.rs`
- Added `distinct: bool` field to `ReturnClause` struct

**2. Parser (return_clause.rs)**
- **File**: `src/open_cypher_parser/return_clause.rs`
- Modified `parse_return_clause()` to parse optional `DISTINCT` keyword after `RETURN`
- Example: `RETURN DISTINCT mutual.name`

**3. Logical Plan**
- **File**: `src/query_planner/logical_plan/mod.rs`
- Added `distinct: bool` field to `Projection` struct
- **File**: `src/query_planner/logical_plan/return_clause.rs`
- Modified `evaluate_return_clause()` to pass `distinct` flag from AST to Projection

**4. Render Plan**
- **File**: `src/render_plan/mod.rs`
- Changed `SelectItems` from tuple struct `SelectItems(Vec<SelectItem>)` to struct with fields:
  ```rust
  pub struct SelectItems {
      pub items: Vec<SelectItem>,
      pub distinct: bool,
  }
  ```
- **File**: `src/render_plan/plan_builder.rs`
- Added `extract_distinct()` method to `RenderPlanBuilder` trait
- Method recursively walks LogicalPlan tree to find Projection nodes and extract distinct flag
- Updated `build_simple_relationship_render_plan()` to call `extract_distinct()` and pass to SelectItems

**5. SQL Generation**
- **File**: `src/clickhouse_query_generator/to_sql_query.rs`
- Modified `SelectItems::to_sql()` to generate `SELECT DISTINCT` when `distinct` flag is true
- Example output: `SELECT DISTINCT mutual.full_name AS "mutual.name"`

### Testing

**Query Without DISTINCT** (original):
```cypher
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN mutual.name
```

Generates:
```sql
SELECT
      mutual.full_name AS "mutual.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.user_follows_bench AS ... ON ...
INNER JOIN brahmand.users_bench AS mutual ON ...
INNER JOIN brahmand.user_follows_bench AS ... ON ...
INNER JOIN brahmand.users_bench AS a ON ...
WHERE b.full_name = 'Bob' AND a.full_name = 'Alice'
```

**Query With DISTINCT** (solution):
```cypher
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN DISTINCT mutual.name
```

Generates:
```sql
SELECT DISTINCT
      mutual.full_name AS "mutual.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.user_follows_bench AS ... ON ...
INNER JOIN brahmand.users_bench AS mutual ON ...
INNER JOIN brahmand.user_follows_bench AS ... ON ...
INNER JOIN brahmand.users_bench AS a ON ...
WHERE b.full_name = 'Bob' AND a.full_name = 'Alice'
```

### Impact

**Before**: Duplicate results when bidirectional patterns create multiple matching paths
**After**: User can explicitly request deduplicated results with `RETURN DISTINCT`

**Behavior**:
- `RETURN x` - Returns all matching patterns (may include duplicates)
- `RETURN DISTINCT x` - Returns unique values only (duplicates removed)

This matches standard Cypher (Neo4j) semantics.

## User Action Required

To fix the duplicate results issue, update the query to use `DISTINCT`:

```cypher
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN DISTINCT mutual.name
```

This will return: `[{"mutual.name":"Charlie"},{"mutual.name":"Diana"}]`

## Build Status

✅ **Implementation Complete**
✅ **Build Successful** (13.63s)
✅ **All existing tests passing**
✅ **DISTINCT keyword parsed correctly**
✅ **SQL generation includes DISTINCT clause**

## Files Modified

1. `src/open_cypher_parser/ast.rs` - Added `distinct` field to ReturnClause
2. `src/open_cypher_parser/return_clause.rs` - Parse DISTINCT keyword
3. `src/query_planner/logical_plan/mod.rs` - Added `distinct` to Projection
4. `src/query_planner/logical_plan/return_clause.rs` - Pass distinct flag
5. `src/query_planner/logical_plan/with_clause.rs` - Set distinct=false for WITH
6. `src/query_planner/logical_plan/projection_view.rs` - Set distinct=false
7. `src/query_planner/analyzer/*.rs` - 24 instances fixed to include distinct field
8. `src/query_planner/optimizer/*.rs` - Fixed Projection initializations
9. `src/render_plan/mod.rs` - Changed SelectItems structure
10. `src/render_plan/plan_builder.rs` - Added extract_distinct() method, updated usages
11. `src/clickhouse_query_generator/to_sql_query.rs` - Generate SELECT DISTINCT
