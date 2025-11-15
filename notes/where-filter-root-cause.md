# WHERE Filter Fix - CRITICAL DISCOVERY

## The Real Problem (Found!)

**Filter nodes are being REMOVED during query planning** - they never reach the render stage!

### Evidence

Query with WHERE clause:
```cypher
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice' RETURN a.name
```

Expected plan structure:
```
Projection (RETURN)
 └─ Filter (WHERE) ← Should be here!
     └─ GraphRel (MATCH pattern)
```

**Actual plan structure** (via debug logging):
```
Projection (RETURN)  ← to_render_plan() called here
 └─ GraphRel (MATCH pattern) ← No Filter node!
```

### Debug Evidence

```
[TO_RENDER_PLAN] Starting render plan generation
  Plan type: Projection
  
[PROJECTION_NODE] Recursing into projection input
  Projection input type: GraphRel  ← Filter is missing!
```

### What This Means

1. ❌ **My "1-line fix" was wrong** - adding filter context passing doesn't work because Filter node doesn't exist
2. ❌ **Infrastructure is incomplete** - filters ARE being removed somewhere in query_planner/
3. ✅ **CTE generation code is correct** - it would work IF filters reached it

## Root Cause Location

The filter removal is happening in **query_planner/** module, likely in one of these places:

1. `query_planner/optimizer/` - filter pushdown/removal optimizations?
2. `query_planner/analyzer/` - query rewriting that removes filters?
3. `query_planner/logical_plan/` - plan construction that skips Filter nodes?

## Next Steps

### Step 1: Find Where Filters Are Removed
Search for:
- Filter node creation/removal in query planner
- Optimization passes that might remove filters
- Variable-length path handling that bypasses filters

### Step 2: Fix Options

**Option A** (Preferred): Store filters in GraphRel during planning
- Add `where_clause: Option<Expression>` field to GraphRel struct
- Populate it during query planning
- Extract it during CTE generation

**Option B**: Prevent filter removal for variable-length paths
- Identify the optimization pass removing filters
- Add check: if GraphRel has variable_length, DON'T remove Filter node
- Let existing infrastructure handle it

## Test Validation

Use `sql_only: true` flag to quickly see generated SQL without execution:

```python
import requests
r = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = \\'Alice\\' RETURN a.name',
    'sql_only': True
})
print(r.json()['generated_sql'])
```

Expected SQL (when fixed):
```sql
WITH RECURSIVE path_inner AS (
  SELECT ... 
  FROM users start_node ...
  WHERE start_node.full_name = 'Alice'  ← Filter HERE
  ...
)
```

Current SQL (broken):
```sql
WITH RECURSIVE path_inner AS (
  SELECT ... 
  FROM users start_node ...
  -- No WHERE clause!
)
```

## Impact on Previous Session

This explains why the previous debugging session failed:
- We were looking at render-time issues (property extraction, CTE generation)
- But the real problem was at **plan-time** (Filter node removal)
- All the property extraction fixes were unnecessary
- The baseline code already worked - filters just weren't reaching it!



