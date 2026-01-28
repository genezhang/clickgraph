# ShortestPath CTE Wrapping Fix (Dec 17, 2025)

## Problem

ShortestPath queries were generating duplicate CTE declarations when multiple variable-length CTEs were present in a single query.

### Example Bug

Query:
```cypher
MATCH path = shortestPath((a:Person)-[:KNOWS*1..2]-(b:Person)) RETURN a.id, b.id LIMIT 5
```

Generated SQL (before fix):
```sql
WITH RECURSIVE vlp_cte1_inner AS (...),
vlp_cte1 AS (...),
vlp_cte2 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte2_inner AS (...),
    vlp_cte2 AS (...)  -- ❌ DUPLICATE: vlp_cte2 declared inside itself!
    SELECT * FROM vlp_cte2
  )
)
```

ClickHouse error: `Syntax error: failed at position 749 (WHERE)`

## Root Cause

The issue was in `src/clickhouse_query_generator/to_sql_query.rs` at line 374-400.

**CTE Grouping Logic:**
- ClickHouse allows only ONE `WITH RECURSIVE` block
- Multiple recursive CTEs must be wrapped in nested subqueries
- Code wraps 2nd+ recursive CTE groups as: `vlp_cte2 AS (SELECT * FROM (WITH RECURSIVE ...))`

**The Bug:**
- VLP CTEs with shortest path mode generate multi-tier structure:
  ```sql
  vlp_cte2_inner AS (...),
  vlp_cte2_to_target AS (...),
  vlp_cte2 AS (...)
  ```
- This entire string was stored as `CteContent::RawSql`
- When wrapping code added `vlp_cte2 AS (SELECT * FROM (WITH RECURSIVE ...))`, it created duplicate `vlp_cte2 AS (...)` declarations

## Solution

Modified `to_sql_query.rs` lines 374-412 to detect nested CTE structures:

```rust
// Check if the first CTE already contains nested CTE definitions (VLP multi-tier pattern)
// This is indicated by the presence of multiple " AS (" in RawSql content
let first_cte_content = match &first_cte_in_group.content {
    CteContent::RawSql(s) => Some(s.as_str()),
    _ => None,
};

let has_nested_ctes = first_cte_content
    .map(|s| s.matches(" AS (").count() > 1)
    .unwrap_or(false);

if has_nested_ctes && group.len() == 1 {
    // VLP CTE with multi-tier structure (e.g., "vlp_inner AS..., vlp AS...")
    // Wrap the entire nested structure as-is (don't call cte.to_sql())
    sql.push_str(&format!("{} AS (\n", last_cte_name));
    sql.push_str("  SELECT * FROM (\n");
    sql.push_str("    WITH RECURSIVE ");
    sql.push_str(first_cte_content.unwrap());  // Use raw content directly
    sql.push_str("\n    SELECT * FROM ");
    sql.push_str(last_cte_name);
    sql.push_str("\n  )\n)");
} else {
    // Standard case: wrap each CTE normally
    ...
}
```

**Key Insight:**
- When a CTE's RawSql content contains multiple ` AS (` patterns, it's a nested multi-tier structure
- Use the raw content directly instead of calling `cte.to_sql()` which would add another CTE wrapper
- This prevents the duplicate declaration

## Results

✅ **Fixed**: Duplicate CTE declarations eliminated
✅ **IC1 Query**: SQL generation now works (no duplicates)
✅ **Complex queries**: Multi-tier CTEs properly nested

### After Fix

Query:
```cypher
MATCH (p:Person {id: X}), (friend:Person) WHERE NOT p=friend WITH p, friend LIMIT 2 
MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend)) WITH friend LIMIT 1 
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City) RETURN friend.id, friendCity.name
```

Generated SQL:
```sql
WITH RECURSIVE with_friend_p_cte_1 AS (...),
vlp_cte3 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte3_inner AS (...),
    vlp_cte3 AS (...)  -- ✅ Only one vlp_cte3 in entire query
    SELECT * FROM vlp_cte3
  )
),
rel_friend_friendCity AS (...)
```

## Remaining Issues

❌ **Alias Mapping Bug**: Simple shortestPath queries fail with "Unknown expression identifier"

Query:
```cypher
MATCH path = shortestPath((a:Person)-[:KNOWS*1..2]-(b:Person)) RETURN a.id, b.id LIMIT 5
```

Error:
```
Unknown expression identifier `a.id` in scope SELECT a.id AS `a.id`, b.id AS `b.id` 
FROM vlp_cte1 AS vlp1 
INNER JOIN ldbc.Person AS start_node ON vlp1.start_id = start_node.id 
INNER JOIN ldbc.Person AS end_node ON vlp1.end_id = end_node.id
```

**Problem**: 
- SELECT uses Cypher aliases (`a.id`, `b.id`)
- FROM clause uses SQL table aliases (`start_node`, `end_node`)
- Should use `start_node.id` and `end_node.id`

**Root Cause**: 
- Undirected path UNION branch generation doesn't map Cypher aliases to VLP table aliases
- VLP CTEs store mapping info (`vlp_start_alias`, `vlp_cypher_start_alias`) but it's not used in SELECT generation
- Needs deeper refactoring in `plan_builder.rs` union rendering logic

## Files Modified

- `src/clickhouse_query_generator/to_sql_query.rs` (lines 374-412): Added nested CTE detection

## Testing

```bash
# Test simple shortestPath
curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query":"MATCH path = shortestPath((a:Person)-[:KNOWS*1..2]-(b:Person)) RETURN a.id LIMIT 5","sql_only":true}'

# Test IC1
curl -X POST http://localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query":"MATCH (p:Person {id: 4398046511333}), (friend:Person) WHERE NOT p=friend WITH p, friend LIMIT 2 MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend)) WITH friend LIMIT 1 MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City) RETURN friend.id, friendCity.name","sql_only":true}'
```

## Next Steps

1. Fix alias mapping for simple shortestPath queries
   - Map Cypher node aliases to VLP table aliases in UNION branches
   - Use VLP CTE metadata (`vlp_cypher_start_alias` → `start_node`)
   - Modify union branch SELECT item generation

2. Test IC1 execution after alias mapping fix

3. Continue with Priority 2 queries (22 remaining)
