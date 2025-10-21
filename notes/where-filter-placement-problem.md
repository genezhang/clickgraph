# WHERE Filter Placement Problem in shortestPath/Variable-Length Paths

## Problem Statement

When executing queries like:
```cypher
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE a.name = 'Alice Johnson' AND b.name = 'Charlie Brown'
RETURN a.name, b.name, length(p)
```

The WHERE filter needs to be **split** across different parts of the generated SQL:
- **Start node filter** (`a.name = 'Alice'`) → CTE base case (performance optimization)
- **End node filter** (`b.name = 'Charlie'`) → Final SELECT (correctness requirement)

## Current Behavior (Baseline)

Looking at generated SQL from `baseline_out.log`:

```sql
WITH RECURSIVE variable_path_xxx AS (
  -- BASE CASE: No WHERE filter on start_node!
  SELECT
      start_node.user_id as start_id,
      end_node.user_id as end_id,
      1 as hop_count,
      [start_node.user_id] as path_nodes,
      end_node.full_name as end_name,
      start_node.full_name as start_name
  FROM social.users start_node
  JOIN social.user_follows rel ON start_node.user_id = rel.follower_id
  JOIN social.users end_node ON rel.followed_id = end_node.user_id
  -- ❌ Missing: WHERE start_node.full_name = 'Alice Johnson'
  
  UNION ALL
  
  -- RECURSIVE CASE
  SELECT
      vp.start_id,
      end_node.user_id as end_id,
      vp.hop_count + 1 as hop_count,
      arrayConcat(vp.path_nodes, [current_node.user_id]) as path_nodes,
      end_node.full_name as end_name,
      vp.start_name as start_name
  FROM variable_path_xxx vp
  JOIN social.users current_node ON vp.end_id = current_node.user_id
  JOIN social.user_follows rel ON current_node.user_id = rel.follower_id
  JOIN social.users end_node ON rel.followed_id = end_node.user_id
  WHERE vp.hop_count < 10
)
SELECT t.start_name, t.end_name, t.hop_count
FROM variable_path_xxx t
-- ❌ Missing: WHERE t.end_name = 'Charlie Brown'
LIMIT 1
```

**Problem**: All WHERE filters are likely applied in the final SELECT, causing:
1. **Performance issue**: CTE generates ALL paths from ALL start nodes (no early filtering)
2. **Correctness issue**: Results show wrong data (Test 1 returned `start: Bob Smith` instead of `Alice Johnson`)

## Desired Behavior

```sql
WITH RECURSIVE variable_path_xxx AS (
  -- BASE CASE: Filter on start_node HERE
  SELECT ...
  FROM social.users start_node
  JOIN social.user_follows rel ON ...
  JOIN social.users end_node ON ...
  WHERE start_node.full_name = 'Alice Johnson'  -- ✅ Start filter in base case
  
  UNION ALL
  
  -- RECURSIVE CASE: No WHERE on end node yet
  SELECT ...
  FROM variable_path_xxx vp
  ...
  WHERE vp.hop_count < 10  -- Only recursion depth limit
)
SELECT t.start_name, t.end_name, t.hop_count
FROM variable_path_xxx t
WHERE t.end_name = 'Charlie Brown'  -- ✅ End filter in final SELECT
LIMIT 1
```

## Root Cause Analysis

Need to investigate in `brahmand/src/clickhouse_query_generator/variable_length_cte.rs`:

1. **Where are WHERE clauses currently applied?**
   - Likely all filters go to final SELECT
   - Need to split based on variable binding

2. **How to identify start vs end node filters?**
   - Parse WHERE expression tree
   - Check if filter references `start_node` alias → base case
   - Check if filter references `end_node` alias → final SELECT
   - Check if filter references relationship → both cases?

3. **What's the data structure?**
   - `FilterExpr` from logical plan
   - Need to traverse and classify predicates

## Incremental Fix Strategy

### Phase 1: Add Filter Classification (No Breaking Changes)

1. Create helper function in `variable_length_cte.rs`:
   ```rust
   fn classify_where_filters(
       filter_expr: &FilterExpr,
       start_alias: &str,
       end_alias: &str,
   ) -> (Vec<FilterExpr>, Vec<FilterExpr>) {
       // Returns: (base_case_filters, final_select_filters)
   }
   ```

2. Add unit tests for filter classification
3. Don't change SQL generation yet - just verify classification works

### Phase 2: Apply Base Case Filters (Minimal Change)

1. Modify CTE base case generation to include start_node filters
2. Keep existing final SELECT filters unchanged
3. Test with start-node-only WHERE clauses

### Phase 3: Apply Final SELECT Filters (Complete Fix)

1. Modify final SELECT to include end_node filters
2. Remove end_node filters from wherever they're incorrectly applied now
3. Test with end-node-only and combined WHERE clauses

### Phase 4: Handle Edge Cases

1. Relationship property filters (apply to both base and recursive?)
2. Complex boolean expressions (AND/OR combinations)
3. Path variable filters (`WHERE length(p) < 5`)

## Test Cases

```cypher
-- Test 1: Start node only (base case filter)
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE a.name = 'Alice Johnson'
RETURN a.name, b.name

-- Test 2: End node only (final SELECT filter)
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE b.name = 'Charlie Brown'
RETURN a.name, b.name

-- Test 3: Both nodes (split placement)
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE a.name = 'Alice Johnson' AND b.name = 'Charlie Brown'
RETURN a.name, b.name

-- Test 4: Complex boolean
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE (a.name = 'Alice' OR a.name = 'Bob') AND b.name = 'Charlie'
RETURN a.name, b.name
```

## Key Files to Modify

1. `brahmand/src/clickhouse_query_generator/variable_length_cte.rs` - Main fix location
2. `brahmand/src/query_planner/logical_plan/filter.rs` - Filter expression analysis
3. `brahmand/src/render_plan/plan_builder.rs` - CTE context (may need filter_expr field)

## Success Criteria

- [ ] Test 1 returns only paths starting from Alice (not Bob)
- [ ] Test 2 returns only paths ending at Charlie
- [ ] Test 3 returns shortest path from Alice to Charlie
- [ ] No regression in existing 274 passing tests
- [ ] Performance improvement (fewer paths generated in CTE)
