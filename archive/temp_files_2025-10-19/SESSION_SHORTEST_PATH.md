# Shortest Path Implementation - Session Progress

**Date:** October 18, 2025  
**Branch:** graphview1  
**Feature:** Shortest Path Algorithms (`shortestPath()` and `allShortestPaths()`)

## Session Summary

Successfully completed **Phase 1: Parser & AST Implementation** for shortest path support.

## ✅ Completed Work

### 1. AST Extension
**File:** `brahmand/src/open_cypher_parser/ast.rs`

Added two new variants to `PathPattern` enum:
```rust
pub enum PathPattern<'a> {
    Node(NodePattern<'a>),
    ConnectedPattern(Vec<ConnectedPattern<'a>>),
    ShortestPath(Box<PathPattern<'a>>),          // NEW
    AllShortestPaths(Box<PathPattern<'a>>),      // NEW
}
```

### 2. Parser Implementation  
**File:** `brahmand/src/open_cypher_parser/path_pattern.rs`

Implemented `parse_shortest_path_function()` using nom combinators:
- Parses `shortestPath((pattern))` syntax
- Parses `allShortestPaths((pattern))` syntax  
- Uses `delimited()` and `preceded()` for clean combinator chaining
- Properly handles whitespace and nested patterns
- Falls back to regular pattern parsing if not a shortest path function

### 3. Logical Expression Support
**File:** `brahmand/src/query_planner/logical_expr/mod.rs`

- Added `ShortestPath` and `AllShortestPaths` variants to logical PathPattern
- Implemented recursive `From` trait conversion
- Properly boxes inner patterns for recursive structure

### 4. Query Planner Integration
**File:** `brahmand/src/query_planner/logical_plan/match_clause.rs`

- Added `evaluate_single_path_pattern()` helper function
- Handles shortest path variants in `evaluate_match_clause()`
- Currently unwraps to inner pattern (special SQL generation logic TBD)
- Fixed exhaustive pattern matching throughout planner

### 5. Test Coverage
**Added 6 comprehensive parser tests:**
1. `test_parse_shortest_path_simple` - Basic syntax
2. `test_parse_all_shortest_paths` - allShortestPaths variant
3. `test_parse_shortest_path_with_relationship_type` - With [:KNOWS*]
4. `test_parse_shortest_path_with_whitespace` - Whitespace handling
5. `test_parse_regular_pattern_not_shortest_path` - No false positives
6. `test_parse_shortest_path_directed` - Directed relationships

**Test Results:** 267/268 tests passing (1 unrelated failure in bolt_protocol)

## 📝 Git Commits

1. `ecf8d2c` - test(parser): add comprehensive shortest path parser tests
2. `5737a0d` - fix(tests): add exhaustive pattern matching for ShortestPath variants  
3. `1c73581` - feat(parser): add shortest path function parsing

## 🎯 Next Steps

### Task 6: SQL Generation with Depth Tracking (IN PROGRESS)

**Goal:** Generate recursive CTEs with hop count tracking for shortest path queries.

**Implementation Plan:**

1. **Identify CTE generation code**
   - File: `brahmand/src/clickhouse_query_generator/`
   - Current variable-length path logic generates recursive CTEs
   - Need to add depth tracking column

2. **Add depth tracking to CTEs**
   ```sql
   -- For variable-length paths, add:
   SELECT ..., 1 as depth  -- base case
   UNION ALL
   SELECT ..., prev.depth + 1 as depth  -- recursive case
   ```

3. **Implement shortestPath() logic**
   ```sql
   -- After CTE, add:
   ORDER BY depth ASC
   LIMIT 1
   ```

4. **Implement allShortestPaths() logic**
   ```sql
   -- After CTE, filter by minimum depth:
   WHERE depth = (
     SELECT MIN(depth) FROM cte_name
   )
   ```

5. **Update render plan**
   - Detect ShortestPath/AllShortestPaths variants
   - Set flags for special SQL generation
   - Pass through to query generator

**Files to Modify:**
- `brahmand/src/clickhouse_query_generator/to_sql.rs`
- `brahmand/src/clickhouse_query_generator/view_query.rs` (if using ViewScan)
- `brahmand/src/render_plan/plan_builder.rs` (possibly)

### Task 7: Integration Testing
- Test with actual ClickHouse database
- Verify correct results for shortest paths
- Test edge cases: disconnected graphs, self-loops, etc.

### Task 8: Documentation
- Create `notes/shortest-path.md`
- Update `STATUS.md`
- Update `NEXT_STEPS.md`
- Add examples to `docs/`

## 🔍 Technical Details

### OpenCypher Syntax
```cypher
-- Shortest path (returns first/any shortest path)
MATCH p = shortestPath((a:Person)-[*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p

-- All shortest paths (returns all paths with minimum length)
MATCH p = allShortestPaths((a:Person)-[*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p
```

### SQL Generation Strategy (Designed)

**For variable-length paths with depth tracking:**
```sql
WITH RECURSIVE path_cte AS (
  -- Base case: direct connections (depth = 1)
  SELECT 
    start_node.id as start_id,
    end_node.id as end_id,
    1 as depth,
    [start_node.id, end_node.id] as path
  FROM nodes start_node
  JOIN relationships r ON start_node.id = r.from_id
  JOIN nodes end_node ON r.to_id = end_node.id
  
  UNION ALL
  
  -- Recursive case: extend paths (depth = prev.depth + 1)
  SELECT
    prev.start_id,
    end_node.id as end_id,
    prev.depth + 1 as depth,
    arrayConcat(prev.path, [end_node.id]) as path
  FROM path_cte prev
  JOIN relationships r ON prev.end_id = r.from_id
  JOIN nodes end_node ON r.to_id = end_node.id
  WHERE prev.depth < max_depth
    AND NOT has(prev.path, end_node.id)  -- prevent cycles
)
-- For shortestPath(): ORDER BY depth LIMIT 1
-- For allShortestPaths(): WHERE depth = (SELECT MIN(depth) FROM path_cte)
SELECT * FROM path_cte
WHERE start_id = ? AND end_id = ?
ORDER BY depth ASC
LIMIT 1;  -- Remove for allShortestPaths, add WHERE depth = MIN instead
```

## 📊 Current Status

**Phase 1: Parser & AST** ✅ COMPLETE (100%)
- AST extension ✅
- Parser implementation ✅  
- Query planner integration ✅
- Test coverage ✅

**Phase 2: SQL Generation** 🔨 IN PROGRESS (0%)
- Depth tracking in CTEs ⏳
- shortestPath() SQL logic ⏳
- allShortestPaths() SQL logic ⏳

**Phase 3: Testing & Documentation** ⏸️ PENDING
- Integration tests ⏳
- Documentation ⏳

## 🎓 Lessons Learned

1. **Nom Combinator Pattern:** `ws` wraps parsers, not input strings
   - Correct: `ws(char('('))` then `.parse(input)?`
   - Wrong: `ws(input)?`

2. **Exhaustive Pattern Matching:** Adding enum variants requires updating ALL match statements
   - Found errors in: logical_expr, match_clause, path_pattern tests, optional_match_clause
   - Fixed with `PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => ...`

3. **Test-First Approach:** Writing parser tests early caught issues immediately
   - 6 tests validated correct parsing behavior
   - Confirmed no false positives (regular patterns not wrapped)

4. **Recursive AST Structures:** Boxing inner patterns enables recursive definitions
   - `ShortestPath(Box<PathPattern>)` allows `shortestPath(shortestPath(...))`
   - Though semantically unclear, parser handles it correctly

## 🔗 Related Work

**Previous Feature:** ViewScan implementation (completed Oct 17, 2025)
- Established testing patterns (test_runner.py)
- Created git workflow best practices  
- Tagged as `viewscan-complete`

**Current Branch:** graphview1 (3 commits ahead)
- Building on ViewScan infrastructure
- Will leverage view-based graph model for shortest paths

## ⏭️ Immediate Next Action

**Start Task 6:** Investigate current CTE generation code
```bash
# Find existing variable-length path CTE generation
grep -r "WITH RECURSIVE" brahmand/src/clickhouse_query_generator/

# Examine render plan structure
grep -r "variable_length" brahmand/src/render_plan/
```

Then modify to add depth tracking column and shortest path filtering logic.
