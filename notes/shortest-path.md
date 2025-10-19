# Shortest Path Implementation

**Status**: ✅ Core functionality complete, ⚠️ WHERE clause support pending  
**Date**: October 18, 2025  
**Commits**: 440e1de → 53b4852 (11 commits)

## Summary

Implemented `shortestPath()` and `allShortestPaths()` functions for ClickGraph, translating Cypher shortest path queries into ClickHouse recursive CTEs with depth-based filtering.

**What Works**:
- ✅ Parser: `shortestPath((a)-[:TYPE*]-(b))` and `allShortestPaths((a)-[:TYPE*]-(b))`
- ✅ Query planning: ShortestPath/AllShortestPaths pattern handling
- ✅ SQL generation: Nested CTE structure with hop count tracking
- ✅ Cycle detection: `NOT has(path_nodes, current_node.id)`
- ✅ Integration: Queries execute successfully against ClickHouse

**Known Limitations**:
- ⚠️ WHERE clause filtering not applied to recursive CTEs (see "Next Steps")
- ⚠️ Path variable assignment (`p = shortestPath(...)`) not yet supported
- ⚠️ Property filters in relationships not tested

## How It Works

### 1. Parser (Task 4)

**File**: `brahmand/src/open_cypher_parser/path_pattern.rs`

```rust
// AST variants
pub enum PathPattern<'a> {
    ShortestPath(Box<PathPattern<'a>>),       // shortestPath(...)
    AllShortestPaths(Box<PathPattern<'a>>),   // allShortestPaths(...)
    // ... other variants
}

// Parser function
fn parse_shortest_path_function(input: &str) -> IResult<...> {
    // Parses: shortestPath((a:Person)-[:KNOWS*]-(b:Person))
    // Returns: PathPattern::ShortestPath(inner_pattern)
}
```

**Tests**: 4/4 passing in `path_pattern.rs`

**Debugging Story - Whitespace Handling Bug**:
- **Issue**: Parser unit tests passed (4/4) but integration tests failed with "Error in match clause"
- **Root Cause**: `parse_shortest_path_function()` expected input without leading whitespace (`"shortestPath(...)`), but `parse_match_clause()` passed input WITH leading space (`" shortestPath(...)`) after consuming "MATCH"
- **Fix**: Added `multispace0` at start of parser tuples to consume optional leading whitespace
- **Commit**: d7ebe6d
- **Lesson**: Always test parsers in integration context, not just in isolation

### 2. Query Planner (Task 5)

**File**: `brahmand/src/query_planner/logical_plan/match_clause.rs`

```rust
fn evaluate_single_path_pattern(pattern: &PathPattern) -> LogicalPlan {
    match pattern {
        PathPattern::ShortestPath(inner) => {
            let mut plan = evaluate_single_path_pattern(inner);
            plan.shortest_path_mode = Some(ShortestPathMode::Shortest);
            plan
        }
        PathPattern::AllShortestPaths(inner) => {
            let mut plan = evaluate_single_path_pattern(inner);
            plan.shortest_path_mode = Some(ShortestPathMode::AllShortest);
            plan
        }
        // ... handle other patterns
    }
}
```

**Key Decision**: Use a `shortest_path_mode` flag that propagates through the logical plan rather than creating separate plan node types.

### 3. SQL Generation (Task 6)

**File**: `brahmand/src/clickhouse_query_generator/variable_length_cte.rs`

**Architecture**: Two-pass nested CTE approach

```sql
-- INNER CTE: Generate all paths with recursive traversal
WITH RECURSIVE variable_path_xxx_inner AS (
    -- Base case: Direct connections (1 hop)
    SELECT
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [start_node.id] as path_nodes,
        start_node.name as start_name,
        end_node.name as end_name
    FROM users start_node
    JOIN follows rel ON start_node.id = rel.from_id
    JOIN users end_node ON rel.to_id = end_node.id
    
    UNION ALL
    
    -- Recursive case: Extend paths by one hop
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_nodes, [current_node.id]) as path_nodes,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM variable_path_xxx_inner vp
    JOIN users current_node ON vp.end_id = current_node.id
    JOIN follows rel ON current_node.id = rel.from_id
    JOIN users end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 10                           -- Max depth
      AND NOT has(vp.path_nodes, current_node.id)     -- Cycle detection
),

-- OUTER CTE: Filter to shortest paths only
variable_path_xxx AS (
    -- For shortestPath(): Take single shortest
    SELECT * FROM variable_path_xxx_inner 
    ORDER BY hop_count ASC LIMIT 1
    
    -- OR for allShortestPaths(): Take all shortest
    -- SELECT * FROM variable_path_xxx_inner 
    -- WHERE hop_count = (SELECT MIN(hop_count) FROM variable_path_xxx_inner)
)

-- Final query
SELECT t.start_name, t.end_name
FROM variable_path_xxx AS t
```

**Why Two CTEs?**
1. **Separation of concerns**: Graph traversal vs shortest-path filtering
2. **Works with ClickHouse limitations**: Can't directly filter during recursion
3. **Easy to extend**: Just change outer CTE for different algorithms

**Debugging Story - Nested CTE Bug**:
- **Issue**: SQL syntax error - `WITH RECURSIVE cte_inner AS (cte AS (...))`
- **Root Cause**: Code was wrapping query body with `cte_name AS (...)`, then wrapping THAT again with `cte_inner AS (...)`
- **Fix**: Generate query body without CTE wrapper, then apply appropriate wrapper based on `shortest_path_mode`
- **Commit**: 53b4852
- **Lesson**: Be careful with string formatting when generating nested structures

### 4. Integration Testing (Task 8 - Partial)

**Test Data**: Social network with 6 users, 5 follow relationships

```
Alice -> Bob -> Carol -> David -> Eve
       |        |
       +--------+  (shortcut)
Frank (isolated)
```

**Test Results**:
```bash
$ python test_shortest_path_integration.py

✅ Queries execute without SQL errors
✅ Returns results (1 row for shortestPath, 5 rows for allShortestPaths)
⚠️  WHERE clause not applied - all queries return same result
```

**Expected vs Actual**:
- Query: `WHERE a.name = 'Alice' AND b.name = 'Bob'`
- Expected: 1 row (Alice -> Bob)
- Actual: 1 row (Alice -> Bob) ✅ but only because it's the first shortest path
- Query: `WHERE a.name = 'Alice' AND b.name = 'Frank'` (disconnected)
- Expected: 0 rows
- Actual: 1 row (Alice -> Bob) ❌ WHERE clause ignored

## Key Files

### Core Implementation
- `brahmand/src/open_cypher_parser/ast.rs` - AST variants (lines 200-202)
- `brahmand/src/open_cypher_parser/path_pattern.rs` - Parser (lines 27-62, tests at bottom)
- `brahmand/src/query_planner/logical_plan/match_clause.rs` - Query planning (lines 70-120)
- `brahmand/src/query_planner/logical_plan/logical_expr.rs` - ShortestPathMode enum
- `brahmand/src/clickhouse_query_generator/variable_length_cte.rs` - SQL generation (lines 112-161)

### Testing & Documentation
- `test_shortest_path.py` - SQL generation tests (RETURN clause only)
- `test_shortest_path_integration.py` - Integration tests with WHERE clause
- `test_shortest_path_real_data.sql` - Test data setup
- `notes/shortest-path.md` - This file

## Design Decisions

### ✅ Why Nested CTEs?
**Alternative considered**: Add filtering directly in recursive CTE WHERE clause

**Chosen approach**: Two-level CTE structure
- **Pro**: Clean separation, easy to understand, works with ClickHouse
- **Pro**: Can reuse same recursive traversal code
- **Con**: Slightly more verbose SQL

### ✅ Why hop_count tracking?
**Alternative considered**: Use `length(path_nodes)` to calculate depth

**Chosen approach**: Explicit `hop_count` column
- **Pro**: More efficient (no array length calculation)
- **Pro**: Clearer intent in SQL
- **Con**: Extra column to maintain

### ⚠️ WHERE clause application (pending)
**Options**:
1. **Filter in base case**: Add WHERE conditions when generating initial hops
2. **Filter in final SELECT**: Add WHERE after CTE completes
3. **Filter in outer CTE**: Add WHERE between inner and outer CTEs

**Recommendation**: Option 1 (filter in base case) - most efficient, prevents unnecessary recursion

## Gotchas

1. **Parser Whitespace**: Always consume leading whitespace in nom parsers when called from parent contexts
2. **CTE Wrapping**: Be careful not to double-wrap when generating nested CTEs
3. **Relationship Labels Required**: Current schema inference requires explicit relationship type (`:KNOWS`) - can't use unlabeled patterns yet
4. **Windows Testing**: Use `ENGINE = Memory` for ClickHouse tables (persistent engines fail on Windows)

## Limitations

### Current (Known Issues)
1. **WHERE clause not applied**: Queries find shortest path in entire graph, ignore WHERE filters
2. **Path variable assignment**: `p = shortestPath(...)` not yet supported
3. **RETURN path**: Can't return full path object, only node properties
4. **Relationship labels required**: Must specify `:TYPE`, can't use `-[*]-` pattern

### By Design (Read-Only Engine)
- ❌ Cannot create/modify graph structure
- ❌ No path mutation operations
- ✅ Analytical queries only

## Next Steps

### Immediate (Tonight?)
- [x] Commit SQL generation fix
- [x] Document implementation (this file)
- [ ] Update STATUS.md with current state
- [ ] Update CHANGELOG.md

### Short Term (Next Session)
1. **WHERE clause support** (High Priority)
   - Apply filters in base case of recursive CTE
   - Test: `WHERE a.name = 'Alice' AND b.name = 'Bob'` returns correct result
   - Test: Disconnected nodes return empty results

2. **Property filtering in relationships**
   - Test: `WHERE r.since > '2023-01-01'`
   - Verify relationship property mappings work

3. **Comprehensive integration tests**
   - Test all graph topologies: linear, tree, cyclic, disconnected
   - Test various hop ranges: `*1`, `*2..5`, `*..10`
   - Performance testing with larger graphs

### Future Enhancements
4. **Path variable assignment**: `p = shortestPath(...) RETURN p`
5. **Path object support**: Return `[nodes, relationships]` structure
6. **Weighted shortest paths**: Consider edge weights (requires Dijkstra algorithm)
7. **Bidirectional BFS**: Optimize for very long paths
8. **Path predicates**: `WHERE all(n IN nodes(p) WHERE n.active = true)`

## Performance Considerations

**Current Implementation**:
- Max depth configurable via environment variable (default: 10)
- Cycle detection prevents infinite loops
- ClickHouse setting: `max_recursive_cte_evaluation_depth = 100`

**Optimization Opportunities**:
1. **Early termination**: Stop recursion once shortest path found (requires ClickHouse support)
2. **Bidirectional search**: Start from both ends (complex to implement in SQL)
3. **Index hints**: Add index hints for relationship table joins
4. **Materialized views**: Pre-compute common paths for frequently-queried graphs

## Testing Checklist

### Parser Tests ✅
- [x] Basic shortestPath syntax
- [x] Basic allShortestPaths syntax
- [x] Nested pattern: `shortestPath((a)-[]->(b))`
- [x] With relationship type: `shortestPath((a)-[:TYPE*]-(b))`

### SQL Generation Tests ✅
- [x] Generates ORDER BY hop_count ASC LIMIT 1 for shortestPath
- [x] Generates WHERE hop_count = MIN(...) for allShortestPaths
- [x] Regular paths don't add filtering
- [x] Nested CTE structure is valid SQL

### Integration Tests ⚠️ (Partial)
- [x] Queries execute without syntax errors
- [x] Returns results for connected nodes
- [ ] WHERE clause filters applied correctly
- [ ] Returns empty for disconnected nodes
- [ ] Prefers shorter paths over longer ones
- [ ] allShortestPaths returns all minimum paths

## Git Commit History

```
440e1de - Initial research and design notes
ba8e214 - feat(ast): add ShortestPath variants to PathPattern enum
2a7110d - feat(parser): implement shortest path function parsing
32dc88f - test: add comprehensive tests for shortest path parser
96c40f6 - refactor(sql): wire shortest_path_mode through CTE generator
32b4d23 - feat(sql): implement shortest path SQL generation with depth filtering
63a8ed7 - test: add shortest path SQL generation test script
f8a7cf1 - fix(parser): improve shortest path function parsing with case-insensitive matching
d7ebe6d - fix(parser): consume leading whitespace in shortest path functions
53b4852 - fix(sql): correct nested CTE structure for shortest path queries
```

Total: 11 commits over ~4 hours of focused development

## Lessons Learned

1. **Test Integration Early**: Parser unit tests passed but integration failed - test the full pipeline
2. **Whitespace Matters**: nom parsers are sensitive to whitespace - always consume it explicitly
3. **String Generation is Tricky**: Be careful with nested string formatting and wrapper logic
4. **Incremental Progress**: Breaking down into 6 small tasks made complex feature manageable
5. **Document As You Go**: Writing this doc helped clarify design decisions and limitations

## References

- OpenCypher Specification: https://opencypher.org/
- ClickHouse Recursive CTEs: https://clickhouse.com/docs/en/sql-reference/statements/select/with
- Project STATUS.md for overall feature tracking
- CHANGELOG.md for release history
