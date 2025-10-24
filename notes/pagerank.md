# PageRank Algorithm Implementation

*Completed: October 23, 2025*

## Overview

PageRank algorithm implementation for ClickGraph, enabling graph centrality analysis through Cypher CALL statements.

## Syntax

```cypher
CALL pagerank(maxIterations: 10, dampingFactor: 0.85)
CALL pagerank(graph: 'User', maxIterations: 10, dampingFactor: 0.85)
CALL pagerank(nodeLabels: 'Person,Company', relationshipTypes: 'KNOWS,WORKS_FOR', maxIterations: 10, dampingFactor: 0.85)
```

### Parameters

- `graph` (optional): Name of the node type to run PageRank on. Defaults to 'User' for backward compatibility
- `nodeLabels` (optional): Comma-separated list of node labels to include in PageRank calculation (e.g., 'Person,Company')
- `relationshipTypes` (optional): Comma-separated list of relationship types to include (e.g., 'KNOWS,WORKS_FOR')
- `maxIterations` (optional): Maximum number of PageRank iterations. Defaults to 10
- `dampingFactor` (optional): Damping factor between 0.0 and 1.0. Defaults to 0.85

### Backward Compatibility

The following legacy parameter names are also supported:
- `iterations` (alias for `maxIterations`)
- `damping` (alias for `dampingFactor`)

## Algorithm

PageRank assigns importance scores to nodes based on their connectivity:

**PR(A) = (1-d) + d × Σ(PR(Ti)/C(Ti)) for all pages Ti linking to A**

Where:
- `d` = damping factor (typically 0.85)
- `C(Ti)` = out-degree of node Ti
- `(1-d)` = random jump probability

## Implementation Details

### SQL Generation Strategy

**Challenge**: ClickHouse recursive CTEs hit subquery depth limits (>100 levels)

**Solution**: Iterative UNION ALL approach instead of recursive CTEs

```sql
WITH
-- All relationships union
all_relationships AS (
    SELECT from_node_id, to_node_id FROM user_follows
    UNION ALL
    SELECT user1_id, user2_id FROM friendships
),

-- Calculate out-degrees
node_out_degrees AS (
    SELECT from_node_id AS node_id, count(*) AS out_degree
    FROM all_relationships GROUP BY from_node_id
),

-- Initial PageRank values
initial_pagerank AS (
    SELECT user_id AS node_id, 1.0 / (SELECT count(*) FROM users) AS pagerank, 0 AS iteration
    FROM users
),

-- Iterative PageRank computation
pagerank_iterations AS (
    -- Iteration 0: initial values
    SELECT node_id, pagerank, iteration FROM initial_pagerank

    UNION ALL

    -- Iteration 1
    SELECT r.to_node_id AS node_id,
           (1 - 0.85) + 0.85 * sum(pr.pagerank / coalesce(nod.out_degree, 1)) AS pagerank,
           1 AS iteration
    FROM all_relationships r
    JOIN pagerank_iterations pr ON pr.node_id = r.from_node_id AND pr.iteration = 0
    LEFT JOIN node_out_degrees nod ON nod.node_id = r.from_node_id
    GROUP BY r.to_node_id

    UNION ALL

    -- Iteration 2, 3, ... (generated dynamically)
    ...
)

-- Final result: last iteration
SELECT node_id, pagerank, iteration
FROM pagerank_iterations
WHERE iteration = (SELECT MAX(iteration) FROM pagerank_iterations)
ORDER BY pagerank DESC
```

### Key Components

1. **Parser Extension** (`open_cypher_parser/call_clause.rs`)
   - Added `CallClause` and `CallArgument` AST nodes
   - Named parameter parsing: `iterations: 10, damping: 0.85`
   - Error type conversion wrapper for expression parsing

2. **Query Planning** (`query_planner/mod.rs`)
   - `evaluate_call_query()` function for CALL statement handling
   - `PageRank` logical plan variant with iterations and damping_factor

3. **SQL Generation** (`clickhouse_query_generator/pagerank.rs`)
   - `PageRankGenerator` struct with configurable parameters
   - Dynamic iteration generation via `generate_iterations_sql()`
   - Schema-aware table/column name resolution

4. **Server Integration** (`server/handlers.rs`)
   - Direct SQL execution bypassing render plan processing
   - JSON result formatting with node_id, pagerank, iteration columns

## Test Results

### Convergence Testing

```sql
-- 3 iterations, damping=0.85
node_id | pagerank
--------|----------
5       | 0.65734375
1       | 0.62430000
4       | 0.52522187
3       | 0.37657812
2       | 0.36005625

-- 5 iterations, damping=0.85 (more converged)
node_id | pagerank
--------|----------
5       | 0.83885607
1       | 0.78704312
4       | 0.67094222
3       | 0.47712190
2       | 0.45121542
```

### Parameter Sensitivity

```sql
-- damping=0.9 (vs 0.85) - lower scores due to higher random jump weight
node_id | pagerank
--------|----------
5       | 0.53875
1       | 0.53020
4       | 0.42107
3       | 0.29912
2       | 0.29485
```

## Design Decisions

### Iterative vs Recursive Approach

**Why not recursive CTEs?**
- ClickHouse limits recursive depth to prevent infinite loops
- Complex self-joins create exponential subquery nesting
- UNION ALL approach is more predictable and debuggable

**Benefits of iterative approach:**
- Explicit control over iteration depth
- No recursive CTE limitations
- Easier debugging and performance monitoring
- Clear separation between iterations

### Schema Integration

**Node Type Assumption:**
- Currently assumes "User" node type for simplicity
- Future: Support configurable node types via query parameters
- Schema resolution through existing `GraphSchema` infrastructure

### Parameter Validation

**Bounds checking:**
- `iterations`: 1-100 (reasonable convergence range)
- `damping`: 0.0-1.0 (standard PageRank range)
- Future: Add validation in query planner

## Performance Characteristics

- **Time Complexity**: O(iterations × |E|) where |E| is edge count
- **Space Complexity**: O(iterations × |V|) for storing all iterations
- **ClickHouse Optimization**: Leverages parallel aggregation and JOIN performance

## Future Enhancements

1. **Personalized PageRank**: Add source node bias parameter
2. **Weighted PageRank**: Support relationship weight properties
3. **Convergence Detection**: Stop when scores stabilize within epsilon
4. **Multiple Node Types**: Support heterogeneous graphs
5. **Result Caching**: Cache results for repeated queries

## Gotchas & Limitations

- **Memory Tables Only**: Requires ClickHouse Memory engine for Windows compatibility
- **No Persistence**: Results not cached between queries
- **Fixed Iterations**: No automatic convergence detection
- **Single Node Type**: Assumes homogeneous node types
- **No Error Recovery**: Fails fast on invalid parameters

## Testing

- **Unit Tests**: SQL generation validation
- **Integration Tests**: End-to-end query execution
- **Parameter Testing**: Different iterations/damping combinations
- **Schema Testing**: Multiple relationship types (FOLLOWS + FRIENDS_WITH)

## Files Modified

- `open_cypher_parser/ast.rs` - Added CallClause/CallArgument
- `open_cypher_parser/call_clause.rs` - CALL statement parser
- `query_planner/mod.rs` - CALL query evaluation
- `query_planner/logical_plan/mod.rs` - PageRank plan variant
- `clickhouse_query_generator/pagerank.rs` - SQL generator
- `render_plan/plan_builder.rs` - Match arm additions
- `server/handlers.rs` - Direct SQL execution
- `server/mod.rs` - Query type routing