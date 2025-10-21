# Path Variables & Functions

*Completed: October 21, 2025*

## Summary

Complete implementation of Cypher path variables and path functions for variable-length path queries. Enables queries like `MATCH p = (a)-[:TYPE*]-(b) RETURN p, length(p), nodes(p), relationships(p)`.

## How It Works

### 1. Parser Integration
- **File**: `brahmand/src/open_cypher_parser/match_clause.rs`
- **Function**: `parse_match_clause()`
- **Logic**: Detects `p =` pattern in MATCH clauses, stores path variable name in `MatchClause.path_variable`

### 2. Logical Plan Storage
- **File**: `brahmand/src/query_planner/logical_plan/mod.rs`
- **Structure**: `GraphRel.path_variable: Option<String>`
- **Flow**: Path variable propagates from parser → logical plan → render plan

### 3. CTE Column Generation
- **File**: `brahmand/src/clickhouse_query_generator/variable_length_cte.rs`
- **Columns**:
  - `hop_count`: Tracks path length (for `length(p)`)
  - `path_nodes`: Array of node IDs along path (for `nodes(p)`)
  - `path_relationships`: Placeholder array (for `relationships(p)`)

### 4. Path Function Mapping
- **File**: `brahmand/src/render_plan/plan_builder.rs`
- **Function**: `rewrite_expr_for_var_len_cte()`
- **Mapping**:
  - `length(p)` → `t.hop_count`
  - `nodes(p)` → `t.path_nodes`
  - `relationships(p)` → `array()` (empty array placeholder)

### 5. SQL Generation
- **Base Case**: `SELECT start_id, end_id, 1 as hop_count, [start_id] as path_nodes, ['FOLLOWS'] as path_relationships`
- **Recursive Case**: `SELECT ..., hop_count + 1, arrayConcat(path_nodes, [end_id]), arrayConcat(path_relationships, ['FOLLOWS'])`

## Key Files

- `brahmand/src/open_cypher_parser/match_clause.rs` - Path variable parsing
- `brahmand/src/query_planner/logical_plan/mod.rs` - GraphRel.path_variable field
- `brahmand/src/clickhouse_query_generator/variable_length_cte.rs` - CTE column generation
- `brahmand/src/render_plan/plan_builder.rs` - Path function mapping
- `test_path_variables.py` - End-to-end testing

## Design Decisions

### Path Variable Storage
- **Decision**: Store path variable name in GraphRel struct
- **Rationale**: Enables path function resolution during SQL generation
- **Alternative**: Could store in separate path context, but GraphRel integration was cleaner

### Relationship Tracking
- **Decision**: Placeholder empty array for `relationships(p)`
- **Rationale**: Node tracking (`path_nodes`) was primary use case, relationship tracking requires additional schema work
- **Future**: Could enhance to track relationship IDs/types along path

### Function Mapping
- **Decision**: Direct column mapping in render plan
- **Rationale**: CTE columns provide exact data needed, no complex transformations required
- **Benefit**: Efficient SQL generation, leverages ClickHouse array functions

## Gotchas

### WHERE Clause Limitations
- **Issue**: WHERE clauses on path variables don't work yet (e.g., `WHERE u1.name = 'Alice'`)
- **Root Cause**: Filter extraction doesn't separate start/end node filters for CTE generation
- **Workaround**: Apply filters directly on start/end nodes, not through path variables
- **Future Fix**: Enhance filter extraction to pass start/end filters to CTE generator

### Relationship Functions
- **Issue**: `relationships(p)` returns empty array
- **Status**: Placeholder implementation, not fully functional
- **Impact**: Path analysis works for nodes and length, but not relationships
- **Future**: Implement relationship ID tracking in CTEs

## Limitations

1. **WHERE on Path Variables**: `MATCH p = (a)-[*]->(b) WHERE a.name = 'Alice'` works, but complex path filters don't
2. **Relationship Functions**: `relationships(p)` returns empty array (placeholder)
3. **Path Object Structure**: Path `p` returns as ClickHouse map, not native Cypher path object

## Future Work

1. **Enhanced WHERE Clauses**: Support `WHERE length(p) > 2` and path property filters
2. **Relationship Tracking**: Implement `relationships(p)` with actual relationship data
3. **Path Comprehensions**: Support `[p = (a)-[*]->(b) | length(p)]` syntax
4. **Path Properties**: Allow accessing path properties like `p.length`, `p.nodes`

## Testing

- **Unit Tests**: Path variable parsing tests pass
- **Integration Tests**: End-to-end path query execution verified
- **Coverage**: Path functions work with variable-length patterns
- **Validation**: `length(p)` and `nodes(p)` return correct data types

## Performance Notes

- **Memory**: Path arrays stored in ClickHouse memory (not persisted)
- **Query Time**: CTE generation adds overhead but leverages ClickHouse recursion
- **Optimization**: Cycle detection prevents infinite loops
- **Scalability**: Works with configured max depths (10-1000 hops)