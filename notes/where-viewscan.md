# WHERE Clause Support for ViewScan Queries

**Completed**: November 3, 2025  
**Test Status**: ✅ 318/318 tests passing (100%)

## Summary

Implemented WHERE clause filtering for simple MATCH queries that use ViewScan nodes. Previously, filters were only applied to variable-length path queries (GraphRel nodes).

**Before**:
```cypher
MATCH (u:User) WHERE u.name = 'Alice' RETURN u
-- ❌ Returns ALL 5 users (filter ignored)
-- SQL: SELECT u.name FROM users AS u
```

**After**:
```cypher
MATCH (u:User) WHERE u.name = 'Alice' RETURN u
-- ✅ Returns only Alice (filter applied)
-- SQL: SELECT * FROM (SELECT * FROM users WHERE name = 'Alice') AS u
```

## How It Works

The fix required changes at two levels:

### 1. Logical Plan Level - Filter Injection

**File**: `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs` (lines 209-315)

The `FilterIntoGraphRel` optimizer was enhanced to handle ViewScan nodes:

```rust
LogicalPlan::Projection(proj) => {
    // Check if projection has ViewScan child
    if let LogicalPlan::ViewScan(scan) = proj.input.as_ref() {
        // Get Cypher label from plan context
        let label = plan_ctx.get_node_label_for_alias(&alias);
        
        // Map label to table name via schema
        let table_name = GLOBAL_GRAPH_SCHEMA.get_table_for_label(label);
        
        // Match against ViewScan's source_table
        if scan.source_table == table_name {
            // Inject filter into ViewScan.view_filter
            scan.view_filter = Some(filter_expr);
        }
    }
}
```

**Key Insight**: Use schema lookup to map Cypher label → table name, then match against ViewScan's `source_table` field.

### 2. SQL Generation Level - Subquery Wrapping

**File**: `brahmand/src/clickhouse_query_generator/to_sql_query.rs` (lines 83-127)

Modified `FromTableItem::to_sql()` to generate subqueries when ViewScan has filters:

```rust
// Check if ViewScan has view_filter
let needs_subquery = if let LogicalPlan::ViewScan(scan) = view_ref.source.as_ref() {
    scan.view_filter.is_some()
} else {
    false
};

if needs_subquery {
    // Wrap in subquery
    sql.push_str("(SELECT * FROM ");
    sql.push_str(&view_ref.name);
    if let Some(ref filter) = scan.view_filter {
        sql.push_str(" WHERE ");
        sql.push_str(&filter.to_sql()?);
    }
    sql.push_str(") AS ");
    sql.push_str(&alias);
}
```

**Pattern**: Generate `(SELECT * FROM table WHERE filter) AS alias` instead of just `table AS alias`.

## Key Files Modified

1. **`filter_into_graph_rel.rs`**: Added Projection handler for ViewScan
2. **`to_sql_query.rs`**: Added subquery wrapping logic + ToSql trait import
3. **`to_sql.rs`**: Added WHERE clause to ViewScan::to_sql() (not used in practice)

## Design Decisions

### Schema-Based Matching
- **Why**: ViewScan uses actual table names ("users"), but plan_ctx stores Cypher labels ("User")
- **How**: Use `GLOBAL_GRAPH_SCHEMA.get_table_for_label()` to translate
- **Benefit**: Maintains separation between logical (Cypher) and physical (SQL) layers

### Column vs PropertyAccess
- **Issue**: Initial implementation used `PropertyAccess(table, column)` which generated `users.name`
- **Problem**: Subquery wraps table, making qualified names invalid
- **Solution**: Use bare `Column(name)` which generates unqualified `name`
- **Result**: Works correctly in subquery context

### ToSql Trait Import
- **Issue**: `LogicalExpr.to_sql()` method not found
- **Root Cause**: Method defined in `clickhouse_query_generator::to_sql` module
- **Fix**: Added `use super::to_sql::ToSql as LogicalToSql;` import
- **Lesson**: RenderPlan and LogicalPlan have separate ToSql implementations

## Gotchas

1. **RenderPlan vs LogicalPlan Paths**
   - Query execution uses RenderPlan path (`to_sql_query.rs`)
   - LogicalPlan::to_sql() in `to_sql.rs` is NOT used for actual queries
   - Modified both files for completeness, but only `to_sql_query.rs` matters

2. **FilterTagging Timing**
   - FilterTagging analyzer runs BEFORE optimizer
   - Removes Filter nodes, stores predicates in `plan_ctx`
   - FilterIntoGraphRel must retrieve filters from `plan_ctx`, not from plan tree

3. **Subquery Necessity**
   - Can't just add WHERE to outer query (SELECT u.name FROM users WHERE...)
   - Breaks when projection has complex expressions
   - Subquery ensures filter applies before projection

## Limitations

- Filters must be single-table predicates
- Multi-table filters not supported for ViewScan (use GraphRel for joins)
- Filter expressions must be convertible to SQL (no Cypher-specific functions)

## Future Work

- [ ] Support multi-column filters with AND/OR
- [ ] Optimize away subquery when projection is `SELECT *`
- [ ] Push filters into ClickHouse views for better performance
- [ ] Add end-to-end tests for complex WHERE clauses

## Test Coverage

**Unit Tests**: 318 passing
- FilterTagging tests: Extract filters correctly
- FilterIntoGraphRel tests: Inject filters into ViewScan
- SQL generation tests: Generate correct subqueries

**Integration Tests**:
- `test_where_simple.py`: Simple equality filter
- End-to-end validation: MATCH with WHERE returns filtered results

## Debugging Story

The fix was discovered through a multi-phase investigation:

1. **Initial Observation**: WHERE clause ignored, all rows returned
2. **Debug Logging**: Added prints showing `FilterTagging` extracts filters to `plan_ctx`
3. **Optimizer Analysis**: Found `FilterIntoGraphRel` ignores ViewScan patterns
4. **First Fix**: Added ViewScan handling to `FilterIntoGraphRel`
5. **Debug Again**: Filters injected but still all rows returned!
6. **SQL Inspection**: Generated SQL has no WHERE clause
7. **Final Fix**: Modified SQL generation to wrap ViewScan in subquery
8. **Success**: Test returns 1 row instead of 5

**Key Tool**: `start_server_background.ps1` with `Receive-Job` for capturing `println!` debug output

## Related Features

- **Variable-Length Paths**: WHERE clause support for `*` patterns (already working)
- **OPTIONAL MATCH**: LEFT JOIN semantics for optional patterns
- **FilterIntoGraphRel Optimizer**: Pushes filters into graph operators
- **FilterTagging Analyzer**: Pre-optimizer filter extraction
