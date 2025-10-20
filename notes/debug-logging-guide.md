# Debug Logging Guide

## Overview
The WHERE clause filter feature includes structured logging that can be enabled for debugging. All logging uses Rust's standard `log` crate and can be controlled via the `RUST_LOG` environment variable.

## Logging Levels

### TRACE Level (Most Detailed)
Shows detailed step-by-step operations including data structures and intermediate values.

**Enable**: `RUST_LOG=trace` or `RUST_LOG=brahmand=trace`

**What You'll See**:
- Filter categorization process with input/output expressions
- Column qualification operations
- Plan structure before and after optimization
- Detailed filter extraction from plan context
- SQL generation for start/end filters
- CTE generation steps

### DEBUG Level (Key Operations)
Shows important events and decisions without overwhelming detail.

**Enable**: `RUST_LOG=debug` or `RUST_LOG=brahmand=debug`

**What You'll See**:
- Filter injection into GraphRel nodes
- Number of filters found for each alias
- Filter extraction success/failure
- Combined filter application
- GraphRel filter extraction status

### WARN Level (Errors Only)
Shows only warnings and errors (always enabled by default).

**What You'll See**:
- Failed LogicalExpr → RenderExpr conversions
- Other recoverable errors

## Usage Examples

### PowerShell (Windows)
```powershell
# Enable TRACE logging
$env:RUST_LOG="trace"
cargo run --bin brahmand

# Enable DEBUG logging for just the query planner
$env:RUST_LOG="brahmand::query_planner=debug"
cargo run --bin brahmand

# Enable DEBUG for multiple modules
$env:RUST_LOG="brahmand::query_planner=debug,brahmand::render_plan=debug"
cargo run --bin brahmand
```

### Bash (Linux/Mac)
```bash
# Enable TRACE logging
RUST_LOG=trace cargo run --bin brahmand

# Enable DEBUG logging for just the query planner
RUST_LOG=brahmand::query_planner=debug cargo run --bin brahmand
```

## Key Log Messages

### Filter Injection (DEBUG)
```
Found 1 filters for left connection alias 'a'
Found 1 filters for right connection alias 'b'
Injecting combined filter into GraphRel
```

### Filter Categorization (DEBUG + TRACE)
```
Categorizing filters for start alias 'a' and end alias 'b'  [DEBUG]
Filter expression: PropertyAccessExp(...)  [TRACE]
Filter categorization result:  [TRACE]
  Start filters: Some(...)
  End filters: Some(...)
```

### Optimizer Plan Structure (TRACE)
```
Initial optimization: Plan structure before FilterIntoGraphRel:
Projection
  GraphRel (alias: r)
    ...
```

## Debugging Workflows

### Issue: Filters Not Appearing in SQL
1. Enable DEBUG logging: `RUST_LOG=brahmand=debug`
2. Look for "Found X filters for alias" messages
   - If count is 0, filters aren't being stored in plan_ctx
   - Check optimizer pass registration
3. Check for "Injecting combined filter" message
   - If missing, FilterIntoGraphRel pass may not be running

### Issue: Filters Applied to Wrong Node
1. Enable TRACE logging: `RUST_LOG=brahmand=trace`
2. Look for "Filter categorization result" messages
3. Verify start_filters vs end_filters match expected aliases
4. Check column qualification: Column("name") should become PropertyAccessExp(a.name)

### Issue: SQL Generation Problems
1. Enable TRACE logging
2. Find "Converted filters to SQL" messages
3. Compare start_filter_sql and end_filter_sql with expected output
4. Check for proper WHERE clause placement in recursive CTE

## Performance Impact

- **TRACE logging**: Significant overhead (~10-30% slowdown due to frequent logging)
- **DEBUG logging**: Minimal overhead (~1-5% slowdown)
- **Production**: Always disable TRACE/DEBUG in production (use WARN or INFO)

## Code Locations

Logging is implemented in these files:
- `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs` - Filter injection logic
- `brahmand/src/query_planner/optimizer/mod.rs` - Plan structure logging
- `brahmand/src/render_plan/plan_builder.rs` - CTE generation and filter categorization

## Related Documentation

- [WHERE Clause Implementation](../notes/where-clause-filters.md)
- [FilterIntoGraphRel Optimizer Pass](../notes/filter-into-graph-rel.md)
- [Testing Guide](../TESTING_GUIDE.md)
