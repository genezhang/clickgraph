# Configurable CTE Depth Limit

## Overview

The maximum recursive CTE evaluation depth for variable-length path queries is now configurable instead of hardcoded. This allows you to balance between query flexibility and resource protection.

## Configuration Methods

### 1. Environment Variable (Recommended)

Set the `CLICKGRAPH_MAX_CTE_DEPTH` environment variable:

```bash
# Linux/macOS
export CLICKGRAPH_MAX_CTE_DEPTH=150

# Windows PowerShell
$env:CLICKGRAPH_MAX_CTE_DEPTH=150

# Windows CMD
set CLICKGRAPH_MAX_CTE_DEPTH=150
```

### 2. Command-Line Argument

Pass the `--max-cte-depth` flag when starting the server:

```bash
cargo run --bin brahmand -- --max-cte-depth 150

# Or with the built binary
./brahmand --max-cte-depth 150
```

### 3. Docker Environment

In your docker-compose.yaml:

```yaml
services:
  clickgraph:
    environment:
      - BRAHMAND_MAX_CTE_DEPTH=150
```

## Default Value

**Default: 100**

This is a reasonable default that:
- Prevents resource exhaustion from overly deep recursion
- Handles most practical graph traversal scenarios
- Provides clear error messages when depth is exceeded

## Why 100?

The original default of 1000 was too large and could lead to:
- Very long execution times for inefficient queries
- Memory exhaustion on large graphs
- Difficulty debugging "runaway" queries

With 100 as the default:
- Most real-world queries (friends of friends, recommendation systems, etc.) complete successfully
- Resource usage is more predictable
- Failures are caught earlier with clearer error messages

## Tuning Guidelines

### Conservative (Small Graphs, Strict Control)
```bash
BRAHMAND_MAX_CTE_DEPTH=50
```
Good for: Development, testing, small datasets

### Balanced (Default)
```bash
BRAHMAND_MAX_CTE_DEPTH=100
```
Good for: Most production workloads, medium-sized graphs

### Aggressive (Large Graphs, Flexible)
```bash
BRAHMAND_MAX_CTE_DEPTH=500
```
Good for: Large social networks, complex recommendation systems
⚠️ Warning: Monitor resource usage carefully

### Maximum (Use with Caution)
```bash
BRAHMAND_MAX_CTE_DEPTH=1000
```
Good for: One-time analytics, offline batch processing
⚠️ Warning: Can cause significant performance issues

## Query Examples

### Within Default Limit (100 hops)
```cypher
// Find paths up to 5 hops - works fine
MATCH (u:User {name: 'Alice'})-[:FOLLOWS*1..5]->(other:User)
RETURN other.name

// Find friends of friends - works fine  
MATCH (u:User {name: 'Alice'})-[:FOLLOWS*2]->(other:User)
RETURN other.name
```

### Exceeding Default Limit
```cypher
// This will fail with default limit of 100
MATCH (u:User {name: 'Alice'})-[:FOLLOWS*1..150]->(other:User)
RETURN other.name

// Error: ClickHouse will report:
// "Code: 403. DB::Exception: Maximum recursive CTE evaluation depth exceeded"
```

### Solution: Increase Limit for Specific Use Case
```bash
# Temporarily increase limit
BRAHMAND_MAX_CTE_DEPTH=200 cargo run --bin brahmand
```

## SQL Generation Impact

The configuration affects the ClickHouse `SETTINGS` clause in generated SQL:

```sql
-- With default (100)
WITH RECURSIVE path_cte AS (...)
SELECT ...
SETTINGS max_recursive_cte_evaluation_depth = 100;

-- With custom value (200)
WITH RECURSIVE path_cte AS (...)
SELECT ...
SETTINGS max_recursive_cte_evaluation_depth = 200;
```

## Performance Optimization

For better performance on large graphs:

1. **Use Exact Hop Counts**: `*3` is much faster than `*1..3` (uses chained JOINs instead of recursive CTEs)
2. **Limit Result Size**: Add `LIMIT` clauses to your queries
3. **Add Property Filters**: Reduce search space with `WHERE` conditions
4. **Use Appropriate Depth**: Don't set max_cte_depth higher than you need

## Monitoring

Watch for these signs that you may need to adjust the limit:

### Too Low
- Frequent "Maximum recursive CTE evaluation depth exceeded" errors
- Valid queries failing that should succeed

### Too High
- Long query execution times
- ClickHouse memory/CPU spikes
- Unresponsive server during queries

## Implementation Details

**Files Modified:**
- `brahmand/src/server/mod.rs`: ServerConfig struct with max_cte_depth field
- `brahmand/src/main.rs`: CLI argument parsing
- `brahmand/src/clickhouse_query_generator/to_sql_query.rs`: Dynamic SQL generation
- `brahmand/src/server/handlers.rs`: Pass config to SQL generator

**Configuration Flow:**
1. Environment variable or CLI argument parsed
2. Stored in `ServerConfig.max_cte_depth`
3. Passed to `AppState`
4. Used in `generate_sql()` function
5. Injected into ClickHouse SQL SETTINGS clause

## Related Documentation

- [Variable-Length Paths](../STATUS_REPORT.md#variable-length-paths)
- [Chained JOIN Optimization](../CHAINED_JOIN_OPTIMIZATION.md)
- [Known Issues](../KNOWN_ISSUES.md)
