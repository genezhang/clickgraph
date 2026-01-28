# Query Performance Metrics

*Completed: October 25, 2025*

## Summary

Implemented comprehensive query performance metrics for ClickGraph, providing detailed timing information for each phase of query processing. This enables monitoring, debugging, and optimization of query performance.

## How It Works

### Performance Metrics Structure

The `QueryPerformanceMetrics` struct tracks timing for all major query processing phases:

```rust
pub struct QueryPerformanceMetrics {
    pub total_time: f64,           // Total end-to-end time
    pub parse_time: f64,           // Cypher parsing time
    pub planning_time: f64,        // Query planning time
    pub render_time: f64,          // Plan rendering time (debug only)
    pub sql_generation_time: f64,  // SQL generation time
    pub execution_time: f64,       // ClickHouse execution time
    pub query_type: String,        // "read", "ddl", "update", "delete", "call"
    pub sql_queries_count: usize,  // Number of SQL queries generated
    pub result_rows: Option<usize>, // Result row count (when available)
}
```

### Phase-by-Phase Timing

The query handler now measures timing for each phase:

1. **Parse Phase**: Time to parse Cypher query into AST
2. **Planning Phase**: Time to create logical query plan
3. **Render Phase**: Time to render plan (debug builds only)
4. **SQL Generation Phase**: Time to generate ClickHouse SQL
5. **Execution Phase**: Time to execute SQL and return results

### Structured Logging

Performance metrics are logged at INFO level with millisecond precision:

```
Query performance - Total: 45.2ms, Parse: 2.1ms, Planning: 15.3ms, Render: 0.1ms, SQL Gen: 8.7ms, Exec: 19.0ms, Type: read, Queries: 1, Rows: 42
```

### HTTP Response Headers

All query responses include performance headers:

```
X-Query-Total-Time: 5.466ms
X-Query-Parse-Time: 0.192ms
X-Query-Planning-Time: 0.199ms
X-Query-Render-Time: 0.049ms
X-Query-SQL-Gen-Time: 0.016ms
X-Query-Execution-Time: 4.972ms
X-Query-Type: read
X-Query-SQL-Count: 1
```

## Key Files

- **`src/server/handlers.rs`**: Main implementation
  - `QueryPerformanceMetrics` struct and methods
  - Phase-by-phase timing in `query_handler()`
  - HTTP header injection

## Design Decisions

### Timing Strategy
- **Start timing at handler entry**: Captures total request processing time
- **Phase-level granularity**: Separate timing for each major processing step
- **Error handling**: Timing continues even when phases fail (for debugging)

### Logging Approach
- **Structured format**: Consistent, parseable log format
- **Millisecond precision**: Sufficient for performance analysis
- **Query truncation**: Long queries truncated to 100 chars in debug logs

### HTTP Headers
- **Standard naming**: `X-Query-*` prefix for custom headers
- **Millisecond units**: Consistent with logging
- **Always included**: Headers added to both success and error responses

## Gotchas & Limitations

### Result Count Extraction
- **Current limitation**: Result count extraction not fully implemented
- **Reason**: Axum's `Body` type makes response parsing complex
- **Future enhancement**: Could track row counts during execution

### Memory Engine Impact
- **Windows constraint**: Memory engine tables don't persist
- **Performance testing**: Need to account for data reloading between queries

### Debug-Only Features
- **Render timing**: Only measured in debug builds to avoid production overhead
- **Plan logging**: Full logical plans only in debug mode

## Future Enhancements

### Aggregated Metrics Endpoint
For monitoring dashboards and long-term performance tracking, consider adding a `/metrics` endpoint that provides:

- Query count and throughput over time
- Average response times by query type
- Error rates and slow query detection
- Most expensive queries

This would require in-memory metric aggregation and possibly Prometheus-style output format.


