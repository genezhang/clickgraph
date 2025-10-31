# ClickGraph Benchmark Suite

Comprehensive performance benchmarking for ClickGraph - a Cypher-on-ClickHouse graph query engine.

## Overview

This benchmark suite evaluates ClickGraph performance across multiple dimensions:

- **Query Types**: Simple lookups, traversals, aggregations, shortest paths, variable-length paths
- **Datasets**: Social network and e-commerce scenarios
- **Scalability**: Small, medium, and large dataset sizes
- **Metrics**: Latency, throughput, resource usage, query success rates

## Quick Start

### 1. Set up ClickHouse and ClickGraph

```bash
# Start ClickHouse
docker-compose up -d

# Build and start ClickGraph
cargo build --release
./target/release/brahmand
```

### 2. Generate Benchmark Data

```bash
# Small social network dataset (1K users, 5K follows)
python benchmark/setup_benchmark_data.py --dataset social --size small

# Small e-commerce dataset (1K customers, 500 products, 3K orders)
python benchmark/setup_benchmark_data.py --dataset ecommerce --size small

# Both datasets
python benchmark/setup_benchmark_data.py --dataset all --size small
```

### 3. Run Benchmarks

```bash
# Basic benchmark (social network, all query types, 3 iterations)
python benchmark/benchmark.py --dataset social --queries all --iterations 3

# Specific query types
python benchmark/benchmark.py --dataset social --queries simple traversal --iterations 5

# E-commerce benchmark with results saved
python benchmark/benchmark.py --dataset ecommerce --queries aggregation --iterations 10 --output benchmark/benchmark_results/ecommerce_results.json

# Full benchmark suite
python benchmark/run_benchmarks.py                    # Default suite
python benchmark/run_benchmarks.py --comprehensive   # Full evaluation
python benchmark/run_benchmarks.py --quick           # Fast validation
```

## Datasets

### Social Network Dataset

**Schema**:
- **Users**: user_id, full_name, email, registration_date, is_active, country, city
- **Follows**: follower_id, followed_id, follow_date
- **Posts**: post_id, author_id, title, content, post_date
- **Likes**: user_id, post_id, like_date

**Sizes**:
- Small: 1K users, 5K follows, 2K posts
- Medium: 10K users, 80K follows, 30K posts
- Large: 50K users, 500K follows, 250K posts

### E-commerce Dataset

**Schema**:
- **Customers**: customer_id, email, name, age, gender, location, spending
- **Products**: product_id, name, category, brand, price, rating, reviews
- **Orders**: order_id, customer_id, product_id, quantity, amounts, dates
- **Reviews**: review_id, customer_id, product_id, rating, text, helpfulness

**Sizes**:
- Small: 1K customers, 500 products, 3K orders
- Medium: 10K customers, 2K products, 50K orders
- Large: 50K customers, 10K products, 400K orders

## Query Types

### Simple Queries
- Node lookups by ID
- Basic filtering and counting
- Single table operations

### Traversal Queries
- Direct relationship navigation
- Multi-hop traversals
- Relationship property access

### Variable-Length Path Queries
- Fixed-length paths (`*2`, `*3`)
- Variable-length ranges (`*1..3`, `*2..5`)
- Path length calculations

### Shortest Path Queries
- `shortestPath()` function
- Path length and node/relationship extraction
- Filtered shortest paths

### Aggregation Queries
- COUNT, SUM, AVG operations
- GROUP BY with graph patterns
- Top-N ranking queries

### Complex Queries
- Multi-pattern matching
- Nested aggregations
- Advanced filtering

## Performance Metrics

The benchmark collects comprehensive performance data:

### Timing Metrics
- **Total Query Time**: End-to-end response time
- **Parse Time**: Cypher parsing duration
- **Planning Time**: Query planning and optimization
- **Render Time**: Logical plan to SQL conversion
- **SQL Generation Time**: Final SQL string creation
- **Execution Time**: ClickHouse query execution

### Success Metrics
- **Success Rate**: Percentage of successful queries
- **Error Analysis**: Failure patterns and root causes
- **Result Counts**: Number of returned records

### Statistical Analysis
- **Mean/Median/Min/Max**: Response time distributions
- **Standard Deviation**: Query performance consistency
- **Percentiles**: P50, P95, P99 response times

## Output Formats

### Console Output
Real-time progress with per-query results:
```
🔍 Running: social_simple_node_lookup
📝 Simple node lookup by ID
  Iteration 1/3... ✅ 0.045s
  Iteration 2/3... ✅ 0.042s
  Iteration 3/3... ✅ 0.048s

📊 BENCHMARK SUMMARY
Dataset: social
Query Types: simple, traversal, variable_length
Iterations: 3

✅ Overall Success: 12/12 queries successful
📈 Total Time - Mean: 0.125s, Median: 0.089s, P95: 0.234s
```

### JSON Output
Detailed results for analysis:
```json
{
  "benchmark_info": {
    "dataset": "social",
    "query_types": ["simple", "traversal"],
    "iterations": 3,
    "timestamp": "2025-10-30T10:30:00",
    "server_url": "http://localhost:8080"
  },
  "results": [
    {
      "query_name": "social_simple_node_lookup",
      "description": "Simple node lookup by ID",
      "category": "simple",
      "success_rate": 1.0,
      "total_time_stats": {
        "mean": 0.045,
        "median": 0.045,
        "min": 0.042,
        "max": 0.048,
        "stdev": 0.003
      }
    }
  ]
}
```

## Benchmark Scenarios

### Development Testing
```bash
# Quick validation with small dataset
python setup_benchmark_data.py --dataset social --size small
python benchmark.py --dataset social --queries simple --iterations 1
```

### Performance Regression Testing
```bash
# Consistent benchmark for comparing versions
python benchmark.py --dataset social --queries all --iterations 10 --output regression_test.json
```

### Scalability Testing
```bash
# Test performance at different scales
python setup_benchmark_data.py --dataset social --size small
python benchmark.py --dataset social --queries all --iterations 5

python setup_benchmark_data.py --dataset social --size medium
python benchmark.py --dataset social --queries all --iterations 3
```

### Comparative Analysis
```bash
# Compare different query types
python benchmark.py --dataset social --queries simple --iterations 10 --output simple.json
python benchmark.py --dataset social --queries complex --iterations 10 --output complex.json
```

## Interpreting Results

### Performance Expectations

**Simple Queries**: < 50ms average
- Node lookups, basic filters
- Should be very fast (< 10ms)

**Traversal Queries**: 50-200ms average
- Direct relationships, multi-hop
- Depends on join complexity

**Variable-Length Paths**: 200-1000ms average
- Recursive CTEs with depth limits
- Performance depends on path length and data size

**Shortest Path Queries**: 500-2000ms average
- Complex recursive algorithms
- Most expensive query type

**Aggregation Queries**: 100-500ms average
- GROUP BY operations on graph patterns
- Depends on result set size

### Common Issues

**High Variance**: Some queries may have inconsistent performance
- **Cause**: ClickHouse caching, concurrent queries
- **Solution**: Run multiple iterations, focus on median performance

**Memory Issues**: Large datasets may cause out-of-memory errors
- **Cause**: ClickHouse memory limits, complex queries
- **Solution**: Reduce dataset size, optimize queries, increase memory limits

**Timeout Errors**: Long-running queries may timeout
- **Cause**: Complex shortest path queries, large datasets
- **Solution**: Increase timeout limits, optimize queries

## Advanced Usage

### Custom Query Suites

You can modify `benchmark.py` to add custom queries:

```python
def get_custom_queries(self):
    return [
        {
            "name": "my_custom_query",
            "query": "MATCH (u:User) WHERE u.age > 25 RETURN COUNT(u)",
            "description": "Custom age filter query",
            "category": "custom"
        }
    ]
```

### Performance Profiling

Enable detailed logging for performance analysis:

```bash
RUST_LOG=debug ./target/release/clickgraph
```

### Comparative Benchmarks

Compare ClickGraph against other graph databases:

1. **Neo4j**: Use the same Cypher queries
2. **TigerGraph**: Adapt queries to GSQL
3. **Amazon Neptune**: Use Gremlin or SPARQL
4. **ArangoDB**: Use AQL

## Contributing

To add new benchmark queries or datasets:

1. **New Query Types**: Add to `get_benchmark_queries()` method
2. **New Datasets**: Create new setup method in `BenchmarkDataGenerator`
3. **Custom Metrics**: Extend `QueryPerformanceMetrics` struct
4. **Analysis Tools**: Add result analysis functions

## Troubleshooting

### Connection Issues
```
❌ Cannot connect to server at http://localhost:8080
```
- Ensure ClickGraph is running: `cargo run --bin clickgraph`
- Check server logs for startup errors
- Verify port 8080 is not in use

### ClickHouse Connection Failed
```
❌ Cannot connect to ClickHouse server
```
- Ensure ClickHouse is running: `docker ps | grep clickhouse`
- Check ClickHouse logs: `docker logs clickhouse`
- Verify connection URL and credentials

### Out of Memory Errors
```
❌ Query execution failed: Memory limit exceeded
```
- Reduce dataset size: `--size small`
- Increase ClickHouse memory limits
- Optimize complex queries

### Timeout Errors
```
❌ Query execution failed: Timeout
```
- Increase timeout in benchmark script
- Optimize long-running queries
- Consider query complexity vs. dataset size

---

**For questions or issues, please file a GitHub issue with benchmark results and system information.**