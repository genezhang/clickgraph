> **Note**: This documentation is for ClickGraph v0.5.2. [View latest docs →](../../wiki/Home.md)
# Performance & Query Optimization

Comprehensive guide to optimizing ClickGraph query performance and schema design.

## Table of Contents
- [Understanding Query Performance](#understanding-query-performance)
- [Query Optimization Techniques](#query-optimization-techniques)
- [Schema Design Best Practices](#schema-design-best-practices)
- [ClickHouse Optimization](#clickhouse-optimization)
- [Caching Strategies](#caching-strategies)
- [Benchmarking and Profiling](#benchmarking-and-profiling)
- [Common Performance Issues](#common-performance-issues)

---

## Understanding Query Performance

### Performance Baseline

**Expected Performance** (with proper optimization):

| Graph Size | Simple Query | 2-Hop Traversal | Variable-Length Path |
|-----------|--------------|-----------------|---------------------|
| **1K nodes** | < 10ms | < 50ms | < 100ms |
| **10K nodes** | < 20ms | < 100ms | < 500ms |
| **100K nodes** | < 50ms | < 200ms | < 1s |
| **1M nodes** | < 100ms | < 500ms | < 3s |
| **10M nodes** | < 500ms | < 2s | < 10s |

**Cache Impact**:
- **Cold cache**: Full query execution time
- **Warm cache**: 10-100x faster (1-5ms typical)

### Query Cost Factors

```
Total Query Time = 
  Parse Time (1-5ms)
  + Plan Time (1-10ms, cached after first run)
  + SQL Generation (1-5ms)
  + ClickHouse Execution (varies widely)
  + Result Serialization (1-10ms)
```

**Most Expensive Operations**:
1. **Full table scans** - Reading all rows without filters
2. **Large JOINs** - Joining millions of rows
3. **Variable-length paths** - Recursive queries with deep traversals
4. **Unfiltered aggregations** - Aggregating millions of rows

---

## Query Optimization Techniques

### 1. Use Filters Early

**❌ Bad: Filter after traversal**
```cypher
MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User)
WHERE u1.user_id = 1
RETURN u2.name
```

**✅ Good: Filter before traversal**
```cypher
MATCH (u1:User {user_id: 1})-[:FOLLOWS*1..3]->(u2:User)
RETURN u2.name
```

**Why**: Filters in the MATCH pattern are applied earlier, reducing the number of rows joined.

### 2. Limit Result Sets

**❌ Bad: No limit**
```cypher
MATCH (u:User)-[:FOLLOWS]->(friend:User)
WHERE u.country = 'USA'
RETURN friend.name
```

**✅ Good: Use LIMIT**
```cypher
MATCH (u:User)-[:FOLLOWS]->(friend:User)
WHERE u.country = 'USA'
RETURN friend.name
LIMIT 100
```

**Impact**: Can reduce query time from seconds to milliseconds.

### 3. Use Exact Hops for Fixed-Length Paths

**❌ Bad: Using variable-length for fixed hops**
```cypher
MATCH (u1:User)-[:FOLLOWS*2]->(u2:User)
RETURN u2.name
```

**✅ Good: Use explicit hops**
```cypher
MATCH (u1:User)-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u2:User)
RETURN u2.name
```

**Why**: ClickGraph optimizes explicit hops into direct JOINs, avoiding recursive CTEs.

**Performance Comparison**:
```
Explicit 2-hop:  50ms  (direct JOIN)
Variable *2:     200ms (recursive CTE)
Variable *1..2:  150ms (recursive CTE)
```

### 4. Minimize Path Depth

**❌ Bad: Deep traversal without limits**
```cypher
MATCH path = (u1:User)-[:FOLLOWS*..10]->(u2:User)
WHERE u1.user_id = 1
RETURN length(path)
```

**✅ Good: Shallow traversal with LIMIT**
```cypher
MATCH path = (u1:User)-[:FOLLOWS*1..3]->(u2:User)
WHERE u1.user_id = 1
RETURN length(path)
LIMIT 1000
```

**Depth Impact**:
```
Depth 1-2:  Fast   (< 100ms)
Depth 3-4:  Medium (< 1s)
Depth 5-6:  Slow   (1-10s)
Depth 7+:   Very slow (10s+)
```

**Configure Max Recursion**:
```bash
# Limit recursion depth (default: 100)
export MAX_RECURSION_DEPTH=10

# Or via CLI
clickgraph --max-recursion 10
```

### 5. Use Indexed Columns in WHERE

**❌ Bad: Filter on non-indexed column**
```cypher
MATCH (u:User)
WHERE u.email = 'alice@example.com'
RETURN u
```

**✅ Good: Filter on primary key**
```cypher
MATCH (u:User)
WHERE u.user_id = 1
RETURN u
```

**Why**: Primary key lookups use ClickHouse's primary index (10-1000x faster).

### 6. Aggregate Before Traversal

**❌ Bad: Traverse then aggregate**
```cypher
MATCH (u:User)-[:FOLLOWS]->(friend:User)
WITH u, collect(friend) AS friends
WHERE size(friends) > 100
RETURN u.name
```

**✅ Good: Aggregate in ClickHouse**
```cypher
MATCH (u:User)-[:FOLLOWS]->(friend:User)
WITH u, count(friend) AS friend_count
WHERE friend_count > 100
RETURN u.name
```

**Impact**: Reduces data transferred and processed.

### 7. Use DISTINCT Sparingly

**❌ Bad: Unnecessary DISTINCT**
```cypher
MATCH (u:User)
WHERE u.country = 'USA'
RETURN DISTINCT u.name
```

**✅ Good: Let ClickHouse optimize**
```cypher
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name
```

**When to use DISTINCT**:
- Only when you expect duplicates from multi-hop traversals
- Use `LIMIT` first to reduce rows before `DISTINCT`

### 8. Optimize Relationship Queries

**❌ Bad: Multiple relationship types as separate queries**
```cypher
MATCH (u:User)-[:FOLLOWS]->(f1:User)
RETURN f1.name
UNION
MATCH (u:User)-[:FRIENDS_WITH]->(f2:User)
RETURN f2.name
```

**✅ Good: Use alternate relationship types**
```cypher
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH]->(friend:User)
RETURN friend.name
```

**Why**: Single query with UNION ALL is faster than separate queries.

---

## Schema Design Best Practices

### 1. Choose the Right Primary Key

**For Node Tables**:
```yaml
nodes:
  User:
    source_table: users_bench
    identifier_property: user_id  # Primary key column
    property_mappings:
      user_id: user_id  # Indexed column
```

**ClickHouse Table**:
```sql
CREATE TABLE users_bench (
    user_id UInt64,
    full_name String,
    email_address String,
    country String,
    city String
) ENGINE = MergeTree()
ORDER BY user_id  -- Primary key for fast lookups
;
```

**Composite Keys for Common Filters**:
```sql
-- If you often filter by country + city
ORDER BY (country, city, user_id)

-- ✅ Fast: Uses index
SELECT * FROM users_bench WHERE country = 'USA' AND city = 'NYC'

-- ❌ Slower: Partial index use
SELECT * FROM users_bench WHERE city = 'NYC'
```

### 2. Optimize Relationship Tables

**Forward Index** (for outgoing edges):
```sql
CREATE TABLE user_follows_bench (
    follower_id UInt64,
    followed_id UInt64,
    follow_date Date
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id)  -- Fast "who does X follow?"
;
```

**Reverse Index** (for incoming edges):
```sql
CREATE MATERIALIZED VIEW user_follows_reverse
ENGINE = MergeTree()
ORDER BY (followed_id, follower_id)
AS SELECT followed_id, follower_id, follow_date
FROM user_follows_bench;
```

**Usage**:
```cypher
-- Forward: Uses user_follows_bench
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(friend)
RETURN friend.name

-- Backward: Uses user_follows_reverse
MATCH (u:User)<-[:FOLLOWS]-(follower {user_id: 1})
RETURN follower.name
```

**Impact**: 10-100x faster bidirectional traversals.

### 3. Partitioning for Large Tables

```sql
-- Partition by date for time-series data
CREATE TABLE events (
    event_id UInt64,
    user_id UInt64,
    event_type String,
    event_date Date
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)  -- Monthly partitions
ORDER BY (event_date, user_id);

-- Partition by country for geo data
CREATE TABLE users_global (
    user_id UInt64,
    country String,
    ...
) ENGINE = MergeTree()
PARTITION BY country
ORDER BY user_id;
```

**Benefits**:
- Query only relevant partitions (partition pruning)
- Easier data lifecycle management (drop old partitions)
- Parallel query execution across partitions

**Queries**:
```cypher
-- Only scans 2023-11 partition
MATCH (u:User)
WHERE u.registration_date >= '2023-11-01' 
  AND u.registration_date < '2023-12-01'
RETURN count(u)
```

### 4. Use Appropriate Data Types

**❌ Bad: String for numeric IDs**
```sql
user_id String  -- Wastes space, slower joins
```

**✅ Good: Numeric types**
```sql
user_id UInt64  -- 8 bytes, fast joins
```

**Type Comparison**:
```
UInt64:   8 bytes,  10-100x faster than String
UInt32:   4 bytes,  sufficient for IDs < 4B
String:   variable, slower comparisons
Date:     2 bytes,  use for date columns
DateTime: 4 bytes,  use for timestamps
```

### 5. Materialized Views for Common Queries

**Pre-compute Aggregations**:
```sql
-- Expensive query (runs every time)
SELECT user_id, count(*) AS post_count
FROM posts
GROUP BY user_id;

-- Materialized view (pre-computed)
CREATE MATERIALIZED VIEW user_post_counts
ENGINE = SummingMergeTree()
ORDER BY user_id
AS SELECT user_id, count() AS post_count
FROM posts
GROUP BY user_id;

-- Now queries are instant
SELECT * FROM user_post_counts WHERE user_id = 1;
```

---

## ClickHouse Optimization

### 1. Memory Settings

```xml
<!-- /etc/clickhouse-server/config.xml -->
<yandex>
    <!-- Increase memory limits for large queries -->
    <max_memory_usage>20000000000</max_memory_usage> <!-- 20GB -->
    <max_bytes_before_external_sort>30000000000</max_bytes_before_external_sort>
    <max_bytes_before_external_group_by>30000000000</max_bytes_before_external_group_by>
    
    <!-- Parallel execution -->
    <max_threads>8</max_threads>
    
    <!-- Query limits -->
    <max_execution_time>60</max_execution_time> <!-- 60 seconds -->
</yandex>
```

### 2. Table Compression

```sql
-- Default compression (LZ4)
CREATE TABLE users (...) ENGINE = MergeTree()
SETTINGS index_granularity = 8192;

-- Better compression (ZSTD)
CREATE TABLE users_compressed (...) ENGINE = MergeTree()
SETTINGS index_granularity = 8192,
         compression_method = 'zstd',
         compression_level = 3;  -- 1-9, higher = better compression
```

**Compression Impact**:
```
LZ4:   Fast,    2-3x compression
ZSTD:  Slower,  3-5x compression (recommended)
```

### 3. Index Granularity

```sql
-- Default: Good for most workloads
SETTINGS index_granularity = 8192

-- Smaller: Better for selective queries (high cardinality)
SETTINGS index_granularity = 4096

-- Larger: Better for scans (low cardinality)
SETTINGS index_granularity = 16384
```

**Trade-offs**:
```
Smaller granularity: More index entries, faster point queries, more memory
Larger granularity:  Fewer index entries, faster scans, less memory
```

### 4. OPTIMIZE TABLE

```sql
-- After bulk inserts
INSERT INTO users SELECT * FROM staging_users;
OPTIMIZE TABLE users FINAL;

-- Scheduled optimization (cron)
OPTIMIZE TABLE users;  -- Merge small parts
```

**When to Optimize**:
- After large batch inserts
- Before running critical analytics
- Weekly for active tables

### 5. Query Result Cache

```xml
<!-- Enable query cache -->
<query_cache>
    <max_size_in_bytes>10000000000</max_size_in_bytes> <!-- 10GB -->
    <max_entries>10000</max_entries>
    <max_entry_size_in_bytes>1000000</max_entry_size_in_bytes>
</query_cache>
```

---

## Caching Strategies

### 1. ClickGraph Query Plan Cache

**Built-in caching** (automatic):
- **First query**: Parse → Plan → Execute (100ms)
- **Second query**: Use cached plan (1ms)
- **Speedup**: 10-100x for repeated queries

**Cache Behavior**:
```cypher
-- First execution: 150ms (cold)
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f:User)
RETURN f.name

-- Second execution: 2ms (cached plan)
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f:User)
RETURN f.name

-- Different parameter: Still uses cache!
MATCH (u:User {user_id: 2})-[:FOLLOWS]->(f:User)
RETURN f.name  -- 2ms (same plan structure)
```

**Cache Statistics**:
```bash
# View cache hit rate in logs
[INFO] Query plan cache hit rate: 87.3%
[INFO] Cache entries: 1,247 / 10,000
```

### 2. Application-Level Caching

**Redis for Results**:
```python
import redis
import hashlib
import json

redis_client = redis.Redis(host='localhost', port=6379)

def cached_query(query: str, ttl: int = 300):
    # Generate cache key
    cache_key = f"cg:{hashlib.md5(query.encode()).hexdigest()}"
    
    # Check cache
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    # Execute query
    result = requests.post('http://clickgraph:8080/query', 
                          json={'query': query}).json()
    
    # Cache result
    redis_client.setex(cache_key, ttl, json.dumps(result))
    return result

# Usage
result = cached_query("MATCH (u:User) RETURN count(u)")
```

### 3. CDN for Static Results

**Cache static queries at edge**:
```nginx
# nginx caching
proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=query_cache:10m max_size=1g inactive=1h;

location /query {
    proxy_cache query_cache;
    proxy_cache_valid 200 5m;  # Cache 200 OK for 5 minutes
    proxy_cache_key "$request_method$request_uri$request_body";
    
    proxy_pass http://clickgraph_backend;
}
```

---

## Benchmarking and Profiling

### 1. Built-in Benchmarks

```bash
# Run performance benchmarks
cd benchmarks
python run_benchmarks.py --schema schemas/social_benchmark.yaml

# Sample output:
# Query 1 (Node Count):          12ms  (cold: 45ms)
# Query 2 (2-hop Friends):        89ms  (cold: 234ms)
# Query 3 (Variable-length *1..3): 456ms (cold: 1.2s)
```

### 2. Query Timing

**Measure End-to-End Time**:
```bash
# PowerShell
Measure-Command {
    Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
        -ContentType "application/json" `
        -Body '{"query":"MATCH (u:User) RETURN count(u)"}'
}

# Output: TotalMilliseconds: 47.32
```

**ClickHouse Query Log**:
```sql
-- Enable query log
SET log_queries = 1;

-- View slow queries
SELECT 
    query,
    query_duration_ms,
    read_rows,
    read_bytes
FROM system.query_log
WHERE query_duration_ms > 1000  -- Queries > 1 second
ORDER BY query_duration_ms DESC
LIMIT 10;
```

### 3. EXPLAIN Query Plans

**ClickGraph SQL Inspection**:
```bash
# Generate SQL without executing
export GRAPH_CONFIG_PATH="benchmarks/schemas/social_benchmark.yaml"

# Use sql_only parameter (not implemented yet, coming soon!)
# For now, check server logs with debug level
export RUST_LOG=debug
cargo run --bin clickgraph
```

**ClickHouse EXPLAIN**:
```sql
-- See query execution plan
EXPLAIN SELECT * FROM users_bench WHERE user_id = 1;

-- Output:
-- Expression (Projection)
--   Filter (WHERE user_id = 1)
--     ReadFromMergeTree (users_bench)
```

### 4. Load Testing

**Apache Bench**:
```bash
# 1000 requests, 10 concurrent
ab -n 1000 -c 10 -p query.json -T application/json \
   http://localhost:8080/query

# query.json
{"query": "MATCH (u:User {user_id: 1}) RETURN u.name"}
```

**Custom Load Test** (`load_test.py`):
```python
import asyncio
import aiohttp
import time

async def run_query(session, query):
    async with session.post('http://localhost:8080/query',
                           json={'query': query}) as resp:
        return await resp.json()

async def load_test(num_requests=1000, concurrency=10):
    query = "MATCH (u:User) RETURN u.name LIMIT 10"
    
    async with aiohttp.ClientSession() as session:
        start = time.time()
        
        # Run queries in batches
        for i in range(0, num_requests, concurrency):
            tasks = [run_query(session, query) for _ in range(concurrency)]
            await asyncio.gather(*tasks)
        
        duration = time.time() - start
        qps = num_requests / duration
        
        print(f"Completed {num_requests} requests in {duration:.2f}s")
        print(f"QPS: {qps:.2f}")

# Run test
asyncio.run(load_test())
```

---

## Common Performance Issues

### Issue 1: Slow Node Scans

**Symptoms**:
- Query takes seconds for simple MATCH
- No WHERE clause with indexed columns

**Diagnosis**:
```cypher
-- Slow: Full table scan
MATCH (u:User)
RETURN u.name
```

**Solutions**:
```cypher
-- ✅ Add LIMIT
MATCH (u:User)
RETURN u.name
LIMIT 1000

-- ✅ Add WHERE filter on indexed column
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name

-- ✅ Use specific ID
MATCH (u:User {user_id: 1})
RETURN u.name
```

### Issue 2: Deep Path Queries

**Symptoms**:
- Variable-length queries timeout
- High memory usage

**Diagnosis**:
```cypher
-- Slow: Deep traversal
MATCH path = (u1:User)-[:FOLLOWS*..10]->(u2:User)
WHERE u1.user_id = 1
RETURN length(path)
```

**Solutions**:
```cypher
-- ✅ Limit depth
MATCH path = (u1:User)-[:FOLLOWS*1..3]->(u2:User)
WHERE u1.user_id = 1
RETURN length(path)
LIMIT 100

-- ✅ Configure max recursion
# export MAX_RECURSION_DEPTH=5
```

### Issue 3: Large Result Sets

**Symptoms**:
- High network traffic
- Slow response times
- Memory issues

**Diagnosis**:
```cypher
-- Returns millions of rows
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u.name, f.name
```

**Solutions**:
```cypher
-- ✅ Use aggregation
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u.name, count(f) AS friend_count

-- ✅ Add LIMIT
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u.name, f.name
LIMIT 1000

-- ✅ Filter earlier
MATCH (u:User {country: 'USA'})-[:FOLLOWS]->(f:User)
RETURN u.name, f.name
LIMIT 100
```

### Issue 4: Unoptimized JOINs

**Symptoms**:
- Slow multi-hop queries
- ClickHouse using full scans

**Diagnosis**:
```sql
-- Generated SQL with missing indexes
SELECT t2.name
FROM users_bench t1
JOIN user_follows_bench r ON t1.user_id = r.follower_id
JOIN users_bench t2 ON r.followed_id = t2.user_id
WHERE t1.email = 'alice@example.com'  -- Not indexed!
```

**Solutions**:
```yaml
# Add indexed properties to schema
nodes:
  User:
    identifier_property: user_id  # Indexed
    property_mappings:
      user_id: user_id  # Use this in WHERE

# ✅ Query with indexed column
# MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f)
```

### Issue 5: Cache Misses

**Symptoms**:
- Inconsistent query performance
- All queries slow (no caching benefit)

**Diagnosis**:
```bash
# Check cache hit rate in logs
[INFO] Query plan cache hit rate: 12.3%  # Too low!
```

**Causes**:
- Queries with varying structure (not parameterized)
- Schema changes invalidating cache

**Solutions**:
```cypher
-- ❌ Different query structures
MATCH (u:User) WHERE u.user_id = 1 RETURN u.name
MATCH (u:User {user_id: 1}) RETURN u.name  -- Different structure!

-- ✅ Use consistent structure
MATCH (u:User) WHERE u.user_id = 1 RETURN u.name
MATCH (u:User) WHERE u.user_id = 2 RETURN u.name  -- Same structure
```

---

## Performance Optimization Checklist

**Query Level**:
- [ ] Use filters in MATCH patterns (`MATCH (u:User {id: 1})`)
- [ ] Filter on indexed columns (primary key)
- [ ] Add LIMIT to all queries (unless aggregating)
- [ ] Use exact hops for fixed-length paths (`-[:REL]->-[:REL]->`)
- [ ] Limit path depth for variable-length (`*1..3` not `*..10`)
- [ ] Aggregate in ClickHouse (not application)
- [ ] Use alternate relationship types (`[:TYPE1|TYPE2]`)

**Schema Level**:
- [ ] Choose appropriate primary keys
- [ ] Create reverse indexes for bidirectional traversals
- [ ] Use numeric types for IDs (not String)
- [ ] Partition large tables (by date/region)
- [ ] Create materialized views for common aggregations
- [ ] Optimize index granularity (8192 default)

**ClickHouse Level**:
- [ ] Configure adequate memory limits (20GB+)
- [ ] Enable query result cache
- [ ] Use ZSTD compression
- [ ] Run OPTIMIZE TABLE after bulk inserts
- [ ] Monitor query_log for slow queries
- [ ] Set max_execution_time limit

**Infrastructure Level**:
- [ ] Run 3+ ClickGraph instances for HA
- [ ] Enable connection pooling (nginx keepalive)
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Configure resource limits (CPU/memory)
- [ ] Use CDN/caching for static results
- [ ] Implement rate limiting

---

## Next Steps

Now that you understand performance optimization:

- **[Benchmarking Guide](Benchmarking-Guide.md)** - Run performance tests
- **[Schema Configuration](Schema-Configuration-Basics.md)** - Design efficient schemas
- **[Production Best Practices](Production-Best-Practices.md)** - Deploy optimally
- **[Troubleshooting](Troubleshooting-Guide.md)** - Debug performance issues

---

[← Back: Production Best Practices](Production-Best-Practices.md) | [Home](Home.md) | [Next: Benchmarking →](Benchmarking-Guide.md)
