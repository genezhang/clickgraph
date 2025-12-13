# ClickHouse Function Pass-Through

ClickGraph provides direct access to **any ClickHouse function** using the `ch.` and `chagg.` prefixes. This enables ClickHouse's powerful analytics capabilities directly from Cypher queries.

## Quick Reference

| Prefix | Use Case | GROUP BY |
|--------|----------|----------|
| `ch.` | Scalar functions + known aggregates (~150) | Auto for aggregates |
| `chagg.` | **Any** aggregate (explicit declaration) | Always generates |

```cypher
-- Scalar function
MATCH (u:User) RETURN ch.cityHash64(u.email) AS hash

-- Known aggregate (auto GROUP BY)
MATCH (u:User) RETURN u.country, ch.uniq(u.user_id) AS unique_users

-- Custom/new aggregate (explicit)
MATCH (u:User) RETURN u.country, chagg.myCustomAgg(u.score) AS result
```

---

## Table of Contents

- [Scalar Functions](#scalar-functions)
  - [Hash Functions](#hash-functions)
  - [JSON Functions](#json-functions)
  - [URL Functions](#url-functions)
  - [IP Address Functions](#ip-address-functions)
  - [Geo Functions](#geo-functions)
  - [Date/Time Functions](#datetime-functions)
  - [String Functions](#string-functions)
  - [Array Functions](#array-functions)
- [Aggregate Functions](#aggregate-functions)
  - [Unique Counting (HyperLogLog)](#unique-counting-hyperloglog)
  - [Quantiles and Percentiles](#quantiles-and-percentiles)
  - [TopK - Most Frequent Values](#topk---most-frequent-values)
  - [ArgMin/ArgMax](#argminargmax)
  - [Array Collection](#array-collection)
  - [Funnel Analysis](#funnel-analysis)
  - [Statistics](#statistics)
  - [Map Aggregates](#map-aggregates)
- [Lambda Expressions](#lambda-expressions) ⭐ **NEW**
- [Explicit Aggregate Prefix: chagg.](#explicit-aggregate-prefix-chagg)
- [Function Reference Tables](#function-reference-tables)
- [Limitations](#limitations)
- [External References](#external-references)

---

## Scalar Functions

The `ch.` prefix works with all ClickHouse scalar (row-level) functions.

### Hash Functions

```cypher
-- Generate hash of email for anonymization
MATCH (u:User)
RETURN u.name, ch.cityHash64(u.email) AS email_hash

-- MD5/SHA256 hashing
MATCH (u:User)
RETURN ch.MD5(u.password) AS md5_hash,
       ch.SHA256(u.password) AS sha256_hash

-- Fast hash for partitioning
MATCH (e:Event)
RETURN ch.xxHash64(e.session_id) % 100 AS partition
```

### JSON Functions

```cypher
-- Extract fields from JSON columns
MATCH (e:Event)
WHERE ch.JSONExtractString(e.metadata, 'type') = 'click'
RETURN e.id, 
       ch.JSONExtractInt(e.metadata, 'x') AS x,
       ch.JSONExtractInt(e.metadata, 'y') AS y

-- Check JSON structure
MATCH (d:Document)
WHERE ch.JSONHas(d.data, 'author')
RETURN d.title, ch.JSONExtractString(d.data, 'author') AS author

-- Navigate nested JSON
MATCH (o:Order)
RETURN ch.JSONExtractString(o.details, 'shipping', 'address', 'city') AS city
```

### URL Functions

```cypher
-- Parse URL components
MATCH (p:Page)
RETURN ch.domain(p.url) AS domain,
       ch.protocol(p.url) AS protocol,
       ch.path(p.url) AS path,
       ch.extractURLParameter(p.url, 'utm_source') AS utm_source

-- Extract query parameters
MATCH (r:Request)
RETURN ch.extractURLParameterNames(r.url) AS param_names
```

### IP Address Functions

```cypher
-- Convert IP formats
MATCH (c:Connection)
RETURN ch.IPv4NumToString(c.src_ip) AS source_ip,
       ch.IPv4NumToString(c.dst_ip) AS dest_ip

-- Check IP ranges (CIDR)
MATCH (c:Connection)
WHERE ch.isIPAddressInRange(ch.IPv4NumToString(c.src_ip), '192.168.0.0/16')
RETURN c

-- IPv6 support
MATCH (c:Connection)
RETURN ch.IPv6NumToString(c.ipv6_addr) AS ipv6_string
```

### Geo Functions

```cypher
-- Calculate distance between coordinates
MATCH (u:User), (s:Store)
RETURN u.name, s.name,
       ch.greatCircleDistance(u.lat, u.lon, s.lat, s.lon) / 1000 AS distance_km
ORDER BY distance_km
LIMIT 5

-- H3 geospatial indexing
MATCH (l:Location)
RETURN l.name, ch.geoToH3(l.lon, l.lat, 7) AS h3_index

-- Point in polygon
MATCH (p:Point)
WHERE ch.pointInPolygon((p.lon, p.lat), [(0,0), (10,0), (10,10), (0,10)])
RETURN p.name
```

### Date/Time Functions

```cypher
-- Format dates with ClickHouse formatDateTime
MATCH (u:User)
RETURN u.name,
       ch.formatDateTime(u.registration_date, '%Y-%m-%d %H:%M:%S') AS formatted_date

-- Date truncation (time series aggregation)
MATCH (e:Event)
RETURN ch.toStartOfHour(e.timestamp) AS hour,
       count(*) AS event_count
ORDER BY hour

-- Date arithmetic
MATCH (o:Order)
RETURN o.id,
       ch.dateDiff('day', o.created_at, o.shipped_at) AS days_to_ship

-- Date components
MATCH (u:User)
RETURN ch.toYYYYMM(u.registration_date) AS month,
       count(*) AS registrations
```

### String Functions

```cypher
-- Regular expression extraction
MATCH (u:User)
RETURN u.email,
       ch.extractAll(u.email, '([^@]+)@([^.]+)') AS email_parts

-- String similarity (fuzzy matching)
MATCH (p:Product)
WHERE ch.ngramDistance(p.name, 'laptop') < 0.3
RETURN p.name, ch.ngramDistance(p.name, 'laptop') AS distance
ORDER BY distance

-- Position search (case insensitive)
MATCH (d:Document)
WHERE ch.positionCaseInsensitive(d.content, 'error') > 0
RETURN d.title
```

### Array Functions

```cypher
-- Array aggregation with special functions
MATCH (u:User)-[:PURCHASED]->(p:Product)
RETURN u.name,
       ch.arrayStringConcat(collect(p.name), ', ') AS products_purchased,
       ch.arraySum(collect(p.price)) AS total_spent

-- Array operations
MATCH (p:Product)
RETURN p.name,
       ch.arrayDistinct(p.tags) AS unique_tags,
       ch.length(p.tags) AS tag_count
```

### Scalar Function Categories

| Category | Examples |
|----------|----------|
| **Hash** | `cityHash64`, `sipHash64`, `MD5`, `SHA256`, `xxHash64` |
| **JSON** | `JSONExtract*`, `JSONHas`, `JSONLength`, `JSONType` |
| **URL** | `domain`, `protocol`, `path`, `extractURLParameter` |
| **IP** | `IPv4NumToString`, `IPv4StringToNum`, `isIPAddressInRange` |
| **Geo** | `greatCircleDistance`, `geoToH3`, `h3ToGeo`, `pointInPolygon` |
| **String** | `extractAll`, `ngramDistance`, `positionCaseInsensitive` |
| **Date** | `toStartOf*`, `dateDiff`, `formatDateTime`, `toYYYYMM` |
| **Array** | `arrayStringConcat`, `arraySum`, `arrayDistinct` |
| **Math** | `intDiv`, `intDivOrZero`, `modulo`, `gcd`, `lcm` |
| **Type** | `reinterpret*`, `accurateCast`, `toTypeName` |

---

## Aggregate Functions

ClickHouse aggregate functions are automatically detected and generate proper GROUP BY clauses.

### Unique Counting (HyperLogLog)

```cypher
-- Approximate unique count (fast, memory efficient)
MATCH (u:User)
RETURN u.country, ch.uniq(u.user_id) AS unique_users

-- Exact unique count (more memory, slower)
MATCH (e:Event)
RETURN e.event_type, ch.uniqExact(e.user_id) AS exact_unique_users

-- HyperLogLog variants for different accuracy/speed tradeoffs
MATCH (p:PageView)
RETURN ch.uniqCombined(p.session_id) AS sessions,
       ch.uniqHLL12(p.user_id) AS users_approx
```

**When to use which:**
| Function | Memory | Speed | Accuracy |
|----------|--------|-------|----------|
| `uniq` | Low | Fast | ~2% error |
| `uniqCombined` | Medium | Medium | ~1% error |
| `uniqExact` | High | Slow | Exact |
| `uniqHLL12` | Very Low | Very Fast | ~3% error |

### Quantiles and Percentiles

```cypher
-- Median (50th percentile)
MATCH (o:Order)
RETURN ch.quantile(0.5)(o.amount) AS median_order_value

-- Multiple quantiles at once (efficient)
MATCH (o:Order)
RETURN ch.quantiles(0.25, 0.5, 0.75, 0.95)(o.amount) AS quartiles

-- High-precision quantile for SLA reporting
MATCH (l:Latency)
RETURN ch.quantileExact(0.99)(l.response_time) AS p99_latency

-- T-Digest for streaming/approximate (memory efficient)
MATCH (m:Metric)
RETURN ch.quantileTDigest(0.95)(m.value) AS p95_approx

-- DDSketch for guaranteed relative error
MATCH (m:Metric)
RETURN ch.quantileDD(0.01)(0.99)(m.value) AS p99_dd
```

### TopK - Most Frequent Values

```cypher
-- Top 10 most common error codes
MATCH (e:Error)
RETURN ch.topK(10)(e.error_code) AS top_errors

-- Weighted TopK (by occurrence count)
MATCH (s:Search)
RETURN ch.topKWeighted(5)(s.query, s.count) AS popular_searches

-- Approximate TopK (even faster for large datasets)
MATCH (l:Log)
RETURN ch.approx_top_k(20)(l.message) AS common_messages
```

### ArgMin/ArgMax

Find the value of one column at the min/max of another:

```cypher
-- Find user with highest score
MATCH (u:User)
RETURN ch.argMax(u.name, u.score) AS top_scorer,
       ch.max(u.score) AS top_score

-- Find earliest event per category
MATCH (e:Event)
RETURN e.category,
       ch.argMin(e.id, e.timestamp) AS first_event_id,
       ch.min(e.timestamp) AS first_timestamp

-- Most recent order per customer
MATCH (o:Order)
RETURN o.customer_id,
       ch.argMax(o.order_id, o.order_date) AS latest_order
```

### Array Collection

```cypher
-- Collect all values into array
MATCH (u:User)-[:PURCHASED]->(p:Product)
RETURN u.user_id, ch.groupArray(p.name) AS purchased_products

-- Sample N random values
MATCH (u:User)
RETURN u.country, ch.groupArraySample(5)(u.name) AS sample_users

-- Collect unique values only
MATCH (t:Transaction)
RETURN t.user_id, ch.groupUniqArray(t.merchant) AS unique_merchants

-- Sorted array (top N)
MATCH (p:Product)
RETURN p.category, ch.groupArraySorted(10)(p.name) AS top_products

-- Moving average
MATCH (m:Metric)
RETURN ch.groupArrayMovingAvg(5)(m.value) AS moving_avg
```

### Funnel Analysis

```cypher
-- Window funnel: how far users progress in conversion funnel within time window
MATCH (e:Event)
WHERE e.user_id = 123
RETURN ch.windowFunnel(86400)(  -- 1 day window in seconds
    e.timestamp,
    e.event_type = 'view',
    e.event_type = 'cart',
    e.event_type = 'purchase'
) AS funnel_step

-- Retention analysis: which stages users complete
MATCH (e:Event)
RETURN e.user_id,
       ch.retention(
           e.event_type = 'signup',
           e.event_type = 'day1_active',
           e.event_type = 'day7_active'
       ) AS retention_flags

-- Sequence matching: did user follow this pattern?
MATCH (e:Event)
RETURN ch.sequenceMatch('(?1).*(?2).*(?3)')(
    e.timestamp,
    e.action = 'search',
    e.action = 'view',
    e.action = 'buy'
) AS completed_funnel

-- Count sequence matches
MATCH (e:Event)
RETURN ch.sequenceCount('(?1).*(?2)')(
    e.timestamp,
    e.action = 'click',
    e.action = 'purchase'
) AS purchase_sequences
```

### Statistics

```cypher
-- Variance and standard deviation
MATCH (m:Measurement)
RETURN ch.varPop(m.value) AS population_variance,
       ch.stddevSamp(m.value) AS sample_stddev

-- Correlation between metrics
MATCH (d:Data)
RETURN ch.corr(d.x, d.y) AS correlation_coefficient,
       ch.covarPop(d.x, d.y) AS covariance

-- Skewness and kurtosis
MATCH (d:Distribution)
RETURN ch.skewPop(d.value) AS skewness,
       ch.kurtPop(d.value) AS kurtosis

-- Linear regression
MATCH (d:Data)
RETURN ch.simpleLinearRegression(d.x, d.y) AS regression_params
```

### Map Aggregates

```cypher
-- Sum values by key in nested maps
MATCH (s:Sale)
RETURN s.region,
       ch.sumMap(s.product_counts) AS total_by_product

-- Average map values
MATCH (m:Metrics)
RETURN ch.avgMap(m.hourly_values) AS avg_by_hour

-- Min/Max maps
MATCH (s:Sensor)
RETURN ch.minMap(s.readings) AS min_readings,
       ch.maxMap(s.readings) AS max_readings
```

---

## Explicit Aggregate Prefix: `chagg.`

For aggregate functions **not in the registry**, use the `chagg.` prefix to explicitly tell ClickGraph it's an aggregate function.

```cypher
-- chagg. prefix: ALWAYS treated as aggregate (auto GROUP BY)
MATCH (u:User)
RETURN u.country, chagg.myCustomAggregate(u.score) AS custom_metric

-- Works for any function, including new/custom ClickHouse aggregates
MATCH (e:Event)
RETURN e.type, chagg.newExperimentalAgg(e.value) AS result

-- Also works for known aggregates (redundant but explicit)
MATCH (u:User)
RETURN u.country, chagg.uniq(u.email) AS unique_emails
```

**When to use `chagg.` vs `ch.`:**

| Prefix | Use Case | GROUP BY |
|--------|----------|----------|
| `ch.` | Scalar functions OR known aggregates from registry | Auto for known aggregates |
| `chagg.` | **Any** aggregate function (explicit declaration) | Always auto-generates |

**Use `chagg.` for:**
- Custom user-defined aggregates (UDAFs)
- New ClickHouse aggregates not yet in registry
- Experimental aggregate functions
- Third-party aggregate functions

---

## Function Reference Tables

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| **Unique Counting** | | |
| `ch.uniq(x)` | Approximate unique count (HLL) | `ch.uniq(u.user_id)` |
| `ch.uniqExact(x)` | Exact unique count | `ch.uniqExact(u.email)` |
| `ch.uniqCombined(x)` | Combined HLL (more accurate) | `ch.uniqCombined(u.id)` |
| `ch.uniqHLL12(x)` | HLL with 12-bit precision | `ch.uniqHLL12(u.id)` |
| **Quantiles** | | |
| `ch.quantile(p)(x)` | Single quantile | `ch.quantile(0.95)(latency)` |
| `ch.quantiles(p1,p2,...)(x)` | Multiple quantiles | `ch.quantiles(0.5,0.9,0.99)(latency)` |
| `ch.median(x)` | Median (50th percentile) | `ch.median(o.amount)` |
| `ch.quantileExact(p)(x)` | Exact quantile | `ch.quantileExact(0.99)(latency)` |
| `ch.quantileTDigest(p)(x)` | T-Digest approximate | `ch.quantileTDigest(0.95)(value)` |
| **TopK** | | |
| `ch.topK(n)(x)` | Top N frequent values | `ch.topK(10)(error_code)` |
| `ch.topKWeighted(n)(x,w)` | Weighted TopK | `ch.topKWeighted(5)(query, count)` |
| **ArgMin/Max** | | |
| `ch.argMin(val, key)` | Value at min key | `ch.argMin(name, timestamp)` |
| `ch.argMax(val, key)` | Value at max key | `ch.argMax(name, score)` |
| **Array Collection** | | |
| `ch.groupArray(x)` | Collect into array | `ch.groupArray(p.name)` |
| `ch.groupArraySample(n)(x)` | Sample N values | `ch.groupArraySample(5)(u.id)` |
| `ch.groupUniqArray(x)` | Unique values array | `ch.groupUniqArray(tag)` |
| `ch.groupArraySorted(n)(x)` | Sorted top N | `ch.groupArraySorted(10)(name)` |
| **Funnel/Retention** | | |
| `ch.windowFunnel(w)(ts,c1,c2,...)` | Funnel in time window | See examples |
| `ch.retention(c1,c2,...)` | Retention flags | See examples |
| `ch.sequenceMatch(p)(ts,c1,c2,...)` | Sequence pattern | See examples |
| `ch.sequenceCount(p)(ts,c1,c2,...)` | Count sequences | See examples |
| **Statistics** | | |
| `ch.varPop(x)` | Population variance | `ch.varPop(m.value)` |
| `ch.varSamp(x)` | Sample variance | `ch.varSamp(m.value)` |
| `ch.stddevPop(x)` | Population std dev | `ch.stddevPop(m.value)` |
| `ch.stddevSamp(x)` | Sample std dev | `ch.stddevSamp(m.value)` |
| `ch.corr(x,y)` | Correlation | `ch.corr(views, purchases)` |
| `ch.covarPop(x,y)` | Population covariance | `ch.covarPop(x, y)` |
| **Map** | | |
| `ch.sumMap(m)` | Sum map values | `ch.sumMap(counts)` |
| `ch.avgMap(m)` | Average map values | `ch.avgMap(values)` |
| `ch.minMap(m)` | Min map values | `ch.minMap(readings)` |
| `ch.maxMap(m)` | Max map values | `ch.maxMap(readings)` |

---

## Important Notes

1. **No validation**: ClickGraph doesn't validate function names. Invalid functions fail at ClickHouse execution time.

2. **Property mapping works**: Arguments go through property mapping, so `ch.length(u.name)` correctly maps `name` to the underlying column.

3. **Parameters work**: Query parameters are supported: `ch.substring(u.text, $start, $len)`.

4. **Case sensitive**: `ch.JSONExtract` ≠ `ch.jsonextract` - use exact ClickHouse function names.

5. **Prefer standard functions**: For common functions (abs, round), prefer Neo4j names for portability.

6. **Neo4j ecosystem compatible**: Dot notation matches `apoc.*` and `gds.*` patterns.

---

## Lambda Expressions

**Status**: ⭐ **NEW** (v0.5.5+) - Full support for inline anonymous functions.

Lambda expressions enable passing inline functions to ClickHouse higher-order functions, unlocking powerful array manipulation and data transformation capabilities.

### Syntax

**Single Parameter**:
```cypher
parameter -> expression
```

**Multiple Parameters**:
```cypher
(param1, param2, ...) -> expression
```

### Basic Examples

```cypher
-- Filter array elements
RETURN ch.arrayFilter(x -> x > 5, [1,2,3,4,5,6,7,8,9,10]) AS filtered
-- Result: [6,7,8,9,10]

-- Transform array values
RETURN ch.arrayMap(x -> x * 2, [1,2,3,4,5]) AS doubled
-- Result: [2,4,6,8,10]

-- Check if any element matches
RETURN ch.arrayExists(x -> x > 100, [10,20,30]) AS has_large
-- Result: false

-- Check if all elements match
RETURN ch.arrayAll(x -> x > 0, [1,2,3,4,5]) AS all_positive
-- Result: true

-- Combine two arrays element-wise
RETURN ch.arrayMap((x, y) -> x + y, [1,2,3], [10,20,30]) AS sums
-- Result: [11,22,33]
```

### Lambda in Graph Queries

**Filter User Scores**:
```cypher
MATCH (u:User)
RETURN u.name, 
       ch.arrayFilter(x -> x > 90, u.scores) AS high_scores
ORDER BY ch.length(high_scores) DESC
LIMIT 10
```

**Transform and Aggregate**:
```cypher
MATCH (p:Post)
WHERE ch.arrayExists(tag -> tag IN ['tech', 'science'], p.tags)
RETURN p.title,
       ch.arrayMap(tag -> ch.upper(tag), p.tags) AS normalized_tags
```

**Data Validation**:
```cypher
MATCH (u:User)
WHERE NOT ch.arrayAll(x -> x >= 0 AND x <= 100, u.scores)
RETURN u.user_id, u.scores AS invalid_scores
```

### Supported Functions

**Array Transformation**:
- `ch.arrayFilter(lambda, array)` - Filter elements matching condition
- `ch.arrayMap(lambda, array1, [array2, ...])` - Transform elements
- `ch.arrayFill(lambda, array)` - Fill forward based on condition
- `ch.arrayCumSum(lambda, array)` - Cumulative sum with lambda
- `ch.arraySplit(lambda, array)` - Split array by condition

**Array Predicates**:
- `ch.arrayExists(lambda, array)` - Check if any element matches
- `ch.arrayAll(lambda, array)` - Check if all elements match
- `ch.arrayFirst(lambda, array)` - Get first matching element
- `ch.arrayFirstIndex(lambda, array)` - Get index of first match

**Array Aggregation**:
- `ch.arrayFold(lambda, array, initial)` - Reduce array to single value
- `ch.arrayReduce('aggFunc', array)` - Apply aggregate to array elements

### Real-World Examples

**Price Analysis**:
```cypher
MATCH (p:Product)
RETURN p.name,
       p.prices AS original_prices,
       ch.arrayMap(x -> x * 0.8, p.prices) AS discounted_prices
```

**Tag Normalization**:
```cypher
MATCH (a:Article)
RETURN a.title,
       ch.arrayFilter(
         tag -> ch.length(tag) > 2,
         ch.arrayMap(t -> ch.lower(ch.trim(t)), a.tags)
       ) AS clean_tags
```

**Time Series Filtering**:
```cypher
MATCH (s:Sensor)
WITH s, ch.now() AS current_time
RETURN s.sensor_id,
       ch.arrayFilter(
         ts -> ts > current_time - 3600,
         s.event_timestamps
       ) AS recent_events
```

**Chaining Operations**:
```cypher
RETURN ch.arrayMap(
  x -> x * x,
  ch.arrayFilter(n -> n % 2 = 0, [1,2,3,4,5,6,7,8,9,10])
) AS even_squares
-- Result: [4,16,36,64,100]
```

### Variable Scoping

- **Lambda parameters** are local variables (e.g., `x`, `score`, `tag`)
- **Lambda body** can reference:
  - Lambda parameters
  - Node/edge properties (e.g., `u.threshold`)
  - WITH clause variables
  - Literal values

```cypher
MATCH (u:User)
WITH u, 80 AS passing_grade
RETURN u.name,
       ch.arrayFilter(score -> score >= passing_grade, u.scores) AS passed
```

### Performance Tips

1. **Use arrayExists for early termination** instead of arrayFilter + count:
   ```cypher
   -- ✅ Fast (stops at first match)
   WHERE ch.arrayExists(x -> x > 100, scores)
   
   -- ❌ Slower (processes entire array)
   WHERE ch.length(ch.arrayFilter(x -> x > 100, scores)) > 0
   ```

2. **Push filters before transformations**:
   ```cypher
   -- ✅ Better (filter first, transform less)
   ch.arrayMap(x -> x * 2, ch.arrayFilter(x -> x > 50, numbers))
   
   -- ❌ Worse (transform all, then filter)
   ch.arrayFilter(x -> x > 100, ch.arrayMap(x -> x * 2, numbers))
   ```

3. **Use arrayAll for validation**:
   ```cypher
   -- ✅ Efficient (dedicated function)
   WHERE ch.arrayAll(x -> x >= 0, values)
   
   -- ❌ Less efficient (manual comparison)
   WHERE ch.length(values) = ch.length(ch.arrayFilter(x -> x >= 0, values))
   ```

### Common Errors

**Lambda parameter conflicts with alias**:
```cypher
-- ❌ Wrong: 'user' conflicts with node alias
MATCH (user:User)
RETURN ch.arrayFilter(user -> user > 0, user.scores)

-- ✅ Correct: Use different parameter name
MATCH (user:User)
RETURN ch.arrayFilter(x -> x > 0, user.scores)
```

**Missing array argument**:
```cypher
-- ❌ Wrong: Lambda alone doesn't make sense
RETURN ch.arrayFilter(x -> x > 5)

-- ✅ Correct: Provide array to filter
RETURN ch.arrayFilter(x -> x > 5, [1,2,3,4,5,6,7,8,9])
```

**Type mismatch**:
```cypher
-- ❌ Wrong: Comparing number to string
RETURN ch.arrayFilter(x -> x > 'abc', [1,2,3])

-- ✅ Correct: Use appropriate comparison
RETURN ch.arrayFilter(s -> s > 'abc', ['aaa', 'bbb', 'ccc'])
```

### Lambda Limitations

- **No nested lambdas**: `x -> y -> x + y` not supported (use multiple calls)
- **No destructuring**: Parameters must be simple identifiers
- **No type checking**: All validation happens at ClickHouse query time
- **No closure mutation**: Lambda parameters are read-only

---

## Limitations

### Parametric Aggregates

Parametric aggregates like `quantile(0.95)(x)` use special ClickHouse syntax - test to ensure correct parsing.

---

## External References

- [ClickHouse Functions Reference](https://clickhouse.com/docs/en/sql-reference/functions)
- [Aggregate Functions](https://clickhouse.com/docs/en/sql-reference/aggregate-functions)
- [Array Functions](https://clickhouse.com/docs/en/sql-reference/functions/array-functions)
- [Date/Time Functions](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions)
- [JSON Functions](https://clickhouse.com/docs/en/sql-reference/functions/json-functions)

---

[← Back: Cypher Functions](Cypher-Functions.md) | [Home](Home.md) | [Next: Vector Search →](Vector-Search.md)
