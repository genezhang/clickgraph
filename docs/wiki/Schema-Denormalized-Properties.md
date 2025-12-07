# Schema Feature: Denormalized Properties

**Feature Version**: v0.5.2+  
**Status**: Production-ready  
**Performance**: 10-100x faster than JOIN-based queries  
**Use Case**: Node properties embedded in edge tables

---

## Supported Query Patterns

All standard Cypher patterns work with denormalized schemas:

| Pattern | Status | Example |
|---------|--------|---------|
| Single-hop | ✅ | `(a)-[:FLIGHT]->(b)` |
| Multi-hop | ✅ | `(a)-[:FLIGHT]->(b)-[:FLIGHT]->(c)` |
| Variable-length | ✅ | `(a)-[:FLIGHT*1..3]->(b)` |
| Zero-hop | ✅ | `(a)-[:FLIGHT*0..2]->(b)` |
| Unbounded | ✅ | `(a)-[:FLIGHT*]->(b)` |
| shortestPath | ✅ | `shortestPath((a)-[:FLIGHT*1..5]->(b))` |
| allShortestPaths | ✅ | `allShortestPaths((a)-[:FLIGHT*1..5]->(b))` |
| PageRank | ✅ | `CALL pagerank(graph: 'Airport', ...)` |
| Aggregations | ✅ | `COUNT`, `SUM`, `AVG`, etc. |
| WHERE clauses | ✅ | Filter on any denormalized property |
| **Coupled edges** | ✅ | Multiple edges on same table row (v0.5.2+) |
| **UNWIND arrays** | ✅ | Flatten array columns with ARRAY JOIN |

---

## Overview

**Denormalized properties** allow you to query node properties directly from edge tables without requiring JOINs. This is a major performance optimization for star-schema and denormalized table designs common in ClickHouse.

### The Problem: Expensive JOINs

Traditional graph queries require JOINs to access node properties:

```cypher
-- Traditional approach
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.city = 'San Francisco'
RETURN dest.city
```

**Generated SQL** (3 JOINs required):
```sql
SELECT dest.city
FROM flights AS e
JOIN airports AS origin ON e.origin_id = origin.airport_code
JOIN airports AS dest ON e.dest_id = dest.airport_code
WHERE origin.city = 'San Francisco'
```

**Performance**: 3 table scans, 2 hash joins, ~500ms on 10M rows

### The Solution: Denormalized Properties

Store node properties directly in the edge table:

```yaml
edges:
  - type: FLIGHT
    from_node_properties:
      city: OriginCityName     # Read from edge table
      state: OriginState
    to_node_properties:
      city: DestCityName
      state: DestState
```

**Generated SQL** (0 JOINs!):
```sql
SELECT DestCityName AS city
FROM flights
WHERE OriginCityName = 'San Francisco'
```

**Performance**: 1 table scan, ~5ms on 10M rows ⚡ **100x faster!**

---

## Schema Configuration

### Basic Setup

```yaml
nodes:
  - label: Airport
    database: brahmand
    table: flights  # Virtual node - properties come from edge table
    node_id: airport_code
    property_mappings: {}

edges:
  - type: FLIGHT
    database: brahmand
    table: flights
    from_id: Origin
    to_id: Dest
    from_node: Airport
    to_node: Airport
    
    # Denormalized source node properties
    from_node_properties:
      code: Origin
      city: OriginCityName
      state: OriginState
      airport: OriginAirportName
    
    # Denormalized destination node properties
    to_node_properties:
      code: Dest
      city: DestCityName
      state: DestState
      airport: DestAirportName
    
    # Edge properties
    property_mappings:
      flight_date: FlightDate
      flight_num: FlightNum
      carrier: Carrier
```

### Field Mapping

**Node → Property → Column**:
```yaml
from_node_properties:
  city: OriginCityName
  # ^^^^ Cypher property name
  #      ^^^^^^^^^^^^^^ ClickHouse column in edge table
```

**Usage in Cypher**:
```cypher
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.city = 'San Francisco'
#           ^^^^ Uses OriginCityName column from flights table (no JOIN!)
RETURN dest.city
#           ^^^^ Uses DestCityName column from flights table
```

---

## Table Schema Design

### Example: OnTime Flight Data

```sql
CREATE TABLE flights (
    -- Edge columns
    Origin String,              -- from_id
    Dest String,                -- to_id
    FlightDate Date,
    FlightNum String,
    Carrier String,
    
    -- Denormalized origin airport properties
    OriginCityName String,      -- origin.city
    OriginState String,         -- origin.state
    OriginAirportName String,   -- origin.airport
    
    -- Denormalized destination airport properties  
    DestCityName String,        -- dest.city
    DestState String,           -- dest.state
    DestAirportName String,     -- dest.airport
    
    -- Edge properties
    DepTime UInt16,
    ArrTime UInt16,
    Distance UInt32
) ENGINE = MergeTree()
ORDER BY (Origin, Dest, FlightDate);
```

**Data Example**:
```
Origin | Dest | OriginCityName    | DestCityName   | Carrier | FlightDate
-------|------|-------------------|----------------|---------|------------
SFO    | LAX  | San Francisco     | Los Angeles    | UA      | 2025-01-15
SFO    | JFK  | San Francisco     | New York       | AA      | 2025-01-15
LAX    | ORD  | Los Angeles       | Chicago        | DL      | 2025-01-15
```

---

## Querying with Denormalized Properties

### Pattern Detection

ClickGraph **automatically detects** when to use denormalized properties vs JOINs:

```cypher
-- Uses denormalized property (no JOIN)
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.city = 'San Francisco'
RETURN dest.city

-- Uses JOIN (property not in from_node_properties)
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)  
WHERE origin.timezone = 'PST'  -- Not denormalized, requires JOIN
RETURN dest.city
```

### Query Examples

**1. Filter by source node property**:
```cypher
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.city = 'San Francisco' AND origin.state = 'CA'
RETURN dest.city, count(*) AS flights
ORDER BY flights DESC
```

**Generated SQL**:
```sql
SELECT DestCityName, count(*) AS flights
FROM flights
WHERE OriginCityName = 'San Francisco' AND OriginState = 'CA'
GROUP BY DestCityName
ORDER BY flights DESC
```

**2. Return both source and destination properties**:
```cypher
MATCH (origin:Airport {city: 'San Francisco'})-[:FLIGHT]->(dest:Airport)
RETURN origin.city + ', ' + origin.state AS origin_loc,
       dest.city + ', ' + dest.state AS dest_loc
```

**3. Aggregation by denormalized property**:
```cypher
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
RETURN origin.state, dest.state, count(*) AS flights
ORDER BY flights DESC
LIMIT 10
```

**4. Multi-hop with denormalized properties**:
```cypher
MATCH (a:Airport)-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport)
WHERE a.city = 'San Francisco' AND c.city = 'New York'
RETURN b.city AS connection_city, count(*) AS routes
```

**5. Variable-length paths** (`*min..max`):
```cypher
-- Find all routes within 1-3 hops
MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport)
WHERE a.city = 'San Francisco' AND b.city = 'New York'
RETURN count(*) AS route_count
```

**6. Zero-hop paths** (includes self-match):
```cypher
-- Zero to 2 hops (node can match itself at 0 hops)
MATCH (a:Airport)-[:FLIGHT*0..2]->(b:Airport)
WHERE a.code = 'SFO'
RETURN DISTINCT b.city
```

**7. Shortest path**:
```cypher
MATCH p = shortestPath((a:Airport)-[:FLIGHT*1..5]->(b:Airport))
WHERE a.code = 'SEA' AND b.code = 'MIA'
RETURN p
```

**8. PageRank** (requires named argument syntax):
```cypher
CALL pagerank(graph: 'Airport', relationshipTypes: 'FLIGHT', iterations: 10, dampingFactor: 0.85)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

---

## Performance Benefits

### Benchmark: 10M Flights Dataset

| Query Type | JOIN-based | Denormalized | Speedup |
|------------|------------|--------------|---------|
| Single filter (`WHERE origin.city = 'X'`) | 450ms | 5ms | **90x** |
| Aggregation by state | 780ms | 12ms | **65x** |
| Multi-hop (2 hops) | 2.1s | 35ms | **60x** |
| Multi-hop (3 hops) | 4.5s | 85ms | **53x** |
| Variable-length `*1..3` | 3.2s | 120ms | **27x** |
| shortestPath (5 hops max) | 5.8s | 180ms | **32x** |
| Top routes by city | 920ms | 18ms | **51x** |

### Why So Fast?

**JOIN-based approach**:
1. ❌ Scan flights table
2. ❌ Hash join with airports table (origin)
3. ❌ Hash join with airports table (dest)
4. ❌ 3 table scans, 2 hash tables in memory

**Denormalized approach**:
1. ✅ Scan flights table only
2. ✅ Filter directly on columns
3. ✅ ClickHouse column compression optimized
4. ✅ 1 table scan, 0 hash tables

---

## When to Use Denormalized Properties

### ✅ Ideal Use Cases

**1. Star Schema / Fact Tables**
```
Fact: orders (order_id, customer_id, customer_name, customer_region, product_id, product_name, ...)
Dimensions: customers, products (can be omitted!)
```

**2. Event Streams with Context**
```
events (event_id, user_id, user_name, user_country, product_id, product_category, ...)
```

**3. Time-Series Data**
```
metrics (timestamp, server_id, server_name, server_datacenter, metric_value, ...)
```

**4. Denormalized OLAP Tables**
- Data warehouses optimized for reads
- Pre-joined dimensions
- ClickHouse materialized views

### ❌ When NOT to Use

**1. Normalized OLTP Schemas**
- Frequent updates to node properties
- Storage efficiency critical
- Dimension tables < 1M rows (JOIN is fast enough)

**2. Dynamic Properties**
- Node properties change frequently
- Edge table would need constant updates

**3. Many-to-Many Without Duplication**
- Same property appears in many edges
- Storage bloat from duplication

---

## Hybrid Approach: Mix Denormalized + JOINs

You can denormalize **frequently queried properties** while keeping others in node tables:

```yaml
nodes:
  - label: Airport
    database: brahmand
    table: airports  # Node table still exists
    node_id: airport_code
    property_mappings:
      code: airport_code
      timezone: timezone        # Only in node table
      elevation: elevation      # Only in node table
      latitude: latitude
      longitude: longitude

edges:
  - type: FLIGHT
    from_node_properties:
      city: OriginCityName      # Denormalized (frequently queried)
      state: OriginState        # Denormalized
    to_node_properties:
      city: DestCityName
      state: DestState
    # timezone, elevation, lat/lon require JOIN
```

**Query 1** (uses denormalized properties):
```cypher
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.city = 'San Francisco'  -- ✅ No JOIN
RETURN dest.city                      -- ✅ No JOIN
```

**Query 2** (requires JOIN):
```cypher
MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport)
WHERE origin.timezone = 'PST'         -- ❌ Needs JOIN
RETURN dest.city                       -- ✅ Still denormalized
```

---

## Complete Example

### Schema YAML

```yaml
name: ontime_flights
version: "1.0"

graph_schema:
  nodes:
    - label: Airport
      database: brahmand
      table: flights
      node_id: airport_code
      property_mappings: {}

  edges:
    - type: FLIGHT
      database: brahmand
      table: flights
      from_id: Origin
      to_id: Dest
      from_node: Airport
      to_node: Airport
      
      edge_id: [FlightDate, FlightNum, Origin, Dest]
      
      from_node_properties:
        code: Origin
        city: OriginCityName
        state: OriginState
        airport: OriginAirportName
      
      to_node_properties:
        code: Dest
        city: DestCityName
        state: DestState
        airport: DestAirportName
      
      property_mappings:
        flight_date: FlightDate
        flight_num: FlightNum
        carrier: Carrier
        departure_time: DepTime
        arrival_time: ArrTime
        distance: Distance
```

### Sample Queries

```cypher
-- 1. Top destinations from San Francisco
MATCH (sfo:Airport {city: 'San Francisco'})-[:FLIGHT]->(dest:Airport)
RETURN dest.city, count(*) AS flights
ORDER BY flights DESC
LIMIT 10

-- 2. California to California flights
MATCH (origin:Airport {state: 'CA'})-[:FLIGHT]->(dest:Airport {state: 'CA'})
RETURN origin.city, dest.city, count(*) AS flights
ORDER BY flights DESC

-- 3. Cross-country routes
MATCH (west:Airport)-[:FLIGHT]->(east:Airport)
WHERE west.state IN ['CA', 'OR', 'WA']
  AND east.state IN ['NY', 'MA', 'CT']
RETURN west.city, east.city, count(*) AS flights

-- 4. Hub detection (most connections)
MATCH (:Airport)-[:FLIGHT]->(hub:Airport)
RETURN hub.city, hub.state, count(*) AS incoming_flights
ORDER BY incoming_flights DESC
LIMIT 10

-- 5. Two-hop routes with connection city
MATCH (a:Airport {city: 'San Francisco'})-[:FLIGHT]->(b:Airport)-[:FLIGHT]->(c:Airport {city: 'Boston'})
RETURN b.city AS connection, count(*) AS route_count
ORDER BY route_count DESC
```

---

## Coupled Edges (v0.5.2+)

**Coupled edges** are an advanced optimization for denormalized schemas where multiple edge types are defined on the same physical table AND connect through a common "coupling node". ClickGraph automatically detects these patterns and eliminates unnecessary self-joins.

### What Are Coupled Edges?

When a single table row represents multiple graph edges, those edges are "coupled":

```
Single DNS Log Row:
┌─────────────────────────────────────────────────────────────┐
│ source_ip: 192.168.4.76                                     │
│ query: testmyids.com                                        │  
│ answers: [31.3.245.133, 31.3.245.134]                       │
└─────────────────────────────────────────────────────────────┘
         ↓                    ↓
    REQUESTED edge       RESOLVED_TO edge
    (IP → Domain)        (Domain → ResolvedIP)
         ↓                    ↓
       Same row, no JOIN needed!
```

### Schema Configuration

```yaml
# Two edge types from ONE table
edges:
  - type: REQUESTED
    database: zeek
    table: dns_log
    from_id: "id.orig_h"       # Source IP
    to_id: query               # Domain (COUPLING NODE)
    from_node: IP
    to_node: Domain
    
  - type: RESOLVED_TO
    database: zeek
    table: dns_log
    from_id: query             # Domain (COUPLING NODE)
    to_id: answers             # Resolved IP
    from_node: Domain
    to_node: ResolvedIP
```

**Coupling Node**: The `Domain` node connects both edges (`REQUESTED.to_node = RESOLVED_TO.from_node`).

### Query Optimization

**Cypher Query**:
```cypher
MATCH (ip:IP)-[r1:REQUESTED]->(d:Domain)-[r2:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
RETURN ip.ip, d.name, rip.ips
```

**Without Coupled Edge Optimization** (naive approach):
```sql
-- ❌ Self-join on same table - WRONG!
SELECT r1."id.orig_h", r1.query, r2.answers
FROM zeek.dns_log AS r1
INNER JOIN zeek.dns_log AS r2 ON r2.query = r1.query  -- Unnecessary!
WHERE r1."id.orig_h" = '192.168.4.76'
```

**With Coupled Edge Optimization** (v0.5.2+):
```sql
-- ✅ Single table scan - CORRECT!
SELECT r1."id.orig_h" AS "ip.ip", r1.query AS "d.name", r1.answers AS "rip.ips"
FROM zeek.dns_log AS r1
WHERE r1."id.orig_h" = '192.168.4.76'
```

### Alias Unification

ClickGraph automatically **unifies aliases** for coupled edges:
- Both `r1` and `r2` resolve to the same alias (`r1`)
- All SELECT, WHERE, and ORDER BY clauses use consistent alias
- Properties from both edges access the same table row

### When Edges Are Coupled

Edges are automatically detected as coupled when:

| Requirement | Description |
|-------------|-------------|
| ✅ Same table | Both edges defined on same `database.table` |
| ✅ Coupling node | `edge1.to_node = edge2.from_node` (sequential chain) |
| ✅ Linear pattern | No branching (e.g., not `(a)-[r1]->(b), (a)-[r2]->(c)`) |

### UNWIND with Coupled Edges

When your denormalized table contains **array columns**, use UNWIND to flatten them:

```cypher
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
UNWIND rip.ips AS resolved_ip
RETURN ip.ip, d.name, resolved_ip
```

**Generated SQL**:
```sql
SELECT 
  r1."id.orig_h" AS "ip.ip",
  r1.query AS "d.name",
  resolved_ip AS "resolved_ip"
FROM zeek.dns_log AS r1
ARRAY JOIN r1.answers AS resolved_ip
WHERE r1."id.orig_h" = '192.168.4.76'
```

The property `rip.ips` is automatically mapped to the correct SQL column (`answers`) using the schema's `to_node_properties` definition.

### Supported Cypher Features

All standard Cypher features work with coupled edges:

| Feature | Example | Status |
|---------|---------|--------|
| WHERE filters | `WHERE ip.ip = '...'` | ✅ |
| Aggregations | `COUNT(*)`, `SUM()`, `AVG()` | ✅ |
| ORDER BY | `ORDER BY d.name` | ✅ |
| DISTINCT | `RETURN DISTINCT d.name` | ✅ |
| Edge properties | `RETURN r1.rcode` | ✅ |
| UNWIND arrays | `UNWIND rip.ips AS ip` | ✅ |
| Property access | `ip.ip`, `d.name`, `rip.ips` | ✅ |

### Performance Benefits

| Query Pattern | With Self-JOIN | Coupled Edge | Speedup |
|---------------|----------------|--------------|---------|
| 2-hop DNS lookup | 180ms | 8ms | **22x** |
| Aggregation by domain | 320ms | 15ms | **21x** |
| UNWIND answers array | 250ms | 12ms | **21x** |
| Count unique domains | 150ms | 6ms | **25x** |

*Benchmarks on 5M row DNS log dataset*

### Complete Example Schema

```yaml
# Zeek DNS Log - Coupled Edges Example
name: zeek_dns_log
version: "1.0"

graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: dns_log
      node_id: ip
      property_mappings: {}
      from_node_properties:
        ip: "id.orig_h"
      to_node_properties:
        ip: "id.resp_h"

    - label: Domain
      database: zeek
      table: dns_log
      node_id: name
      property_mappings: {}
      from_node_properties:
        name: query
      to_node_properties:
        name: query

    - label: ResolvedIP
      database: zeek
      table: dns_log
      node_id: answers
      property_mappings: {}
      to_node_properties:
        ips: answers

  edges:
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_id: "id.orig_h"
      to_id: query
      from_node: IP
      to_node: Domain
      edge_id: uid
      property_mappings:
        timestamp: ts
        rcode: rcode_name

    - type: RESOLVED_TO
      database: zeek
      table: dns_log
      from_id: query
      to_id: answers
      from_node: Domain
      to_node: ResolvedIP
      edge_id: [uid, answers]
```

### Sample Queries

```cypher
-- 1. Find all domains requested by an IP
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)
WHERE ip.ip = '192.168.4.76'
RETURN d.name

-- 2. Full DNS resolution chain
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
RETURN ip.ip, d.name, rip.ips

-- 3. Flatten resolved IPs
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
UNWIND rip.ips AS resolved_ip
RETURN ip.ip, d.name, resolved_ip

-- 4. Count requests per domain
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)
RETURN d.name, count(*) AS request_count
ORDER BY request_count DESC

-- 5. Top requesting IPs
MATCH (ip:IP)-[r:REQUESTED]->(d:Domain)
RETURN ip.ip, count(*) AS requests
ORDER BY requests DESC
LIMIT 10
```

---

## Migration Strategies

### From Normalized to Denormalized

**Option 1: Materialized View**
```sql
-- Create denormalized view from normalized tables
CREATE MATERIALIZED VIEW flights_denormalized
ENGINE = MergeTree()
ORDER BY (Origin, Dest, FlightDate)
AS SELECT
    f.Origin,
    f.Dest,
    f.FlightDate,
    o.city AS OriginCityName,
    o.state AS OriginState,
    d.city AS DestCityName,
    d.state AS DestState,
    f.Carrier,
    f.DepTime,
    f.ArrTime
FROM flights AS f
LEFT JOIN airports AS o ON f.Origin = o.airport_code
LEFT JOIN airports AS d ON f.Dest = d.airport_code;
```

**Option 2: ETL Pipeline**
```python
# Denormalize during data loading
import pandas as pd

flights = pd.read_csv('flights.csv')
airports = pd.read_csv('airports.csv')

# Join and flatten
flights_denorm = flights.merge(
    airports.add_prefix('Origin'),
    left_on='Origin',
    right_on='Originairport_code'
).merge(
    airports.add_prefix('Dest'),
    left_on='Dest',
    right_on='Destairport_code'
)

# Load to ClickHouse
flights_denorm.to_sql('flights', con=engine, if_exists='replace')
```

---

## Limitations

1. **Storage Duplication**
   - Node properties duplicated in every edge row
   - Larger table size (typically 1.5-3x)
   - Mitigated by ClickHouse column compression

2. **Update Complexity**
   - Changing a node property requires updating all edges
   - Best for immutable or slowly changing data

3. **Schema Requirements**
   - Requires denormalized source data or materialized views
   - Not suitable for highly normalized schemas without preprocessing

4. **Property Coverage**
   - Only listed properties are denormalized
   - Other properties require JOINs

---

## Implementation Details

### Code References

**Property Access**:
- `src/render_plan/cte_generation.rs` - Lines 328-640
  - `extract_properties_from_node()` - Detects denormalized vs JOIN
  - `get_denormalized_property()` - Resolves property from edge table

**Schema Configuration**:
- `src/graph_catalog/config.rs` - `StandardEdgeDefinition`
  - `from_node_properties: Option<HashMap<String, String>>`
  - `to_node_properties: Option<HashMap<String, String>>`

**Testing**:
- `src/render_plan/tests/denormalized_property_tests.rs` - 3 unit tests
- `schemas/examples/ontime_denormalized.yaml` - Example schema

---

## Best Practices

1. **✅ Denormalize frequently queried properties**
   - City, state, category, status
   - Properties used in WHERE clauses

2. **✅ Use for read-heavy workloads**
   - Analytics, reporting, dashboards
   - Time-series and event data

3. **✅ Compress duplicated data**
   - ClickHouse LZ4/ZSTD compression
   - Deduplicate with dictionaries

4. **✅ Monitor query plans**
   - Check generated SQL for JOINs
   - Verify denormalized properties are used

5. **✅ Combine with edge_id**
   - Single-column edge_id for performance
   - Denormalized properties for fast filters

---

## See Also

- [Schema Configuration Advanced](Schema-Configuration-Advanced.md) - Advanced schema features
- [Schema Polymorphic Edges](Schema-Polymorphic-Edges.md) - Multiple edge types in one table
- [Edge ID Best Practices](../edge-id-best-practices.md) - Edge uniqueness optimization
- [Performance Query Optimization](Performance-Query-Optimization.md) - Query performance tuning
