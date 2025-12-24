# Variable-Length Path Queries - User Guide

**ClickGraph Feature:** Complete variable-length path support for multi-hop graph traversals  
**Status:** Production-ready (October 2025)  
**Version:** 0.1.0+

---

## Table of Contents

1. [Introduction](#introduction)
2. [Syntax Reference](#syntax-reference)
3. [Common Use Cases](#common-use-cases)
4. [Real-World Examples](#real-world-examples)
5. [Performance Tuning](#performance-tuning)
6. [Best Practices](#best-practices)
7. [Common Patterns & Anti-Patterns](#common-patterns--anti-patterns)
8. [Troubleshooting](#troubleshooting)

---

## Introduction

Variable-length path queries allow you to traverse relationships across multiple hops without knowing the exact distance in advance. This is essential for:

- **Social Network Analysis**: Find friends-of-friends, influencer reach
- **Recommendation Systems**: Discover similar items through connections
- **Organizational Hierarchies**: Navigate management chains, reporting structures
- **Knowledge Graphs**: Explore concept relationships, semantic connections
- **Supply Chain**: Track product origins, dependency chains

### What's Supported

✅ **Range patterns**: `*1..3` (1 to 3 hops)  
✅ **Fixed length**: `*2` (exactly 2 hops)  
✅ **Unbounded**: `*` (any number of hops, up to configured limit)  
✅ **Max-only**: `*..5` (up to 5 hops)  
✅ **Property selection**: Access node and relationship properties  
✅ **Aggregations**: COUNT, SUM, AVG with GROUP BY  
✅ **Filtering**: WHERE clauses on paths and properties  
✅ **Cycle detection**: Automatic path deduplication

---

## Syntax Reference

### Basic Syntax

```cypher
MATCH (start)-[relationship*length]->(end)
RETURN start, end
```

### Length Specifications

| Pattern | Meaning | Example Query |
|---------|---------|---------------|
| `*` | Any length (up to limit) | `MATCH (a)-[*]->(b)` |
| `*2` | Exactly 2 hops | `MATCH (a)-[*2]->(b)` |
| `*1..3` | 1 to 3 hops | `MATCH (a)-[*1..3]->(b)` |
| `*..5` | Up to 5 hops | `MATCH (a)-[*..5]->(b)` |
| `*2..` | 2 or more hops | `MATCH (a)-[*2..]->(b)` |

### Relationship Types

```cypher
-- With relationship type
MATCH (u:User)-[f:FOLLOWS*1..2]->(follower:User)

-- Multiple types (future support)
MATCH (u:User)-[r:FOLLOWS|FRIEND*1..3]->(other:User)

-- Bidirectional (any direction)
MATCH (a:Person)-[*1..2]-(b:Person)
```

### Property Access

```cypher
-- Node properties
MATCH (u1:User)-[*1..2]->(u2:User)
RETURN u1.name, u2.name, u1.age

-- With filtering
MATCH (u1:User)-[*1..2]->(u2:User)
WHERE u1.age > 25
RETURN u2.name
```

---

## Common Use Cases

### 1. Social Network: Friend Recommendations

**Goal**: Find people within 2-3 degrees of connection

```cypher
-- Friends and friends-of-friends
MATCH (me:User {user_id: 123})-[f:FOLLOWS*1..2]->(suggested:User)
WHERE suggested.user_id <> 123  -- Exclude self
RETURN DISTINCT suggested.name, suggested.email
LIMIT 10
```

**Use Case**: "People you may know" feature

### 2. Organizational Chart: Management Chain

**Goal**: Find all managers up the hierarchy

```cypher
-- Employee to CEO path
MATCH (employee:Employee {id: 456})-[r:REPORTS_TO*]->(manager:Employee)
RETURN manager.name, manager.title
ORDER BY length(r) DESC
```

**Use Case**: Org chart visualization, approval workflows

### 3. E-commerce: Product Recommendations

**Goal**: Find products liked by similar users

```cypher
-- Products liked by users who liked similar products
MATCH (user:User {id: 789})-[:LIKED]->(product1:Product)
      <-[:LIKED]-(other:User)-[:LIKED]->(product2:Product)
RETURN DISTINCT product2.name, product2.category, COUNT(*) as score
ORDER BY score DESC
LIMIT 20
```

**Use Case**: "Customers who bought this also bought..." feature

### 4. Knowledge Graph: Concept Exploration

**Goal**: Find related concepts within semantic distance

```cypher
-- Related concepts within 3 semantic hops
MATCH (start:Concept {name: "Machine Learning"})-[r:RELATED_TO*1..3]->(end:Concept)
RETURN end.name, end.category, length(r) as distance
ORDER BY distance, end.name
```

**Use Case**: Research exploration, topic suggestions

### 5. Network Analysis: Influencer Reach

**Goal**: Measure influence by follower network size

```cypher
-- Count total reach within 2 hops
MATCH (influencer:User)-[f:FOLLOWS*1..2]->(reached:User)
WHERE influencer.verified = true
RETURN influencer.username, 
       COUNT(DISTINCT reached) as total_reach,
       AVG(reached.engagement_rate) as avg_engagement
ORDER BY total_reach DESC
LIMIT 50
```

**Use Case**: Influencer marketing, network analysis

---

## Shortest Path Queries

**Status**: ✅ Production-ready (December 2025)

Shortest path queries find the minimum-hop path(s) between two nodes using breadth-first search. ClickGraph implements this using recursive CTEs with early result filtering.

### Syntax

```cypher
-- Single shortest path
MATCH path = shortestPath((start)-[*]-(end))
RETURN path

-- All shortest paths (all paths with minimum length)
MATCH path = allShortestPaths((start)-[*]-(end))
RETURN path
```

### How It Works

1. **Breadth-First Search**: Recursive CTE explores paths level-by-level (1 hop, then 2 hops, etc.)
2. **First Match Wins**: Due to BFS nature, the first path reaching the target IS the shortest
3. **ROW_NUMBER() Filtering**: Post-processing selects shortest path per start node
4. **Automatic Deduplication**: Cycle detection prevents revisiting edges

### Basic Examples

```cypher
-- Find shortest connection between two people
MATCH path = shortestPath((p1:Person {id: 123})-[:KNOWS*]-(p2:Person {id: 456}))
RETURN CASE path IS NULL
         WHEN true THEN -1
         ELSE length(path)
       END AS distance

-- Get the actual path
MATCH path = shortestPath((p1:Person {id: 123})-[:KNOWS*]-(p2:Person {id: 456}))
RETURN nodes(path) AS people,
       relationships(path) AS connections,
       length(path) AS hops
```

### ⚠️ Important: Always Use Explicit Hop Bounds for Large Graphs

**The Challenge**: On large graphs (10K+ nodes), unbounded shortest path queries can consume significant memory because ClickGraph must explore all possible paths up to the default limit before filtering to the target.

**Best Practice**: Always specify explicit hop bounds based on your graph's characteristics.

```cypher
-- ❌ AVOID: Unbounded on large graphs (may exhaust memory)
MATCH path = shortestPath((p1:Person {id: X})-[:KNOWS*]-(p2:Person {id: Y}))
RETURN length(path)

-- ✅ GOOD: Explicit bounds (fast and memory-efficient)
MATCH path = shortestPath((p1:Person {id: X})-[:KNOWS*1..5]-(p2:Person {id: Y}))
RETURN length(path)

-- ✅ BEST: Inline node patterns with bounds
MATCH path = shortestPath((p1:Person {id: X})-[:KNOWS*1..5]-(p2:Person {id: Y}))
RETURN length(path)
```

### Performance Guidelines

| Graph Type | Recommended Max Hops | Rationale |
|------------|---------------------|-----------|
| **Social Networks** | `*1..4` | Small-world property: 6 degrees of separation |
| **Organization Charts** | `*1..5` | Typically shallow hierarchies |
| **Citation Networks** | `*1..3` | Most citations are recent/nearby |
| **File Dependencies** | `*1..10` | Deeper dependency chains common |
| **General Purpose** | `*1..5` | Safe default for most graphs |

### Default Behavior

- **Unbounded `*`**: Defaults to 5 hops for shortestPath (10 hops for regular variable-length)
- **Explicit bounds**: Always recommended for production queries
- **User control**: Specify exact bounds in your query for optimal performance

### Advanced Patterns

```cypher
-- Shortest path with property filtering
MATCH path = shortestPath((p1:Person {verified: true})-[:KNOWS*1..4]-(p2:Person {verified: true}))
WHERE p1.id = 123 AND p2.id = 456
RETURN length(path) AS degrees_of_separation

-- Multiple shortest paths (all minimum-length paths)
MATCH path = allShortestPaths((p1:Person {id: 123})-[:KNOWS*1..5]-(p2:Person {id: 456}))
RETURN path,
       length(path) AS path_length,
       [n IN nodes(path) | n.name] AS path_members

-- Shortest path with direction
MATCH path = shortestPath((p1:Person {id: 123})-[:KNOWS*1..5]->(p2:Person {id: 456}))
RETURN length(path)  -- Only following KNOWS in forward direction
```

### When to Use Explicit Bounds

**Always use explicit bounds when:**
- Graph has >1,000 nodes
- Querying in production environments
- Memory constraints exist
- Query timeouts occur
- You know the typical path length (e.g., org charts rarely exceed 5 levels)

**Example: Social Network Query**
```cypher
-- Most people are within 4 degrees in social networks
MATCH path = shortestPath((me:Person {id: $myId})-[:KNOWS*1..4]-(friend:Person {id: $friendId}))
RETURN CASE path IS NULL
         WHEN true THEN "Not connected within 4 hops"
         ELSE "Connected: " + toString(length(path)) + " degrees"
       END AS connection_status
```

### Troubleshooting

**Problem**: Query times out or runs out of memory

**Solutions**:
1. ✅ Add explicit hop bounds: `*1..4` instead of `*`
2. ✅ Use inline node patterns: `(p1:Person {id: X})` instead of comma-separated
3. ✅ Add indexes on node ID columns in ClickHouse
4. ✅ Check if path actually exists first with smaller bound
5. ✅ Consider using approximate algorithms for very large graphs

**Problem**: No results returned

**Solutions**:
1. Check if path exists with regular variable-length: `MATCH (a)-[*1..10]-(b)`
2. Try bidirectional: `*` instead of `*->`
3. Increase hop limit: `*1..6` instead of `*1..3`
4. Verify node IDs are correct

---

## Real-World Examples

### Example 1: LinkedIn-Style Connection Degree (Updated with shortestPath)

```cypher
-- Show shortest connection path between two users
MATCH path = shortestPath((user1:User {email: "alice@example.com"})
                          -[:CONNECTS_WITH*1..4]-
                          (user2:User {email: "bob@example.com"}))
RETURN user1.name AS start,
       user2.name AS end,
       length(path) AS degree,
       [n IN nodes(path) | n.name] AS connection_path
```

**Output**:
```
start: "Alice Johnson"
end: "Bob Smith"  
degree: 2
connection_path: ["Alice Johnson", "Carol White", "Bob Smith"]
```

**Note**: Using `shortestPath()` with explicit bounds (`*1..4`) ensures optimal performance and finds the minimum-hop connection.

### Example 2: GitHub-Style Repository Discovery

```cypher
-- Find repositories through stargazer network
MATCH (me:User {login: "myuser"})-[:STARRED]->(repo1:Repo)
      <-[:STARRED]-(other:User)-[:STARRED*1..2]->(discovered:Repo)
WHERE NOT (me)-[:STARRED]->(discovered)
  AND discovered.language = "Rust"
RETURN discovered.name, 
       discovered.stars,
       COUNT(DISTINCT other) as common_stargazers
ORDER BY common_stargazers DESC, discovered.stars DESC
LIMIT 25
```

**Use Case**: Repository recommendations based on similar developers

### Example 3: Supply Chain Traceability

```cypher
-- Trace product origin through suppliers
MATCH (product:Product {sku: "WIDGET-123"})-[r:SOURCED_FROM*]->(supplier:Supplier)
RETURN product.name,
       [s in nodes(r) | s.company_name] as supply_chain,
       length(r) as chain_length,
       supplier.country as origin_country
ORDER BY chain_length DESC
```

**Output**:
```
product.name: "Premium Widget"
supply_chain: ["Acme Corp", "Parts Inc", "Raw Materials Ltd", "Mine Co"]
chain_length: 4
origin_country: "Australia"
```

### Example 4: Citation Network Analysis

```cypher
-- Find influential papers by citation depth
MATCH (paper:Paper)-[c:CITES*1..3]->(cited:Paper)
WHERE paper.year >= 2020
RETURN cited.title,
       cited.authors,
       COUNT(DISTINCT paper) as citation_count,
       AVG(length(c)) as avg_citation_distance
ORDER BY citation_count DESC
LIMIT 100
```

**Use Case**: Academic research, finding seminal papers

### Example 5: File System Dependencies

```cypher
-- Find all transitive dependencies
MATCH (file:File {name: "main.rs"})-[d:DEPENDS_ON*]->(dependency:File)
RETURN dependency.name,
       dependency.type,
       MIN(length(d)) as shortest_path,
       COUNT(*) as num_paths
GROUP BY dependency.name, dependency.type
ORDER BY shortest_path, num_paths DESC
```

**Use Case**: Build systems, dependency analysis

---

## Performance Tuning

### Understanding Query Strategies

ClickGraph automatically chooses the optimal strategy:

**Chained JOINs** (for exact hop counts like `*2`, `*3`, `*5`):
- ✅ 2-5x faster for small hop counts
- ✅ Predictable memory usage
- ✅ Best for exact-distance queries
- ⚠️ Complexity increases with hop count

**Recursive CTEs** (for ranges like `*1..3`, `*..5`):
- ✅ Flexible for variable ranges
- ✅ Handles deep traversals efficiently
- ✅ Automatic cycle detection
- ⚠️ Memory usage depends on graph size

### Configuration Parameters

```bash
# Set maximum recursion depth (default: 100)
export BRAHMAND_MAX_CTE_DEPTH=200

# Or via CLI
./brahmand --max-cte-depth 200
```

### Depth Recommendations by Use Case

| Graph Size | Recommended Depth | Use Case |
|------------|-------------------|----------|
| < 1,000 nodes | 50-100 | Small teams, projects |
| 1K-10K nodes | 100-200 | Medium organizations |
| 10K-100K nodes | 100-300 | Social networks |
| 100K-1M nodes | 200-500 | Large enterprises |
| > 1M nodes | 300-1000 | Internet-scale graphs |

**Social Networks**: 200-300 (most connections within 3-4 hops)  
**Org Charts**: 50-100 (shallow hierarchies)  
**Knowledge Graphs**: 500-1000 (complex semantic relationships)  
**Supply Chains**: 100-200 (moderate depth)

### Query Optimization Tips

#### 1. **Add Specific Filters Early**

❌ **Inefficient**:
```cypher
MATCH (u1:User)-[*1..3]->(u2:User)
WHERE u1.country = "USA"
RETURN u2.name
```

✅ **Efficient**:
```cypher
MATCH (u1:User {country: "USA"})-[*1..3]->(u2:User)
RETURN u2.name
```

#### 2. **Limit Result Set Size**

```cypher
-- Always use LIMIT for exploratory queries
MATCH (u1:User)-[*1..2]->(u2:User)
RETURN u2.name
LIMIT 100
```

#### 3. **Use DISTINCT to Reduce Duplicates**

```cypher
-- Remove duplicate paths
MATCH (u1:User)-[*1..3]->(u2:User)
RETURN DISTINCT u2.name, u2.email
```

#### 4. **Prefer Exact Hop Counts When Possible**

```cypher
-- Faster: exact hop count
MATCH (u1:User)-[*2]->(u2:User)

-- Slower: range query
MATCH (u1:User)-[*1..2]->(u2:User)
```

#### 5. **Index Starting Nodes**

Ensure your ClickHouse tables have appropriate indexes:

```sql
-- Create index on frequently queried columns
CREATE INDEX idx_user_id ON users (user_id) TYPE bloom_filter;
CREATE INDEX idx_country ON users (country) TYPE set(100);
```

---

## Best Practices

### ✅ DO

1. **Set Appropriate Depth Limits**
   - Start with smaller depths (1-3) and increase as needed
   - Monitor query performance

2. **Use Specific Relationship Types**
   - `[r:FOLLOWS*1..2]` is better than `[r*1..2]`
   - Helps query planner optimize

3. **Filter Early and Often**
   - Add WHERE clauses on starting nodes
   - Reduce search space before traversal

4. **Use LIMIT for Exploration**
   - Especially with unbounded queries (`*`)
   - Prevents overwhelming result sets

5. **Test with Representative Data**
   - Verify performance with realistic graph sizes
   - Check memory usage under load

6. **Use Inline Node Patterns for Shortest Path**
   ```cypher
   -- Inline patterns ensure proper filter application
   MATCH path = shortestPath((p1:Person {id: X})-[:KNOWS*1..5]-(p2:Person {id: Y}))
   ```

7. **Always Set Explicit Hop Bounds on Large Graphs**
   - For graphs with >1,000 nodes
   - Prevents memory exhaustion
   - Typical bounds: social networks `*1..4`, org charts `*1..5`

### ❌ DON'T

1. **Avoid Unbounded Queries on Large Graphs**
   ```cypher
   -- Dangerous on million-node graphs
   MATCH (a)-[*]->(b)
   RETURN a, b
   ```

2. **Don't Ignore Cycle Detection**
   - ClickGraph handles this automatically
   - But be aware it can affect performance

3. **Don't Return Entire Path Objects Unnecessarily**
   ```cypher
   -- Heavy: returns full path structures
   MATCH path = (a)-[*1..5]->(b)
   RETURN path
   
   -- Lighter: return only needed properties
   MATCH (a)-[*1..5]->(b)
   RETURN a.id, b.id
   ```

4. **Avoid Very Deep Traversals on First Try**
   - Start with `*1..3`, not `*1..50`
   - Increase gradually based on results

5. **Don't Forget Resource Monitoring**
   - Watch memory usage with large result sets
   - Monitor query execution times

---

## Common Patterns & Anti-Patterns

### Pattern: Shortest Path Discovery

```cypher
-- Find shortest connection between two nodes
MATCH path = (start:User {id: 123})-[*1..5]->(end:User {id: 456})
RETURN path
ORDER BY length(path)
LIMIT 1
```

### Pattern: Network Neighborhood

```cypher
-- Get all nodes within N hops
MATCH (center:User {id: 789})-[*1..2]->(neighbor:User)
RETURN DISTINCT neighbor.name, neighbor.email
```

### Pattern: Bidirectional Search

```cypher
-- Find connections in either direction
MATCH (a:Person {name: "Alice"})-[*1..3]-(b:Person {name: "Bob"})
RETURN length(path) as distance
```

### Anti-Pattern: Cartesian Explosion

❌ **Avoid**:
```cypher
-- Creates huge intermediate result set
MATCH (u1:User)-[*1..3]->(u2:User)
MATCH (u3:User)-[*1..3]->(u4:User)
WHERE u2 = u4
```

✅ **Better**:
```cypher
-- Single traversal path
MATCH (u1:User)-[*1..3]->(u2:User)<-[*1..3]-(u3:User)
```

### Anti-Pattern: Unfiltered Aggregation

❌ **Avoid**:
```cypher
-- Processes entire graph
MATCH (u:User)-[*1..3]->(other:User)
RETURN COUNT(*)
```

✅ **Better**:
```cypher
-- Filtered subset
MATCH (u:User {country: "USA"})-[*1..3]->(other:User)
WHERE other.active = true
RETURN COUNT(*)
```

---

## Troubleshooting

### Query Takes Too Long

**Symptoms**: Query runs for minutes without completing

**Solutions**:
1. Reduce hop count: `*1..5` → `*1..3`
2. Add more specific filters on starting nodes
3. Use LIMIT to cap result size
4. Check graph size - might need indexing
5. Increase `max_cte_depth` if hitting limit prematurely

### Out of Memory Errors

**Symptoms**: Query fails with memory error

**Solutions**:
1. Reduce result set with LIMIT
2. Use DISTINCT to eliminate duplicates
3. Return only necessary properties, not full nodes
4. Consider breaking into multiple smaller queries
5. Increase ClickHouse memory limits

### No Results Returned

**Symptoms**: Query completes but returns 0 rows

**Check**:
1. Relationship directions: `->` vs `<-` vs `-`
2. Relationship type spelling: `FOLLOWS` vs `FOLLOW`
3. Node labels match schema: `User` vs `Users`
4. Starting node exists: verify with simple query
5. Hop count too restrictive: try wider range

### Unexpected Results

**Symptoms**: Too many or wrong results

**Verify**:
1. Cycle detection is working (should be automatic)
2. Filters are correctly placed (WHERE vs pattern)
3. DISTINCT is used where needed
4. Relationship types match intent
5. Direction matches graph structure

### Performance Degradation

**Symptoms**: Queries slow down over time

**Actions**:
1. Check ClickHouse table sizes
2. Verify indexes are maintained
3. Monitor memory usage trends
4. Review query patterns for inefficiencies
5. Consider partitioning large tables

---

## Examples by Domain

### Social Media

```cypher
-- Viral content tracking
MATCH (post:Post {id: 12345})<-[:SHARED*1..3]-(user:User)
RETURN COUNT(DISTINCT user) as viral_reach,
       MAX(length(path)) as max_spread_depth
```

### Healthcare

```cypher
-- Contact tracing
MATCH (patient:Person {case_id: "COVID-001"})-[c:CONTACTED*1..2]->(contact:Person)
WHERE c.date >= '2024-01-01'
RETURN contact.name, contact.phone, MIN(length(c)) as exposure_distance
```

### Finance

```cypher
-- Transaction chain analysis
MATCH (account:Account {id: "ACC-123"})-[t:TRANSFERRED*1..5]->(destination:Account)
WHERE ALL(tx in t WHERE tx.amount > 10000)
RETURN destination.account_number, SUM([tx in t | tx.amount]) as total_flow
```

### Transportation

```cypher
-- Route planning with stops
MATCH path = (origin:Station {name: "Grand Central"})-[r:CONNECTS*1..4]->(dest:Station {name: "Penn Station"})
WHERE ALL(conn in r WHERE conn.active = true)
RETURN [station in nodes(path) | station.name] as route,
       SUM([conn in r | conn.duration]) as total_time
ORDER BY total_time
LIMIT 5
```

---

## Advanced Topics

### Combining Fixed and Variable Paths

```cypher
-- Friend's followers (1 fixed + variable length)
MATCH (me:User {id: 123})-[:FRIEND]->(friend:User)-[:FOLLOWS*1..2]->(influencer:User)
RETURN influencer.username, COUNT(*) as connection_score
ORDER BY connection_score DESC
```

### Path Filtering

```cypher
-- Only paths through verified users
MATCH path = (start:User)-[*1..3]->(end:User)
WHERE ALL(user IN nodes(path) WHERE user.verified = true)
RETURN end.name
```

### Aggregations on Paths

```cypher
-- Average connection strength
MATCH (u1:User)-[connections*1..3]->(u2:User)
RETURN u1.name, 
       u2.name,
       AVG([c in connections | c.strength]) as avg_strength,
       length(connections) as path_length
ORDER BY avg_strength DESC
```

---

## Configuration Reference

### Environment Variables

```bash
# Maximum CTE recursion depth (default: 100)
export BRAHMAND_MAX_CTE_DEPTH=200

# ClickHouse connection
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_DATABASE="my_graph"
export CLICKHOUSE_USER="graph_user"
export CLICKHOUSE_PASSWORD="secure_password"
```

### Command-Line Flags

```bash
# Start server with custom settings
brahmand \
  --http-port 8080 \
  --bolt-port 7687 \
  --max-cte-depth 200
```

### YAML Configuration

```yaml
# Graph view configuration
nodes:
  User:
    table: social.users
    node_id: user_id
    properties:
      name: full_name
      email: email_address

relationships:
  FOLLOWS:
    table: social.follows
    from_id: follower_id
    to_id: followee_id
    from_node: User
    to_node: User
```

---

## Performance Benchmarks

### Test Graph Specifications

- **Small**: 1,000 nodes, 5,000 edges
- **Medium**: 10,000 nodes, 50,000 edges  
- **Large**: 100,000 nodes, 500,000 edges

### Query Performance (Approximate)

| Query Pattern | Small Graph | Medium Graph | Large Graph |
|--------------|-------------|--------------|-------------|
| `*1` (1 hop) | < 10ms | < 50ms | < 200ms |
| `*2` (2 hops) | < 20ms | < 100ms | < 500ms |
| `*1..2` (range) | < 30ms | < 150ms | < 800ms |
| `*1..3` (range) | < 50ms | < 300ms | 1-3s |
| `*` (unbounded) | < 100ms | 500ms-2s | 5-15s |

*Note: Times vary based on graph density, filters, and hardware*

---

## Getting Help

### Resources

- **Documentation**: `docs/` directory
- **Examples**: `examples/` directory  
- **Issue Tracker**: GitHub Issues
- **Test Suite**: `src/render_plan/tests/variable_length_tests.rs`

### Reporting Issues

When reporting performance or correctness issues, include:

1. Your query (Cypher)
2. Graph size (node/edge counts)
3. Configuration settings
4. Expected vs actual results
5. Query execution time

---

## What's Next?

### Planned Enhancements

- [ ] Multiple relationship types: `[r:FOLLOWS|FRIEND*1..3]` *(In Progress)*
- [ ] Path length weighting: `shortestPath((a)-[*]-(b), weight: r.distance)`
- [ ] All paths enumeration with limits: `allPaths((a)-[*1..3]-(b)) LIMIT 100`
- [ ] Conditional path traversal: More complex WHERE on path segments
- [ ] Graph algorithms: PageRank, centrality measures, community detection

### Recently Completed

- [x] **Shortest path algorithms**: `shortestPath()` and `allShortestPaths()` *(December 2025)*
- [x] Variable-length paths with property access *(October 2025)*
- [x] Path variables and functions: `nodes()`, `relationships()`, `length()` *(October 2025)*

### Current Limitations

- Unbounded shortestPath on large graphs (>10K nodes) requires explicit hop bounds for optimal performance
- Single relationship type per pattern (use multiple MATCH clauses for now)
- No named path variables in highly complex nested patterns
- Early termination optimization not available in ClickHouse recursive CTEs

---

**Version**: 0.1.0  
**Last Updated**: October 17, 2025  
**Status**: Production-Ready

For the latest updates, see [STATUS_REPORT.md](../STATUS_REPORT.md)



