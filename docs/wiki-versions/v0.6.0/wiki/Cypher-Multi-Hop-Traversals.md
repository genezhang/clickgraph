> **Note**: This documentation is for ClickGraph v0.6.0. [View latest docs →](../../wiki/Home.md)
# Cypher Multi-Hop Traversals

Master advanced graph traversals including variable-length paths, shortest paths, and path functions.

## Table of Contents
- [Fixed-Length Multi-Hop](#fixed-length-multi-hop)
- [Variable-Length Paths](#variable-length-paths)
  - [Variable-Length with Chained Patterns](#variable-length-with-chained-patterns)
- [Shortest Path Algorithms](#shortest-path-algorithms)
- [Path Variables and Functions](#path-variables-and-functions)
- [Performance Optimization](#performance-optimization)
- [Common Multi-Hop Patterns](#common-multi-hop-patterns)

---

## Fixed-Length Multi-Hop

Fixed-length patterns specify exact number of hops between nodes.

### 2-Hop Traversals (Friends of Friends)

```cypher
-- Find friends of friends
MATCH (me:User {name: 'Alice'})-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
RETURN DISTINCT fof.name

-- With friend information
MATCH (me:User {name: 'Alice'})-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
RETURN friend.name AS friend, fof.name AS friend_of_friend
```

**Use cases**:
- Friend recommendations (people you might know)
- Second-degree connections
- Indirect connections

**⚠️ Important**: Always name intermediate nodes in multi-hop patterns:
```cypher
-- ✅ Correct (named intermediate)
MATCH (u1:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(u2:User)
WHERE u1.name = 'Alice'
RETURN u2.name

-- ❌ Known issue (anonymous intermediate in multi-hop)
MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User)
WHERE u1.name = 'Alice'
RETURN u2.name
```

### 3-Hop Traversals

```cypher
-- Three degrees of separation
MATCH (me:User {name: 'Alice'})
  -[:FOLLOWS]->(hop1)
  -[:FOLLOWS]->(hop2)
  -[:FOLLOWS]->(hop3)
RETURN hop3.name, hop3.country

-- Count by hop level
MATCH (me:User {name: 'Alice'})
  -[:FOLLOWS]->(hop1)
  -[:FOLLOWS]->(hop2)
  -[:FOLLOWS]->(hop3)
RETURN 'Hop 1' AS level, count(DISTINCT hop1) AS count
UNION ALL
MATCH (me:User {name: 'Alice'})
  -[:FOLLOWS]->(hop1)
  -[:FOLLOWS]->(hop2)
RETURN 'Hop 2' AS level, count(DISTINCT hop2) AS count
UNION ALL
MATCH (me:User {name: 'Alice'})
  -[:FOLLOWS]->(hop1)
RETURN 'Hop 1' AS level, count(DISTINCT hop1) AS count
```

### Mixed Edge Types

```cypher
-- Different edge types at each hop
MATCH (user:User)-[:AUTHORED]->(post:Post)<-[:LIKES]-(liker:User)
RETURN user.name AS author, post.title, liker.name AS fan

-- Chain of different actions
MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:AUTHORED]->(p:Post)-[:TAGGED]->(tag:Tag)
RETURN u1.name, u2.name, p.title, tag.name
```

---

## Variable-Length Paths

Variable-length paths match patterns with flexible hop counts.

### Basic Variable-Length Syntax

**Pattern**: `-[:TYPE*min..max]->`

| Pattern | Meaning | Example |
|---------|---------|---------|  
| `*` | Any number of hops (1 or more) | `-[:FOLLOWS*]->` |
| `*0..` | Zero or more hops (includes starting node) | `-[:FOLLOWS*0..]->` |
| `*0..5` | Zero to 5 hops | `-[:FOLLOWS*0..5]->` |
| `*2` | Exactly 2 hops | `-[:FOLLOWS*2]->` |
| `*1..3` | 1 to 3 hops | `-[:FOLLOWS*1..3]->` |
| `*..5` | Up to 5 hops (1-5) | `-[:FOLLOWS*..5]->` |
| `*2..` | 2 or more hops | `-[:FOLLOWS*2..]->` |### Any Number of Hops (`*`)

```cypher
-- All reachable users (any distance)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*]->(reachable)
RETURN DISTINCT reachable.name

-- All followers at any distance
MATCH (me:User {name: 'Alice'})<-[:FOLLOWS*]-(follower)
RETURN DISTINCT follower.name
```

**⚠️ Warning**: Unbounded paths can be expensive on large graphs. Always use LIMIT:

```cypher
-- ✅ Safe with LIMIT
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*]->(reachable)
RETURN DISTINCT reachable.name
LIMIT 100

-- ❌ Dangerous (could return millions)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*]->(reachable)
RETURN DISTINCT reachable.name
```

### Zero or More Hops (`*0..`)

```cypher
-- Include starting node (zero hops) plus all reachable nodes
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*0..]->(reachable)
RETURN DISTINCT reachable.name
LIMIT 100

-- Zero to 3 hops (includes starting node)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*0..3]->(nearby)
RETURN DISTINCT nearby.name, nearby.country

-- Useful for "this node and its neighbors"
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*0..1]->(immediate)
RETURN immediate.name
```

**Use cases**:
- Include the starting node in results
- "This entity and all related entities"
- Self-referential patterns

**⚠️ Important**: `*0..` includes the starting node with zero hops!

### Exact Hop Count (`*N`)

```cypher
-- Exactly 2 hops (friends of friends)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*2]->(fof)
RETURN DISTINCT fof.name

-- Exactly 3 hops
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*3]->(distant)
RETURN DISTINCT distant.name

-- Exactly 1 hop (same as no asterisk)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*1]->(friend)
RETURN friend.name
-- Equivalent to:
MATCH (me:User {name: 'Alice'})-[:FOLLOWS]->(friend)
RETURN friend.name
```

**Performance**: Exact hop queries are optimized with chained JOINs (fast!)

### Range Patterns (`*min..max`)

```cypher
-- 1 to 3 hops away
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*1..3]->(nearby)
RETURN DISTINCT nearby.name, nearby.country

-- 2 to 4 hops (exclude direct friends)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*2..4]->(distant)
RETURN DISTINCT distant.name

-- Filter by properties at end
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*1..3]->(nearby:User)
WHERE nearby.country = 'USA'
RETURN DISTINCT nearby.name
```

### Open-Ended Ranges

```cypher
-- Up to 5 hops (*..5 means 1-5)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*..5]->(reachable)
RETURN DISTINCT reachable.name
LIMIT 100

-- At least 2 hops (*2.. means 2 or more)
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*2..]->(distant)
RETURN DISTINCT distant.name
LIMIT 100
```

**⚠️ Performance**: Open-ended ranges (*2..) can be expensive. Always use LIMIT!

### Variable-Length with Edge Variable

```cypher
-- Access edges in the path
MATCH (a:User)-[edges:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN b.name, length(edges) AS hops

-- Filter on edge properties
MATCH (a:User)-[edges:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice' AND all(e IN edges WHERE e.since > '2024-01-01')
RETURN b.name
```

### Variable-Length with Chained Patterns

Combine variable-length paths with additional graph patterns:

```cypher
-- Recursive group membership + permission access
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File)
RETURN u.name, g.name AS via_group, f.name

-- Recursive folder hierarchy + file access
MATCH (root:Folder)-[:CONTAINS*]->(folder:Folder)-[:CONTAINS]->(f:File)
WHERE root.name = 'Root'
RETURN root.path, folder.name AS subfolder, f.name

-- Multi-level with aggregation
MATCH (u:User)-[:MEMBER_OF*]->(g:Group)-[:HAS_ACCESS]->(f:File)
RETURN u.name AS user, COUNT(DISTINCT f) AS accessible_files, COUNT(DISTINCT g) AS via_groups
```

**How it works**:
1. Variable-length portion (`-[:MEMBER_OF*]->`) uses recursive CTE
2. CTE results are JOINed back to retrieve start/end node properties
3. Additional patterns (`-[:HAS_ACCESS]->(f:File)`) are chained as regular JOINs

**Generated SQL structure**:
```sql
WITH RECURSIVE path_cte AS (...)
SELECT u.name, g.name, f.name
FROM path_cte AS t
JOIN users AS u ON t.start_id = u.id        -- Start node properties
JOIN groups AS g ON t.end_id = g.id          -- End node properties
JOIN permissions AS p ON p.group_id = g.id   -- Chained pattern
JOIN files AS f ON f.id = p.file_id          -- Chained pattern endpoint
```

**Use cases**:
- Security audit: "What files can this user access via group membership?"
- Permission propagation: "All resources accessible through recursive groups"
- Folder hierarchies: "Find all files in nested folder structure"

---

## Shortest Path Algorithms

Find the shortest path(s) between two nodes.

### `shortestPath()` Function

Returns a **single shortest path** between two nodes.

```cypher
-- Shortest path between Alice and Bob
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN path

-- Return path length
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN length(path) AS degrees_of_separation

-- Return nodes in path
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN [node IN nodes(path) | node.name] AS path_names
```

**Key features**:
- Returns only ONE shortest path (even if multiple exist)
- Uses bidirectional search (efficient!)
- Undirected by default (use `-` not `->`)

### `allShortestPaths()` Function

Returns **all shortest paths** between two nodes (same length).

```cypher
-- All shortest paths between Alice and Bob
MATCH path = allShortestPaths((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN path

-- Count number of shortest paths
MATCH path = allShortestPaths((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN count(path) AS num_shortest_paths

-- Show all shortest path routes
MATCH path = allShortestPaths((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN [node IN nodes(path) | node.name] AS route
```

**When to use**:
- Find all equally-short paths
- Analyze alternative routes
- Network redundancy analysis

### Shortest Path with Length Constraints

```cypher
-- Shortest path up to 5 hops
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*..5]-(b:User {name: 'Bob'}))
RETURN path

-- Shortest path at least 2 hops (exclude direct connection)
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*2..]-(b:User {name: 'Bob'}))
RETURN path

-- Shortest path within range
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*2..4]-(b:User {name: 'Bob'}))
RETURN path
```

### Directed Shortest Path

```cypher
-- Shortest path following edge direction (-> not -)
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]->(b:User {name: 'Bob'}))
RETURN length(path), [node IN nodes(path) | node.name]

-- Reverse direction
MATCH path = shortestPath((a:User {name: 'Alice'})<-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN length(path)
```

### Shortest Path Between Multiple Pairs

```cypher
-- Shortest paths from Alice to multiple users
MATCH (a:User {name: 'Alice'}), (b:User)
WHERE b.country = 'Canada'
WITH a, b
MATCH path = shortestPath((a)-[:FOLLOWS*]-(b))
RETURN b.name, length(path) AS distance
ORDER BY distance
LIMIT 10
```

### Filtering Shortest Path Results

```cypher
-- Only return paths longer than 2 hops
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
WHERE length(path) > 2
RETURN path

-- Filter by intermediate nodes
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
WHERE any(node IN nodes(path) WHERE node.country = 'USA')
RETURN path

-- Exclude certain nodes from path
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
WHERE none(node IN nodes(path) WHERE node.name IN ['Charlie', 'Diana'])
RETURN path
```

---

## Path Variables and Functions

Capture and manipulate entire paths.

### Path Variable Assignment

```cypher
-- Assign path to variable
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN path
```

### `length()` Function

Returns number of **edges** in path (not nodes).

```cypher
-- Path length (number of hops)
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN b.name, length(path) AS hops

-- Group by distance
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN length(path) AS distance, count(DISTINCT b) AS num_users
ORDER BY distance
```

**Note**: `length(path)` counts edges, not nodes
- 1 hop = 1 edge = 2 nodes
- 2 hops = 2 edges = 3 nodes

### `nodes()` Function

Returns all nodes in a path as a list.

```cypher
-- Extract all nodes in path
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN nodes(path)

-- Get node names
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN [node IN nodes(path) | node.name] AS path_names

-- Count nodes in path
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN b.name, size(nodes(path)) AS num_nodes
```

### `edges()` Function

Returns all edges in a path as a list.

```cypher
-- Extract all edges
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN edges(path)

-- Get edge properties
MATCH path = (a:User {name: 'Alice'})-[*1..3]->(b:User)
RETURN b.name, [e IN edges(path) | e.since] AS follow_dates

-- Check all edges meet condition
MATCH path = (a:User {name: 'Alice'})-[*1..3]->(b:User)
WHERE all(e IN edges(path) WHERE e.since > '2024-01-01')
RETURN b.name
```

### List Comprehensions on Paths

```cypher
-- Extract specific properties from nodes
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN [node IN nodes(path) | node.name + ' (' + node.country + ')'] AS path_info

-- Filter nodes in path
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
RETURN [node IN nodes(path) WHERE node.country = 'USA' | node.name] AS usa_users_in_path

-- Transform edge properties
MATCH path = (a:User {name: 'Alice'})-[*1..3]->(b:User)
RETURN [e IN edges(path) | {type: type(e), since: e.since}] AS edge_info
```

---

## Performance Optimization

### Recursion Depth Configuration

ClickGraph limits recursion depth for safety. Configure via CLI or environment:

```bash
# Default: 100 levels
cargo run --bin clickgraph

# Increase for deeper traversals (up to 1000)
cargo run --bin clickgraph -- --max-recursion-depth 500

# Environment variable
export MAX_RECURSION_DEPTH=500
```

**Guidelines**:
- Default (100): Good for most social networks
- 500: Large enterprise graphs
- 1000: Maximum (use with caution on dense graphs)

### Prefer Exact Hops Over Ranges

```cypher
-- ✅ Fast (optimized with chained JOINs)
MATCH (a:User)-[:FOLLOWS*2]->(b:User)
RETURN b.name

-- ⚠️ Slower (uses recursive CTEs)
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
RETURN b.name
```

**When you know exact distance**: Use `*N` for best performance

### Always Use LIMIT with Unbounded Paths

```cypher
-- ❌ Dangerous (could scan entire graph)
MATCH (a:User)-[:FOLLOWS*]->(b:User)
WHERE a.name = 'Alice'
RETURN b.name

-- ✅ Safe (limits result size)
MATCH (a:User)-[:FOLLOWS*]->(b:User)
WHERE a.name = 'Alice'
RETURN b.name
LIMIT 100
```

### Filter Early in the Pattern

```cypher
-- ✅ Good (filter source node first)
MATCH (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
WHERE b.country = 'USA'
RETURN b.name

-- ⚠️ Less optimal (filter after traversal)
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice' AND b.country = 'USA'
RETURN b.name
```

### Use Shortest Path for Single Path Queries

```cypher
-- ✅ Fast (shortest path algorithm)
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN length(path)

-- ⚠️ Slower (finds all paths, returns shortest)
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'})
RETURN length(path) AS dist
ORDER BY dist
LIMIT 1
```

### Leverage Query Cache

Variable-length queries benefit significantly from caching:

```cypher
-- First run: ~200ms (cold cache, builds recursive CTE)
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN count(b)

-- Second run: ~2ms (cached query plan)
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN count(b)
```

**100x speedup for repeated patterns!**

---

## Common Multi-Hop Patterns

### Friend Recommendations

```cypher
-- Friends of friends you don't already follow
MATCH (me:User {name: 'Alice'})-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
WHERE NOT (me)-[:FOLLOWS]->(fof) AND fof <> me
RETURN fof.name, count(*) AS mutual_friends
ORDER BY mutual_friends DESC
LIMIT 10
```

### Network Reach Analysis

```cypher
-- Count reachable users by distance
MATCH path = (me:User {name: 'Alice'})-[:FOLLOWS*1..5]->(reachable)
RETURN length(path) AS distance, count(DISTINCT reachable) AS num_users
ORDER BY distance
```

### Influencer Detection

```cypher
-- Users with most followers within 2 hops
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*1..2]->(nearby)
WITH nearby
MATCH (nearby)<-[:FOLLOWS]-(follower)
RETURN nearby.name, count(DISTINCT follower) AS followers
ORDER BY followers DESC
LIMIT 10
```

### Connected Components

```cypher
-- All users connected to Alice (any distance)
MATCH (alice:User {name: 'Alice'})-[:FOLLOWS*]-(connected)
RETURN count(DISTINCT connected) AS component_size
```

### Degrees of Separation

```cypher
-- Average degrees of separation in network
MATCH (a:User), (b:User)
WHERE a.user_id < b.user_id  -- Avoid duplicates
WITH a, b
MATCH path = shortestPath((a)-[:FOLLOWS*]-(b))
RETURN avg(length(path)) AS avg_separation,
       min(length(path)) AS min_separation,
       max(length(path)) AS max_separation
```

### Path Existence Check

```cypher
-- Check if path exists between two users
MATCH (a:User {name: 'Alice'}), (b:User {name: 'Bob'})
RETURN CASE
  WHEN EXISTS((a)-[:FOLLOWS*]-(b)) THEN 'Connected'
  ELSE 'Not connected'
END AS status
```

### Bottleneck Detection

```cypher
-- Find users who appear in many shortest paths (potential bottlenecks)
MATCH (a:User), (b:User)
WHERE a.user_id < b.user_id
WITH a, b
MATCH path = shortestPath((a)-[:FOLLOWS*]-(b))
UNWIND nodes(path) AS node
WITH node, count(*) AS centrality
WHERE node:User
RETURN node.name, centrality
ORDER BY centrality DESC
LIMIT 10
```

---

## Advanced Examples

### Multi-Type Variable-Length

```cypher
-- Follow chains through different edge types
MATCH (user:User)-[:FOLLOWS|:FRIENDS_WITH*1..3]->(connected)
WHERE user.name = 'Alice'
RETURN DISTINCT connected.name

-- Mixed edge types in path
MATCH path = (user:User)-[:FOLLOWS|:AUTHORED*1..3]->(endpoint)
WHERE user.name = 'Alice'
RETURN endpoint, length(path)
```

### Conditional Path Filtering

```cypher
-- Only paths through active users
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
WHERE all(node IN nodes(path) WHERE node.is_active = true)
RETURN b.name

-- No paths through blocked users
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
WHERE none(node IN nodes(path) WHERE node.is_blocked = true)
RETURN b.name
```

### Path Aggregations

```cypher
-- Average age along path
MATCH path = (a:User {name: 'Alice'})-[:FOLLOWS*1..3]->(b:User)
WITH b, path
RETURN b.name, avg([node IN nodes(path) | node.age]) AS avg_age_in_path
ORDER BY avg_age_in_path DESC
```

---

## Practice Exercises

### Exercise 1: Basic Multi-Hop
```cypher
-- 1. Find all friends of friends for Alice
-- 2. Count users at each distance (1-3 hops) from Alice
-- 3. Find the longest path from Alice (up to 5 hops)
```

### Exercise 2: Shortest Paths
```cypher
-- 1. Find shortest path between Alice and Bob
-- 2. Find all shortest paths between Alice and Bob
-- 3. Find average degrees of separation in your network
```

### Exercise 3: Path Functions
```cypher
-- 1. List all node names in shortest path Alice→Bob
-- 2. Find paths where all users are from USA
-- 3. Count edges in paths of length 3
```

### Exercise 4: Recommendations
```cypher
-- 1. Recommend friends (FOF not already followed)
-- 2. Find influencers within 3 hops
-- 3. Detect users who appear in many shortest paths
```

**Solutions**: [Multi-Hop Traversals Solutions](Cypher-Multi-Hop-Solutions.md)

---

## Known Limitations

### Anonymous Intermediate Nodes in Multi-Hop

**Issue**: Multi-hop patterns with anonymous intermediate nodes have alias preservation issues.

```cypher
-- ❌ Known issue (broken SQL generation)
MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User)
WHERE u1.user_id = 1
RETURN u2.name

-- ✅ Workaround (use named intermediate)
MATCH (u1:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(u2:User)
WHERE u1.user_id = 1
RETURN u2.name
```

**Status**: Low priority, simple workaround available, will be fixed in future release.

**See**: [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md) for details

---

## Next Steps

You've mastered multi-hop traversals! Continue learning:

- **[Aggregations & Functions](Cypher-Functions.md)** - COUNT, SUM, string/date functions
- **[Optional Patterns](Cypher-Optional-Patterns.md)** - LEFT JOIN semantics
- **[Advanced Patterns](Cypher-Advanced-Patterns.md)** - CASE, UNION, complex queries

Or explore performance:
- **[Performance Tuning](Performance-Query-Optimization.md)** - Optimize graph queries
- **[Schema Optimization](Schema-Optimization.md)** - Design efficient schemas

Or see real examples:
- **[Social Network Analysis](Use-Case-Social-Network.md)** - Complete working example
- **[Fraud Detection](Use-Case-Fraud-Detection.md)** - Transaction networks

---

[← Back: Basic Patterns](Cypher-Basic-Patterns.md) | [Home](Home.md) | [Next: Functions →](Cypher-Functions.md)
