# Cypher Basic Patterns

Master the fundamentals of Cypher query language with practical examples. This guide covers node patterns, relationship patterns, filtering, and returning results.

## Table of Contents
- [Node Patterns](#node-patterns)
- [Relationship Patterns](#relationship-patterns)
- [Property Filtering](#property-filtering)
- [Return Statements](#return-statements)
- [Ordering and Limiting](#ordering-and-limiting)
- [Anonymous Patterns](#anonymous-patterns)
- [Common Patterns Cheat Sheet](#common-patterns-cheat-sheet)

---

## Node Patterns

Node patterns match nodes in your graph. They use parentheses `()` and can specify labels and properties.

### Match All Nodes

```cypher
-- Match all User nodes (use specific labels)
MATCH (u:User)
RETURN u
LIMIT 10
```

**When to use**: Explore nodes of a specific type in your graph

<!-- 
⚠️ FUTURE FEATURE - Commented out until labelless node support is implemented

Labelless node matching is not yet supported due to architectural limitations in columnar storage.
To implement: Need to add UNION ALL across all node types or implement type inference.

```cypher
-- Match all nodes (any label) - NOT YET SUPPORTED
MATCH (n)
RETURN n
LIMIT 10
```
-->

### Match Nodes by Label

```cypher
-- Match all User nodes
MATCH (u:User)
RETURN u.name, u.country

-- Match all Post nodes
MATCH (p:Post)
RETURN p.title, p.date
```

**When to use**: Query specific node types in your graph

**Variable naming tips**:
- Use descriptive single letters: `u` for User, `p` for Post
- Use full words for clarity: `user`, `post`, `author`

### Match Nodes by Property

```cypher
-- Match user by name (use WHERE clause)
MATCH (u:User)
WHERE u.name = 'Alice'
RETURN u

-- Match users by country
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name, u.city

-- Multiple properties
MATCH (u:User)
WHERE u.country = 'USA' AND u.is_active = true
RETURN u.name
```

**When to use**: Find specific nodes or filter by exact property values

**Best practice**: Always use WHERE clause for property filtering - it's more flexible and fully supported.

<!-- 
⚠️ FUTURE FEATURE - Commented out until inline property filter support is implemented

Inline property filters are not yet supported. Parser needs enhancement for `{property: value}` syntax.

```cypher
-- Inline property syntax - NOT YET SUPPORTED
MATCH (u:User {name: 'Alice'})
RETURN u

MATCH (u:User {country: 'USA', is_active: true})
RETURN u.name
```

Use WHERE clause instead (shown in working examples above).
-->

<!-- 
⚠️ FUTURE FEATURE - Commented out until inline property filter support is added

Inline property filters need parser and planner updates to handle {property: value} syntax.
To implement: Update match_clause.rs to expand inline properties to WHERE conditions.

```cypher
-- Inline property syntax - NOT YET SUPPORTED
MATCH (u:User {name: 'Alice'})
RETURN u

MATCH (u:User {country: 'USA', is_active: true})
RETURN u.name
```
-->

### Anonymous Nodes

```cypher
-- Anonymous node (no variable assigned)
MATCH (:User)-[:FOLLOWS]->(friend:User)
RETURN friend.name
```

**When to use**: When you don't need to reference the node in RETURN or WHERE

**Performance tip**: Anonymous nodes can be slightly more efficient as they don't require variable binding.

---

## Relationship Patterns

Relationship patterns connect nodes with edges. They use square brackets `[]` and arrows `->`, `<-`, or `-` for direction.

### Basic Relationship

```cypher
-- Directed relationship (left to right)
MATCH (a:User)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name

-- Directed relationship (right to left)
MATCH (a:User)<-[:FOLLOWS]-(b:User)
RETURN a.name, b.name

-- Undirected relationship (either direction)
MATCH (a:User)-[:FOLLOWS]-(b:User)
RETURN a.name, b.name
```

**Direction semantics**:
- `->` : Relationship goes from left to right
- `<-` : Relationship goes from right to left
- `-` : Relationship in either direction (generates OR logic)

### Relationship with Variable

```cypher
-- Assign relationship to variable
MATCH (a:User)-[r:FOLLOWS]->(b:User)
RETURN a.name, b.name, r.since
```

**When to use**: Access relationship properties or return relationship details

### Multiple Relationship Types

```cypher
-- Match any of several relationship types
MATCH (a:User)-[:FOLLOWS|:FRIENDS_WITH]->(b:User)
RETURN a.name, b.name

-- Three or more types
MATCH (a:User)-[:FOLLOWS|:FRIENDS_WITH|:LIKES]->(b)
RETURN a.name, b.name, type(b) as node_type
```

**SQL generation**: Creates UNION of all relationship tables for optimal performance

**When to use**: Query multiple types of connections between nodes

### Anonymous Relationships

```cypher
-- Anonymous relationship (no type specified)
MATCH (a:User)-[]->(b:User)
RETURN a.name, b.name

-- With variable but no type
MATCH (a:User)-[r]->(b:User)
RETURN a.name, b.name, type(r)
```

**Behavior**: Auto-expands to ALL relationship types in schema (UNION generation)

**When to use**: Explore all connections, discover relationship types

**⚠️ Performance**: Can be expensive on large graphs - prefer explicit types when possible

### Relationship Property Filtering

```cypher
### Relationship Property Filtering

```cypher
-- Filter by relationship property (use WHERE clause)

<!--
⚠️ FUTURE FEATURE: Inline relationship property filters not yet supported
MATCH (a:User)-[:FOLLOWS {follow_date: '2024-01-01'}]->(b:User)
RETURN a.name, b.name

Reason: Parser needs enhancement for inline property syntax on relationships.
Use WHERE clause instead.
-->
MATCH (a:User)-[r:FOLLOWS]->(b:User)
WHERE r.follow_date > '2024-01-01'
RETURN a.name, b.name, r.follow_date

-- Multiple relationship property conditions
MATCH (a:User)-[r:FOLLOWS]->(b:User)
WHERE r.follow_date >= '2024-01-01' AND r.follow_date <= '2024-12-31'
RETURN a.name, b.name, r.follow_date
```

<!-- 
⚠️ FUTURE FEATURE - Commented out until inline relationship property filters are supported

To implement: Extend relationship pattern parser to handle property maps.

```cypher
-- Inline relationship property filter - NOT YET SUPPORTED
MATCH (a:User)-[:FOLLOWS {follow_date: '2024-01-01'}]->(b:User)
RETURN a.name, b.name
```
-->

---

## Property Filtering

Use WHERE clause for complex filtering conditions.

### Basic WHERE Clause

```cypher
-- Single condition
MATCH (u:User)
WHERE u.is_active = true
RETURN u.name, u.email

-- Multiple conditions (AND)
MATCH (u:User)
WHERE u.is_active = true AND u.country = 'USA'
RETURN u.name, u.country

-- Multiple conditions (OR)
MATCH (u:User)
WHERE u.country = 'USA' OR u.country = 'Canada'
RETURN u.name, u.country
```

### Comparison Operators

```cypher
-- Equality
MATCH (u:User) WHERE u.country = 'USA' RETURN u.name

-- Inequality
MATCH (u:User) WHERE u.country <> 'USA' RETURN u.name
MATCH (u:User) WHERE u.country != 'USA' RETURN u.name

-- Date comparison
MATCH (u:User) WHERE u.registration_date > '2024-01-01' RETURN u.name
MATCH (u:User) WHERE u.registration_date >= '2024-01-01' RETURN u.name
MATCH (u:User) WHERE u.registration_date < '2024-01-01' RETURN u.name
MATCH (u:User) WHERE u.registration_date <= '2024-01-01' RETURN u.name

-- Date range
MATCH (u:User) 
WHERE u.registration_date >= '2024-01-01' AND u.registration_date <= '2024-12-31' 
RETURN u.name, u.registration_date
```

### String Matching

```cypher
-- String equality (case-sensitive)
MATCH (u:User)
WHERE u.name = 'Alice'
RETURN u

-- String pattern matching (LIKE)
MATCH (u:User)
WHERE u.email LIKE '%@example.com'
RETURN u.name, u.email

-- Case-insensitive matching
MATCH (u:User)
WHERE toLower(u.name) = 'alice'
RETURN u.name

-- Contains substring
MATCH (u:User)
WHERE u.name LIKE '%alice%'
RETURN u.name
```

**Supported string functions**:
- `toLower(str)` - Convert to lowercase
- `toUpper(str)` - Convert to uppercase
- `trim(str)` - Remove whitespace
- `substring(str, start, length)` - Extract substring

### NULL Checks

```cypher
-- Check for NULL
MATCH (u:User)
WHERE u.email IS NULL
RETURN u.name

-- Check for NOT NULL
MATCH (u:User)
WHERE u.email IS NOT NULL
RETURN u.name, u.email

-- Null-safe access with COALESCE
MATCH (u:User)
RETURN u.name, COALESCE(u.email, 'no-email@example.com') as email
```

### Boolean Logic

```cypher
-- AND (both conditions must be true)
MATCH (u:User)
WHERE u.age > 30 AND u.country = 'USA'
RETURN u.name

-- OR (at least one condition must be true)
MATCH (u:User)
WHERE u.age > 50 OR u.country = 'USA'
RETURN u.name

-- NOT (negation)
MATCH (u:User)
WHERE NOT (u.age < 18)
RETURN u.name

-- Complex combinations with parentheses
MATCH (u:User)
WHERE (u.age > 30 AND u.country = 'USA') OR (u.age > 50 AND u.country = 'Canada')
RETURN u.name, u.age, u.country
```

### IN Operator

```cypher
-- Match multiple values
MATCH (u:User)
WHERE u.country IN ['USA', 'Canada', 'Mexico']
RETURN u.name, u.country

-- Numbers (use actual numeric properties)
MATCH (u:User)
WHERE u.user_id IN [1, 2, 3, 4, 5]
RETURN u.name, u.user_id
```

---

## Return Statements

Control what data is returned from your queries.

### Return Node Properties

```cypher
-- Single property
MATCH (u:User)
RETURN u.name

-- Multiple properties
MATCH (u:User)
RETURN u.name, u.email, u.country

-- All properties of a node
MATCH (u:User)
RETURN u
```

### Return with Aliases

```cypher
-- Rename columns in output
MATCH (u:User)
RETURN u.name AS user_name, u.email AS user_email

-- Useful for clarity
MATCH (a:User)-[:FOLLOWS]->(b:User)
RETURN a.name AS follower, b.name AS followed
```

### Return Expressions

```cypher
-- Return computed values
MATCH (u:User)
RETURN u.name, u.user_id, u.user_id * 100 AS scaled_id

-- String concatenation (use CONCAT function)
MATCH (u:User)
RETURN concat(u.name, ' - ', u.country) AS user_info

-- Boolean expressions
MATCH (u:User)
RETURN u.name, u.is_active, 
       CASE WHEN u.is_active THEN 'Active' ELSE 'Inactive' END AS status
```

<!-- 
⚠️ EXAMPLE USES NON-EXISTENT PROPERTY - Commented out

The 'age' property doesn't exist in benchmark schema. 
Use registration_date or other actual properties instead.

```cypher
-- Age-based categorization - PROPERTY DOESN'T EXIST IN SCHEMA
MATCH (u:User)
RETURN u.name,
       CASE
         WHEN u.age < 18 THEN 'Minor'
         WHEN u.age < 65 THEN 'Adult'
         ELSE 'Senior'
       END AS age_group
```
-->

### Return Distinct Values

```cypher
-- Remove duplicates
MATCH (u:User)
RETURN DISTINCT u.country

-- Distinct combinations
MATCH (u:User)
RETURN DISTINCT u.country, u.city
```

### Return Count

```cypher
-- Count all matching nodes
MATCH (u:User)
RETURN count(u) AS total_users

-- Count with filtering
MATCH (u:User)
WHERE u.is_active = true
RETURN count(u) AS active_users

-- Count distinct values
MATCH (u:User)
RETURN count(DISTINCT u.country) AS num_countries
```

---

## Ordering and Limiting

Control the order and size of result sets.

### ORDER BY

```cypher
-- Ascending order (default)
MATCH (u:User)
RETURN u.name, u.age
ORDER BY u.age

-- Explicit ascending
MATCH (u:User)
RETURN u.name, u.age
ORDER BY u.age ASC

-- Descending order
MATCH (u:User)
RETURN u.name, u.age
ORDER BY u.age DESC

-- Multiple columns
MATCH (u:User)
RETURN u.name, u.age, u.country
ORDER BY u.country, u.age DESC
```

**Order semantics**:
- `ASC` : Ascending (A→Z, 0→9) - default
- `DESC` : Descending (Z→A, 9→0)
- Multiple columns: Sort by first, then second for ties, etc.

### LIMIT

```cypher
-- Limit number of results
MATCH (u:User)
RETURN u.name
LIMIT 10

-- Top 10 newest users
MATCH (u:User)
RETURN u.name, u.registration_date
ORDER BY u.registration_date DESC
LIMIT 10

-- Combine with WHERE
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name
ORDER BY u.registration_date DESC
LIMIT 5
```

### SKIP

```cypher
-- Skip first N results
MATCH (u:User)
RETURN u.name
ORDER BY u.registration_date
SKIP 10
LIMIT 10

-- Pagination: Page 2 (results 11-20)
MATCH (u:User)
RETURN u.name, u.registration_date
ORDER BY u.registration_date
SKIP 10
LIMIT 10

-- Pagination: Page 3 (results 21-30)
MATCH (u:User)
RETURN u.name, u.registration_date
ORDER BY u.registration_date
SKIP 20
LIMIT 10
```

**Pagination formula**:
- Page N (1-indexed): `SKIP (N-1) * PageSize LIMIT PageSize`
- Example: Page 3 with 10 items per page: `SKIP 20 LIMIT 10`

---

## Anonymous Patterns

Anonymous patterns omit variable names when you don't need to reference nodes or relationships.

### Anonymous Nodes

```cypher
-- Don't need to reference the first user
MATCH (alice:User)-[:FOLLOWS]->(friend:User)
WHERE alice.name = 'Alice'
RETURN friend.name

-- Count relationships without naming nodes
MATCH (:User)-[:FOLLOWS]->(:User)
RETURN count(*) AS total_follows
```

**When to use**: Simplify queries when nodes aren't referenced in RETURN or WHERE

**⚠️ Known limitation**: Multi-hop patterns with anonymous intermediate nodes have issues. Use named nodes for multi-hop:

```cypher
-- ❌ Broken (anonymous intermediate in multi-hop)
MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User)
WHERE u1.name = 'Alice'
RETURN u2.name

-- ✅ Works (named intermediate)
MATCH (u1:User)-[:FOLLOWS]->(friend:User)-[:FOLLOWS]->(u2:User)
WHERE u1.name = 'Alice'
RETURN u2.name
```

### Anonymous Relationships

```cypher
-- No relationship variable needed
MATCH (a:User)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name

-- Anonymous relationship (no type specified) - expands to all types
MATCH (a:User)-[]->(b:User)
RETURN a.name, b.name

-- Count all relationships
MATCH ()-[r]->()
RETURN count(r) AS total_relationships
```

---

## Common Patterns Cheat Sheet

Quick reference for frequently used patterns.

### Find Specific Nodes

```cypher
-- By ID
MATCH (u:User) WHERE u.user_id = 123 RETURN u

-- By property
MATCH (u:User) WHERE u.name = 'Alice' RETURN u

-- By multiple properties
MATCH (u:User) 
WHERE u.country = 'USA' AND u.is_active = true 
RETURN u.name
```

### Find Neighbors

```cypher
-- Direct neighbors (1-hop)
MATCH (u:User)-[:FOLLOWS]->(friend)
WHERE u.name = 'Alice'
RETURN friend.name

-- Reverse relationships
MATCH (u:User)<-[:FOLLOWS]-(follower)
WHERE u.name = 'Alice'
RETURN follower.name

-- Both directions
MATCH (u:User)-[:FOLLOWS]-(connected)
WHERE u.name = 'Alice'
RETURN connected.name
```

### Count Relationships

```cypher
-- Count outgoing relationships
MATCH (u:User)-[:FOLLOWS]->()
RETURN u.name, count(*) AS following_count

-- Count incoming relationships
MATCH (u:User)<-[:FOLLOWS]-()
RETURN u.name, count(*) AS follower_count

-- Total relationship count
MATCH ()-[r:FOLLOWS]->()
RETURN count(r) AS total_follows
```

### Filter by Relationship Existence

```cypher
-- Users who follow someone
MATCH (u:User)-[:FOLLOWS]->()
RETURN DISTINCT u.name

-- Users who are followed by someone
MATCH (u:User)<-[:FOLLOWS]-()
RETURN DISTINCT u.name

-- Users with no outgoing follows
MATCH (u:User)
WHERE NOT (u)-[:FOLLOWS]->()
RETURN u.name
```

### Top N Queries

```cypher
-- Top 10 most followed users
MATCH (u:User)<-[:FOLLOWS]-()
RETURN u.name, count(*) AS followers
ORDER BY followers DESC
LIMIT 10

-- Top 10 oldest users
MATCH (u:User)
RETURN u.name, u.age
ORDER BY u.age DESC
LIMIT 10

-- Users in most common countries
MATCH (u:User)
RETURN u.country, count(*) AS user_count
ORDER BY user_count DESC
LIMIT 5
```

---

## Performance Tips

### Use Explicit Labels

```cypher
-- ✅ Good: Explicit label
MATCH (u:User) WHERE u.age > 30 RETURN u.name

-- ❌ Avoid: No label (scans all nodes)
MATCH (n) WHERE n.age > 30 RETURN n.name
```

### Use Explicit Relationship Types

```cypher
-- ✅ Good: Explicit type
MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name

-- ❌ Avoid: Anonymous relationship (scans all relationship types)
MATCH (a:User)-[]->(b:User) RETURN a.name, b.name
```

### Push Filters to WHERE Clause

```cypher
-- ✅ Good: Filter in WHERE (optimized)
MATCH (u:User)
WHERE u.age > 30 AND u.country = 'USA'
RETURN u.name

-- ⚠️ OK but less optimal: Inline properties
MATCH (u:User {country: 'USA'})
WHERE u.age > 30
RETURN u.name
```

### Use LIMIT Early

```cypher
-- ✅ Good: Limit reduces result set size
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name
ORDER BY u.registration_date DESC
LIMIT 100

-- ❌ Avoid: Processing all results without LIMIT
MATCH (u:User)
WHERE u.country = 'USA'
RETURN u.name
ORDER BY u.registration_date DESC
```

### Leverage Query Cache

**ClickGraph caches query plans** (10-100x speedup for repeated queries):

```cypher
-- First run: ~100ms (cold cache)
MATCH (u:User) WHERE u.country = 'USA' RETURN count(u)

-- Second run: ~1ms (cached plan)
MATCH (u:User) WHERE u.country = 'USA' RETURN count(u)
```

**Tip**: Use parameterized queries for maximum cache efficiency

---

## Practice Exercises

Try these exercises to master basic patterns:

### Exercise 1: User Queries
```cypher
-- 1. Find all users
MATCH (u:User) RETURN u LIMIT 10

-- 2. Find active users
MATCH (u:User) WHERE u.is_active = true RETURN u.name

-- 3. Find users in USA or Canada
MATCH (u:User) 
WHERE u.country = 'USA' OR u.country = 'Canada' 
RETURN u.name, u.country

-- 4. Find the 5 newest users
MATCH (u:User) 
RETURN u.name, u.registration_date 
ORDER BY u.registration_date DESC 
LIMIT 5

-- 5. Count users by country
MATCH (u:User) 
RETURN u.country, count(*) AS user_count 
ORDER BY user_count DESC
```

### Exercise 2: Relationship Queries
```cypher
-- 1. Find who Alice follows
MATCH (alice:User)-[:FOLLOWS]->(friend:User)
WHERE alice.name = 'Alice'
RETURN friend.name

-- 2. Find who follows Alice
MATCH (follower:User)-[:FOLLOWS]->(alice:User)
WHERE alice.name = 'Alice'
RETURN follower.name

-- 3. Find all FOLLOWS relationships
MATCH (a:User)-[:FOLLOWS]->(b:User)
RETURN a.name AS follower, b.name AS followed
LIMIT 100

-- 4. Count total relationships
MATCH ()-[r:FOLLOWS]->()
RETURN count(r) AS total_follows

-- 5. Find users with no followers
MATCH (u:User)
WHERE NOT EXISTS((u)<-[:FOLLOWS]-())
RETURN u.name
```

<!-- Note: EXISTS clause support may vary - test before using in production -->

### Exercise 3: Complex Filters
```cypher
-- 1. Users who registered in 2024
MATCH (u:User)
WHERE u.registration_date >= '2024-01-01' 
  AND u.registration_date < '2025-01-01'
RETURN u.name, u.registration_date

-- 2. Users whose name contains 'Alice'
MATCH (u:User)
WHERE u.name LIKE '%Alice%'
RETURN u.name

-- 3. Active users with email addresses
MATCH (u:User)
WHERE u.is_active = true AND u.email IS NOT NULL
RETURN u.name, u.email

-- 4. Top 10 most followed users
MATCH (u:User)<-[:FOLLOWS]-()
RETURN u.name, count(*) AS follower_count
ORDER BY follower_count DESC
LIMIT 10

-- 5. Users who have mutual follows
MATCH (u1:User)-[:FOLLOWS]->(u2:User)
WHERE EXISTS((u2)-[:FOLLOWS]->(u1))
RETURN u1.name, u2.name
LIMIT 20
```

**Solutions**: [Basic Patterns Solutions](Cypher-Basic-Patterns-Solutions.md)

---

## Next Steps

You've mastered basic Cypher patterns! Continue learning:

- **[Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md)** - Variable-length paths and shortest paths
- **[Optional Patterns](Cypher-Optional-Patterns.md)** - LEFT JOIN semantics with OPTIONAL MATCH
- **[Aggregations & Functions](Cypher-Functions.md)** - COUNT, SUM, string functions
- **[Advanced Patterns](Cypher-Advanced-Patterns.md)** - CASE, UNION, complex queries

Or explore real-world use cases:
- **[Social Network Analysis](Use-Case-Social-Network.md)**
- **[Fraud Detection](Use-Case-Fraud-Detection.md)**

---

[← Back to Home](Home.md) | [Next: Multi-Hop Traversals →](Cypher-Multi-Hop-Traversals.md)
