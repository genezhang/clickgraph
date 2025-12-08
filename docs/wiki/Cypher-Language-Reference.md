# Cypher Language Reference

Complete syntax reference for Cypher queries supported by ClickGraph.

> **Note**: ClickGraph is a **read-only** graph query engine. Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are not supported.

> **Terminology (v0.5.2+)**: ClickGraph uses **"node"** and **"edge"** terminology following ISO standards (SQL/PGQ ISO/IEC 9075-16:2023, GQL ISO/IEC 39075:2024). The term "relationship" is deprecated but still supported for backward compatibility with Neo4j Cypher. In this documentation, we use "edge" to refer to connections between nodes.

---

## Table of Contents
- [USE Clause](#use-clause)
- [MATCH Clause](#match-clause)
- [WHERE Clause](#where-clause)
- [RETURN Clause](#return-clause)
- [WITH Clause](#with-clause)
- [UNWIND Clause](#unwind-clause)
- [ORDER BY, LIMIT, SKIP](#order-by-limit-skip)
- [Aggregation Functions](#aggregation-functions)
- [Path Expressions](#path-expressions)
- [Functions](#functions)
- [Operators](#operators)
- [Data Types](#data-types)
- [Parameters](#parameters)
- [Enterprise Features](#enterprise-features)

---

## USE Clause

The `USE` clause selects which graph schema to query. This is essential for multi-schema deployments where different logical graphs are mapped to different ClickHouse tables.

> **Critical**: USE clause takes a **graph schema name** (logical identifier), NOT a database name (physical storage).

### Syntax

```cypher
USE schema_name;
MATCH (n:User) RETURN n;
```

### Schema Name vs Database Name

**Graph Schema Name** (used in USE clause):
- Defined in YAML configuration files
- Logical identifier for a graph model
- Example: `social_graph`, `commerce_graph`, `test_graph_schema`

**Database Name** (ClickHouse physical storage):
- Physical ClickHouse database containing tables
- Example: `social_db`, `test_integration`
- **NOT** used in USE clause

### Examples

```cypher
-- Select social network schema
USE social_graph;
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, friend.name;

-- Select commerce schema
USE commerce_graph;
MATCH (p:Product)<-[:PURCHASED]-(customer:Customer)
RETURN p.name, count(customer) AS buyers;

-- Schema selection persists for session
USE test_graph_schema;
MATCH (n) RETURN count(n);  -- Uses test_graph_schema
MATCH (u:User) RETURN u;    -- Still uses test_graph_schema
```

### Schema Selection Priority

1. **USE clause** (highest priority)
2. **schema_name API parameter**
3. **"default" schema** (fallback)

```bash
# USE clause overrides schema_name parameter
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE social_graph; MATCH (n) RETURN count(n)",
    "schema_name": "commerce_graph"
  }'
# Query uses social_graph (from USE clause)
```

### Multi-Tenant Usage

Combine USE clause with view_parameters for tenant isolation:

```bash
curl -X POST http://localhost:8080/query \
  -d '{
    "query": "USE tenant_graph; MATCH (u:User) RETURN u",
    "view_parameters": {"tenant_id": "acme_corp"}
  }'
```

See [Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md) for complete documentation.

### Common Errors

**❌ Using database name instead of schema name:**
```cypher
USE test_integration;  -- WRONG: this is a database name
MATCH (n) RETURN n;
-- Error: Schema 'test_integration' not found
```

**✅ Correct usage:**
```cypher
USE test_graph_schema;  -- CORRECT: this is a schema name
MATCH (n) RETURN n;
```

---

## MATCH Clause

The `MATCH` clause specifies graph patterns to find.

### Basic Patterns

**Node Patterns:**
```cypher
-- Match any node
MATCH (n) RETURN n

-- Match nodes with label
MATCH (u:User) RETURN u

-- Match nodes with multiple labels
MATCH (u:User:Active) RETURN u

-- Anonymous nodes
MATCH () RETURN count(*)
```

**Edge Patterns:**
```cypher
-- Match edge with type
MATCH (a)-[e:FOLLOWS]->(b) RETURN a, e, b

-- Match any direction
MATCH (a)-[e:FOLLOWS]-(b) RETURN a, b

-- Match left direction
MATCH (a)<-[e:FOLLOWS]-(b) RETURN a, b

-- Anonymous edge (any type)
MATCH (a)-[e]->(b) RETURN a, b

-- Untyped anonymous edge
MATCH (a)-->(b) RETURN a, b
```

### Smart Type Inference (v0.5.4+)

Anonymous patterns can automatically infer types in many scenarios:

**Node Inference:**
```cypher
-- Single-schema inference: MATCH (n) infers type when only one node type exists
MATCH (n) RETURN n.name

-- Label inference from edges: unlabeled nodes get labels from relationship schema
MATCH ()-[r:FLIGHT]->()  -- Infers (Airport)-[r:FLIGHT]->(Airport)
RETURN r
```

**Edge Inference:**
```cypher
-- Single-relationship inference: infers type when only one edge type defined
MATCH (a:User)-[r]->(b:User) RETURN type(r)  -- Infers r:FOLLOWS if only FOLLOWS exists

-- Node-type inference: finds matching relationships for typed nodes
MATCH (a:Airport)-[r]->() RETURN type(r)  -- Infers r:FLIGHT based on Airport node
```

**Safety Limits:**
- Maximum 4 types can be inferred automatically
- More types require explicit specification
- Ambiguous patterns generate helpful error messages

**Multiple Edge Types:**
```cypher
-- Match either FOLLOWS or FRIENDS_WITH
MATCH (a)-[e:FOLLOWS|FRIENDS_WITH]->(b)
RETURN a.name, type(e), b.name

-- Three or more types
MATCH (a)-[:TYPE1|TYPE2|TYPE3]->(b)
RETURN a, b
```

### Variable-Length Paths

**Syntax:**
```cypher
-- Any length (unbounded)
MATCH (a)-[*]->(b)

-- Exact length
MATCH (a)-[*3]->(b)

-- Range (min..max)
MATCH (a)-[*1..3]->(b)

-- Min only (open-ended)
MATCH (a)-[*2..]->(b)

-- Max only
MATCH (a)-[*..5]->(b)
```

**Examples:**
```cypher
-- Friends of friends (exactly 2 hops)
MATCH (me:User {user_id: 1})-[:FOLLOWS*2]->(friend)
RETURN friend.name

-- Up to 3 hops away
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
RETURN a.name, b.name, length(path)

-- Unlimited depth (use with caution!)
MATCH (start)-[*]->(end)
WHERE start.user_id = 1
RETURN end.name
```

**Configuration:**
- Default max depth: 10 hops
- Configure via `--max-var-len-hops` CLI flag
- Environment: `MAX_VAR_LEN_HOPS=50`

### Path Variables

Store entire path for later use:

```cypher
-- Store path in variable
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
RETURN p, length(p)

-- Use path functions
MATCH p = (a)-[:FOLLOWS*]->(b)
RETURN nodes(p), edges(p), length(p)
```

### OPTIONAL MATCH

Match patterns optionally (LEFT JOIN semantics):

```cypher
-- Return users even if they don't follow anyone
MATCH (u:User)
OPTIONAL MATCH (u)-[e:FOLLOWS]->(friend)
RETURN u.name, friend.name

-- Multiple optional patterns
MATCH (u:User)
OPTIONAL MATCH (u)-[:FOLLOWS]->(f)
OPTIONAL MATCH (u)-[:LIKED]->(p:Post)
RETURN u.name, count(f), count(p)
```

**Null Handling:**
```cypher
-- Optional match returns NULL when no match
MATCH (u:User {user_id: 999})
OPTIONAL MATCH (u)-[:FOLLOWS]->(friend)
RETURN u.name, 
       CASE WHEN friend IS NULL THEN 'No friends' 
       ELSE friend.name END
```

### Pattern Predicates

Use patterns in WHERE clause:

```cypher
-- Filter by edge existence
MATCH (u:User)
WHERE (u)-[:FOLLOWS]->(:User {name: 'Alice'})
RETURN u.name

-- Negation
MATCH (u:User)
WHERE NOT (u)-[:FOLLOWS]->()
RETURN u.name
```

---

## WHERE Clause

Filter query results based on conditions.

### Property Filters

```cypher
-- Comparison operators
WHERE u.age > 25
WHERE u.age >= 18
WHERE u.age < 65
WHERE u.age <= 100
WHERE u.age = 30
WHERE u.age <> 30

-- Multiple conditions
WHERE u.age > 25 AND u.country = 'USA'
WHERE u.age < 18 OR u.age > 65
WHERE NOT u.is_active
```

### String Operators

```cypher
-- Pattern matching
WHERE u.name STARTS WITH 'A'
WHERE u.email ENDS WITH '@example.com'
WHERE u.bio CONTAINS 'developer'

-- Case-insensitive (use ClickHouse functions)
WHERE toLower(u.name) CONTAINS 'alice'
```

### List Operators

```cypher
-- Membership
WHERE u.country IN ['USA', 'Canada', 'UK']
WHERE u.status NOT IN ['banned', 'suspended']

-- Empty check
WHERE u.tags = []
```

### Null Checks

```cypher
-- Null comparison
WHERE u.nickname IS NULL
WHERE u.email IS NOT NULL
```

### Logical Operators

```cypher
-- AND, OR, NOT
WHERE u.age > 18 AND u.country = 'USA'
WHERE u.age < 18 OR u.age > 65
WHERE NOT (u.is_active AND u.verified)

-- Precedence with parentheses
WHERE (u.age > 25 OR u.verified) AND u.country = 'USA'
```

### Pattern Predicates in WHERE

```cypher
-- Edge existence
WHERE (a)-[:FOLLOWS]->(b)

-- Negation
WHERE NOT (a)-[:FOLLOWS]->(b)

-- Variable-length patterns
WHERE (a)-[:FOLLOWS*1..2]->(b)
```

---

## RETURN Clause

Specify what to return from the query.

### RETURN DISTINCT

De-duplicate results when multiple paths lead to the same node.

**Syntax:**
```cypher
RETURN DISTINCT expression [AS alias]
```

**When to Use:**
- Multi-hop traversals where multiple paths reach the same node
- Avoiding duplicate results in complex graph patterns
- Queries with multiple edge types to the same target

**Examples:**

```cypher
// Friend-of-friend with de-duplication
MATCH (me:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
WHERE me.name = 'Alice'
RETURN DISTINCT fof.name
// Without DISTINCT: May return same person multiple times (via different friends)
// With DISTINCT: Each person appears once

// Find all users connected within 2 hops
MATCH (start:User)-[:FOLLOWS*1..2]->(connected:User)
WHERE start.user_id = 1
RETURN DISTINCT connected.name

// Multiple edge types
MATCH (a:User)-[:FOLLOWS|FRIENDS_WITH]->(b:User)
RETURN DISTINCT b.name
```

**Implementation:**
- Generates `SELECT DISTINCT` in ClickHouse SQL
- Applied after all filters and joins
- Works with expressions, not just node properties

```cypher
// DISTINCT on computed values
MATCH (u:User)
RETURN DISTINCT u.age / 10 AS age_decade
```

### Basic Returns

```cypher
-- Return nodes
RETURN u

-- Return properties
RETURN u.name, u.email

-- Return edges
RETURN e

-- Return edge properties
RETURN type(e), e.since
```

### Expressions

```cypher
-- Arithmetic
RETURN u.age + 5 AS age_in_5_years

-- String concatenation
RETURN u.first_name + ' ' + u.last_name AS full_name

-- Functions
RETURN toLower(u.email) AS normalized_email
```

### Aliases

```cypher
-- Column aliases
RETURN u.name AS user_name
RETURN count(*) AS total_users
RETURN u.age * 12 AS months_old
```

### DISTINCT

```cypher
-- Unique values only
RETURN DISTINCT u.country

-- Distinct with multiple columns
RETURN DISTINCT u.country, u.city
```

### All Properties

```cypher
-- Return all properties as map (limited support)
RETURN u

-- Individual properties
RETURN u.name, u.email, u.age
```

---

## WITH Clause

Chain query parts and perform intermediate processing.

### Basic WITH

```cypher
-- Pipeline queries
MATCH (u:User)
WITH u, count(*) AS friend_count
WHERE friend_count > 10
RETURN u.name, friend_count
```

### Filtering After Aggregation

```cypher
-- Count followers, then filter
MATCH (u:User)<-[:FOLLOWS]-(follower)
WITH u, count(follower) AS followers
WHERE followers > 100
RETURN u.name, followers
```

### Transformations

```cypher
-- Transform before further matching
MATCH (u:User)
WITH u, toLower(u.email) AS email_lower
WHERE email_lower CONTAINS '@example.com'
RETURN u.name
```

### Ordering & Limiting

```cypher
-- Top N pattern
MATCH (u:User)
WITH u ORDER BY u.follower_count DESC LIMIT 10
MATCH (u)-[:POSTED]->(p:Post)
RETURN u.name, count(p) AS post_count
```

### Cross-Table Correlation (v0.5.4+)

Use WITH to correlate data across different tables:

```cypher
-- Correlate DNS requests with network connections (Zeek log analysis)
MATCH (ip1:IP)-[:DNS_REQUESTED]->(d:Domain)
WITH ip1, d
MATCH (ip2:IP)-[:CONNECTED_TO]->(dest:IP)
WHERE ip1.ip = ip2.ip
RETURN ip1.ip, d.name AS domain, dest.ip AS destination

-- Cross-table correlation with shared node variables
MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain), (src)-[:CONNECTED_TO]->(dest:IP)
RETURN src.ip, d.name, dest.ip
```

**Key Points**:
- The WHERE clause creates the JOIN condition between tables
- Works with denormalized edge schemas
- Generates efficient INNER JOINs in the SQL

---

## UNWIND Clause

Expand a list into individual rows. Particularly useful with array columns in denormalized tables.

### Basic Syntax

```cypher
UNWIND list AS item
RETURN item
```

### With Path Functions

```cypher
-- Expand nodes from a path
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
UNWIND nodes(p) AS node
RETURN DISTINCT node.name
```

### With Array Columns (Denormalized Tables)

```cypher
-- Flatten array column from denormalized edge table
MATCH (q:Query)-[rip:RESOLVED_IP]->(d:Domain)
UNWIND rip.ips AS resolved_ip
RETURN q.query, d.name, resolved_ip
```

**Note**: UNWIND on array columns generates ClickHouse `ARRAY JOIN` for optimal performance.

---

## ORDER BY, LIMIT, SKIP

Control result ordering and pagination.

### ORDER BY

```cypher
-- Ascending (default)
RETURN u.name ORDER BY u.name

-- Descending
RETURN u.name ORDER BY u.age DESC

-- Multiple keys
RETURN u.name, u.age 
ORDER BY u.country ASC, u.age DESC

-- By expression
ORDER BY length(u.name) DESC
```

### LIMIT

```cypher
-- Top N results
RETURN u.name ORDER BY u.follower_count DESC LIMIT 10

-- With SKIP for pagination
RETURN u.name ORDER BY u.age SKIP 20 LIMIT 10
```

### SKIP

```cypher
-- Skip first N results
RETURN u.name ORDER BY u.age SKIP 100

-- Pagination pattern
RETURN u.name ORDER BY u.user_id SKIP 0 LIMIT 10   -- Page 1
RETURN u.name ORDER BY u.user_id SKIP 10 LIMIT 10  -- Page 2
```

---

## Aggregation Functions

Compute aggregate values over groups.

### Basic Aggregations

```cypher
-- Count
RETURN count(*) AS total
RETURN count(u) AS user_count
RETURN count(DISTINCT u.country) AS countries

-- Sum
RETURN sum(u.follower_count) AS total_followers

-- Average
RETURN avg(u.age) AS average_age

-- Min/Max
RETURN min(u.registration_date) AS first_user
RETURN max(u.follower_count) AS most_followers
```

### collect()

```cypher
-- Collect into list
MATCH (u:User)
RETURN collect(u.name) AS all_names

-- Collect distinct
RETURN collect(DISTINCT u.country) AS countries

-- Collect properties
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, collect(friend.name) AS friends
```

### GROUP BY

```cypher
-- Implicit GROUP BY (by RETURN non-aggregates)
MATCH (u:User)
RETURN u.country, count(*) AS users_per_country

-- Multiple grouping keys
RETURN u.country, u.city, count(*) AS users
GROUP BY u.country, u.city

-- With expressions
RETURN u.age / 10 AS age_decade, count(*) AS users
```

### HAVING (via WITH)

```cypher
-- Filter after aggregation
MATCH (u:User)
WITH u.country AS country, count(*) AS user_count
WHERE user_count > 100
RETURN country, user_count
```

---

## Path Expressions

Work with paths and graph traversals.

### Shortest Path

```cypher
-- Single shortest path
MATCH p = shortestPath((a:User)-[*]-(b:User))
WHERE a.user_id = 1 AND b.user_id = 100
RETURN p, length(p)

-- All shortest paths
MATCH p = allShortestPaths((a:User)-[*]-(b:User))
WHERE a.user_id = 1 AND b.user_id = 100
RETURN p
```

### Path Functions

```cypher
-- Path length
MATCH p = (a)-[*]->(b)
RETURN length(p)

-- Nodes in path
MATCH p = (a)-[*1..3]->(b)
RETURN nodes(p)

-- Edges in path
MATCH p = (a)-[*]->(b)
RETURN edges(p)

-- Combined
MATCH p = (a:User {user_id: 1})-[:FOLLOWS*]->(b)
RETURN length(p) AS hops,
       [n IN nodes(p) | n.name] AS names,
       [e IN edges(p) | type(e)] AS edge_types
```

---

## Functions

### String Functions

```cypher
-- Case conversion
RETURN toLower('HELLO') AS lower    -- 'hello'
RETURN toUpper('hello') AS upper    -- 'HELLO'

-- Trimming
RETURN trim('  hello  ') AS trimmed -- 'hello'

-- Substring
RETURN substring('hello', 0, 3)     -- 'hel'

-- Length
RETURN length('hello')               -- 5
```

### Mathematical Functions

```cypher
-- Absolute value
RETURN abs(-42)                      -- 42

-- Rounding
RETURN ceil(3.14)                    -- 4
RETURN floor(3.14)                   -- 3
RETURN round(3.14)                   -- 3

-- Sign
RETURN sign(-42)                     -- -1
RETURN sign(0)                       -- 0
RETURN sign(42)                      -- 1
```

### Type Functions

```cypher
-- Edge type
MATCH (a)-[e]->(b)
RETURN type(e)

-- Node labels
MATCH (n)
RETURN labels(n)

-- Properties (limited support)
RETURN properties(n)
```

### List Functions

```cypher
-- Size
RETURN size([1, 2, 3])               -- 3

-- Head/Last
RETURN head([1, 2, 3])               -- 1
RETURN last([1, 2, 3])               -- 3

-- Range
RETURN range(1, 10)                  -- [1,2,3,4,5,6,7,8,9,10]
```

---

## Operators

### Arithmetic

```cypher
+  -- Addition
-  -- Subtraction
*  -- Multiplication
/  -- Division
%  -- Modulo
^  -- Exponentiation (ClickHouse only)
```

### Comparison

```cypher
=   -- Equal
<>  -- Not equal
<   -- Less than
<=  -- Less than or equal
>   -- Greater than
>=  -- Greater than or equal
```

### Logical

```cypher
AND  -- Logical AND
OR   -- Logical OR
NOT  -- Logical NOT
```

### String

```cypher
STARTS WITH  -- Prefix match
ENDS WITH    -- Suffix match
CONTAINS     -- Substring match
+            -- Concatenation
```

### List

```cypher
IN      -- Membership test
NOT IN  -- Negated membership
```

### Null

```cypher
IS NULL      -- Null check
IS NOT NULL  -- Not null check
```

---

## Data Types

### Scalar Types

```cypher
-- Integer
42, -100, 0

-- Float
3.14, -0.5, 1e10

-- String
'hello', "world", 'can\'t'

-- Boolean
true, false

-- Null
null
```

### Collections

```cypher
-- Lists
[1, 2, 3]
['a', 'b', 'c']
[1, 'mixed', true]

-- List comprehension
[x IN range(1, 10) | x * 2]
```

### Graph Types

```cypher
-- Nodes
(u:User)

-- Edges
-[e:FOLLOWS]->

-- Paths
p = (a)-[*]->(b)
```

---

## Parameters

Use parameterized queries for security and performance.

### Syntax

```cypher
-- Parameter syntax
WHERE u.user_id = $userId
WHERE u.email = $email
WHERE u.age > $minAge
```

### Usage

**HTTP API:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = $id RETURN u",
    "parameters": {"id": 123}
  }'
```

**Multiple Parameters:**
```cypher
MATCH (u:User)
WHERE u.age > $minAge 
  AND u.age < $maxAge
  AND u.country = $country
RETURN u.name, u.age
```

```json
{
  "query": "...",
  "parameters": {
    "minAge": 25,
    "maxAge": 65,
    "country": "USA"
  }
}
```

### Benefits

- ✅ SQL injection prevention
- ✅ Query plan caching
- ✅ Cleaner query syntax
- ✅ Type safety

---

## Enterprise Features

ClickGraph provides enterprise-grade features for multi-tenancy, security, and production deployments.

### Multi-Tenancy with view_parameters

**View Parameters** enable row-level security and tenant isolation by passing parameters to ClickHouse parameterized views.

**Schema Configuration:**
```yaml
name: tenant_graph
nodes:
  - label: User
    view: users_view
    view_parameters: [tenant_id]  # Define parameter
    properties:
      user_id:
        column: user_id
        type: integer
```

**ClickHouse View:**
```sql
CREATE VIEW users_view AS
SELECT * FROM users
WHERE tenant_id = {tenant_id:String};  -- Parameterized filter
```

**Query with Tenant Isolation:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u",
    "schema_name": "tenant_graph",
    "view_parameters": {
      "tenant_id": "acme_corp"
    }
  }'
# Only returns users for acme_corp tenant
```

### RBAC with Role Passthrough

**Role passthrough** enables ClickHouse role-based access control (RBAC) for fine-grained permissions.

**HTTP API:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u",
    "role": "analyst_role"
  }'
```

**ClickHouse Setup:**
```sql
-- Create role with limited permissions
CREATE ROLE analyst_role;
GRANT SELECT ON database.users TO analyst_role;
GRANT SELECT ON database.posts TO analyst_role;
-- No access to sensitive tables

-- Assign role to user
GRANT analyst_role TO analyst_user;
```

**Query Execution:**
- ClickGraph impersonates specified role when executing queries
- ClickHouse enforces role permissions
- Unauthorized access returns permission denied errors

### Schema Selection Methods

**Method 1: USE Clause (Cypher)**
```cypher
USE social_graph;
MATCH (u:User) RETURN u;
```

**Method 2: schema_name Parameter (API)**
```bash
curl -X POST http://localhost:8080/query \
  -d '{"query":"...", "schema_name":"social_graph"}'
```

**Method 3: Environment Variable (Server Default)**
```bash
export DEFAULT_SCHEMA=social_graph
clickgraph --http-port 8080
```

### Complete API Request Example

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE tenant_graph; MATCH (u:User) WHERE u.age > $minAge RETURN u",
    "parameters": {
      "minAge": 18
    },
    "view_parameters": {
      "tenant_id": "acme_corp"
    },
    "role": "analyst_role",
    "schema_name": "fallback_schema"
  }'
```

**Feature Interaction:**
1. `USE tenant_graph` selects schema (overrides schema_name parameter)
2. `parameters` substitutes `$minAge` → `18`
3. `view_parameters` passes `tenant_id` to ClickHouse views (tenant isolation)
4. `role` impersonates analyst_role (RBAC enforcement)
5. Query returns only acme_corp users aged > 18 with analyst_role permissions

### Production Best Practices

**Multi-Tenancy:**
- ✅ Always use view_parameters for tenant isolation
- ✅ Validate tenant_id at API gateway layer
- ✅ Use parameterized views with WHERE clauses
- ✅ Test cross-tenant data leakage scenarios

**Security:**
- ✅ Use RBAC roles for all production queries
- ✅ Never use admin/default role in application code
- ✅ Implement least-privilege access policies
- ✅ Audit role assignments regularly

**Schema Management:**
- ✅ Use descriptive schema names (social_graph, not db1)
- ✅ Version schema configurations in git
- ✅ Test schema changes in staging first
- ✅ Document schema → database mappings

See [Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md) for complete documentation.

---

## Advanced Features

### Graph Algorithms

**PageRank:**
```cypher
CALL pagerank(
  'User',              -- node label
  'FOLLOWS',           -- edge type
  'outgoing',          -- direction
  {
    iterations: 20,
    dampingFactor: 0.85,
    weightProperty: null
  }
) YIELD nodeId, rank
RETURN nodeId, rank
ORDER BY rank DESC
LIMIT 10
```

---

## Query Examples

### Simple Queries

```cypher
-- List all users
MATCH (u:User) 
RETURN u.name, u.email
LIMIT 10

-- Count edges
MATCH ()-[e:FOLLOWS]->()
RETURN count(e) AS total_follows

-- User details
MATCH (u:User {user_id: 1})
RETURN u
```

### Pattern Matching

```cypher
-- Find mutual followers
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a)
RETURN a.name, b.name

-- Triangle pattern
MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)-[:FOLLOWS]->(a)
RETURN a, b, c
```

### Aggregations

```cypher
-- Users by country
MATCH (u:User)
RETURN u.country, count(*) AS user_count
ORDER BY user_count DESC

-- Average followers per user
MATCH (u:User)<-[r:FOLLOWS]-()
RETURN avg(count(r)) AS avg_followers
```

### Path Queries

```cypher
-- Friends of friends
MATCH (me:User {user_id: 1})-[:FOLLOWS*2]->(fof)
WHERE NOT (me)-[:FOLLOWS]->(fof)
RETURN DISTINCT fof.name

-- Shortest path between users
MATCH p = shortestPath((a:User {user_id: 1})-[*]-(b:User {user_id: 100}))
RETURN length(p), [n IN nodes(p) | n.name]
```

### Complex Queries

```cypher
-- Influential users (high follower count, active)
MATCH (u:User)<-[:FOLLOWS]-(follower)
WITH u, count(follower) AS followers
WHERE followers > 100
MATCH (u)-[:POSTED]->(p:Post)
WITH u, followers, count(p) AS posts
WHERE posts > 10
RETURN u.name, followers, posts
ORDER BY followers DESC
LIMIT 20

-- Community detection (via mutual connections)
MATCH (u:User)-[:FOLLOWS]->(friend)<-[:FOLLOWS]-(other)
WHERE u <> other
WITH u, other, count(friend) AS mutual_friends
WHERE mutual_friends > 5
RETURN u.name, other.name, mutual_friends
ORDER BY mutual_friends DESC
```

---

## Limitations

See [Known Limitations](Known-Limitations.md) for complete list.

**Not Supported:**
- ❌ Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)
- ❌ Complex subqueries (`CALL { ... }`)
- ❌ Named path expressions (partial support)
- ❌ Advanced list comprehensions with filters

**Workarounds available** - see Known Limitations page.

---

## See Also

- [Quick Start Guide](Quick-Start-Guide.md) - Get started with ClickGraph
- [API Reference](API-Reference-HTTP.md) - HTTP API documentation
- [Schema Configuration](Schema-Configuration-Advanced.md) - Map ClickHouse tables
- [Performance Optimization](Performance-Query-Optimization.md) - Query tuning
- [Known Limitations](Known-Limitations.md) - Current limitations and workarounds
