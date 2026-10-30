# Known Limitations

This page documents current limitations, known issues, and workarounds for ClickGraph.

**Current Status**: Production-ready for read-only analytical queries  
**Version**: 0.5.4 (December 2025)  
**Test Results**: 596 unit tests (100%), 782 integration tests (100%)  
**Total**: 1,378 tests passing  
**Active Issues**: 2 (see [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md))

---

## Table of Contents
- [Read-Only by Design](#read-only-by-design)
- [Cypher Feature Support](#cypher-feature-support)
- [ClickHouse-Specific Limitations](#clickhouse-specific-limitations)
- [Known Issues](#known-issues)
- [Performance Considerations](#performance-considerations)
- [Platform-Specific Issues](#platform-specific-issues)

---

## Read-Only by Design

⚠️ **ClickGraph is a read-only graph query engine**

### Not Supported (By Design)
- ❌ `CREATE` - Creating nodes/relationships
- ❌ `SET` - Updating properties
- ❌ `DELETE`, `REMOVE` - Deleting nodes/relationships
- ❌ `MERGE` - Upsert operations
- ❌ Transaction management (`BEGIN`, `COMMIT`)
- ❌ Schema modifications (`CREATE INDEX`, `CREATE CONSTRAINT`)

### Why Read-Only?
ClickGraph is designed for **analytical queries** over existing ClickHouse data, not as a transactional graph database. For write operations, use:
- ClickHouse native INSERT/UPDATE operations
- ETL pipelines to populate tables
- Graph databases like Neo4j for transactional workloads

---

## Cypher Feature Support

### ✅ Fully Supported

**Pattern Matching:**
- ✅ Basic patterns: `(a:Label)`, `(a)-[r:TYPE]->(b)`
- ✅ Variable-length paths: `*`, `*2`, `*1..3`, `*..5`, `*2..`
- ✅ Multiple relationship types: `[:TYPE1|TYPE2|TYPE3]`
- ✅ Optional matching: `OPTIONAL MATCH`
- ✅ Untyped relationships: `(a)-[r]->(b)` (expands to UNION ALL)

**Path Functions:**
- ✅ `shortestPath()`, `allShortestPaths()`
- ✅ `length(path)`, `nodes(path)`, `relationships(path)`

**WHERE Clauses:**
- ✅ Property filters: `WHERE u.age > 25`
- ✅ Logical operators: `AND`, `OR`, `NOT`
- ✅ Pattern predicates: `WHERE (a)-[:FOLLOWS]->(b)`
- ✅ String operators: `STARTS WITH`, `ENDS WITH`, `CONTAINS`
- ✅ List operators: `IN`, `NOT IN`

**Aggregations:**
- ✅ `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- ✅ `collect()`, `DISTINCT`
- ✅ `GROUP BY`, `HAVING`

**Ordering & Limits:**
- ✅ `ORDER BY`, `LIMIT`, `SKIP`
- ✅ Multiple sort keys
- ✅ `ASC`/`DESC` ordering

**List Operations:**
- ✅ `UNWIND` - Expand lists into rows (generates ARRAY JOIN for arrays)

**Functions:**
- ✅ String functions: `toLower()`, `toUpper()`, `trim()`, `substring()`
- ✅ Math functions: `abs()`, `ceil()`, `floor()`, `round()`
- ✅ Type functions: `type()`, `labels()`, `properties()`

**Graph Algorithms:**
- ✅ `CALL pagerank(...)`

### ⚠️ Partially Supported

**List Comprehensions:**
- ✅ Basic: `[x IN list | x.prop]`
- ❌ Complex filters: `[x IN list WHERE x.age > 25 | x.name]` (workaround: use WITH clause)

**CASE Expressions:**
- ✅ Simple CASE: `CASE WHEN ... THEN ... END`
- ⚠️ May have edge cases in complex GROUP BY scenarios

**Nondeterministic draws in a CTE read twice** (#1495):
- ✅ A drawn value (`rand()`, `randomUUID()`, the clock) that no second read decides on lowers correctly
- ❌ A drawn value joined back as a join key of its reads, or one that filters / orders / groups the CTE's rows, is refused: `not lowered yet: a value drawn in WITH … by rand() draws again on every read and is a join key of its reads`. ClickHouse re-evaluates a CTE once per read, so the reads of the draw disagree where Neo4j's one execution agrees. The clock's refusal is conservative: two reads of a same-query `now()` may in practice agree, but may not — a query this rule refuses should be reshaped. Workaround: reshape the query so the drawn value is read once.

### ✅ Recently Implemented (v0.5.3-v0.6.0)

- ✅ Path variables: `p = (a)-[:TYPE*]->(b)` with `nodes(p)`, `relationships(p)`, `length(p)`
- ✅ `UNION`, `UNION ALL` between queries
- ✅ `EXISTS { MATCH ... }` pattern predicates
- ✅ Cross-table query correlation with WITH...MATCH
- ✅ Smart type inference for anonymous patterns
- ✅ FK-Edge patterns for hierarchical data
- ✅ **Pattern Comprehensions** (v0.6.0): `[(pattern) WHERE condition | projection]`
  - Works in RETURN items
  - Works nested inside functions like `size([(...)| ...])`
  - ⚠️ Multiple comprehensions may create Cartesian products (see [Cypher-Language-Reference](Cypher-Language-Reference.md#limitations-1))

### ❌ Not Yet Implemented

A read query outside what ClickGraph translates is refused with a `Not supported: …` error naming the construct (HTTP 400, a Bolt client error); it is not answered by another pipeline. A procedure `CALL` is supported as a standalone statement only; inside a query or a UNION it is refused too.

**Advanced Patterns:**
- ❌ Relationship property constraints in patterns: `()-[r {since: 2020}]->()`
- ❌ Variable-length paths inside pattern comprehensions

**Subqueries:**
- ❌ `CALL { ... }` subqueries

**Additional Functions:**
- ❌ Date/time functions (use ClickHouse functions directly)
- ❌ Spatial functions

**Graph Algorithms:**
- ❌ Centrality measures (planned)
- ❌ Community detection (planned)
- ❌ Connected components (planned)

---

## ClickHouse-Specific Limitations

### Schema Requirements

**Tables Must Be Pre-Created:**
ClickGraph maps to existing ClickHouse tables. You must:
1. Create tables in ClickHouse first
2. Define YAML schema mapping graph → tables
3. Ensure column types are compatible

**Example:**
```sql
-- Create table first
CREATE TABLE users (
    user_id UInt64,
    name String,
    email String
) ENGINE = MergeTree() ORDER BY user_id;
```

```yaml
# Then map in schema
nodes:
  - label: User
    table: users
    id_property: user_id
    property_mappings:
      name: name
      email: email
```

### Column Naming

**Property Mappings Required:**
If Cypher property names differ from ClickHouse column names, you must define mappings:

```yaml
property_mappings:
  # Cypher property: ClickHouse column
  name: full_name
  email: email_address
```

**Auto-Discovery:**
Use `auto_discover_columns: true` to map every column of the table as a property (under its own name, or camelCase with `naming_convention: camelCase`). The columns are read when the schema loads, so it needs a database connection (server, embedded chdb, or remote ClickHouse); SQL-only mode and Databricks refuse such a schema:

```yaml
nodes:
  - label: User
    table: users
    id_property: user_id
    auto_discover_columns: true  # Maps all columns automatically
```

### Data Types

**ClickHouse → Cypher Mapping:**
| ClickHouse Type | Cypher Type | Notes |
|-----------------|-------------|-------|
| UInt8-64, Int8-64 | Integer | Direct mapping |
| Float32, Float64 | Float | Direct mapping |
| String, FixedString | String | Direct mapping |
| Date, DateTime | String | No native date type in results |
| Array(T) | List | Converted to JSON arrays |
| Nullable(T) | T or null | Proper null handling |
| Enum8, Enum16 | String | Converted to string values |

**Limitations:**
- ❌ No native date/time type in Cypher results (returned as strings)
- ❌ Nested structures (Tuple, Nested) require custom handling
- ❌ Map type not directly supported
- ⚠️ A boolean compared with a number or a string follows Cypher (`=` false, ordering NULL) only when the number's or string's type is known: a literal, arithmetic, a string or number function, or a property whose type the schema declares (`property_types`). A property of undeclared type compared with a boolean is compared as ClickHouse compares it: as a number, `true` being 1. So are values whose kind is not known where they are compared: `coalesce(b, 1) = 1`, lists of lists (`[[b]] = [[1]]`), and a list mixing booleans and numbers (`[b] + [1]` shows `[true, true]`; Neo4j `[true, 1]`).

### Query Performance

**ClickHouse Table Engines:**
Performance varies by engine type:
- ✅ **MergeTree family** (best for analytics)
- ✅ **Memory** (fast for small datasets)
- ⚠️ **ReplacingMergeTree** (may have duplicate rows)
- ❌ **Distributed** (limited support, test thoroughly)

**Optimization Tips:**
- Use appropriate ORDER BY in table definition
- Create secondary indexes for frequent filters
- Partition large tables by time/category
- Use sampling for exploratory queries

---

## Known Issues

### 1. CTE Column Aliasing for Mixed RETURN

**Status**: 🟡 Active  
**Severity**: LOW (workaround available)

When RETURN references both WITH aliases AND additional node properties, the JOIN condition may use incorrect column names.

**Example**:
```cypher
-- May not work correctly
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
```

**Workaround**: Ensure RETURN only references WITH clause output:
```cypher
-- ✅ Works: RETURN only references WITH output
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a.name as name, COUNT(b) as follows
WHERE follows > 1
RETURN name, follows
```

### 2. Anonymous Nodes Without Labels (Safety Limits)

**Status**: ✅ Mostly Working  
**Severity**: LOW

**What Works** ✅:
- Label inference from relationship type: `()-[r:FLIGHT]->()` infers Airport
- Relationship type inference from typed nodes: `(a:Airport)-[r]->()` infers r:FLIGHT  
- Single-schema inference: `()-[r]->()` when only one relationship defined
- Single-node-schema inference: `MATCH (n) RETURN n` when only one node type
- Multi-hop anonymous patterns with single relationship type

**Safety Limit**: Max 4 types can be inferred automatically. More requires explicit label specification.

### 3. Statement Size Limits (1,000 clauses, nesting 500 deep)

**Status**: By design
**Severity**: LOW

A statement may have at most 1,000 clauses, all UNION arms together (`MATCH`, `WITH`, `UNWIND`, a free-standing `ORDER BY` / `SKIP` / `OFFSET` / `LIMIT`, `RETURN`, …), nest brackets (`(`, `[`, `{`) at most 500 deep, and have at most 500 `CASE` expressions. A larger one is refused before it is parsed: "Not supported: a statement of N clauses (at most 1000)" or "… brackets nested N deep (at most 500)" (HTTP 400, Bolt `Neo.ClientError.Statement.FeatureNotSupported`, an `Err` in the library). Translation recurses once per clause and once per bracket, and a stack overflow aborts the whole process: before these limits, a 255 KB request of 15,000 `WITH a LIMIT 100`, or a 40 KB one of 20,000 nested parentheses, or 5,000 nested CASEs, aborted a (debug-build) server, and 800 `WITH`s aborted `cg`. The limits are set for the default 128 MB stack (`CLICKGRAPH_THREAD_STACK_MB`; a smaller one is not taken into account). Real queries have tens of clauses and a few levels of nesting. Neo4j has no such limits.

**Not covered:** a long chain of operators recurses once per operator in the expression parser: `1 + 1 + …` with 20,000 terms (40 KB), or `NOT NOT …` 20,000 deep (80 KB), still overflows a server worker's stack.

The library entry points (embedded `Connection`, FFI, Go, Python, `cg`) translate a statement of more than 64 tokens, or nesting brackets more than 8 deep, with the servers' stack (`CLICKGRAPH_THREAD_STACK_MB`, default 128 MB), whatever thread the caller is on: on a pool of translation threads for translation (`query_to_sql`), on a thread of its own for a query that also runs (`query`). Shorter ones run on the caller's thread. A caller thread smaller than 2 MB (musl's default is 128 KB) may still be too small for those.

### 4. Relationships with a NULL `edge_id` and the same ends are one

**Status**: Known discrepancy (user, 2026-10-24)
**Severity**: LOW

Where a schema declares an `edge_id` whose column can be NULL, a relationship is identified by its stored ends and its `edge_id`, the NULL compared as a value (its element id is `TYPE:\N:from->to-`). Two rows of one relationship type with a NULL `edge_id` and the same two ends therefore have one identity and one element id, where Neo4j, which loads each row as its own relationship, has two:

- `r1 = r2` is true for them, and `r1 <> r2` false: `MATCH ()-[r1]->(), ()-[r2]->() WHERE r1 <> r2` counts fewer pairs than Neo4j (54 for 56 in a test graph), and a pattern of two relationships does not take both;
- `count(DISTINCT r)`, `collect(DISTINCT r)` and `WITH DISTINCT r` keep one of them, except where the query also reads a property of `r` that differs between them (`WITH DISTINCT r RETURN r.w` keeps both, `WITH DISTINCT r RETURN count(*)` one);
- Bolt and the HTTP graph format give both rows one element id and one `id()` number, so a driver or Neo4j Browser shows one relationship (with the first row's properties), and `elementId(r) = $id` or `id(r) = N` matches both rows.

`count(r)`, `RETURN r.w` and every other per-row read still see both rows. The tables have nothing else to tell such rows apart; give them distinct `edge_id`s to have them be two relationships. Relationships whose `edge_id` is not NULL, and schemas whose `edge_id` cannot be NULL, are not affected. The same holds for any two rows with one id: two node rows with the same id (a composite one with a NULL part included) are one node.

### Bolt: a `UInt64` above 9223372036854775807

**Severity**: LOW

Bolt's Integer is 64-bit signed, as Neo4j's. A `UInt64` value above `9223372036854775807` has none, so over Bolt it is sent as a string of its digits (`'18446744073709551615'`), in a column, a node's or relationship's property, a list or a map; it was sent as `-1` in a column, and as a Float with its low digits lost in a node. HTTP returns the number; the library returns a `Float64` for it. A `Decimal` or `Int128` / `UInt128` value in that range gets the same string over Bolt. A `Float64` of 2^63 or more (`9.3e18`) is written by ClickHouse as the digits of an integer, which no reader tells from a `UInt64`, so over Bolt it is the string of the digits of its value too, not a Float. So the type of a Bolt column can depend on the value: a `UInt64` column is an Integer up to 9223372036854775807 and a string above it (give the client `toString()` or a cast of its own if it needs one type).

Not covered: `Int128` / `UInt128` / `Int256` values, and `Decimal`s, above 18446744073709551615 (or with more fractional digits than a double holds) are read from ClickHouse's JSON as doubles, so over HTTP and over Bolt alike they are Floats that hold the digits a Float holds (`340282366920938463463374607431768211455` is `3.402823669209385e38`).

### Integration Test Status

**100% Pass Rate (1,378/1,378)**

| Category | Passing | Total | Rate |
|----------|---------|-------|------|
| Unit Tests | 596 | 596 | 100% |
| Integration (social_benchmark) | 391 | 391 | 100% |
| Integration (security_graph) | 391 | 391 | 100% |
| **Total** | **1,378** | **1,378** | **100%** |

---

## Performance Considerations

### Variable-Length Path Queries

**Recursion Depth:**
- An unbounded range (`*`, `*2..`) is not cut at a fixed depth: the walk ends when no
  trail (path without a repeated relationship) extends.
- ClickHouse bounds the recursion (`max_recursive_cte_evaluation_depth`), set from
  `--max-cte-depth` / `CLICKGRAPH_MAX_CTE_DEPTH` (default 100); a deeper walk fails
  loudly.

**Performance Warning:**
```cypher
-- ⚠️ Can be expensive on large graphs
MATCH (a)-[*]->(b) RETURN count(*)

-- ✅ Better: Use bounded paths
MATCH (a)-[*1..3]->(b) RETURN count(*)
```

**Optimization:**
- Exact hop counts use efficient chained JOINs
- Unbounded/range queries use recursive CTEs (slower)
- Add filters to reduce search space

### JOIN Performance

**Large Result Sets:**
Multi-hop queries generate multiple JOINs. For best performance:
- Use selective WHERE filters early in pattern
- Leverage ClickHouse ORDER BY keys
- Consider table partitioning for time-series data

### Query Cache

**Cache Limitations:**
- LRU eviction (configurable size)
- Cached by query + parameters + schema
- Not persistent across server restarts
- No distributed cache support

**Configure Cache:**
```bash
export QUERY_CACHE_ENABLED=true
export QUERY_CACHE_MAX_ENTRIES=1000
export QUERY_CACHE_TTL_SECONDS=3600
```

---

## Platform-Specific Issues

### Windows Development

**ClickHouse Docker Volume Permissions:**
- Issue: Container cannot write to mounted volumes
- Solution: Use `ENGINE = Memory` for development tables
- Impact: Data not persisted between container restarts (acceptable for dev/test)

**PowerShell Command Differences:**
- Use `Invoke-RestMethod` instead of `curl`
- Use `;` for command chaining (NOT `&&`)
- Use `Start-Job` for background processes

**Examples:**
```powershell
# ✅ Correct
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
  -ContentType "application/json" `
  -Body '{"query":"MATCH (n) RETURN n LIMIT 10"}'

# ✅ Background server
$job = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    cargo run --release --bin clickgraph
}
```

See [Development Environment Checklist](../DEV_ENVIRONMENT_CHECKLIST.md) for complete Windows setup.

### Linux/macOS

**No Known Platform-Specific Issues**

Standard Docker and shell commands work as expected.

---

## Workarounds & Best Practices

### Missing Features Workarounds

**1. List Comprehension Filters:**
```cypher
-- ❌ Not supported
RETURN [x IN nodes WHERE x.age > 25 | x.name]

-- ✅ Use WITH clause
WITH nodes
MATCH (x) WHERE x.age > 25
RETURN collect(x.name)
```

**2. Subqueries:**
```cypher
-- ❌ Not supported
MATCH (a) WHERE EXISTS { MATCH (a)-[:FOLLOWS]->(b) }

-- ✅ Use pattern predicates
MATCH (a) WHERE (a)-[:FOLLOWS]->()
RETURN a
```

**3. Complex Aggregations:**
```cypher
-- ❌ May fail in GROUP BY
CASE WHEN ... complex expression ...

-- ✅ Use WITH clause
WITH nodes, CASE ... END AS category
MATCH (n) RETURN n.type, category, count(*)
GROUP BY n.type, category
```

### Schema Design Best Practices

**1. Use Auto-Discovery:**
```yaml
nodes:
  - label: User
    table: users
    id_property: user_id
    auto_discover_columns: true  # Automatically map all columns
```

**2. Explicit Mappings for Clarity:**
```yaml
property_mappings:
  name: full_name      # Clear mapping
  email: email_addr    # Explicit is better than implicit
```

**3. Test Schema Loading:**
```bash
# Validate schema without executing queries
curl -X POST http://localhost:8080/schemas/load \
  -d '{"schema_name":"test", "config_content":"...", "validate_schema":true}'
```

---

## Reporting Issues

Found a bug or limitation not documented here?

**GitHub Issues**: https://github.com/kooby-data/clickgraph/issues

**Please Include:**
- ClickGraph version
- Cypher query that fails
- Expected vs actual behavior
- ClickHouse version and table schema
- Error messages and logs

---

## See Also

- [Troubleshooting Guide](Troubleshooting-Guide.md) - Debugging common errors
- [Performance Optimization](Performance-Query-Optimization.md) - Query tuning tips
- [Schema Configuration](Schema-Configuration-Advanced.md) - Schema best practices
- [API Reference](API-Reference-HTTP.md) - Complete API documentation
- [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md) - Detailed technical issue tracker
