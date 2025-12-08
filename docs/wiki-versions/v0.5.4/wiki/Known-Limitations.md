> **Note**: This documentation is for ClickGraph v0.5.4. [View latest docs →](../../wiki/Home.md)
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

### ✅ Recently Implemented (v0.5.3-v0.5.4)

- ✅ Path variables: `p = (a)-[:TYPE*]->(b)` with `nodes(p)`, `relationships(p)`, `length(p)`
- ✅ `UNION`, `UNION ALL` between queries
- ✅ `EXISTS { MATCH ... }` pattern predicates
- ✅ Cross-table query correlation with WITH...MATCH
- ✅ Smart type inference for anonymous patterns
- ✅ FK-Edge patterns for hierarchical data

### ❌ Not Yet Implemented

**Advanced Patterns:**
- ❌ Relationship property constraints in patterns: `()-[r {since: 2020}]->()`

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
Use `auto_discover_columns: true` to automatically map columns with matching names (case-insensitive):

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
- Default: 10 hops (reasonable for most graphs)
- Maximum: 1000 hops (configurable)
- Configure via `--max-var-len-hops` or `MAX_VAR_LEN_HOPS` env var

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
