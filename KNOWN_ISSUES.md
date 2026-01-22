# Known Issues

**Active Issues**: 1 bug, 5 feature limitations  
**Last Updated**: January 21, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Current Bugs

### 1. MULTI_TABLE_LABEL Standalone Node Aggregations Missing FROM Clause
**Status**: 🐛 BUG  
**Error**: `Unknown expression or function identifier 'n.ip'` (missing FROM clause in generated SQL)  
**Example**:
```cypher
USE zeek_merged
MATCH (n:IP) RETURN count(DISTINCT n.ip) as unique_count
```
**Generated SQL**:
```sql
SELECT countDistinct(n.ip) AS "unique_count"
-- Missing FROM clause entirely!
```
**Root Cause**: For MULTI_TABLE_LABEL schemas (where same label appears in multiple tables, like IP in both dns_log and conn_log), standalone node queries with aggregations should generate a UNION over all tables. The UNION structure is created in the logical plan but gets lost during SQL rendering.  
**Impact**: Blocks standalone aggregation queries on MULTI_TABLE_LABEL schemas  
**Workaround**: Use relationship patterns to anchor the node: `MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain) RETURN count(DISTINCT src.ip)`  
**Files**: `query_planner/logical_plan/match_clause.rs` (UNION creation), `render_plan/plan_builder.rs` (SQL rendering)

---

## Known Limitations

**Documentation**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md)

---

## Feature Limitations

The following Cypher features are **not implemented** (by design - read-only query engine):

### 1. Variable Alias Renaming in WITH Clause
**Status**: ⚠️ LIMITATION  
**Example**: `MATCH (u:User) WITH u AS person RETURN person.name`  
**Error**: `Property 'name' not found on node 'person'`  
**Root Cause**: When a variable is renamed via `WITH u AS person`, the type information (Node/Relationship/Scalar) is not propagated to the new alias. The new alias `person` doesn't have the label information needed to resolve property mappings.  
**Impact**: Blocks queries that use alias renaming patterns  
**Workaround**: Keep the same alias name: `WITH u RETURN u.name`  
**Files**: `query_planner/analyzer/filter_tagging.rs`, `typed_variable.rs`  
**Added**: January 20, 2026

### 2. Procedure Calls (APOC/GDS)
**Status**: ⚠️ NOT IMPLEMENTED (out of scope)  
**Example**: `CALL apoc.algo.pageRank(...)`  
**Reason**: ClickGraph is a SQL query translator, not a procedure runtime  
**Impact**: Blocks 4 LDBC BI queries (bi-10, bi-15, bi-19, bi-20)

### 3. Bidirectional Relationship Patterns  
**Status**: ⚠️ NOT IMPLEMENTED (non-standard syntax)  
**Example**: `(a)<-[:TYPE]->(b)` (both arrows on same relationship)  
**Workaround**: Use undirected pattern `(a)-[:TYPE]-(b)` or two MATCH clauses  
**Impact**: Blocks 1 LDBC BI query (bi-17)

### 4. Write Operations
**Status**: ❌ OUT OF SCOPE (read-only by design)  
**Not Supported**: `CREATE`, `SET`, `DELETE`, `MERGE`, `REMOVE`  
**Reason**: ClickGraph is a read-only analytical query engine for ClickHouse  
**Alternative**: Use native ClickHouse INSERT statements for data loading

### 5. Shortest Path on Dense Graphs
**Status**: ⚠️ PERFORMANCE LIMITATION  
**Example**:
```cypher
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.user_id = 1 RETURN p
```
**Error**: `MEMORY_LIMIT_EXCEEDED` or query timeout  
**Root Cause**: Recursive CTE-based shortest path algorithms explore all possible paths. On densely connected graphs (e.g., 4 users with 750 FOLLOWS edges = nearly complete graph), this causes exponential path explosion and memory exhaustion.  
**Impact**: shortestPath/allShortestPaths may timeout or OOM on dense graph topologies  
**Workaround**: 
- Add bounded path length: `shortestPath((a)-[:FOLLOWS*1..3]->(b))`
- Use sparser test data with realistic graph density
- For analytics, consider alternative approaches (pagerank, common neighbors)
**Technical Details**: ClickHouse recursive CTEs do not support BFS early termination optimization  
**Added**: January 20, 2026

---

## Test Suite Status

**Integration Tests**: ✅ **High pass rate** (549+ passed core tests)
- Core queries: **549 passed** ✅
- Security graph: **94 passed, 4 xfailed** ✅  
- Variable-length paths: **24 passed, 1 skipped, 2 xfailed** ✅
- VLP cross-functional: **6/6 passing** ✅ (Dec 25, 2025)
- Pattern comprehensions: **5 passed** ✅
- Property expressions: **28 passed, 3 xfailed** ✅
- Node uniqueness: **4 passed** ✅
- Multiple UNWIND: **7 passed** ✅
- **GraphRAG + Parameterized Views**: **6/6 passing (100%)** ✅ (Jan 9, 2026)
- **GraphRAG + Vector Similarity**: **9/9 passing (100%)** ✅ (Jan 9, 2026)

**LDBC Benchmark**: **29/41 queries passing (70%)**
- All SHORT queries pass ✅
- Remaining 12 blocked by: procedures (4), bidirectional patterns (1), other edge cases (7)

---

## Documentation

For comprehensive feature documentation and examples:
- **User Guide**: [docs/wiki/](docs/wiki/)
- **Getting Started**: [docs/getting-started.md](docs/getting-started.md)
- **Cypher Support**: [docs/features.md](docs/features.md)
- **Schema Configuration**: [docs/schema-reference.md](docs/schema-reference.md)

For developers:
- **Architecture**: [docs/architecture/](docs/architecture/)
- **Development Guide**: [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md)
- **Test Infrastructure**: [tests/README.md](tests/README.md)
- **VLP Cross-Functional Testing**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md) ⭐ NEW
