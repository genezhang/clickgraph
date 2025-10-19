# ClickGraph Status

*Updated: October 18, 2025*

---

## ✅ What Works Now

### Query Features
- **Simple node queries**: `MATCH (u:User) RETURN u.name` ✅
- **Property filtering**: `WHERE u.age > 25` ✅
- **Basic relationships**: `MATCH (u)-[r:FRIENDS_WITH]->(f) RETURN u, f` ✅
- **Multi-hop traversals**: `(u)-[r1]->(a)-[r2]->(b)` ✅
- **Variable-length paths**: `(u)-[*1..3]->(f)` with recursive CTEs ✅
- **Shortest path queries**: `shortestPath((a)-[:TYPE*]-(b))` and `allShortestPaths()` ✅ *(see limitations)*
- **OPTIONAL MATCH**: `OPTIONAL MATCH (u)-[]->(f)` with LEFT JOIN ✅
- **ViewScan**: Cypher labels → ClickHouse table names via YAML ✅
- **Aggregations**: `COUNT`, `SUM`, `AVG`, `GROUP BY` ✅
- **Ordering & Limits**: `ORDER BY`, `SKIP`, `LIMIT` ✅

### Infrastructure
- **HTTP API**: RESTful endpoints with Axum (all platforms)
- **Bolt Protocol**: Neo4j wire protocol v4.4
- **YAML Configuration**: View-based schema mapping
- **Docker Deployment**: Ready for containerized environments
- **Windows Support**: Native Windows development working

### Configuration
- **Configurable CTE depth**: Via CLI `--max-cte-depth` or env `BRAHMAND_MAX_CTE_DEPTH`
- **Flexible binding**: HTTP and Bolt ports configurable
- **Environment variables**: Full env var support for all settings

---

## 🚧 In Progress

- **Shortest path WHERE clause support**: Core implementation complete (nested CTEs, hop tracking, cycle detection), but WHERE clause filtering not yet applied to recursive CTEs
  - Parser: ✅ `shortestPath((a)-[:TYPE*]-(b))`
  - SQL Generation: ✅ Nested CTE with `ORDER BY hop_count LIMIT 1`
  - Integration: ✅ Queries execute successfully
  - WHERE filtering: ⏳ Not yet applied (returns shortest path in entire graph)
  - See: `notes/shortest-path.md` for details

---

## 🎯 Next Priorities

1. **WHERE clause for shortest path** - Apply filters in base case of recursive CTEs
2. **ViewScan relationships** - Extend ViewScan to relationship traversal patterns
3. **Path variable assignment** - `p = shortestPath(...)` capture and return
4. **Alternate relationships** - `[:TYPE1|TYPE2]` multiple types in patterns
5. **Performance optimization** - Benchmarking and query caching

---

## 📊 Current Stats

- **Tests**: 261/262 passing (99.6%)
- **Last updated**: Oct 18, 2025
- **Latest feature**: ViewScan for node queries
- **Branch**: graphview1

---

## ❌ Known Issues & Limitations

### Test Failures
- **test_version_string_formatting** fails (Bolt protocol cosmetic issue)

### Feature Limitations
- **Shortest path WHERE clause**: Filters not applied to recursive CTEs, queries return shortest path in entire graph
- **Path variable assignment**: `p = shortestPath(...)` not yet supported
- **ViewScan for relationships**: Only works for node queries, not relationship patterns
- **OPTIONAL MATCH with relationships**: Not yet tested with ViewScan

### Windows Development
- **ClickHouse tables**: Must use `ENGINE = Memory` (persistent engines fail with volume permission issues)
- **curl not available**: Use `Invoke-RestMethod` or Python `requests` for HTTP testing

---

## 📖 Feature Notes

Detailed implementation notes for major features:

- **[notes/shortest-path.md](notes/shortest-path.md)** - Shortest path implementation and debugging story
- **[notes/viewscan.md](notes/viewscan.md)** - View-based SQL translation
- **[notes/optional-match.md](notes/optional-match.md)** - LEFT JOIN semantics
- **[notes/variable-length-paths.md](notes/variable-length-paths.md)** - Recursive CTEs

---

## 🏗️ Architecture

**Data Flow**:
```
Cypher Query → Parser → Query Planner → SQL Generator → ClickHouse → JSON Response
                  ↓           ↓              ↓
               AST    Logical Plan    ClickHouse SQL
```

**Key Components**:
- `open_cypher_parser/` - Parses Cypher to AST
- `query_planner/` - Creates logical query plans
- `clickhouse_query_generator/` - Generates ClickHouse SQL
- `graph_catalog/` - Manages YAML schema configuration
- `server/` - HTTP and Bolt protocol handlers

---

## 🎯 Project Scope

**ClickGraph is a stateless, read-only graph query engine** for ClickHouse.

**What we do**: Translate Cypher graph queries → ClickHouse SQL  
**What we don't do**: Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)

---

## 🚧 Missing Read Features

### High Priority
- ⚠️ **Shortest path WHERE clause**: Core implementation complete, filtering support needed
- ❌ Path variables: `p = (a)-[r]->(b)` capture and return
- ❌ Alternate relationships: `[:TYPE1|TYPE2]` multiple types
- ❌ Pattern comprehensions: `[(a)-[]->(b) | b.name]`

### Medium Priority
- ❌ CASE expressions
- ❌ UNWIND for list expansion
- ❌ Subqueries: `CALL { ... }`
- ❌ EXISTS patterns

### Future
- ❌ Graph algorithms: PageRank, centrality, community detection
- ❌ Performance: Advanced JOIN optimization, query caching
- ❌ Large-scale: Partitioning support for huge graphs

---

## 📝 Recent Changes

### Oct 18, 2025 - ViewScan Implementation
- Added view-based SQL translation for node queries
- Labels now correctly map to table names via YAML schema
- Table aliases propagate from Cypher variable names
- HTTP bind error handling improved
- Logging framework integrated (env_logger)

### Oct 17, 2025 - OPTIONAL MATCH
- Full LEFT JOIN semantics for optional patterns
- Two-word keyword parsing working
- 11/11 OPTIONAL MATCH tests passing

### Oct 17, 2025 - Windows Crash Fix
- Fixed server crash issue on Windows
- Verified with 20+ consecutive requests
- Native Windows development fully supported

### Oct 17, 2025 - Configurable CTE Depth
- CLI and environment variable configuration
- Default 100, configurable 10-1000
- 30 new tests added for depth validation

### Oct 15, 2025 - Variable-Length Paths
- Complete implementation with recursive CTEs
- Property selection in paths (two-pass architecture)
- Schema integration with YAML column mapping
- Cycle detection with array-based path tracking

---

## 🎉 Major Achievements

- ✅ **250+ tests passing** - Comprehensive test coverage
- ✅ **All 4 YAML relationship types working** - AUTHORED, FOLLOWS, LIKED, PURCHASED
- ✅ **Multi-hop graph traversals** - Complex JOIN generation
- ✅ **Dual protocol support** - HTTP + Bolt simultaneously
- ✅ **Cross-platform** - Linux, macOS, Windows support

---

**For detailed technical information, see feature notes in `notes/` directory.**
