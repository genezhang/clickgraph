# ClickGraph Status

*Updated: October 23, 2025*

---

## ✅ What Works Now

### Query Features
- **Simple node queries**: `MATCH (u:User) RETURN u.name` ✅
- **Property filtering**: `WHERE u.age > 25` ✅
- **Basic relationships**: `MATCH (u)-[r:FRIENDS_WITH]->(f) RETURN u, f` ✅
- **Multi-hop traversals**: `(u)-[r1]->(a)-[r2]->(b)` ✅
- **Variable-length paths**: `(u)-[*1..3]->(f)` with recursive CTEs ✅
- **Path variables**: `MATCH p = (a)-[:TYPE*]-(b) RETURN p, length(p)` ✅
- **Path functions**: `length(p)`, `nodes(p)`, `relationships(p)` on path objects ✅
- **Shortest path queries**: `shortestPath((a)-[:TYPE*]-(b))` and `allShortestPaths()` ✅ **[VERIFIED: Oct 20, 2025]**
- **WHERE clause filters**: Work with all variable-length paths and shortestPath queries ✅ **[COMPLETED: Oct 22, 2025]**
  - End node filters: `WHERE b.name = "David Lee"` ✅
  - Parser support for double-quoted strings ✅
  - Proper SQL generation with correct quoting ✅
- **Alternate relationships**: `[:TYPE1|TYPE2]` multiple relationship types in patterns ✅ **[COMPLETED: Oct 21, 2025]**
  - UNION SQL generation: ✅ Working
  - Unit tests: ✅ Passing  
  - End-to-end: ✅ **VERIFIED: Oct 22, 2025** - returns all expected relationships (10 total: 8 FOLLOWS + 2 FRIENDS_WITH)
- **PageRank algorithm**: `CALL pagerank(iterations: 10, damping: 0.85)` graph centrality measures ✅ **[COMPLETED: Oct 23, 2025]**
  - Iterative SQL implementation with UNION ALL approach
  - Configurable iterations and damping factor
  - End-to-end tested with multiple parameter combinations
- **ViewScan**: Cypher labels → ClickHouse table names via YAML, supports both nodes and relationships ✅
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
- **Schema validation**: Optional startup validation of YAML configs against ClickHouse schema ✅ **[COMPLETED: Oct 23, 2025]**
  - CLI flag: `--validate-schema` (opt-in for performance)
  - Environment variable: `BRAHMAND_VALIDATE_SCHEMA`
  - Validates table/column existence and data types
  - Better error messages for misconfigurations

---

## 🚧 In Progress

*(Schema validation enhancement completed - ready for next feature)*

---

## 🎯 Next Priorities

1. **Query Performance Metrics** - Execution time tracking and plan visualization
2. **Performance optimization** - Benchmarking and query caching
3. **Additional graph algorithms** - Community detection, centrality measures

---

## 📊 Current Stats

- **Tests**: 298/298 passing (100%)
  - Python integration tests: 8/8 passing (100%)
  - Rust unit tests: 290/290 passing (100%)
- **Last updated**: Oct 23, 2025
- **Latest feature**: PageRank algorithm implementation with CALL statement support
- **Branch**: main

---

## ❌ Known Issues & Limitations

### Test Failures
- **test_version_string_formatting** fails (Bolt protocol cosmetic issue)

### Feature Limitations
- **ViewScan for relationships**: Only works for node queries, not relationship patterns
- **OPTIONAL MATCH with relationships**: Not yet tested with ViewScan
- **Alternate relationship types**: `[:TYPE1|TYPE2]` patterns fully supported ✅ **[VERIFIED: Oct 22, 2025]**

### Windows Development
- **ClickHouse tables**: Must use `ENGINE = Memory` (persistent engines fail with volume permission issues)
- **curl not available**: Use `Invoke-RestMethod` or Python `requests` for HTTP testing

---

## 📖 Feature Notes

Detailed implementation notes for major features:

- **[notes/pagerank.md](notes/pagerank.md)** - PageRank algorithm implementation with iterative SQL approach
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

### Oct 22, 2025 - WHERE Clause Handling Complete ✅
- **End node filters fully working**: `WHERE b.name = "David Lee"` in variable-length paths
- **Parser fix for double-quoted strings**: Added proper support for double-quoted string literals
- **SQL generation corrected**: Removed JSON-encoded string workaround, proper single-quote usage
- **Context storage implemented**: End filters stored in CteGenerationContext and retrieved correctly
- **Debug logging added**: Comprehensive logging for filter processing and path detection
- **Test results**: 298/298 tests passing (100%), all WHERE clause scenarios validated
- **Validation**: End-to-end testing confirms proper filter rewriting and SQL execution

### Oct 18, 2025 - Phase 2.7 Integration Testing Complete ✅
- **Path variables working end-to-end**: `MATCH p = (a)-[:TYPE*]-(b) RETURN p`
- **Path functions validated**: `length(p)`, `nodes(p)`, `relationships(p)` return correct values
- **5 critical bugs fixed**:
  1. PlanCtx registration - path variables now tracked in analyzer context
  2. Projection expansion - path variables preserved as TableAlias (not `p.*`)
  3. map() type mismatch - all values wrapped in toString() for uniform String type
  4. Property aliasing - CTE columns use property names (not SELECT aliases)
  5. YAML configuration - property mappings corrected to match database schema
- **Test results**: 10/10 integration tests passing with real data from ClickHouse
- **Validation**: Path queries successfully retrieve actual user relationships

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
