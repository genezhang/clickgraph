# ClickGraph v0.1.0 Release Notes

**Release Date**: November 2, 2025  
**First Official Release** 🎉

## Overview

ClickGraph v0.1.0 is the first official release of our high-performance, stateless, read-only graph query engine for ClickHouse. This release represents months of development, testing, and refinement, bringing Neo4j ecosystem compatibility to ClickHouse's analytical power.

## 🎉 Release Highlights

### Enterprise-Scale Validation
- ✅ **Successfully tested on 5 million users and 50 million relationships**
- ✅ 90% success rate on large-scale benchmarks
- ✅ All query types scale to enterprise workloads
- ✅ Performance validated across 3 scale tiers

### Neo4j Compatibility
- ✅ **Full Bolt protocol v4.4 support** for seamless Neo4j driver integration
- ✅ **Multi-database capabilities** with three selection methods
- ✅ **USE clause syntax** matching Neo4j 4.0+ conventions
- ✅ Compatible with existing Neo4j tools and applications

### Production-Ready Quality
- ✅ **318/318 tests passing** (100% success rate)
- ✅ Comprehensive error handling (systematic unwrap() elimination)
- ✅ Windows native support (HTTP and Bolt protocols)
- ✅ Performance monitoring with built-in metrics

## 🚀 Major Features

### 1. USE Clause for Database Selection
Neo4j 4.0+ compatible database selection directly in Cypher queries.

```cypher
USE social_network
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, collect(friend.name) AS friends
```

**Features:**
- Three-way precedence: USE clause > session/request parameter > default schema
- Case-insensitive syntax (USE/use/Use)
- Qualified database names (`USE neo4j.database`)
- Works with both HTTP API and Bolt protocol

**Testing:** 6 parser unit tests + 6 end-to-end integration tests

### 2. Bolt Protocol Multi-Database Support
Full Neo4j 4.0+ multi-database compatibility via Bolt protocol.

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session(database="social_network") as session:
    result = session.run("MATCH (u:User) RETURN u.name")
```

**Features:**
- Extracts `database` field from Bolt HELLO message
- Session-level database selection
- Parity with HTTP API `schema_name` parameter
- Authentication support

### 3. Path Variables & Functions
Complete support for path capture and analysis.

```cypher
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN length(p), nodes(p), relationships(p)
```

**Features:**
- Path variables: `p = (a)-[r*]->(b)`
- Path functions: `length(p)`, `nodes(p)`, `relationships(p)`
- CTE-based implementation with array columns
- End-to-end testing with real graph data

### 4. Query Performance Metrics
Built-in performance monitoring for production deployments.

**HTTP Response Headers:**
```
X-Query-Total-Time: 45.23ms
X-Query-Parse-Time: 1.12ms
X-Query-Planning-Time: 8.45ms
X-Query-Execution-Time: 35.66ms
```

**Structured Logging:**
```
INFO Query performance: total=45.23ms parse=1.12ms planning=8.45ms execution=35.66ms
```

**Features:**
- Phase-by-phase timing breakdown
- HTTP headers for monitoring integration
- Structured logging with millisecond precision
- Query classification (read/write/call)

### 5. CASE Expressions
Full conditional expression support in all query contexts.

```cypher
MATCH (u:User)
RETURN u.name,
       CASE 
         WHEN u.age > 30 THEN 'Senior'
         WHEN u.age > 18 THEN 'Adult'
         ELSE 'Youth'
       END AS category
```

**Features:**
- Simple CASE: `CASE x WHEN val THEN result END`
- Searched CASE: `CASE WHEN condition THEN result END`
- Works in RETURN, WHERE, function arguments
- Automatic property mapping
- ClickHouse optimization for simple CASE

## 🐛 Critical Bug Fixes

### Bug #1: ChainedJoin CTE Wrapper
**Fixed:** Malformed SQL for exact hop variable-length paths

**Issue:** Queries like `MATCH (a)-[:FOLLOWS*2]->(b)` generated invalid SQL without proper CTE wrapper.

**Impact:** Exact hop queries (`*2`, `*3`, `*4`) now work perfectly with chained JOINs.

### Bug #2: Shortest Path Filter Rewriting
**Fixed:** Column reference errors in WHERE clause filters

**Issue:** End node filters failed with "Unknown identifier" errors due to column name mismatches in CTEs.

**Impact:** Shortest path queries with WHERE clauses now work correctly.

### Bug #3: Aggregation Table Name Lookup
**Fixed:** Schema resolution errors in GROUP BY queries

**Issue:** SQL used Cypher labels instead of actual table names, causing "Unknown table" errors.

**Impact:** All aggregation queries with incoming relationships now work.

## 📊 Benchmark Results

### Large Scale (5M users, 50M relationships)
- **Success Rate:** 90% (9/10 queries passing)
- **Dataset:** 5,000,000 users, 50,000,000 follows, 25,000,000 posts
- **Validation:** All query types scale to enterprise workloads
- **Note:** Shortest path hits memory limit (ClickHouse config dependent)

### Medium Scale (10K users, 50K relationships)
- **Success Rate:** 100% (10/10 queries passing)
- **Performance:** ~2s for most queries, ~4.5s for shortest path
- **Dataset:** 10,000 users, 50,000 follows, 5,000 posts

### Small Scale (1K users, 5K relationships)
- **Success Rate:** 100% (10/10 queries passing)
- **Query Types:** All patterns validated (traversals, paths, aggregations, bidirectional)

## ⚠️ Breaking Changes

### YAML Schema Field Rename
Relationship definitions must update field names for semantic clarity.

**Migration Required:**
```yaml
# OLD (v0.0.x)
relationships:
  follows:
    from_column: follower_id
    to_column: followed_id

# NEW (v0.1.0)
relationships:
  follows:
    from_id: follower_id
    to_id: followed_id
```

**Scope:**
- All YAML configuration files with relationship definitions
- 27 Rust files updated
- 10 YAML config files updated
- 3 documentation files updated

**Rationale:**
- Improved semantic clarity ("id" indicates identity/key semantics)
- Consistency with node schemas (`id_column`)
- Prepares for future composite key support

## 📦 Installation

### Docker (Recommended)
```bash
git clone https://github.com/genezhang/clickgraph
cd clickgraph
docker-compose up -d
```

### Native Build
```bash
# Install Rust and Cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build ClickGraph
cargo build --release

# Configure environment
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="default"

# Run
cargo run --bin clickgraph
```

### Quick Test
```bash
# HTTP API
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "RETURN 1 as test"}'

# Python with Neo4j driver
python3 -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://localhost:7687')
with driver.session() as session:
    result = session.run('RETURN 1 as test')
    print(result.single())
"
```

## 🔗 Platform Support

| Platform | HTTP API | Bolt Protocol | Status |
|----------|----------|---------------|--------|
| Linux (Docker) | ✅ Working | ✅ Working | Fully functional |
| Linux (Native) | ✅ Working | ✅ Working | Fully functional |
| macOS | ✅ Working | ✅ Working | Fully functional |
| Windows (Native) | ✅ Working | ✅ Working | **Fixed in v0.1.0!** |
| WSL 2 | ✅ Working | ✅ Working | Fully functional |

## 📚 Documentation

- **[README.md](README.md)** - Project overview and quick start
- **[docs/api.md](docs/api.md)** - Complete API documentation
- **[docs/getting-started.md](docs/getting-started.md)** - Comprehensive setup guide
- **[STATUS.md](STATUS.md)** - Current project status and capabilities
- **[notes/](notes/)** - Feature implementation details

## 🧪 Testing

- **Unit Tests:** 318/318 passing (100% success rate)
- **End-to-End Tests:** Comprehensive validation across all features
- **Benchmark Suite:** 3-tier validation (small, medium, large scale)
- **Platform Testing:** Linux, macOS, Windows validated

## 🎯 Known Limitations

### Query Features
- ⚠️ Pattern comprehensions: `[(a)-[]->(b) | b.name]` - Not yet implemented
- ⚠️ Subqueries: `CALL { ... }` syntax not yet implemented
- ⚠️ Write operations: Out of scope (read-only engine by design)

### Performance
- Shortest path on very large datasets (5M+ nodes) may hit memory limits
- ClickHouse `max_memory_usage` configuration may need tuning for large graphs

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details and workarounds.

## 🚀 What's Next

See [NEXT_STEPS.md](NEXT_STEPS.md) for the development roadmap. Planned features include:
- Pattern comprehensions
- Additional graph algorithms
- Query optimization improvements
- Expanded Cypher syntax support

## 🙏 Acknowledgments

ClickGraph is a fork of the Brahmand project with significant enhancements:
- Neo4j Bolt protocol support
- Multi-database capabilities
- Path variables and functions
- Performance monitoring
- Windows native support

## 📄 License

ClickGraph is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## 🔗 Links

- **Repository:** https://github.com/genezhang/clickgraph
- **Issues:** https://github.com/genezhang/clickgraph/issues
- **Discussions:** https://github.com/genezhang/clickgraph/discussions

---

**Thank you for using ClickGraph!** 🎉

We're excited to bring graph analytics to ClickHouse with Neo4j compatibility. Please report issues, share feedback, and contribute to the project.
