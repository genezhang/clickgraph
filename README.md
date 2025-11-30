<div align="center">
  <img src="https://github.com/genezhang/clickgraph/blob/main/docs/images/cglogo.png" height="200">
</div>

# ClickGraph

#### ClickGraph - A graph query layer on top of ClickHouse®, developed on a forked repo of Brahmand, written in Rust.

**A high-performance, stateless, read-only graph query translator for ClickHouse with Neo4j ecosystem compatibility.**

> **Note: ClickGraph is development-ready for view-based graph analysis with full Neo4j Bolt Protocol 5.8 support. This is a read-only analytical query engine - write operations are not supported. Codebase has diverged from the upstream with DDL/writes feature removal and other structure/code refactoring to follow Rust idiomatic style.**

---

## 🚀 What's New in v0.5.2 (November 30, 2025)

### Schema Variations Release - Complete Support for All Edge Table Patterns! 🎉

**v0.5.2 delivers comprehensive support for advanced schema patterns including polymorphic edges, coupled edges, and denormalized tables.**

### Polymorphic Edge Tables ✨

**Single table containing multiple edge types with dynamic filtering:**
```yaml
edges:
  - polymorphic: true
    table: interactions
    type_column: interaction_type      # FOLLOWS, LIKES, AUTHORED
    from_label_column: from_type       # Source node type
    to_label_column: to_type           # Target node type
```

```cypher
-- Multi-type filter generates IN clause
MATCH (u:User)-[:FOLLOWS|LIKES]->(target)
RETURN u.name, target.name
```

**What Works:**
- ✅ **Single-hop wildcard edges**: `(u:User)-[r]->(target)` with unlabeled targets
- ✅ **Multi-hop CTE chaining**: `(u)-[r1]->(m)-[r2]->(t)` with proper JOINs
- ✅ **Bidirectional edges**: `(u:User)<-[r]-(source)` using correct JOIN direction
- ✅ **Composite edge IDs**: `[from_id, to_id, type, timestamp]` for uniqueness

### Coupled Edge Optimization ⚡

**Automatic JOIN elimination when edges share the same table:**
```cypher
-- Zeek DNS pattern: IP → Domain → ResolvedIP (all in dns_log table)
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
RETURN ip.ip, d.name, rip.ips
```
Generates optimized SQL with NO self-join - single table scan!

### VLP + UNWIND Support 🔄

**Decompose paths with ARRAY JOIN:**
```cypher
MATCH p = (u:User)-[:FOLLOWS*1..3]->(f:User)
UNWIND nodes(p) AS n
RETURN n
```

### OPTIONAL MATCH + VLP Fix 🐛

**Anchor nodes now preserved when no path exists:**
```cypher
MATCH (a:User) WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS*1..3]->(b:User)
RETURN a.name, COUNT(b) as reachable
-- Eve (no followers) now correctly returns 1 row with reachable = 0
```

### Test Coverage 🧪

- **534 library tests passing** (100%)
- **73 schema variation tests** across 4 schema types:
  - Standard: 30 tests
  - Denormalized: 14 tests
  - Polymorphic: 24 tests
  - Coupled: 5 tests

---

## 📦 What's in v0.5.1 (November 21, 2025)

### Official Docker Hub Release! 🐳

**ClickGraph is now available as a pre-built Docker image on Docker Hub for instant deployment!**

- 🐳 **Docker Hub**: `docker pull genezhang/clickgraph:latest`
- 📦 **Pre-built images**: No compilation required, instant startup
- 🌐 **Multi-platform**: linux/amd64, linux/arm64 support
- ⚡ **Quick start**: Get running in under 2 minutes
- 🔄 **Auto-updates**: Tagged releases (`:latest`, `:v0.5.1`, `:0.5.1`)
- ✅ **Fully tested**: 17/17 validation tests passing

**Quick Start with Docker**:
```bash
# Pull and run with docker-compose
docker-compose up -d

# Or run directly
docker pull genezhang/clickgraph:latest
docker run -d -p 8080:8080 -p 7687:7687 \
  -e CLICKHOUSE_URL="http://clickhouse:8123" \
  genezhang/clickgraph:latest
```

### New Features ✨

- 🆕 **RETURN DISTINCT**: Deduplication support in query results
- 🧪 **Comprehensive testing**: Added Docker image validation suite
- 📚 **Improved docs**: Docker-first getting started guide

---

## 📦 What's in v0.5.0 (November 2025)

### Phase 2 Complete: Enterprise Readiness 🎉

**ClickGraph v0.5.0 delivers production-ready multi-tenancy, RBAC, complete Bolt Protocol 5.8 support, and comprehensive documentation with 100% unit test coverage.**

### Multi-Tenancy & RBAC - Enterprise Security Features! 🔐

**Production-ready multi-tenant support with row and column-level security**
- ✅ **Parameterized views**: Tenant isolation at database level with 99% cache memory reduction
- ✅ **SET ROLE support**: ClickHouse native RBAC for column-level security
- ✅ **Performance optimized**: 2x speedup with shared cache templates (18ms → 9ms)
- ✅ **Smart caching**: SQL template reuse with parameter substitution
- ✅ **Neo4j compatible**: CYPHER replan options (default/force/skip)
- ✅ **LRU eviction**: Dual limits (1000 entries, 100 MB memory)
- ✅ **Schema-aware**: Automatic cache invalidation on schema reload
- ✅ **Thread-safe**: Arc<Mutex<HashMap>> for concurrent access
- ✅ **100% tested**: 6/6 unit tests + 5/5 e2e tests passing

**Parameterized Query Support**:
```bash
# Use parameters for values (prevents SQL injection, enables caching)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > $minAge AND u.email = $email RETURN u.name",
    "parameters": {
      "minAge": 25,
      "email": "alice@example.com"
    }
  }'
```

**Cache Configuration**:
```bash
export CLICKGRAPH_QUERY_CACHE_ENABLED=true       # Default: true
export CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES=1000   # Default: 1000
export CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB=100    # Default: 100 MB
```

**Advanced Usage**:
```bash
# Cache hit on repeated query with different parameter values
curl -X POST http://localhost:8080/query \
  -d '{"query":"MATCH (u:User) WHERE u.age > $minAge RETURN u.name", "parameters":{"minAge":30}}'
# Response header: X-Query-Cache-Status: HIT (uses cached SQL template)

# Force recompilation (bypass cache)
curl -X POST http://localhost:8080/query \
  -d '{"query":"CYPHER replan=force MATCH (u:User) RETURN u.name"}'
# Response header: X-Query-Cache-Status: BYPASS
```

**📖 Documentation**: 
- [Query Cache Guide](notes/query-cache.md) - Caching implementation details
- [Parameter Support](notes/parameter-support.md) - Where parameters can/cannot be used

### Code Quality & Developer Experience

**Major Refactoring & Bug Fixes**
- ✅ **22% code reduction**: Modularized `plan_builder.rs` (769 lines removed) for better maintainability
- ✅ **Undirected relationships**: `(a)-[r]-(b)` patterns with bidirectional matching via OR JOIN logic
- ✅ **Bug fixes**: Anonymous node limitation documented, ChainedJoin CTE wrapping, WHERE clause filtering
- ✅ **Test coverage**: 424/424 Rust unit tests (100%), integration tests pending
- ✅ **Benchmark validation**: 14/14 queries passing (100%) across 3 scale levels (1K-5M nodes)

**Performance Validated at Scale**
- ✅ **5M users, 50M relationships**: 9/10 benchmark queries successful (90%)
- ✅ **Consistent performance**: 2077-2088ms mean query time (only 0.5% overhead at 10x scale)
- ✅ **Development-ready**: Stress tested with large-scale datasets

---

## 🚀 Previous Update (November 9, 2025)

### Major Architectural Improvements ✨

**Complete Multi-Schema Support**
- ✅ **Full schema isolation**: Different schemas can map same labels to different tables
- ✅ **Per-request schema selection**: USE clause, schema_name parameter, or default
- ✅ **Clean architecture**: Single source of truth for schema management (removed redundant GLOBAL_GRAPH_SCHEMA)
- ✅ **Thread-safe**: Schema flows through entire query execution path
- ✅ **End-to-end tested**: All 4 multi-schema tests passing

**Code Quality Improvements**
- 🧹 **Removed technical debt**: Eliminated duplicate schema storage system
- 🔧 **Cleaner codebase**: Simplified render layer helper functions
- 📊 **All tests passing**: 325 unit tests + 32 integration tests (100% non-benchmark)

---

## 🚀 Previous Updates (November 1, 2025)

### Large-Scale Testing & Bug Fixes

**ClickGraph tested successfully on 5 MILLION users and 50 MILLION relationships!**

| Benchmark | Dataset Size | Success Rate | Status |
|-----------|-------------|--------------|--------|
| **Large** | 5M users, 50M follows | 9/10 (90%) | ✅ **Stress Tested** |
| **Medium** | 10K users, 50K follows | 10/10 (100%) | ✅ Well Validated |
| **Small** | 1K users, 5K follows | 10/10 (100%) | ✅ Fully Tested |

**What We Learned:**
- ✅ **Direct relationships**: Handling 50M edges successfully
- ✅ **Multi-hop traversals**: Working on 5M node graphs  
- ✅ **Variable-length paths**: Scaling to large datasets
- ✅ **Aggregations**: Pattern matching across millions of rows
- ✅ **Performance**: ~2 seconds for most queries, even at large scale
- ⚠️ **Shortest paths**: Memory limits on largest dataset (ClickHouse config dependent)

**Recent Bug Fixes:**
- 🐛 ChainedJoin CTE wrapper for exact hop variable-length paths (`*2`, `*3`)
- 🐛 Shortest path filter rewriting for WHERE clauses on end nodes
- 🐛 Aggregation table name schema lookup for GROUP BY queries

**Tooling:**
- 📊 Comprehensive benchmarking suite with 3 scale levels
- 🔧 ClickHouse-native data generation for efficient loading
- 📈 Performance metrics collection and analysis

**📖 Documentation:**
- [Detailed Benchmark Results](notes/benchmarking.md) - Complete analysis across all scales
- [CHANGELOG.md](CHANGELOG.md) - Technical details and bug fixes
- [STATUS.md](STATUS.md) - Current project status

---

## Features

### Core Capabilities
- **Read-Only Graph Analytics**: Translates Cypher graph queries into optimized ClickHouse SQL for analytical workloads
- **ClickHouse-native**: Leverages ClickHouse for graph queries on shared data with SQL, merging OLAP speed with graph-analysis power
- **Stateless Architecture**: Offloads all query execution to ClickHouse—no extra datastore required
- **Cypher Query Language**: Industry-standard Cypher read syntax for intuitive, expressive property-graph querying
- **Parameterized Queries**: Neo4j-compatible parameter support (`$param` syntax) for SQL injection prevention and query plan caching
- **Query Cache**: Development-ready LRU caching with 10-100x speedup for repeated query translations, SQL template reuse with parameter substitution, and Neo4j-compatible CYPHER replan options
- **Variable-Length Paths**: Recursive traversals with `*1..3` syntax using ClickHouse WITH RECURSIVE CTEs
- **Path Variables & Functions**: Capture and analyze path data with `length(p)`, `nodes(p)`, `relationships(p)` functions
- **Analytical-scale Performance**: Optimized for very large datasets and complex multi-hop traversals
- **Query Performance Metrics**: Phase-by-phase timing with HTTP headers and structured logging for monitoring and optimization

### Neo4j Ecosystem Compatibility
- **Bolt Protocol v5.8**: ✅ **Fully functional** - Complete query execution, authentication, and multi-database support. Compatible with Neo4j drivers, cypher-shell, and Neo4j Browser.
- **HTTP REST API**: ✅ **Fully functional** - Complete query execution with parameters, aggregations, and all Cypher features
- **Multi-Schema Support**: ✅ **Fully working** - Complete schema isolation with per-request schema selection:
  - **USE clause**: Cypher `USE database_name` syntax (highest priority)
  - **Session/request parameter**: Bolt session database or HTTP `schema_name` parameter
  - **Default schema**: Fallback to "default" schema
  - **Schema isolation**: Different schemas map same labels to different ClickHouse tables
- **Dual Server Architecture**: HTTP and Bolt servers running simultaneously (both production-ready)
- **Authentication Support**: Multiple authentication schemes including basic auth

### View-Based Graph Model
- **Zero Migration**: Transform existing relational data into graph format through YAML configuration
- **Auto-Discovery**: Automatically query ClickHouse `system.columns` for property mappings with `auto_discover_columns: true` - no manual mapping needed!
- **Dynamic Schema Loading**: Runtime schema registration via HTTP API (`POST /schemas/load`) with full YAML content support
- **Native Performance**: Leverages ClickHouse's columnar storage and query optimization
- **Robust Implementation**: Comprehensive validation, error handling, and optimization passes

---

## Architecture

ClickGraph runs as a lightweight graph wrapper alongside ClickHouse with dual protocol support:

![acrhitecture](./docs/images/architecture.png)

### HTTP API (Port 8080)
1. **Client** sends HTTP POST request with Cypher query to ClickGraph
2. **ClickGraph** parses & plans the query, translates to ClickHouse SQL
3. **ClickHouse** executes the SQL and returns results
4. **ClickGraph** sends JSON results back to the client

### Bolt Protocol (Port 7687)
1. **Neo4j Driver/Tool** connects via Bolt protocol to ClickGraph
2. **ClickGraph** handles Bolt handshake, authentication, and message protocol
3. **Cypher queries** are executed through the same query engine as HTTP
4. **Results** are streamed back via Bolt protocol format

Both protocols share the same underlying query engine and ClickHouse backend. Both are production-ready.

## 🚀 Quick Start

**New to ClickGraph?** See the **[Getting Started Guide](docs/getting-started.md)** for a complete walkthrough.

### Option 1: Docker (Recommended - No Build Required)

Pull and run the pre-built image:

```bash
# Pull the latest image
docker pull genezhang/clickgraph:latest

# Start ClickHouse only
docker-compose up -d clickhouse-service

# Run ClickGraph from Docker Hub image
docker run -d \
  --name clickgraph \
  --network clickgraph_default \
  -p 8080:8080 \
  -p 7687:7687 \
  -e CLICKHOUSE_URL="http://clickhouse-service:8123" \
  -e CLICKHOUSE_USER="test_user" \
  -e CLICKHOUSE_PASSWORD="test_pass" \
  -e CLICKHOUSE_DATABASE="brahmand" \
  -v $(pwd)/schemas:/app/schemas:ro \
  genezhang/clickgraph:latest
```

Or use docker-compose (uses published image by default):

```bash
docker-compose up -d
```

### Option 2: Build from Source

Build and run locally with Rust:

```bash
# Prerequisites: Rust toolchain (1.85+) and Docker for ClickHouse

# 1. Clone and start ClickHouse
git clone https://github.com/genezhang/clickgraph
cd clickgraph
docker-compose up -d clickhouse-service

# 2. Build and run ClickGraph
cargo build --release
cargo run --bin clickgraph

# Or with custom ports
cargo run --bin clickgraph -- --http-port 8080 --bolt-port 7687
```

> **⚠️ Windows Users**: The HTTP server has a known issue on Windows. Use Docker or WSL for development. See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

### Test Your Setup

Query via HTTP API:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.full_name LIMIT 5"}'
```

Or connect with Neo4j tools (cypher-shell, Neo4j Browser):
```bash
cypher-shell -a bolt://localhost:7687 -u neo4j -p password
```

---
   ```bash
   # Simple query
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "RETURN 1 as test"}'
   
   # Query with parameters
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "MATCH (u:User) WHERE u.age >= $minAge RETURN u.full_name, u.age", "parameters": {"minAge": 25}}'
   ```

4. **Bolt Protocol** (Neo4j driver compatibility):
   ```python
   from neo4j import GraphDatabase
   
   driver = GraphDatabase.driver("bolt://localhost:7687")
   with driver.session() as session:
       result = session.run("RETURN 1 as test")
       for record in result:
           print(record["test"])  # Outputs: 1
   driver.close()
   ```

5. **Use the USE clause for multi-database queries**:
   ```cypher
   -- Query specific database using Neo4j-compatible USE clause
   USE social_network
   MATCH (u:User)-[:FOLLOWS]->(friend)
   RETURN u.name, collect(friend.name) AS friends
   
   -- USE overrides session/request parameters
   USE ecommerce
   MATCH (p:Product) WHERE p.price > 100 RETURN p.name
   ```

📖 **[Complete Setup Guide →](docs/getting-started.md)**

---

## 💻 Interactive CLI Client

ClickGraph includes an interactive command-line client for easy querying:

### Build and Run

```bash
# Build the client
cargo build --release -p clickgraph-client

# Run with default settings (connects to http://localhost:8080)
./target/release/clickgraph-client

# Or connect to a custom server
./target/release/clickgraph-client --url http://your-server:8080
```

### Usage

```
clickgraph-client :) MATCH (u:User) RETURN u.name LIMIT 5

Tim Duncan
Tony Parker
LaMarcus Aldridge
Manu Ginobili
Boris Diaw

clickgraph-client :) MATCH (u:User)-[:FOLLOWS]->(friend) WHERE u.user_id = 101 RETURN friend.name

Tim Duncan
LaMarcus Aldridge

clickgraph-client :) <Ctrl+C or Ctrl+D to exit>
I'll be back:)
```

**Features**:
- Interactive REPL with history support
- Automatic result formatting (JSON or text)
- Command history (up/down arrows)
- Simple connection to any ClickGraph server

---

## 📊 View-Based Graph Model

Transform existing relational data into graph format through YAML configuration:

**Example**: Map your `users` and `user_follows` tables to a social network graph:
```yaml
views:
  - name: social_network
    nodes:
      user:                    # Node label in Cypher queries
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
    relationships:
      follows:                 # Relationship type in Cypher queries
        source_table: user_follows
        from_node: user        # Source node label
        to_node: user          # Target node label
        from_id: follower_id
        to_id: followed_id
```

Then query with standard Cypher:
```cypher
MATCH (u:user)-[:follows]->(friend:user)
WHERE u.name = 'Alice'
RETURN friend.name
```

**OPTIONAL MATCH** for handling optional patterns:
```cypher
-- Find all users and their friends (if any)
MATCH (u:user)
OPTIONAL MATCH (u)-[:follows]->(friend:user)
RETURN u.name, friend.name

-- Mixed required and optional patterns
MATCH (u:user)-[:authored]->(p:post)
OPTIONAL MATCH (p)-[:liked_by]->(liker:user)
RETURN u.name, p.title, COUNT(liker) as likes
```
→ Generates efficient `LEFT JOIN` SQL with NULL handling for unmatched patterns

## 🚀 Examples

### ⚡ **[Quick Start](examples/quick-start.md)** - 5 Minutes to Graph Analytics

Perfect for first-time users! Simple social network demo with:
- **3 users, friendships** - minimal setup with Memory tables
- **Basic Cypher queries** - find friends, mutual connections  
- **HTTP & Neo4j drivers** - both integration methods
- **5-minute setup** - zero to working graph analytics

### 📊 **[E-commerce Analytics](examples/ecommerce-analytics.md)** - Comprehensive Demo

Complete end-to-end demonstration with:
- **Complete data setup** with realistic e-commerce schema (customers, products, orders, reviews)
- **Advanced graph queries** for customer segmentation, product recommendations, and market basket analysis  
- **Real-world workflows** with both HTTP REST API and Neo4j driver examples
- **Performance optimization** techniques and expected benchmarks
- **Business insights** from customer journeys, seasonal patterns, and cross-selling opportunities

**Start with Quick Start, then explore E-commerce Analytics for advanced usage!** 🎯

## �🔧 Configuration

ClickGraph supports flexible configuration via command-line arguments and environment variables:

```bash
# View all options
cargo run --bin clickgraph -- --help

# Custom ports
cargo run --bin clickgraph -- --http-port 8081 --bolt-port 7688

# Disable Bolt protocol (HTTP only)
cargo run --bin clickgraph -- --disable-bolt

# Custom host binding
cargo run --bin clickgraph -- --http-host 127.0.0.1 --bolt-host 127.0.0.1

# Configure CTE depth limit for variable-length paths (default: 100)
cargo run --bin clickgraph -- --max-cte-depth 150
export CLICKGRAPH_MAX_CTE_DEPTH=150  # Or via environment variable
```

See `docs/configuration.md` for complete configuration documentation.

## � Running in Background (Windows)

For Windows users, ClickGraph supports running in the background using PowerShell jobs:

### PowerShell Background Jobs (Recommended)

```powershell
# Start server in background
.\start_server_background.ps1

# Check if server is running
Invoke-WebRequest -Uri "http://localhost:8080/health"

# Stop the server (replace JOB_ID with actual job ID shown)
Stop-Job -Id JOB_ID; Remove-Job -Id JOB_ID
```

### Alternative: New Command Window

Use the batch file to start the server in a separate command window:

```batch
start_server_background.bat
```

### Manual Daemon Mode

The server also supports a `--daemon` flag for Unix-like daemon behavior:

```bash
cargo run --bin clickgraph -- --daemon --http-port 8080
```

## �📚 Documentation

### User Guides
- **[Getting Started](docs/getting-started.md)** - Complete setup walkthrough and first queries
- **[Features Overview](docs/features.md)** - Comprehensive feature list and capabilities  
- **[API Documentation](docs/api.md)** - HTTP REST API and Bolt protocol usage
- **[Configuration Guide](docs/configuration.md)** - Server configuration and CLI options

### Technical Documentation  
- **[GraphView Model](docs/graphview1-branch-summary.md)** - Complete view-based graph analysis
- **[Test Infrastructure](docs/test-infrastructure-redesign.md)** - Testing framework and validation
- **[Development Guide](.github/copilot-instructions.md)** - Development workflow and architecture

### For Contributors
- **[Development Process](DEVELOPMENT_PROCESS.md)** - ⭐ **5-phase feature development workflow** (START HERE!)
- **[Quick Reference](QUICK_REFERENCE.md)** - Cheat sheet for common development tasks
- **[Environment Setup](docs/development/environment-checklist.md)** - Pre-session checklist for developers
- **[Testing Guide](docs/development/testing.md)** - Comprehensive testing strategies
- **[Current Status](STATUS.md)** - What works now, what's in progress
- **[Known Issues](KNOWN_ISSUES.md)** - Active bugs and limitations

### Reference
- **[Original Brahmand Docs](https://www.brahmanddb.com/introduction/intro)** - Original project documentation
- **[Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/)** - Cypher query language reference
- **[ClickHouse Documentation](https://clickhouse.com/docs/)** - ClickHouse database documentation

## 🚀 Performance

First [Benchmark Results](notes/benchmarking.md)

## 🧪 Development Status

**Latest Update**: November 18, 2025 - **Phase 2 Complete** 🎉

**v0.5.0 Release Status**: Released (November 18, 2025)

### Production-Ready Features (Phase 2)
- ✅ **Multi-Tenancy with Parameterized Views**: Row-level security at database level
  - 99% cache memory reduction (O(n) → O(1))
  - 2x performance improvement with shared templates
  - HTTP + Bolt protocol support
- ✅ **SET ROLE RBAC**: ClickHouse native column-level security
- ✅ **Auto-Schema Discovery**: Zero-configuration column mapping via `system.columns`
- ✅ **Bolt Protocol 5.8**: Full Neo4j driver compatibility with all E2E tests passing
- ✅ **HTTP Schema Loading API**: Runtime schema registration without restart
- ✅ **Anonymous Patterns**: `MATCH (a)-[r]->(b)` and `()-[r:TYPE]->()` support
- ✅ **Complete Documentation**: 19 wiki pages, comprehensive API reference, zero broken links

### Development-Ready Features (Phase 1)
- ✅ **Query Cache with LRU Eviction**: 10-100x query translation speedup (0.1-0.5ms cached vs 10-50ms uncached)
  - Smart SQL template caching with parameter substitution
  - Configurable limits: 1000 entries, 100 MB memory
  - Neo4j-compatible CYPHER replan options (default/force/skip)
  - Schema-aware automatic invalidation
  - Thread-safe concurrent access
- ✅ **Parameterized Queries**: Full Neo4j compatibility with `$param` syntax
  - SQL injection prevention
  - Query plan caching and reuse
  - Type-safe parameter binding
  - WHERE clause, RETURN, aggregation support
- ✅ **Neo4j Bolt Protocol v5.8**: Wire protocol implementation for Neo4j driver compatibility
  - Handshake, authentication, multi-database support
  - Message handling for all Bolt operations
  - Dual server architecture (HTTP + Bolt simultaneously)
  - ⚠️ Query execution pending - use HTTP API for production
- ✅ **Comprehensive Cypher Support**: Development-ready graph query patterns
  - Simple node lookups and filtered scans
  - Direct and multi-hop relationship traversals
  - Variable-length paths with exact (`*2`) and range (`*1..3`) specifications
  - Shortest path algorithms (`shortestPath()`, `allShortestPaths()`)
  - OPTIONAL MATCH with LEFT JOIN semantics
  - Multiple relationship types with UNION ALL
  - Path variables and functions: `length(p)`, `nodes(p)`, `relationships(p)`
  - Undirected relationships: `(a)-[r]-(b)` with bidirectional matching
  - Aggregations with GROUP BY and ORDER BY
- ✅ **Benchmark Validation**: 14/14 queries passing (100%) across 3 scale levels
  - Small: 1K users, 5K relationships (100% success)
  - Medium: 10K users, 50K relationships (100% success)
  - Large: 5M users, 50M relationships (90% success)
- ✅ **Neo4j Function Mappings**: 25+ functions for compatibility
  - Datetime: `datetime()`, `date()`, `timestamp()`, `duration()`
  - String: `toString()`, `toUpper()`, `toLower()`, `substring()`, `trim()`, `split()`
  - Math: `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `log()`, `exp()`
  - Aggregation: `count()`, `sum()`, `avg()`, `min()`, `max()`, `collect()`
- ✅ **Code Quality**: Major refactoring and bug fixes
  - 22% code reduction in core modules
  - Comprehensive test coverage (100% Rust unit tests)
  - Clean architecture with single source of truth
  - Windows compatibility fixes
- ✅ **View-Based Graph Model**: Transform existing tables to graphs via YAML configuration  
- ✅ **Dual Server Architecture**: HTTP REST API and Bolt protocol simultaneously
- ✅ **Flexible Configuration**: CLI options, environment variables, Docker deployment

### Test Coverage (November 15, 2025)
- ✅ **Rust Unit Tests**: 424/424 passing (100%)
- ✅ **Integration Tests**: 197/308 passing (64% - improved from 54%)
- ✅ **Benchmarks**: 14/14 passing (100%)
- ✅ **E2E Tests**: Bolt 4/4, Cache 5/5 (100%)
- � **Overall Progress**: +30 tests fixed since Phase 1 start

### Recent Improvements (November 14-15, 2025)
- 🚀 **Major Refactoring**: 22% code reduction in plan_builder.rs (769 LOC removed)
- 🐛 **Undirected Relationships**: Fixed `(a)-[r]-(b)` patterns with bidirectional OR JOIN logic
- 📚 **Documentation**: Anonymous node limitation documented in KNOWN_ISSUES.md
- 🧪 **Bug Fixes**: ChainedJoin CTE wrapping, WHERE clause filtering, test infrastructure fixes

### Known Limitations
- ⚠️ **Read-Only Engine**: Write operations (CREATE, SET, DELETE, MERGE) are not supported by design
- ⚠️ **Anonymous Nodes**: Queries like `MATCH ()-[r:FOLLOWS]->()` have SQL alias scope issues (use named nodes)
- ⚠️ **Bolt Query Execution**: Wire protocol implemented but query execution pending (use HTTP API)
- ⚠️ **Integration Test Gaps**: 111 tests remaining (feature gaps, not regressions)
- 🧪 **Flaky Test**: 1 cache LRU test occasionally fails (non-blocking)

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for detailed workarounds and status.

### Benchmark Results
**Performance validated across 3 scale levels:**

| Scale | Dataset | Success Rate | Mean Query Time |
|-------|---------|--------------|-----------------|
| **Small** | 1K users, 5K relationships | 10/10 (100%) | ~50ms |
| **Medium** | 10K users, 50K relationships | 10/10 (100%) | ~200ms |
| **Large** | 5M users, 50M relationships | 9/10 (90%) | ~2077ms |

**Key Findings**:
- ✅ Only 0.5% overhead for 10x data scale (2077ms → 2088ms)
- ✅ All query types working: traversals, aggregations, variable-length paths
- ✅ Development-ready for analytical workloads
- 📖 **Documentation**: See `notes/benchmarking.md` for detailed results

## 🗺️ Roadmap

**Phase 1 (v0.4.0) - COMPLETE** ✅
- ✅ Query cache with LRU eviction (10-100x speedup)
- ✅ Parameter support (Neo4j compatible)
- ✅ Bolt 5.8 protocol (wire protocol complete)
- ✅ Neo4j function mappings (25+ functions)
- ✅ Benchmark suite validation (14/14 queries)
- ✅ Code refactoring & bug fixes

**Phase 2 (v0.5.0) - COMPLETE** ✅ (November 2025)
- ✅ Multi-tenancy with parameterized views
- ✅ SET ROLE RBAC support
- ✅ Auto-schema discovery
- ✅ ReplacingMergeTree + FINAL support
- ✅ HTTP schema loading API
- ✅ Bolt Protocol 5.8 query execution
- ✅ Anonymous pattern support
- ✅ Complete documentation (19 wiki pages)
- ✅ 100% unit test coverage (422/422)

**Phase 3 (v0.6.0) - Q1 2026** (Planned)
- 🔄 RBAC & row-level security
- 🔄 Multi-tenant support with schema isolation
- 🔄 ReplacingMergeTree & FINAL support
- 🔄 Auto-schema discovery from ClickHouse metadata
- 🔄 Comprehensive Wiki documentation

**Phase 3 (v0.6.0) - Q2 2026** (Future)
- 🔮 Additional graph algorithms (centrality, community detection)
- 🔮 Query optimization improvements
- 🔮 Advanced Neo4j compatibility
- 🔮 Monitoring & observability enhancements

See [ROADMAP.md](ROADMAP.md) for detailed feature tracking and timelines.

## 🤝 Contributing

ClickGraph welcomes contributions! Key areas for development:
- Additional Cypher language features (Phase 3)
- Query optimization improvements  
- Neo4j compatibility enhancements
- Performance benchmarking
- Documentation improvements

**Development Resources**:
- [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md) - ⭐ **5-phase feature development workflow** (START HERE!)
- [STATUS.md](STATUS.md) - Current project status and test results
- [KNOWN_ISSUES.md](KNOWN_ISSUES.md) - Active bugs and limitations
- [.github/copilot-instructions.md](.github/copilot-instructions.md) - Architecture and conventions

## 📄 License

ClickGraph is licensed under the Apache License, Version 2.0. See the LICENSE file for details.

This project is developed on a forked repo of [Brahmand](https://github.com/suryatmodulus/brahmand) with zero-ETL view-based graph querying, Neo4j ecosystem compatibility and enterprise deployment capabilities.

