<div align="center">
  <img src="./logo.svg" height="200">
</div>

# ClickGraph

#### ClickGraph - A Fork of Brahmand

**A high-performance, stateless graph-analysis layer for ClickHouse with Neo4j ecosystem compatibility.**

> **Note: ClickGraph is a fork of Brahmand with additional features including Neo4j Bolt protocol support and view-based graph analysis.**

---

## Features

### Core Capabilities
- **ClickHouse-native**: Extends ClickHouse with native graph modeling, merging OLAP speed with graph-analysis power
- **Stateless Architecture**: Offloads all storage and query execution to ClickHouse—no extra datastore required
- **Cypher Query Language**: Industry-standard Cypher syntax for intuitive, expressive property-graph querying
- **Variable-Length Paths**: Recursive traversals with `*1..3` syntax using ClickHouse WITH RECURSIVE CTEs
- **Analytical-scale Performance**: Optimized for very large datasets and complex multi-hop traversals

### Neo4j Ecosystem Compatibility
- **Bolt Protocol v4.4**: Full Neo4j driver compatibility for seamless integration
- **Dual Server Architecture**: HTTP REST API and Bolt protocol running simultaneously
- **Authentication Support**: Multiple authentication schemes including basic auth
- **Tool Compatibility**: Works with existing Neo4j drivers, browsers, and applications

### View-Based Graph Model
- **Zero Migration**: Transform existing relational data into graph format through YAML configuration
- **Native Performance**: Leverages ClickHouse's columnar storage and query optimization
- **Robust Implementation**: Comprehensive validation, error handling, and optimization passes

---

## Architecture

ClickGraph runs as a lightweight graph wrapper alongside ClickHouse with dual protocol support:

![acrhitecture](./architecture.png)

### HTTP API (Port 8080)
1. **Client** sends HTTP POST request with Cypher query to ClickGraph
2. **ClickGraph** parses & plans the query, translates to ClickHouse SQL
3. **ClickHouse** executes the SQL and returns results
4. **ClickGraph** sends JSON results back to the client

### Bolt Protocol (Port 7687) 
1. **Neo4j Driver/Tool** connects via Bolt protocol to ClickGraph
2. **ClickGraph** handles Bolt handshake, authentication, and message protocol
3. **Cypher queries** are processed through the same query engine as HTTP
4. **Results** are streamed back via Bolt protocol format

Both protocols share the same underlying query engine and ClickHouse backend.

---

## 🚀 Quick Start

**New to ClickGraph?** See the **[Getting Started Guide](docs/getting-started.md)** for a complete walkthrough.

> **⚠️ Windows Users**: The HTTP server has a known issue on Windows. Use Docker or WSL for development. See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

### 5-Minute Setup (Docker - Recommended)

1. **Clone and start services**:
   ```bash
   git clone https://github.com/genezhang/clickgraph
   cd clickgraph
   docker-compose up -d
   ```
   This starts both ClickHouse and ClickGraph with test data pre-loaded.

2. **Test the setup**:
   ```bash
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "MATCH (u:User) RETURN u.full_name LIMIT 5"}'
   ```

### Native Build (Linux/macOS/WSL)

1. **Start ClickHouse**:
   ```bash
   docker-compose up -d clickhouse
   ```

2. **Configure and run**:
   ```bash
   export CLICKHOUSE_URL="http://localhost:8123"
   export CLICKHOUSE_USER="test_user"
   export CLICKHOUSE_PASSWORD="test_pass"
   export CLICKHOUSE_DATABASE="brahmand"
   
   cargo run --bin brahmand
   ```

3. **Test with HTTP API**:
   ```bash
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "RETURN 1 as test"}'
   ```

4. **Test with Neo4j driver**:
   ```python
   from neo4j import GraphDatabase
   
   driver = GraphDatabase.driver("bolt://localhost:7687")
   with driver.session() as session:
       result = session.run("RETURN 1 as test")
   ```

📖 **[Complete Setup Guide →](docs/getting-started.md)**

## 📊 View-Based Graph Model

Transform existing relational data into graph format through YAML configuration:

**Example**: Map your `users` and `user_follows` tables to a social network graph:
```yaml
views:
  - name: social_network
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
    relationships:
      follows:
        source_table: user_follows
        from_column: follower_id
        to_column: followed_id
```

Then query with standard Cypher:
```cypher
MATCH (u:user)-[:follows]->(friend:user)
WHERE u.name = 'Alice'
RETURN friend.name
```

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
cargo run --bin brahmand -- --help

# Custom ports
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688

# Disable Bolt protocol (HTTP only)
cargo run --bin brahmand -- --disable-bolt

# Custom host binding
cargo run --bin brahmand -- --http-host 127.0.0.1 --bolt-host 127.0.0.1
```

See `docs/configuration.md` for complete configuration documentation.

## 📚 Documentation

### User Guides
- **[Getting Started](docs/getting-started.md)** - Complete setup walkthrough and first queries
- **[Features Overview](docs/features.md)** - Comprehensive feature list and capabilities  
- **[API Documentation](docs/api.md)** - HTTP REST API and Bolt protocol usage
- **[Configuration Guide](docs/configuration.md)** - Server configuration and CLI options

### Technical Documentation  
- **[GraphView Model](docs/graphview1-branch-summary.md)** - Complete view-based graph analysis
- **[Test Infrastructure](docs/test-infrastructure-redesign.md)** - Testing framework and validation
- **[Development Guide](.github/copilot-instructions.md)** - Development workflow and architecture

### Reference
- **[Original Brahmand Docs](https://www.brahmanddb.com/introduction/intro)** - Original project documentation
- **[Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/)** - Cypher query language reference
- **[ClickHouse Documentation](https://clickhouse.com/docs/)** - ClickHouse database documentation

## 🚀 Performance

Preliminary informal tests on a MacBook Pro (M3 Pro, 18 GB RAM) running ClickGraph in Docker against a ~12 million-node Stack Overflow dataset show multihop traversals running approximately 10× faster than Neo4j v2025.03. These early, unoptimized results are for reference only; a full benchmark report is coming soon.

## 🧪 Development Status

ClickGraph includes the following completed features:
- ✅ **Neo4j Bolt Protocol v4.4**: Full compatibility with Neo4j drivers and tools
- ✅ **View-Based Graph Model**: Transform existing tables to graphs via YAML configuration  
- ✅ **Dual Server Architecture**: HTTP REST API and Bolt protocol simultaneously
- ✅ **Comprehensive Testing**: 374/374 tests passing with 100% success rate
- ✅ **Flexible Configuration**: CLI options, environment variables, Docker deployment
- ✅ **Query Optimization**: Advanced optimization passes including chained JOIN optimization for exact hop counts

### Known Considerations
- ⚠️ **Schema warnings**: Cosmetic warnings about internal catalog system (functionality unaffected)
- 🔧 **Memory vs MergeTree**: Use Memory engine for development, MergeTree for persistent storage
- 🐳 **Docker permissions**: May require volume permission fixes on some systems

## 🤝 Contributing

ClickGraph welcomes contributions! Key areas for development:
- Additional Cypher language features
- Query optimization improvements  
- Neo4j compatibility enhancements
- Performance benchmarking
- Documentation improvements

## 📄 License

ClickGraph is licensed under the Apache License, Version 2.0. See the LICENSE file for details.

This project is a fork of [Brahmand](https://github.com/suryatmodulus/brahmand) with significant enhancements for Neo4j ecosystem compatibility and enterprise deployment capabilities.