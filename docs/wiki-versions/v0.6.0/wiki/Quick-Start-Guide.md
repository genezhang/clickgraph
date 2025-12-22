> **Note**: This documentation is for ClickGraph v0.6.0. [View latest docs →](../../wiki/Home.md)
# Quick Start Guide

Get ClickGraph running in 5 minutes and execute your first graph query.

## Prerequisites

- **Docker & Docker Compose** (recommended) or
- **Rust 1.85+** for building from source
- 5 minutes of your time ⏱️

## Option 1: Docker Setup (Recommended) ⚡

This is the fastest way to get started. The docker-compose setup includes ClickHouse with sample data.

### Step 1: Clone the Repository

```bash
git clone https://github.com/genezhang/clickgraph.git
cd clickgraph
```

### Step 2: Start ClickGraph

```bash
# Start ClickHouse and ClickGraph
docker-compose up -d

# Check that services are running
docker-compose ps
```

You should see:
- `clickhouse` - ClickHouse database (port 8123)
- `clickgraph` - ClickGraph server (HTTP: 8080, Bolt: 7687)

### Step 3: Load Sample Data

```bash
# Load demo social network data (users and relationships)
docker exec -i clickgraph-clickhouse clickhouse-client < benchmarks/social_network/data/setup_unified.sql
```

**Sample data includes**:
- 10 users (Alice, Bob, Charlie, etc.)
- 15 friendship relationships
- User properties: name, age, country, city

### Step 4: Run Your First Query

**Using curl (HTTP API)**:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u.name, u.age ORDER BY u.age LIMIT 5"
  }'
```

**Expected output**:
```json
{
  "columns": ["u.name", "u.age"],
  "data": [
    ["Alice", 28],
    ["Bob", 32],
    ["Charlie", 35],
    ["Diana", 29],
    ["Eve", 31]
  ],
  "rows_read": 5,
  "elapsed": 0.012
}
```

🎉 **Success!** You've executed your first graph query!

### Step 5: Try a Graph Traversal

Find Alice's friends:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User {name: \"Alice\"})-[:FOLLOWS]->(friend) RETURN friend.name"
  }'
```

Find friends-of-friends (2-hop):
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User {name: \"Alice\"})-[:FOLLOWS*2]->(fof) RETURN fof.name"
  }'
```

### Step 6: Connect with Neo4j Browser (Optional)

ClickGraph supports the Neo4j Bolt protocol, so you can use Neo4j Browser:

1. **Open Neo4j Browser**: http://localhost:7474 (if installed) or use Neo4j Desktop
2. **Connect to**: `bolt://localhost:7687`
3. **No authentication**: Leave username/password empty or use "none"
4. **Run queries** directly in the browser:

```cypher
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, friend.name
LIMIT 10
```

You can now visualize your graph interactively! 🎨

---

## Option 2: Build from Source

If you prefer building from source or don't have Docker:

### Step 1: Install Prerequisites

```bash
# Rust (if not installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Start ClickHouse separately (Docker)
docker run -d --name clickhouse \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server
```

### Step 2: Clone and Build

```bash
git clone https://github.com/genezhang/clickgraph.git
cd clickgraph

# Build in release mode for better performance
cargo build --release
```

### Step 3: Set Environment Variables

```bash
# ClickHouse connection (required)
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""

# ClickHouse database (optional, defaults to "default")
# Only needed if you want to change the default database context
# export CLICKHOUSE_DATABASE="brahmand"

# Graph schema (use benchmark schema)
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

**Windows (PowerShell)**:
```powershell
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "default"
$env:CLICKHOUSE_PASSWORD = ""
# $env:CLICKHOUSE_DATABASE = "default"  # Optional, defaults to "default"
$env:GRAPH_CONFIG_PATH = ".\benchmarks\social_network\schemas\social_benchmark.yaml"
```

### Step 4: Run ClickGraph

```bash
# Run with default settings (HTTP: 8080, Bolt: 7687)
cargo run --release --bin clickgraph

# Or with custom ports
cargo run --release --bin clickgraph -- --http-port 8081 --bolt-port 7688
```

You should see:
```
[INFO] ClickGraph server starting...
[INFO] HTTP server listening on 0.0.0.0:8080
[INFO] Bolt server listening on 0.0.0.0:7687
[INFO] Schema loaded: social_network (1 views, 2 node types, 1 relationship types)
```

### Step 5: Load Sample Data and Query

```bash
# In a new terminal, load demo data
docker exec -i clickhouse clickhouse-client < benchmarks/social_network/data/setup_unified.sql

# Run a query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.name LIMIT 5"}'
```

---

## Verify Your Setup

### Check Server Health

```bash
curl http://localhost:8080/health
```

**Expected response**:
```json
{
  "status": "ok",
  "clickhouse": "connected",
  "version": "0.5.4"
}
```

### Check Schema Loading

```bash
curl http://localhost:8080/schema
```

**Expected response** (partial):
```json
{
  "name": "social_network",
  "version": "1.0",
  "views": [...],
  "nodes": ["User"],
  "relationships": ["FOLLOWS"]
}
```

### Test Query Performance

```bash
# First run (cold cache)
time curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN count(u)"}'

# Second run (cached - should be much faster!)
time curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN count(u)"}'
```

You should see **10-100x speedup** on the second run thanks to query caching! 🚀

---

## Common Issues

### "Connection refused" Error

**Problem**: Can't connect to ClickGraph server

**Solutions**:
```bash
# Check if server is running
docker-compose ps  # (if using Docker)
# or
ps aux | grep clickgraph  # (if built from source)

# Check logs
docker-compose logs clickgraph  # (Docker)
# or check terminal output (source build)

# Restart services
docker-compose restart  # (Docker)
```

### "Schema not found" Error

**Problem**: Schema file not loaded

**Solutions**:
```bash
# Check GRAPH_CONFIG_PATH environment variable
echo $GRAPH_CONFIG_PATH  # Linux/Mac
echo $env:GRAPH_CONFIG_PATH  # Windows PowerShell

# Verify schema file exists
ls -la benchmarks/social_network/schemas/social_benchmark.yaml  # Linux/Mac
Get-Item schemas\demo\users.yaml  # Windows

# Set correct path
export GRAPH_CONFIG_PATH="$(pwd)/benchmarks/social_network/schemas/social_benchmark.yaml"  # Linux/Mac
$env:GRAPH_CONFIG_PATH = "$PWD\schemas\demo\users.yaml"  # Windows
```

### "Table not found" Error

**Problem**: Sample data not loaded

**Solutions**:
```bash
# Load the demo data
docker exec -i clickhouse-clickhouse clickhouse-client < benchmarks/social_network/data/setup_unified.sql

# Verify tables exist
docker exec clickhouse-clickhouse clickhouse-client -q "SHOW TABLES FROM default"

# Check if data is loaded
docker exec clickhouse-clickhouse clickhouse-client -q "SELECT count(*) FROM users"
```

### Windows-Specific: PowerShell Background Jobs

**Problem**: Server exits immediately after starting

**Solution**: Use `Start-Job` for background processes:
```powershell
# ❌ Wrong (exits when script ends)
cargo run --release --bin clickgraph

# ✅ Correct (runs in background)
$job = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    $env:CLICKHOUSE_URL = $using:env:CLICKHOUSE_URL
    $env:GRAPH_CONFIG_PATH = $using:env:GRAPH_CONFIG_PATH
    cargo run --release --bin clickgraph
}

# Check output
Receive-Job -Id $job.Id -Keep

# Stop server
Stop-Job -Id $job.Id; Remove-Job -Id $job.Id
```

👉 **[More troubleshooting →](Troubleshooting-Guide.md)**

---

## Next Steps

Now that ClickGraph is running, continue your journey:

### Learn Cypher Queries
- **[Basic Patterns](Cypher-Basic-Patterns.md)** - Node and relationship matching
- **[Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md)** - Variable-length paths
- **[Aggregations & Functions](Cypher-Functions.md)** - COUNT, SUM, string functions

### Build Your Own Graph
- **[Your First Graph](Your-First-Graph.md)** - Step-by-step tutorial
- **[Schema Basics](Schema-Basics.md)** - Configure YAML schema
- **[Schema Best Practices](Schema-Best-Practices.md)** - Design patterns

### Explore Use Cases
- **[Social Network Analysis](Use-Case-Social-Network.md)** - Friend recommendations
- **[Fraud Detection](Use-Case-Fraud-Detection.md)** - Transaction networks
- **[Knowledge Graphs](Use-Case-Knowledge-Graphs.md)** - Entity relationships

### Prepare for Production
- **[Docker Deployment](Docker-Deployment.md)** - Production setup
- **[Production Best Practices](Production-Best-Practices.md)** - Security and performance
- **[Performance Tuning](Performance-Query-Optimization.md)** - Query optimization

---

## Quick Reference

### Sample Queries

```cypher
-- Find all users
MATCH (u:User) RETURN u.name, u.age

-- Find relationships
MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name

-- Count followers
MATCH (u:User)<-[:FOLLOWS]-(follower)
RETURN u.name, count(follower) as followers
ORDER BY followers DESC

-- Find friends of friends
MATCH (me:User {name: 'Alice'})-[:FOLLOWS*2]->(fof)
RETURN DISTINCT fof.name

-- Shortest path
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS*]-(b:User {name: 'Bob'}))
RETURN length(path), [node IN nodes(path) | node.name]
```

### Configuration Files

```yaml
# benchmarks/social_network/schemas/social_benchmark.yaml
name: social_network
views:
  - name: main
    nodes:
      User:
        source_table: users
        node_id: user_id
        property_mappings:
          name: full_name
          age: user_age
    relationships:
      FOLLOWS:
        source_table: user_follows
        from_node: User
        to_node: User
        from_id: follower_id
        to_id: followed_id
```

### Environment Variables

```bash
# Required
CLICKHOUSE_URL="http://localhost:8123"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

# Optional
CLICKHOUSE_DATABASE="default"  # Defaults to "default" if not set
CLICKGRAPH_HTTP_HOST="0.0.0.0"
CLICKGRAPH_HTTP_PORT="8080"
CLICKGRAPH_BOLT_HOST="0.0.0.0"
CLICKGRAPH_BOLT_PORT="7687"
```

---

**Congratulations!** 🎉 You've successfully set up ClickGraph and executed your first graph queries.

👉 **Continue to: [Your First Graph Tutorial](Your-First-Graph.md)**

---

[← Back to Home](Home.md) | [Next: Your First Graph →](Your-First-Graph.md)
