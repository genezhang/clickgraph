> **Note**: This documentation is for ClickGraph v0.6.0. [View latest docs →](../../wiki/Home.md)
# AI Assistant Integration via MCP Protocol

**Use ClickGraph with AI assistants like Claude through the Model Context Protocol (MCP)**

## Overview

The Model Context Protocol (MCP) enables AI assistants to connect to external data sources and tools. ClickGraph's Bolt protocol implementation is fully compatible with Neo4j's MCP server, allowing you to query your graph data using natural language through AI assistants like Claude.

**Key Benefits**:
- 🎯 **Zero Custom Code**: Use existing Neo4j MCP server
- 🔄 **Zero Maintenance**: Anthropic maintains the protocol implementation
- 🚀 **Instant Setup**: 2-minute configuration
- 💬 **Natural Language**: Query graphs conversationally
- 📊 **Schema Discovery**: AI understands your graph structure automatically

## Quick Start

### Prerequisites

- ClickGraph server running with Bolt protocol enabled (default)
- Node.js and npm/npx installed
- Claude Desktop or compatible MCP client

### 1. Start ClickGraph

Start ClickGraph with both HTTP and Bolt protocols enabled:

```bash
# Using Docker (recommended)
docker run -d \
  -p 8080:8080 \
  -p 7687:7687 \
  -e CLICKHOUSE_URL="http://your-clickhouse:8123" \
  -e CLICKHOUSE_USER="your_user" \
  -e CLICKHOUSE_PASSWORD="your_pass" \
  -e CLICKHOUSE_DATABASE="your_database" \
  -e GRAPH_CONFIG_PATH="/config/schema.yaml" \
  -v $(pwd)/schemas:/config \
  genezhang/clickgraph:latest

# Or using cargo with required environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="your_user"
export CLICKHOUSE_PASSWORD="your_pass"
export CLICKHOUSE_DATABASE="your_database"
export GRAPH_CONFIG_PATH="./your_schemas/your_schema.yaml"
cargo run --bin clickgraph  # Defaults to port 8080 (HTTP) and 7687 (Bolt)
```

Verify Bolt is running:
```bash
# Test with Python Neo4j driver
python3 << EOF
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
with driver.session() as session:
    result = session.run("RETURN 1 as test")
    print(f"✅ Bolt working: {result.single()[0]}")
driver.close()
EOF
```

### 2. Configure Claude Desktop

**Location**: The Claude Desktop MCP configuration file location varies by OS:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

**Configuration**:

```json
{
  "mcpServers": {
    "clickgraph": {
      "command": "npx",
      "args": [
        "@modelcontextprotocol/server-neo4j",
        "bolt://localhost:7687"
      ],
      "env": {
        "NEO4J_USERNAME": "neo4j",
        "NEO4J_PASSWORD": "password"
      }
    }
  }
}
```

**For remote ClickGraph instances**:

```json
{
  "mcpServers": {
    "clickgraph-prod": {
      "command": "npx",
      "args": [
        "@modelcontextprotocol/server-neo4j",
        "bolt://your-server.com:7687"
      ],
      "env": {
        "NEO4J_USERNAME": "your_username",
        "NEO4J_PASSWORD": "your_password"
      }
    }
  }
}
```

### 3. Restart Claude Desktop

Close and reopen Claude Desktop to load the new configuration. You should see the ClickGraph MCP server appear in the available tools.

### 4. Start Querying!

Ask Claude natural language questions about your graph:

**Example Queries**:

```
You: "Show me the schema of the clickgraph database"
Claude: [Queries and displays node types and relationships]

You: "Find all users who follow each other mutually"
Claude: [Writes and executes: MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(a) RETURN ...]

You: "What's the average number of followers per user?"
Claude: [Writes aggregation query with count and average]

You: "Show me users from California who posted in the last 7 days"
Claude: [Combines property filtering with date functions]

You: "Find the shortest path between user 'Alice' and user 'Bob'"
Claude: [Uses shortestPath() function with graph traversal]
```

## Advanced Configuration

### Multiple Graph Databases

Connect to multiple ClickGraph instances with different schemas:

```json
{
  "mcpServers": {
    "social-graph": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-neo4j", "bolt://localhost:7687"]
    },
    "ecommerce-graph": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-neo4j", "bolt://localhost:7688"]
    },
    "fraud-detection": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-neo4j", "bolt://prod-server:7687"],
      "env": {
        "NEO4J_USERNAME": "analyst",
        "NEO4J_PASSWORD": "${PROD_PASSWORD}"  
      }
    }
  }
}
```

Then specify which database in your queries:
```
You: "Using social-graph, show me trending posts"
You: "In ecommerce-graph, find products frequently bought together"
```

### Custom Port Configuration

If running ClickGraph on non-default ports:

```bash
# Start ClickGraph with custom ports
docker run -d -p 9090:9090 -p 8687:8687 \
  genezhang/clickgraph:latest \
  --http-port 9090 --bolt-port 8687
```

Update MCP config:
```json
{
  "mcpServers": {
    "clickgraph": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-neo4j", "bolt://localhost:8687"]
    }
  }
}
```

### Authentication Configuration

ClickGraph supports multiple authentication methods through Bolt:

**Basic Auth** (default):
```json
{
  "env": {
    "NEO4J_USERNAME": "your_username",
    "NEO4J_PASSWORD": "your_password"
  }
}
```

**No Authentication** (development only):
```json
{
  "args": ["@modelcontextprotocol/server-neo4j", "bolt://localhost:7687"],
  "env": {}
}
```

### Docker Networking

When ClickGraph runs in Docker and Claude Desktop runs on host:

**Option 1: Expose ports** (recommended):
```bash
docker run -d -p 7687:7687 genezhang/clickgraph:latest
# Use bolt://localhost:7687 in MCP config
```

**Option 2: Use host network** (Linux only):
```bash
docker run -d --network host genezhang/clickgraph:latest
# Use bolt://localhost:7687 in MCP config
```

**Option 3: Use Docker host IP** (all platforms):
```bash
docker run -d -p 7687:7687 genezhang/clickgraph:latest
# macOS/Windows: Use bolt://host.docker.internal:7687
# Linux: Use bolt://172.17.0.1:7687 (default Docker bridge IP)
```

## Use Cases

### 1. Data Exploration

**Scenario**: Understand your graph structure before writing complex queries

```
You: "What types of nodes and relationships exist in this graph?"
Claude: [Queries schema and provides overview]

You: "Show me a sample of each node type"
Claude: [Executes MATCH queries for each node type with LIMIT]

You: "How many relationships of each type are there?"
Claude: [Aggregates relationship counts by type]
```

### 2. Iterative Query Development

**Scenario**: Refine complex queries conversationally

```
You: "Find users who have posted more than 10 times"
Claude: [Creates query with MATCH and WHERE count > 10]

You: "Now filter to only users from the US"
Claude: [Adds u.country = 'US' to WHERE clause]

You: "And sort by their registration date"
Claude: [Adds ORDER BY u.registration_date]
```

### 3. Data Analysis & Reporting

**Scenario**: Generate insights and summaries

```
You: "Create a report of top influencers (users with most followers)"
Claude: [Writes aggregation with ORDER BY and LIMIT]

You: "What percentage of users have never posted?"
Claude: [Writes query with OPTIONAL MATCH and percentage calculation]

You: "Show trends in user activity over the last 30 days"
Claude: [Groups by date with temporal functions]
```

### 4. Anomaly Detection

**Scenario**: Find unusual patterns in graph data

```
You: "Find users who follow more than 100 people but have fewer than 10 followers"
Claude: [Writes query with relationship count filters]

You: "Show me posts that got likes from users who don't follow the author"
Claude: [Complex multi-hop pattern matching]
```

### 5. Schema Validation

**Scenario**: Verify data quality and integrity

```
You: "Are there any users without a name property?"
Claude: [Checks for null/missing properties]

You: "Find orphaned relationships (pointing to non-existent nodes)"
Claude: [Writes query with OPTIONAL MATCH to find dangling refs]
```

## Features & Capabilities

### What Works

✅ **All ClickGraph Cypher Features**:
- Basic pattern matching (`MATCH`, `WHERE`, `RETURN`)
- Multi-hop traversals (`-[*1..3]->`)
- Aggregations (`count()`, `avg()`, `collect()`)
- Path functions (`shortestPath()`, `length()`, `nodes()`)
- Optional patterns (`OPTIONAL MATCH`)
- Subqueries (`EXISTS`, `WITH`)
- All Cypher operators and functions

✅ **Schema Discovery**:
- Automatic node type enumeration
- Relationship type discovery
- Property inspection
- Index and constraint information (if defined in schema)

✅ **Query Optimization**:
- AI generates optimized Cypher
- Leverages ClickGraph's query optimizer
- Parameterized queries for safety

### Current Limitations

⚠️ **Read-Only Operations**:
- Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are not supported
- This is by design - ClickGraph is a read-only analytics engine

⚠️ **MCP Protocol Constraints**:
- Results limited by MCP message size (typically sufficient for analytics)
- For very large result sets, use direct HTTP/Bolt API instead

⚠️ **AI Query Generation**:
- AI-generated queries may need refinement for complex patterns
- Review generated Cypher for optimal performance
- Provide schema context for better query generation

## Troubleshooting

### Connection Issues

**Problem**: Claude can't connect to ClickGraph

**Solutions**:
```bash
# 1. Verify ClickGraph is running
curl http://localhost:8080/health

# 2. Verify Bolt port is accessible
telnet localhost 7687

# 3. Check firewall rules (if remote)
sudo ufw allow 7687/tcp

# 4. Check Docker port mapping
docker ps | grep clickgraph
# Should show: 0.0.0.0:7687->7687/tcp

# 5. Test with Neo4j driver directly
python3 << EOF
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
with driver.session() as session:
    result = session.run("RETURN 1 as test")
    print(result.single()[0])
driver.close()
EOF
```

### MCP Server Not Loading

**Problem**: MCP server doesn't appear in Claude Desktop

**Solutions**:

1. **Verify JSON syntax**:
   ```bash
   # Validate config file
   cat ~/Library/Application\ Support/Claude/claude_desktop_config.json | python3 -m json.tool
   ```

2. **Check Node.js installation**:
   ```bash
   node --version  # Should be v14 or higher
   npx --version
   ```

3. **Test MCP server manually**:
   ```bash
   npx @modelcontextprotocol/server-neo4j bolt://localhost:7687
   # Should start without errors
   ```

4. **Check Claude Desktop logs** (macOS):
   ```bash
   tail -f ~/Library/Logs/Claude/mcp*.log
   ```

### Query Execution Errors

**Problem**: Queries fail or return unexpected results

**Debugging Steps**:

1. **Test query directly**:
   ```bash
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query": "MATCH (n) RETURN count(n)"}'
   ```

2. **Check schema configuration**:
   ```bash
   # Verify schema is loaded correctly
   curl http://localhost:8080/schemas
   ```

3. **Review generated SQL** (enable debug mode):
   ```bash
   # Set RUST_LOG=debug and check ClickGraph logs
   docker logs clickgraph-container | grep "Generated SQL"
   ```

4. **Test with simpler query**:
   ```
   You: "Return 1 as test"  # Should always work
   ```

### Performance Issues

**Problem**: Queries are slow or time out

**Solutions**:

1. **Add LIMIT clauses**:
   ```
   You: "Find all users LIMIT 100"  # Explicit limits help
   ```

2. **Optimize schema**:
   - Ensure proper indexes in ClickHouse
   - Use node_id columns efficiently
   - Consider denormalized edge tables for hot paths

3. **Use HTTP API for large results**:
   ```bash
   # MCP is better for exploratory queries
   # Use direct API for bulk data extraction
   curl -X POST http://localhost:8080/query \
     -d '{"query": "MATCH (n:User) RETURN n"}' > results.json
   ```

4. **Check ClickGraph logs for optimization opportunities**:
   ```bash
   docker logs clickgraph-container | grep "query_time"
   ```

## Testing

Run the provided test scripts to verify MCP compatibility:

### Automated Compatibility Test

```bash
# Run isolated MCP compatibility test (won't interfere with running instances)
cd /path/to/clickgraph
./scripts/test/test_mcp_compatibility.sh
```

Expected output:
```
✅ Bolt Protocol Connection: PASS
✅ Cypher Query Execution: PASS
```

### Manual MCP Server Test

```bash
# Start test ClickGraph instance on isolated ports
./scripts/test/test_mcp_compatibility.sh

# In another terminal, test Neo4j MCP server
./scripts/test/test_neo4j_mcp_server.sh
```

## Best Practices

### 1. Schema Documentation

Help AI understand your schema better by describing it:

```
You: "The User node has properties: name, email, age, country, and registration_date. 
      Users can FOLLOW other users and LIKE posts. 
      Posts have properties: content, created_at, and author_id."

Claude: [Uses this context for better query generation]
```

### 2. Explicit Constraints

Be specific about what you want:

```
✅ Good: "Find active users from California who posted in December 2025"
❌ Vague: "Find some users"
```

### 3. Iterative Refinement

Start simple and refine:

```
You: "Show me users"
Claude: [Returns basic query]

You: "Add their follower count"
Claude: [Adds aggregation]

You: "Filter to users with > 100 followers"
Claude: [Adds HAVING clause]
```

### 4. Result Verification

Always verify AI-generated queries for correctness:

```
You: "Explain the query you just ran"
Claude: [Breaks down the Cypher logic]

You: "Show me the actual Cypher query"
Claude: [Displays the exact query executed]
```

### 5. Use Direct API for Production

MCP is excellent for exploration and ad-hoc analysis. For production:
- Use HTTP or Bolt API directly
- Parameterize queries properly
- Implement proper error handling
- Monitor query performance

## Alternatives to MCP

If MCP isn't suitable for your use case:

### 1. Direct Neo4j Driver Integration

Use Neo4j drivers directly in your application:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n:User) RETURN n LIMIT 10")
    for record in result:
        print(record["n"])
```

### 2. HTTP REST API

Use ClickGraph's HTTP API for maximum control:

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:User) RETURN n LIMIT 10"}'
```

### 3. Custom MCP Server

Build a ClickGraph-specific MCP server with additional features:
- Custom tools for domain-specific operations
- Advanced caching strategies
- Query optimization hints
- Schema-aware query validation

See [Custom MCP Server Development](#future-enhancements) below.

## Future Enhancements

Potential improvements for MCP integration:

1. **Custom ClickGraph MCP Server**:
   - Leverage ClickGraph's HTTP API directly (no Bolt translation)
   - ClickGraph-specific features (query cache control, schema management)
   - Enhanced error messages with optimization hints

2. **Enhanced Schema Discovery**:
   - Automatic schema documentation generation
   - Example queries for common patterns
   - Property type inference and validation

3. **Query Performance Insights**:
   - Expose ClickGraph's query timing metrics
   - Suggest indexes and optimizations
   - Query plan visualization

4. **Multi-Schema Support**:
   - Schema-aware query generation
   - Cross-schema query validation
   - Dynamic schema switching via USE clause

## Related Documentation

- [Bolt Protocol Implementation](../features/bolt-protocol.md)
- [HTTP API Reference](API-Reference-HTTP.md)
- [Schema Configuration](Schema-Basics.md)
- [Cypher Language Reference](Cypher-Language-Reference.md)
- [Quick Start Guide](Quick-Start-Guide.md)

## Support

**Questions or Issues?**

- 📖 Check [Troubleshooting Guide](Troubleshooting-Guide.md)
- 🐛 Report issues: [GitHub Issues](https://github.com/yourusername/clickgraph/issues)
- 💬 Discuss: [GitHub Discussions](https://github.com/yourusername/clickgraph/discussions)

**MCP Protocol Resources**:

- [Model Context Protocol Specification](https://modelcontextprotocol.io)
- [Neo4j MCP Server Documentation](https://github.com/modelcontextprotocol/servers/tree/main/src/neo4j)
- [Anthropic MCP Documentation](https://docs.anthropic.com/en/docs/model-context-protocol)
