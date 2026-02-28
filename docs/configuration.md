# ClickGraph Configuration Guide

ClickGraph provides flexible server configuration through command-line arguments and environment variables, supporting both HTTP REST API and Neo4j Bolt protocol simultaneously.

## Quick Start

```bash
# 1. Start ClickHouse
docker-compose up -d

# 2. Set required environment variables
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
# CLICKHOUSE_DATABASE is optional (defaults to "default")

# 3. Run ClickGraph with default settings
cargo run --bin brahmand
```

This starts:
- HTTP API server on `http://0.0.0.0:8080`
- Bolt protocol server on `bolt://0.0.0.0:7687`

## Command-Line Options

### Basic Configuration

```bash
# Default configuration (HTTP:8080, Bolt:7687)
cargo run --bin brahmand

# Show all available options
cargo run --bin brahmand -- --help

# Show version information  
cargo run --bin brahmand -- --version
```

### Port Configuration

```bash
# Custom HTTP port
cargo run --bin brahmand -- --http-port 8081

# Custom Bolt port
cargo run --bin brahmand -- --bolt-port 7688

# Both custom ports
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688
```

### Host Binding

```bash
# Bind to localhost only (more secure)
cargo run --bin brahmand -- --http-host 127.0.0.1 --bolt-host 127.0.0.1

# Bind to specific interface
cargo run --bin brahmand -- --http-host 192.168.1.100 --bolt-host 192.168.1.100

# Default: bind to all interfaces (0.0.0.0)
cargo run --bin brahmand -- --http-host 0.0.0.0 --bolt-host 0.0.0.0
```

### Protocol Selection

```bash
# Disable Bolt protocol (HTTP only)
cargo run --bin brahmand -- --disable-bolt

# Enable both protocols (default behavior)
cargo run --bin brahmand
```

### Available Options

| Option | Description | Default |
|--------|-------------|---------|
| `--http-host <HOST>` | HTTP server bind address | `0.0.0.0` |
| `--http-port <PORT>` | HTTP server port | `8080` |
| `--bolt-host <HOST>` | Bolt server bind address | `0.0.0.0` |
| `--bolt-port <PORT>` | Bolt server port | `7687` |
| `--disable-bolt` | Disable Bolt protocol server | false (Bolt enabled by default) |
| `--help` | Show help message | - |
| `--version` | Show version information | - |

## Environment Variables

ClickGraph also supports environment variable configuration for backwards compatibility:

```bash
# ClickHouse connection (required)
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"

# Server configuration (optional)
export CLICKGRAPH_HOST="0.0.0.0"              # HTTP host
export CLICKGRAPH_PORT="8080"                 # HTTP port
export CLICKGRAPH_BOLT_HOST="0.0.0.0"         # Bolt host
export CLICKGRAPH_BOLT_PORT="7687"            # Bolt port
export CLICKGRAPH_BOLT_ENABLED="true"         # Enable/disable Bolt

# Schema configuration (required)
export GRAPH_CONFIG_PATH="./schemas/my_schema.yaml"
```

**Note**: Command-line arguments take precedence over environment variables.

## Schema Configuration

### Single Schema (Traditional)

Load one graph schema from a YAML file:

```bash
export GRAPH_CONFIG_PATH="./schemas/examples/social_network.yaml"
cargo run --bin clickgraph
```

### Multi-Schema Configuration (NEW in v0.6.1)

Load multiple independent graph schemas from a single YAML file:

```yaml
# schemas/multi_schema.yaml
default_schema: social_benchmark
schemas:
  - name: social_benchmark
    graph_schema:
      nodes:
        - label: User
          database: brahmand
          table: users_bench
          node_id: user_id
          property_mappings:
            user_id: user_id
            name: full_name
      edges:
        - type: FOLLOWS
          database: brahmand
          table: user_follows_bench
          from_id: follower_id
          to_id: followed_id
          from_node: User
          to_node: User

  - name: ldbc_snb
    graph_schema:
      nodes:
        - label: Person
          database: ldbc
          table: person
          node_id: personId
          property_mappings:
            personId: personId
            firstName: firstName
      edges:
        - type: KNOWS
          database: ldbc
          table: person_knows_person
          from_id: person1Id
          to_id: person2Id
          from_node: Person
          to_node: Person
```

**Usage**:
```bash
export GRAPH_CONFIG_PATH="./schemas/multi_schema.yaml"
cargo run --bin clickgraph

# Verify schemas loaded
curl -s http://localhost:8080/schemas | jq '.schemas[] | "\(.name): \(.node_count) nodes, \(.relationship_count) edges"'

# Output:
# social_benchmark: 4 nodes, 6 edges
# ldbc_snb: 8 nodes, 16 edges
# default: 4 nodes, 6 edges  (alias for default_schema)
```

**Selecting Schema in Queries**:
```cypher
# Use specific schema
USE social_benchmark
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u.name, f.name

# Switch to different schema
USE ldbc_snb
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.firstName, friend.firstName

# Use default schema (no USE clause needed)
MATCH (u:User) RETURN count(u)
```

**Benefits**:
- ✅ **Isolation**: Each schema maintains independent node/edge definitions
- ✅ **Flexibility**: Switch between schemas using `USE <schema_name>`
- ✅ **Simplified Testing**: Load all test schemas from one file
- ✅ **Backward Compatible**: Single-schema YAML files still work

## LLM Schema Discovery Configuration

The `clickgraph-client` `:discover` command uses an LLM to automatically generate graph schema YAML from ClickHouse table metadata.

Two API formats are supported:
- **Anthropic** (default) — Claude API
- **OpenAI-compatible** — works with OpenAI, Ollama, vLLM, LiteLLM, Together, Groq, and any OpenAI-compatible endpoint

### Anthropic (Default)

```bash
export ANTHROPIC_API_KEY="sk-ant-api03-..."

# Optional overrides
export CLICKGRAPH_LLM_MODEL="claude-sonnet-4-20250514"    # default
export CLICKGRAPH_LLM_MAX_TOKENS=8192                      # default
```

### OpenAI-Compatible

```bash
export CLICKGRAPH_LLM_PROVIDER="openai"
export OPENAI_API_KEY="sk-..."

# Optional overrides
export CLICKGRAPH_LLM_MODEL="gpt-4o"                                      # default for openai
export CLICKGRAPH_LLM_API_URL="https://api.openai.com/v1/chat/completions" # default for openai
export CLICKGRAPH_LLM_MAX_TOKENS=8192                                      # default
```

### Local Models (Ollama, vLLM, LiteLLM)

```bash
export CLICKGRAPH_LLM_PROVIDER="openai"
export OPENAI_API_KEY="not-needed"                         # required but ignored by local servers
export CLICKGRAPH_LLM_API_URL="http://localhost:11434/v1/chat/completions"  # Ollama
export CLICKGRAPH_LLM_MODEL="llama3.1:70b"
```

### Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLICKGRAPH_LLM_PROVIDER` | No | `anthropic` | `anthropic` or `openai`. Controls API format, auth headers, and request/response shape |
| `ANTHROPIC_API_KEY` | Yes (if provider=anthropic) | — | Anthropic API key. Get one at [console.anthropic.com](https://console.anthropic.com/) |
| `OPENAI_API_KEY` | Yes (if provider=openai) | — | OpenAI or compatible API key. Falls back to `ANTHROPIC_API_KEY` if not set |
| `CLICKGRAPH_LLM_MODEL` | No | `claude-sonnet-4-20250514` (anthropic) / `gpt-4o` (openai) | Model ID passed to the API |
| `CLICKGRAPH_LLM_API_URL` | No | Provider-specific | API endpoint URL. Override for proxy, gateway, or local models |
| `CLICKGRAPH_LLM_MAX_TOKENS` | No | `8192` | Maximum tokens in the LLM response. Increase if schemas are truncated |

### Usage

```bash
# Start the client (server must be running)
clickgraph-client --url http://localhost:8080

# Run LLM-powered discovery
clickgraph-client :) :discover mydb

# Output: Generated YAML with nodes, edges, property mappings
# Prompts to save to file and/or load into server
```

### Without an API Key

If `ANTHROPIC_API_KEY` is not set, `:discover` falls back to `:introspect` which displays raw table metadata (columns, types, PKs, sample rows). You can then:
- Write the YAML manually based on the metadata
- Use the `:design` wizard for guided schema creation

### Proxy / Gateway Setup

To route LLM calls through a corporate proxy or API gateway:

```bash
# Route through a local proxy
export CLICKGRAPH_LLM_API_URL="http://localhost:4000/v1/messages"

# Route through AWS Bedrock (requires compatible endpoint)
export CLICKGRAPH_LLM_API_URL="https://bedrock-runtime.us-east-1.amazonaws.com/..."
```

### Cost Estimation

Schema discovery is a one-time operation per database. Typical costs:

| Database Size | Prompt Tokens | Response Tokens | Estimated Cost (Sonnet) |
|--------------|---------------|-----------------|------------------------|
| 5-10 tables | ~2,000 | ~1,000 | ~$0.01 |
| 20-30 tables | ~6,000 | ~3,000 | ~$0.03 |
| 50+ tables | ~15,000 (batched) | ~8,000 | ~$0.08 |

## Protocol Support

### HTTP API
- **Port**: 8080 (default)
- **Endpoint**: `POST /query`
- **Format**: JSON with Cypher queries
- **Usage**: REST API clients, web applications

### Bolt Protocol
- **Port**: 7687 (default)
- **Protocol**: Neo4j Bolt v4.4 compatible
- **Usage**: Neo4j drivers, database tools
- **Features**: Authentication, transactions, streaming

## Production Deployment Examples

### Development Setup
```bash
# Local development with both protocols
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass" 
export CLICKHOUSE_DATABASE="brahmand"

cargo run --bin brahmand
```

### Secure Production Setup
```bash
# Bind to localhost only, custom ports
cargo run --bin brahmand -- \
  --http-host 127.0.0.1 --http-port 8081 \
  --bolt-host 127.0.0.1 --bolt-port 7688
```

### HTTP-Only Deployment
```bash
# Disable Bolt for REST-API only deployments
cargo run --bin brahmand -- --http-port 9090 --disable-bolt
```

### Container Deployment
```bash
# Bind to all interfaces for container deployments
cargo run --bin brahmand -- \
  --http-host 0.0.0.0 --http-port 8080 \
  --bolt-host 0.0.0.0 --bolt-port 7687
```

## Client Connections

### HTTP REST API

Connect to the HTTP API using standard HTTP clients:

```bash
# curl example
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN n LIMIT 10"}'
```

```python
# Python requests example
import requests

response = requests.post('http://localhost:8080/query', 
                        json={'query': 'MATCH (n) RETURN n LIMIT 10'})
result = response.json()
```

### Neo4j Bolt Protocol

Connect using any Neo4j driver:

```python
# Python neo4j driver
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n) RETURN n LIMIT 10")
    for record in result:
        print(record)
```

```javascript
// Node.js neo4j-driver
const neo4j = require('neo4j-driver');

const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();

session.run('MATCH (n) RETURN n LIMIT 10')
  .then(result => {
    result.records.forEach(record => console.log(record));
  });
```

```java
// Java Neo4j driver
Driver driver = GraphDatabase.driver("bolt://localhost:7687");
Session session = driver.session();

Result result = session.run("MATCH (n) RETURN n LIMIT 10");
while (result.hasNext()) {
    Record record = result.next();
    System.out.println(record);
}
```

### Neo4j Browser & Tools

ClickGraph is compatible with:
- **Neo4j Browser**: Connect to `bolt://localhost:7687`
- **Neo4j Desktop**: Add as remote database
- **Cypher Shell**: `cypher-shell -a bolt://localhost:7687`
- **Third-party tools**: Any tool supporting Neo4j Bolt protocol

## Monitoring & Health Checks

### Server Status
When ClickGraph starts successfully, you'll see:
```
ClickGraph v0.0.1 (fork of Brahmand)

Starting HTTP server on 0.0.0.0:8080
Starting Bolt server on 0.0.0.0:7687
Successfully bound Bolt listener to 0.0.0.0:7687
Brahmand server is running
  HTTP API: http://0.0.0.0:8080
  Bolt Protocol: bolt://0.0.0.0:7687
Bolt server loop starting, listening for connections...
```

### Health Check Endpoints
```bash
# Basic connectivity test
curl http://localhost:8080/query -X POST \
  -H "Content-Type: application/json" \
  -d '{"query": "RETURN 1 as health_check"}'
```

### Troubleshooting

**Connection refused errors:**
- Verify ClickHouse is running and accessible
- Check firewall settings for ports 8080/7687
- Ensure proper host binding (0.0.0.0 vs 127.0.0.1)

**ClickHouse connection issues:**
- Verify environment variables are set correctly
- Test ClickHouse connectivity: `curl http://localhost:8123`
- Check ClickHouse user permissions

**Port conflicts:**
- Use custom ports: `--http-port 8081 --bolt-port 7688`
- Check for other services using default ports


