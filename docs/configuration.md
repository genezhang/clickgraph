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
export CLICKHOUSE_DATABASE="brahmand"

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
export BRAHMAND_HOST="0.0.0.0"              # HTTP host
export BRAHMAND_PORT="8080"                 # HTTP port
export BRAHMAND_BOLT_HOST="0.0.0.0"         # Bolt host
export BRAHMAND_BOLT_PORT="7687"            # Bolt port
export BRAHMAND_BOLT_ENABLED="true"         # Enable/disable Bolt
```

**Note**: Command-line arguments take precedence over environment variables.

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