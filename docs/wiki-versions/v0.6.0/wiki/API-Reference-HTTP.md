> **Note**: This documentation is for ClickGraph v0.6.0. [View latest docs →](../../wiki/Home.md)
# HTTP REST API Reference

ClickGraph provides a comprehensive HTTP REST API for executing Cypher queries and managing graph schemas.

## Table of Contents
- [Base URL](#base-url)
- [Authentication](#authentication)
- [Query Cache Control](#query-cache-control)
- [Query Execution](#query-execution)
- [Schema Management](#schema-management)
- [Health Check](#health-check)
- [Error Handling](#error-handling)

---

## Base URL

Default server address:
```
http://localhost:8080
```

Configure via environment variables or CLI:
```bash
# Environment
export HTTP_HOST="0.0.0.0"
export HTTP_PORT="8080"

# CLI
clickgraph --http-host 0.0.0.0 --http-port 8080
```

---

## Authentication

Currently no authentication required for HTTP API. 

> **Production Note**: Use reverse proxy (nginx, Traefik) for authentication, rate limiting, and TLS termination. See [Production Best Practices](Production-Best-Practices.md).

---

## Query Cache Control

ClickGraph implements an LRU cache for SQL query templates, providing **10-100x speedup** for repeated query translations. The cache stores SQL templates with parameter placeholders, enabling fast execution of the same query pattern with different parameter values.

### Cache Behavior

**Default Behavior (LRU):**
- First execution: Query is parsed, planned, and SQL is generated and cached
- Subsequent executions: SQL template retrieved from cache, parameters substituted
- Cache eviction: Least recently used entries removed when cache is full

**Performance Impact:**
- **Cache HIT**: ~1-5ms (template lookup + parameter substitution)
- **Cache MISS**: ~10-100ms (parse + plan + SQL generation)

### Cache Control via CYPHER Prefix

Control caching behavior on a **per-request basis** by prefixing your query with `CYPHER replan=<option>`:

**Syntax:**
```cypher
CYPHER replan=<option> <your-query>
```

**Options:**

| Option | Behavior | Use Case |
|--------|----------|----------|
| `default` | Normal LRU cache behavior | Default (can be omitted) |
| `force` | Bypass cache, regenerate SQL, update cache | Debugging, testing new query translations |
| `skip` | **Always use cache**, error if not cached | Prevent latency spikes in production |

**Examples:**

```bash
# Force regeneration (bypass cache)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "CYPHER replan=force MATCH (u:User) RETURN u.name"}'

# Require cached query (error if not cached)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "CYPHER replan=skip MATCH (u:User) RETURN u.name"}'

# Normal cache behavior (default)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "CYPHER replan=default MATCH (u:User) RETURN u.name"}'
```

**Python Examples:**

```python
import requests

# Force cache bypass for debugging
response = requests.post('http://localhost:8080/query', json={
    'query': 'CYPHER replan=force MATCH (u:User) WHERE u.age > $age RETURN u.name',
    'parameters': {'age': 25}
})

# Require cached query (production - prevent latency spikes)
response = requests.post('http://localhost:8080/query', json={
    'query': 'CYPHER replan=skip MATCH (u:User) WHERE u.age > $age RETURN u.name',
    'parameters': {'age': 30}
})
```

### Neo4j Compatibility

The `CYPHER replan=<option>` syntax is **compatible with Neo4j**, allowing the same queries to work across both systems. The `CYPHER` prefix is automatically stripped before query execution.

### Configuration

Control cache behavior via environment variables:

```bash
# Enable/disable cache (default: true)
export CLICKGRAPH_QUERY_CACHE_ENABLED=true

# Max cache entries (default: 1000)
export CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES=1000

# Max cache size in MB (default: 100)
export CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB=100
```

### Cache Key

Cache key includes:
- ✅ Normalized Cypher query (whitespace collapsed)
- ✅ Schema name
- ❌ **NOT** view_parameters (substituted at execution time)
- ❌ **NOT** query parameters (substituted from template)

This allows parameter changes to reuse the same cached SQL template.

---

## Query Execution

### POST /query

Execute a Cypher query against your graph schema.

**Request:**
```http
POST /query HTTP/1.1
Content-Type: application/json

{
  "query": "MATCH (u:User) WHERE u.user_id = $userId RETURN u.name",
  "parameters": {
    "userId": 123
  },
  "schema_name": "social_network",
  "sql_only": false,
  "format": "json"
}
```

**Parameters:**
- `query` (string, required): Cypher query to execute
  - **Query Cache Control**: Prefix query with `CYPHER replan=<option>` to control caching behavior
    - `CYPHER replan=default` - Normal cache behavior (use cache if available, regenerate if needed)
    - `CYPHER replan=force` - Bypass cache, regenerate SQL, update cache (useful for debugging)
    - `CYPHER replan=skip` - Always use cache, error if not cached (prevent latency spikes)
  - Example: `"CYPHER replan=force MATCH (u:User) RETURN u.name"`
  - The `CYPHER` prefix is automatically stripped before query execution
- `parameters` (object, optional): Query parameters for `$param` placeholders
- `schema_name` (string, optional): Schema to use (overrides USE clause and defaults to "default")
- `sql_only` (boolean, optional): Return generated SQL without executing (default: false)
- `format` (string, optional): Response format - `json` (default) or `table`
- `view_parameters` (object, optional): Parameters for parameterized views (multi-tenancy)
- `tenant_id` (string, optional): Tenant identifier for multi-tenant deployments
- `role` (string, optional): ClickHouse role for RBAC (requires database-managed users)

**Response (JSON format):**
```json
{
  "results": [
    {"u.name": "Alice"},
    {"u.name": "Bob"}
  ]
}
```

**Response (SQL-only mode):**
```json
{
  "sql": "SELECT full_name AS `u.name` FROM brahmand.users WHERE user_id = 123",
  "cypher": "MATCH (u:User) WHERE u.user_id = $userId RETURN u.name",
  "parameters": {"userId": 123}
}
```

**Response Headers:**
- `X-Cypher-Parse-Time-Ms`: Time to parse Cypher (milliseconds)
- `X-Cypher-Plan-Time-Ms`: Time to create logical plan (milliseconds)
- `X-ClickHouse-Query-Time-Ms`: ClickHouse execution time (milliseconds)
- `X-Total-Time-Ms`: Total request processing time (milliseconds)

**Examples:**

**curl:**
```bash
# Basic query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.name LIMIT 10"}'

# With parameters
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = $id RETURN u",
    "parameters": {"id": 123}
  }'

# With schema selection
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN count(u)",
    "schema_name": "social_network"
  }'

# SQL-only mode (debugging)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name",
    "sql_only": true
  }'

# Force cache bypass (regenerate SQL)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CYPHER replan=force MATCH (u:User) RETURN u.name"
  }'

# Require cached query (prevent planning latency)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CYPHER replan=skip MATCH (u:User) RETURN u.name"
  }'
```

**Python:**
```python
import requests

# Basic query
response = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (u:User) RETURN u.name LIMIT 10'
})
data = response.json()

# With parameters
response = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (u:User) WHERE u.user_id = $id RETURN u',
    'parameters': {'id': 123}
})

# Force cache bypass (debugging/testing)
response = requests.post('http://localhost:8080/query', json={
    'query': 'CYPHER replan=force MATCH (u:User) RETURN u.name'
})

# Multi-tenant query
response = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (u:User) RETURN u.name',
    'view_parameters': {'tenant_id': 'acme_corp'}
})
```

**PowerShell:**
```powershell
# Basic query
Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
  -ContentType "application/json" `
  -Body '{"query":"MATCH (u:User) RETURN u.name LIMIT 10"}'

# With parameters
$body = @{
    query = "MATCH (u:User) WHERE u.user_id = `$id RETURN u"
    parameters = @{id = 123}
} | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
  -ContentType "application/json" `
  -Body $body
```

---

### POST /query/sql

**SQL Generation Endpoint** - Translate Cypher to ClickHouse SQL without execution.

This production endpoint returns SQL statements as an array, including role management commands when applicable.

**Request:**
```http
POST /query/sql HTTP/1.1
Content-Type: application/json

{
  "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = 'Alice' RETURN f.name",
  "schema_name": "social_network",
  "role": "analyst"
}
```

**Parameters:**
- `query` (string, required): Cypher query to translate
- `schema_name` (string, optional): Schema to use (defaults to "default")
- `target_database` (string, optional): Target SQL dialect - "clickhouse" (default) or "postgresql" (future)
- `parameters` (object, optional): Query parameters for `$param` placeholders
- `view_parameters` (object, optional): Parameters for parameterized views
- `role` (string, optional): ClickHouse role for RBAC
- `include_plan` (boolean, optional): Include logical plan in response (default: false)

**Response:**
```json
{
  "cypher_query": "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = 'Alice' RETURN f.name",
  "target_database": "clickhouse",
  "sql": [
    "SET ROLE analyst",
    "SELECT f.full_name AS \"f.name\" FROM users AS u INNER JOIN follows ON follows.follower_id = u.user_id INNER JOIN users AS f ON f.user_id = follows.followed_id WHERE u.full_name = 'Alice'"
  ],
  "role": "analyst",
  "metadata": {
    "query_type": "read",
    "cache_status": "HIT",
    "parse_time_ms": 0.152,
    "planning_time_ms": 2.341,
    "sql_generation_time_ms": 0.892,
    "total_time_ms": 3.385
  }
}
```

**Key Features:**
- **Array Format**: SQL returned as array of statements to execute in order
- **Role Management**: Includes `SET ROLE` statement when role parameter provided
- **No Execution**: SQL is generated but not executed against ClickHouse
- **Performance Metrics**: Detailed timing breakdown for optimization
- **Cache Status**: Shows if SQL was retrieved from query cache

**Use Cases:**
- **Query Debugging**: Inspect generated SQL before execution
- **External Execution**: Use SQL in other tools or pipelines
- **Performance Analysis**: Understand query planning decisions
- **Integration**: Embed ClickGraph translation in existing workflows

**Examples:**

**curl:**
```bash
# Basic SQL generation
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.name LIMIT 10"}'

# With role (includes SET ROLE in array)
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name",
    "role": "analyst"
  }'

# With logical plan
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS*1..3]->(f) RETURN f.name",
    "include_plan": true
  }'
```

**Python:**
```python
import requests

# Generate SQL with role
response = requests.post('http://localhost:8080/query/sql', json={
    'query': 'MATCH (u:User)-[:FOLLOWS]->(f) WHERE u.name = $name RETURN f.name',
    'parameters': {'name': 'Alice'},
    'role': 'analyst'
})

data = response.json()
print(f"SQL Statements: {data['sql']}")
print(f"Generation Time: {data['metadata']['total_time_ms']}ms")

# Execute SQL externally (without SET ROLE)
from clickhouse_driver import Client
ch_client = Client(host='localhost')
for stmt in data['sql']:
    if not stmt.startswith('SET ROLE'):
        result = ch_client.execute(stmt)
```

**PowerShell:**
```powershell
# Generate SQL
$response = Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query/sql" `
  -ContentType "application/json" `
  -Body '{"query":"MATCH (u:User) RETURN u.name LIMIT 10"}'

$response.sql
```

> **Note**: The older `/query?sql_only=true` endpoint returns SQL as a single string in `generated_sql` field. The `/query/sql` endpoint is preferred as it returns an array and includes role management statements.

---

## Schema Management

### GET /schemas

List all available graph schemas.

**Request:**
```http
GET /schemas HTTP/1.1
```

**Response:**
```json
{
  "schemas": [
    {
      "name": "default",
      "node_count": 3,
      "relationship_count": 2
    },
    {
      "name": "social_network",
      "node_count": 5,
      "relationship_count": 4
    }
  ]
}
```

**Example:**
```bash
curl http://localhost:8080/schemas
```

### GET /schemas/{name}

Get detailed information about a specific schema.

**Request:**
```http
GET /schemas/social_network HTTP/1.1
```

**Response:**
```json
{
  "schema_name": "social_network",
  "node_types": 3,
  "relationship_types": 2,
  "nodes": ["User", "Post", "Comment"],
  "relationships": ["FOLLOWS", "AUTHORED"]
}
```

**Example:**
```bash
curl http://localhost:8080/schemas/social_network
```

### POST /schemas/load

Load a new graph schema from YAML content at runtime.

**Request:**
```http
POST /schemas/load HTTP/1.1
Content-Type: application/json

{
  "schema_name": "ecommerce",
  "config_content": "name: ecommerce\ngraph_schema:\n  nodes: ...",
  "validate_schema": true
}
```

**Parameters:**
- `schema_name` (string, required): Name to register the schema under
- `config_content` (string, required): Full YAML schema configuration as string
- `validate_schema` (boolean, optional): Validate tables/columns exist in ClickHouse (default: false)

**Response:**
```json
{
  "message": "Schema 'ecommerce' loaded successfully",
  "schema_name": "ecommerce"
}
```

**Features:**
- ✅ Runtime schema registration without server restart
- ✅ Auto-discovery support: `auto_discover_columns: true` in YAML
- ✅ Immediate availability for queries
- ✅ Multiple schemas can coexist
- ✅ Validates tables/columns if requested

**Examples:**

**Load from file (bash):**
```bash
# Read YAML file and send as string
curl -X POST http://localhost:8080/schemas/load \
  -H "Content-Type: application/json" \
  -d "$(jq -Rs '{schema_name: "ecommerce", config_content: ., validate_schema: true}' schemas/ecommerce.yaml)"
```

**Load from file (PowerShell):**
```powershell
$yamlContent = Get-Content "schemas\ecommerce.yaml" -Raw
$body = @{
    schema_name = "ecommerce"
    config_content = $yamlContent
    validate_schema = $true
} | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8080/schemas/load" `
  -ContentType "application/json" `
  -Body $body
```

**Python:**
```python
import requests

# Load schema from file
with open('schemas/ecommerce.yaml', 'r') as f:
    yaml_content = f.read()

response = requests.post('http://localhost:8080/schemas/load', json={
    'schema_name': 'ecommerce',
    'config_content': yaml_content,
    'validate_schema': True
})

print(response.json())
```

---

## Health Check

### GET /health

Simple health check endpoint for monitoring and load balancers.

**Request:**
```http
GET /health HTTP/1.1
```

**Response:**
```
HTTP/1.1 200 OK
Content-Type: text/plain

service    status  version
-------    ------  -------
clickgraph healthy 0.5.0
```

**Example:**
```bash
curl http://localhost:8080/health
```

---

## Error Handling

### Error Response Format

All errors return JSON with an `error` field:

```json
{
  "error": "Error message describing what went wrong"
}
```

### HTTP Status Codes

| Code | Meaning | Example |
|------|---------|---------|
| 200 | Success | Query executed successfully |
| 400 | Bad Request | Invalid Cypher syntax |
| 404 | Not Found | Schema not found |
| 500 | Internal Error | ClickHouse connection failed |

### Common Errors

**Invalid Cypher:**
```json
{
  "error": "Failed to parse Cypher query: Syntax error near 'RETURN'"
}
```

**Schema Not Found:**
```json
{
  "error": "Schema 'unknown_schema' not found"
}
```

**ClickHouse Error:**
```json
{
  "error": "Clickhouse Error: bad response: {\"exception\": \"Code: 47. DB::Exception: Table doesn't exist\"}"
}
```

**Property Not Found:**
```json
{
  "error": "Property 'invalid_prop' not found in node schema 'User'"
}
```

---

## Advanced Features

### Multi-Schema Queries

Use `USE` clause or `schema_name` parameter:

```cypher
-- USE clause (highest priority)
USE social_network;
MATCH (u:User) RETURN u.name;

-- Schema parameter (alternative)
```

```bash
curl -X POST http://localhost:8080/query \
  -d '{"query": "MATCH (u:User) RETURN u.name", "schema_name": "social_network"}'
```

### Parameterized Queries

Prevent SQL injection and enable query plan caching:

```cypher
MATCH (u:User) WHERE u.email = $email AND u.age > $minAge
RETURN u.name, u.country
```

```bash
curl -X POST http://localhost:8080/query \
  -d '{
    "query": "MATCH (u:User) WHERE u.email = $email AND u.age > $minAge RETURN u.name",
    "parameters": {"email": "alice@example.com", "minAge": 25}
  }'
```

### Multi-Tenancy

Use view parameters for tenant isolation:

```bash
curl -X POST http://localhost:8080/query \
  -d '{
    "query": "MATCH (u:User) RETURN u.name",
    "view_parameters": {"tenant_id": "acme_corp"}
  }'
```

See [Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md) for details.

### RBAC with SET ROLE

Use ClickHouse's RBAC system:

```bash
curl -X POST http://localhost:8080/query \
  -d '{
    "query": "MATCH (u:User) RETURN u.name",
    "role": "analyst_role"
  }'
```

Requires database-managed users with granted roles. See [Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md).

---

## Performance Tips

1. **Use Parameters**: Enables query plan caching
   ```cypher
   WHERE u.id = $id  # Good - cached plan
   WHERE u.id = 123  # Bad - new plan each time
   ```

2. **Enable Query Cache**: Set environment variables
   ```bash
   export QUERY_CACHE_ENABLED=true
   export QUERY_CACHE_MAX_ENTRIES=1000
   ```

3. **Check Performance Headers**: Monitor timing
   ```bash
   curl -i http://localhost:8080/query -d '{"query":"..."}'
   # Check X-Total-Time-Ms header
   ```

4. **Use SQL-only Mode**: Debug query generation
   ```bash
   curl http://localhost:8080/query -d '{"query":"...", "sql_only": true}'
   ```

See [Performance & Query Optimization](Performance-Query-Optimization.md) for more details.

---

## Neo4j Bolt Protocol

✅ **Status**: Fully functional - Bolt Protocol 5.8 complete with all E2E tests passing

**Connection Details**:
- Protocol: Bolt v5.8 (backward compatible with 4.4, 5.0-5.7)
- Default Port: 7687
- URI: `bolt://localhost:7687`
- Authentication: NONE, BASIC (username/password)

**Features**:
- ✅ Complete query execution pipeline
- ✅ Multi-database support via session parameter
- ✅ Parameterized queries
- ✅ Result streaming (RECORD messages)
- ✅ Error handling (FAILURE responses)
- ✅ Compatible with Neo4j drivers, cypher-shell, Neo4j Browser

**Production Ready**: Both HTTP and Bolt APIs are fully functional. Choose based on your integration needs.

**Example (Working):**
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session(database="social_network") as session:
    result = session.run("MATCH (u:User) RETURN u.name LIMIT 10")
    for record in result:
        print(record["u.name"])
driver.close()
```

**Use with cypher-shell:**
```bash
cypher-shell -a bolt://localhost:7687 -d social_network
> MATCH (u:User) RETURN u.name LIMIT 5;
```

See [Architecture Internals](Architecture-Internals.md) for Bolt protocol implementation details.

---

## See Also

- [Quick Start Guide](Quick-Start-Guide.md) - Get started quickly
- [Schema Configuration](Schema-Configuration-Advanced.md) - Configure your graph schemas
- [Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md) - Tenant isolation and access control
- [Performance Optimization](Performance-Query-Optimization.md) - Query optimization tips
- [Troubleshooting](Troubleshooting-Guide.md) - Common issues and solutions
- **[Complete API Documentation](../api.md)** - Technical reference with Bolt protocol details
