# ClickGraph API Documentation

ClickGraph provides two API interfaces: 
- **HTTP REST API** ✅ **Fully functional** - Recommended for production use
- **Neo4j Bolt Protocol 5.8** ✅ **Fully functional** - Compatible with Neo4j drivers

> **📌 Recommendation**: Both APIs are production-ready. Use HTTP API for simple integrations or Bolt protocol for Neo4j ecosystem compatibility (Neo4j Browser, cypher-shell, official drivers).

## HTTP REST API

### Base URL
```
http://localhost:8080
```

### Authentication
Currently no authentication required for HTTP API.

### Endpoints

#### POST /query
Execute a Cypher query and return results.

**Request Format:**
```http
POST /query
Content-Type: application/json

{
  "query": "MATCH (n) RETURN n LIMIT 10",
  "parameters": {},
  "schema_name": "my_graph"  // Optional: specify which graph schema to use (defaults to "default")
}
```

**Parameters:**
- `query` (string, required): Cypher query to execute
  - Supports `RETURN DISTINCT` for de-duplicating results ✅ **[ADDED: v0.5.1]**
  - Use when multiple graph paths lead to the same node (e.g., friend-of-friend queries)
  - Example: `MATCH (a)-[:FOLLOWS]->(f)-[:FOLLOWS]->(fof) RETURN DISTINCT fof.name`
- `format` (string, optional): Output format. One of `JSONEachRow` (default), `Pretty`, `PrettyCompact`, `Csv`, `CSVWithNames`, `Graph`
  - `Graph`: Returns structured `{ nodes, edges, stats }` response with deduplicated graph objects. See [Graph Format](#graph-format) below.
- `parameters` (object, optional): Query parameters for parameterized queries ✅ **[COMPLETED: Nov 10, 2025]**
  - Supports all JSON data types: String, Int, Float, Bool, Array, Null
  - Use `$paramName` syntax in queries (e.g., `WHERE n.age >= $minAge`)
  - SQL injection prevention built-in
- `role` (string, optional): ClickHouse RBAC role name for query execution ✅ **[ADDED: v0.5.1]**
  - Uses role-based connection pools for optimal performance
  - No `SET ROLE` overhead on query execution
  - Example: `"analyst"`, `"admin"`
- `schema_name` (string, optional): Graph schema/database name to use for this query. Defaults to `"default"`. Enables multi-database support for queries. **Note**: The `USE` clause in the query itself takes precedence over this parameter. See [Default Schema Behavior](#default-schema-behavior) for details on how the default is determined.
- `max_inferred_types` (integer, optional): Maximum number of relationship types to infer for generic patterns ✅ **[ADDED: v0.6.1]**
  - Default: 5 (prevents excessive UNION expansion)
  - Recommended for GraphRAG: 10-20 (complex knowledge graphs with many relationship types)
  - Example: `{"query": "MATCH (n)-[*1..3]->(m) RETURN m", "max_inferred_types": 15}`
  - Use case: Override when your schema has more than 5 relationship types between nodes

**Response Format:**
```http
200 OK
Content-Type: application/json
X-Query-Total-Time: 15.234ms
X-Query-Parse-Time: 1.234ms
X-Query-Planning-Time: 5.678ms
X-Query-Render-Time: 0.123ms
X-Query-SQL-Gen-Time: 0.456ms
X-Query-Execution-Time: 7.743ms
X-Query-Type: read
X-Query-SQL-Count: 1

{
  "results": [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
  ]
}
```

**Response Fields:**
- `results` (array): Array of result rows, each row is an object with column names as keys
  - Columns use **qualified property names** (e.g., `"u.name"`, `"u.age"`) matching Neo4j behavior
  - Example: `RETURN u.name, u.age` returns columns as `"u.name"` and `"u.age"`
  - Use `AS` for custom aliases: `RETURN u.name AS userName` returns column as `"userName"`

**Note**: The response format changed in November 2025 to wrap results in a `{"results": [...]}` object for consistency with Neo4j format. Previously returned a bare array `[...]`.

### Performance Metrics Headers

All successful query responses include performance timing headers for monitoring and optimization:

- `X-Query-Total-Time`: Total end-to-end query processing time
- `X-Query-Parse-Time`: Time spent parsing Cypher query
- `X-Query-Planning-Time`: Time spent planning the query execution
- `X-Query-Render-Time`: Time spent rendering the query plan
- `X-Query-SQL-Gen-Time`: Time spent generating ClickHouse SQL
- `X-Query-Execution-Time`: Time spent executing SQL in ClickHouse
- `X-Query-Type`: Query type (`read`, `write`, `call`, `ddl`)
- `X-Query-SQL-Count`: Number of SQL queries generated

**Error Response:**
```http
400 Bad Request
Content-Type: application/json

{
  "error": {
    "code": "CYPHER_SYNTAX_ERROR",
    "message": "Syntax error at line 1, column 5",
    "details": "Unexpected token 'INVALID'"
  }
}
```

#### POST /query/sql
Generate SQL from Cypher query without executing it. ✅ **Production-Ready**

This endpoint translates Cypher queries to ClickHouse SQL and returns the generated SQL statements as an array, along with query metadata. Useful for:
- Debugging query translation
- Integrating ClickGraph translation into other tools
- Understanding how Cypher patterns map to SQL
- Testing RBAC role handling (includes `SET ROLE` statements when applicable)

**Request Format:**
```http
POST /query/sql
Content-Type: application/json

{
  "query": "MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age",
  "parameters": {"minAge": 25},
  "role": "analyst",
  "schema_name": "default",
  "include_plan": false
}
```

**Parameters:**
- `query` (string, required): Cypher query to translate
- `parameters` (object, optional): Query parameters (shown in SQL as `$paramName` placeholders)
- `view_parameters` (object, optional): ClickHouse view parameters (ClickHouse-specific)
- `role` (string, optional): ClickHouse RBAC role name (adds `SET ROLE` to SQL array)
- `schema_name` (string, optional): Graph schema to use (defaults to `"default"`)
- `target_database` (string, optional): Target SQL dialect (defaults to `"clickhouse"`)
- `include_plan` (boolean, optional): Include logical plan in response (defaults to `false`)

**Response Format:**
```http
200 OK
Content-Type: application/json

{
  "cypher_query": "MATCH (u:User) WHERE u.age > $minAge RETURN u.name, u.age",
  "target_database": "clickhouse",
  "sql": [
    "SET ROLE analyst",
    "SELECT \n      u.name AS \"u.name\", \n      u.age AS \"u.age\"\nFROM users AS u\nWHERE u.age > $minAge"
  ],
  "parameters": {
    "minAge": 25
  },
  "role": "analyst",
  "metadata": {
    "query_type": "read",
    "cache_status": "HIT",
    "parse_time_ms": 1.234,
    "planning_time_ms": 5.678,
    "sql_generation_time_ms": 0.456,
    "total_time_ms": 7.368
  },
  "logical_plan": null
}
```

**Response Fields:**
- `cypher_query` (string): Original Cypher query
- `target_database` (string): Target SQL dialect (`"clickhouse"`, `"postgresql"`, etc.)
- `sql` (array of strings): Generated SQL statements to execute in order
  - Example: `["SET ROLE analyst", "SELECT ..."]` when role is specified
  - Example: `["SELECT ..."]` when no role specified
  - Future: May include multi-statement queries like `["CREATE TEMP TABLE ...", "SELECT ...", "DROP TABLE ..."]`
- `parameters` (object, optional): Query parameters (if provided in request)
- `view_parameters` (object, optional): View parameters (if provided in request)
- `role` (string, optional): Role name (if provided in request)
- `metadata` (object): Query translation metadata
  - `query_type` (string): Type of query (`"read"`, `"write"`, `"call"`, `"ddl"`)
  - `cache_status` (string): Whether SQL was cached (`"HIT"`, `"MISS"`)
  - `parse_time_ms` (number): Time to parse Cypher
  - `planning_time_ms` (number): Time to plan query
  - `sql_generation_time_ms` (number): Time to generate SQL
  - `total_time_ms` (number): Total translation time
- `logical_plan` (string, optional): Logical plan representation (when `include_plan=true`)

**Example Usage:**

```bash
# Basic SQL generation
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.name LIMIT 10"}'

# With role (includes SET ROLE in SQL array)
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name",
    "role": "analyst"
  }'

# With parameters and logical plan
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > $minAge RETURN u.name",
    "parameters": {"minAge": 25},
    "include_plan": true
  }'
```

**Python Example:**
```python
import requests

response = requests.post('http://localhost:8080/query/sql', json={
    'query': 'MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = $name RETURN f.name',
    'parameters': {'name': 'Alice'},
    'role': 'analyst',
    'include_plan': True
})

data = response.json()
print("SQL Statements:")
for i, stmt in enumerate(data['sql'], 1):
    print(f"{i}. {stmt}")

print(f"\nTranslation time: {data['metadata']['total_time_ms']:.2f}ms")
print(f"Cache: {data['metadata']['cache_status']}")
```

**Error Response:**
```http
400 Bad Request
Content-Type: application/json

{
  "error": "Cypher syntax error",
  "details": {
    "query": "MATCH (u:User WHERE u.age > 25",
    "position": 15,
    "message": "Expected ')' but found 'WHERE'"
  }
}
```

**Notes:**
- SQL generation is **very fast** (typically <10ms) because it doesn't execute queries
- Results are **cached** - identical queries return cached SQL instantly
- The `sql` array format allows representing complex multi-statement operations
- When `role` is specified, `SET ROLE` is included in the SQL array for visibility, but actual query execution uses role-based connection pools (no SET ROLE overhead)
- Parameter placeholders (`$paramName`) are NOT substituted in the generated SQL - they remain as placeholders for you to substitute when executing

#### GET /schemas
List all available graph schemas.

**Request Format:**
```http
GET /schemas
```

**Response Format:**
```http
200 OK
Content-Type: application/json

{
  "schemas": [
    {
      "name": "default",
      "node_count": 3,
      "edge_count": 2
    },
    {
      "name": "social_network",
      "node_count": 5,
      "edge_count": 4
    }
  ]
}
```

**Response Fields:**
- `schemas` (array): List of available schemas
  - `name` (string): Schema identifier used in queries
  - `node_count` (integer): Number of node types defined in this schema
  - `edge_count` (integer): Number of edge types defined in this schema

**Example:**
```bash
curl http://localhost:8080/schemas
```

#### GET /schemas/{name}
Get detailed information about a specific schema.

**Request Format:**
```http
GET /schemas/{name}
```

**Path Parameters:**
- `name` (string, required): The schema name to retrieve

**Response Format:**
```http
200 OK
Content-Type: application/json

{
  "schema_name": "social_network",
  "node_types": 3,
  "edge_types": 2,
  "nodes": ["User", "Post", "Comment"],
  "edges": ["FOLLOWS", "AUTHORED"]
}
```

**Response Fields:**
- `schema_name` (string): The requested schema name
- `node_types` (integer): Number of node type labels defined
- `edge_types` (integer): Number of edge types defined
- `nodes` (array): List of node labels in this schema
- `edges` (array): List of edge types in this schema

**Error Response:**
```http
404 Not Found
Content-Type: application/json

{
  "error": "Schema 'unknown_schema' not found"
}
```

**Example:**
```bash
curl http://localhost:8080/schemas/social_network
```

#### POST /schemas/load
Load a new graph schema from YAML content at runtime.

**Request Format:**
```http
POST /schemas/load
Content-Type: application/json

{
  "schema_name": "social_network",
  "config_content": "name: social_network\ngraph_schema:\n  nodes: ...",
  "validate_schema": true
}
```

**Parameters:**
- `schema_name` (string, required): Name to register the schema under. This name will be used in `USE` clauses and `schema_name` query parameters.
- `config_content` (string, required): Full YAML schema configuration as a string. Supports all schema features including auto-discovery (`auto_discover_columns: true`).
- `validate_schema` (boolean, optional): Whether to validate that tables and columns exist in ClickHouse. Defaults to `false`.

**Response Format:**
```http
200 OK
Content-Type: application/json

{
  "message": "Schema 'social_network' loaded successfully",
  "schema_name": "social_network"
}
```

**Error Response:**
```http
500 Internal Server Error
Content-Type: application/json

{
  "error": "Failed to load schema: File not found: /path/to/social_network.yaml"
}
```

**Notes:**
- Loaded schemas are available immediately for queries
- The schema name in the YAML file is **ignored**; the `schema_name` parameter determines the registration name
- Loading a schema does not affect the "default" schema (set at startup via `GRAPH_CONFIG_PATH`)
- Multiple schemas can coexist and be queried using `USE <schema_name>` or the `schema_name` parameter
- Schema validation (`validate_schema: true`) checks that referenced tables and columns exist in ClickHouse
- Supports auto-discovery: Set `auto_discover_columns: true` in node/edge definitions to automatically query ClickHouse `system.columns` for property mappings

**Example (Loading from File):**
```bash
# Read YAML file and send as string
curl -X POST http://localhost:8080/schemas/load \
  -H "Content-Type: application/json" \
  -d "$(jq -Rs '{schema_name: "ecommerce", config_content: ., validate_schema: true}' schemas/ecommerce.yaml)"
```

**Example (Direct YAML Content):**
```bash
curl -X POST http://localhost:8080/schemas/load \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "ecommerce",
    "config_content": "name: ecommerce\ngraph_schema:\n  nodes:\n    - label: Product\n      table: products\n      node_id: product_id\n      property_mappings:\n        name: product_name\n",
    "validate_schema": true
  }'
```

**PowerShell Example (Windows):**
```powershell
# Load YAML from file
$yamlContent = Get-Content "schemas/ecommerce.yaml" -Raw
$body = @{
    schema_name = "ecommerce"
    config_content = $yamlContent
    validate_schema = $true
} | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8080/schemas/load" `
  -ContentType "application/json" `
  -Body $body
```

#### GET /health
Health check endpoint for monitoring.

**Request Format:**
```http
GET /health
```

**Response Format:**
```http
200 OK
Content-Type: text/plain

OK
```

**Example:**
```bash
curl http://localhost:8080/health
```

### Examples

#### Basic Node Query
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name, u.age LIMIT 5"
  }'
```

#### Edge Traversal
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(friend:User) WHERE u.name = $userName RETURN friend.name",
    "parameters": {"userName": "Alice"}
  }'
```

#### Aggregation Query
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(friend) RETURN u.name, count(friend) as friend_count ORDER BY friend_count DESC"
  }'
```

### Graph Format

The `Graph` output format returns a structured response with deduplicated nodes and edges, along with query performance stats. This is useful for graph visualization, GraphRAG pipelines, and any application that needs typed graph objects rather than flat rows.

**Request:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 5",
    "format": "Graph"
  }'
```

**Response:**
```json
{
  "nodes": [
    {
      "element_id": "User:1",
      "labels": ["User"],
      "properties": {"name": "Alice", "age": 30}
    },
    {
      "element_id": "User:2",
      "labels": ["User"],
      "properties": {"name": "Bob", "age": 25}
    }
  ],
  "edges": [
    {
      "element_id": "FOLLOWS:1->2",
      "rel_type": "FOLLOWS",
      "start_node_element_id": "User:1",
      "end_node_element_id": "User:2",
      "properties": {}
    }
  ],
  "stats": {
    "total_time_ms": 5.1,
    "parse_time_ms": 0.3,
    "planning_time_ms": 1.2,
    "render_time_ms": 0.1,
    "sql_generation_time_ms": 0.2,
    "execution_time_ms": 3.3,
    "query_type": "read",
    "result_rows": 5
  }
}
```

**Response Fields:**
- `nodes` (array): Deduplicated graph nodes. Each node has:
  - `element_id` (string): Unique identifier (format: `Label:id`)
  - `labels` (array): Node labels (e.g., `["User"]`)
  - `properties` (object): Node properties
- `edges` (array): Deduplicated graph edges. Each edge has:
  - `element_id` (string): Unique identifier (format: `Type:from->to`)
  - `rel_type` (string): Relationship type (e.g., `"FOLLOWS"`)
  - `start_node_element_id` (string): Source node element_id
  - `end_node_element_id` (string): Target node element_id
  - `properties` (object): Relationship properties
- `stats` (object): Query performance breakdown in milliseconds

**Notes:**
- Nodes and edges are deduplicated by `element_id` — the same node appearing in multiple result rows is returned once
- Scalar-only queries (e.g., `RETURN u.name`) return empty `nodes` and `edges` arrays
- The `Graph` format requires the full query planning pipeline (cache is bypassed) since it needs type metadata to classify return items as nodes vs relationships
- Uses the same element_id format as the Bolt protocol for consistency

**Python Example:**
```python
import requests

response = requests.post('http://localhost:8080/query', json={
    'query': 'MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN u, r, f LIMIT 10',
    'format': 'Graph'
})

data = response.json()
print(f"Nodes: {len(data['nodes'])}, Edges: {len(data['edges'])}")
print(f"Query time: {data['stats']['total_time_ms']:.1f}ms")

for node in data['nodes']:
    print(f"  {node['element_id']}: {node['properties']}")
```

### Parameterized Queries

✅ **Fully supported** (Nov 10, 2025) - Parameter substitution with SQL injection prevention

#### All Parameter Types

**String parameters:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.name = $userName RETURN u",
    "parameters": {"userName": "Alice"}
  }'
```

**Numeric parameters:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age >= $minAge RETURN u.name, u.age",
    "parameters": {"minAge": 25}
  }'
```

**Multiple parameters:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age >= $minAge AND u.age <= $maxAge RETURN u.name, u.age",
    "parameters": {"minAge": 25, "maxAge": 40}
  }'
```

**Array parameters:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.city IN $cities RETURN u.name, u.city",
    "parameters": {"cities": ["New York", "San Francisco", "Seattle"]}
  }'
```

**Boolean parameters:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.is_active = $active RETURN u.name",
    "parameters": {"active": true}
  }'
```

**PowerShell example:**
```powershell
$query = @{
  query = "MATCH (u:User) WHERE u.age >= `$minAge RETURN u.name, u.age"
  parameters = @{
    minAge = 25
  }
} | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8080/query" `
  -ContentType "application/json" `
  -Body $query
```

**Security**: All parameters are properly escaped to prevent SQL injection attacks.

## Neo4j Bolt Protocol

✅ **Production Ready**: Bolt Protocol 5.8 fully implemented with complete query execution, authentication, and multi-database support. All E2E tests passing (4/4). Compatible with Neo4j official drivers, cypher-shell, and Neo4j Browser.

### Connection Details
- **Protocol**: Bolt v5.8 (backward compatible with 4.4, 5.0-5.7)
- **Default Port**: 7687
- **URI Format**: `bolt://localhost:7687`
- **Status**: ✅ **Fully Functional** - All features working

### Multi-Database Support
ClickGraph supports Neo4j 4.0+ multi-database selection via the Bolt protocol:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")

# Query specific schema
with driver.session(database="social_network") as session:
    result = session.run("MATCH (u:User) RETURN u.name LIMIT 10")
    for record in result:
        print(record["u.name"])
    
# Use default schema
with driver.session() as session:  # Defaults to "default" schema
    result = session.run("MATCH (p:Product) RETURN p.name LIMIT 10")
    for record in result:
        print(record["p.name"])
```

The `database` parameter in the session is sent via the Bolt HELLO message and maps to ClickGraph's `schema_name` configuration. This provides the same multi-schema capability as the HTTP API's `schema_name` parameter.

**Note**: The `USE` clause in Cypher queries takes precedence over the session database parameter.

## USE Clause for Database Selection

ClickGraph supports the `USE` clause in Cypher queries for database/schema selection, following Neo4j 4.0+ conventions. This provides the highest-priority method for database selection.

### Syntax

```cypher
USE database_name
MATCH (n) RETURN n
```

### Database Selection Precedence

ClickGraph supports three ways to select a database, with the following precedence order (highest to lowest):

1. **USE clause in query** (highest priority)
2. **Session/request parameter** (HTTP: `schema_name`, Bolt: `database`)
3. **Default schema** ("default")

### Default Schema Behavior

When no `USE` clause or `schema_name` parameter is provided, ClickGraph uses the "default" schema. How the default is determined depends on your schema configuration:

| Configuration | Default Schema |
|--------------|----------------|
| **Single schema file** | The schema is automatically the default |
| **Multi-schema with `default_schema`** | The named schema becomes the default |
| **Multi-schema without `default_schema`** | The **first schema** in the list becomes the default |

This means:
- **Most common case** (single schema): Just load your schema file - no configuration needed
- **Multiple schemas**: Either specify `default_schema` explicitly, or the first schema is used
- **Override per-request**: Use `schema_name` parameter or `USE` clause

For full schema configuration details, see [Schema Reference - Default Schema Behavior](schema-reference.md#default-schema-behavior).

### Examples

#### Simple Database Selection

```cypher
USE social_network
MATCH (u:User) RETURN u.name LIMIT 10
```

#### Qualified Database Names

```cypher
USE neo4j.social_network
MATCH (u:User)-[:FOLLOWS]->(friend) 
RETURN u.name, collect(friend.name) AS friends
```

#### USE with Complex Queries

```cypher
USE ecommerce
MATCH (p:Product)-[:IN_CATEGORY]->(c:Category)
WHERE c.name = 'Electronics'
RETURN p.name, p.price
ORDER BY p.price DESC
LIMIT 20
```

#### Case Insensitivity

```cypher
-- All of these work identically
USE social_network MATCH (u:User) RETURN count(u)
use social_network MATCH (u:User) RETURN count(u)
Use social_network MATCH (u:User) RETURN count(u)
```

### HTTP API with USE Clause

The `USE` clause overrides the `schema_name` parameter:

```bash
# The USE clause will select 'social_network', not 'ecommerce'
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE social_network MATCH (u:User) RETURN u.name",
    "schema_name": "ecommerce"
  }'
```

### Bolt Protocol with USE Clause

The `USE` clause overrides the session database parameter:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")

# USE clause takes precedence over session database
with driver.session(database="ecommerce") as session:
    # This will use 'social_network', not 'ecommerce'
    result = session.run("USE social_network MATCH (u:User) RETURN u.name LIMIT 5")
    for record in result:
        print(record["u.name"])

driver.close()
```

### Authentication

Bolt protocol supports multiple authentication schemes:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")

# The USE clause will select 'social_network', not 'ecommerce'
with driver.session(database="ecommerce") as session:
    result = session.run("USE social_network MATCH (u:User) RETURN u.name")
    for record in result:
        print(record["u.name"])
```

### Best Practices

- **Use `USE` clause** when you need to override database selection within a query
- **Use session/request parameters** for consistent database selection across multiple queries
- **Omit both** to use the default schema

### Authentication
- **Method**: Basic authentication (username/password)
- **Default**: No authentication required

### Supported Operations
- Query execution (RUN, PULL, DISCARD)
- Transaction management (BEGIN, COMMIT, ROLLBACK) 
- Connection management (HELLO, GOODBYE, RESET)
- Result streaming with configurable batch sizes

### Client Examples

#### Python (neo4j-driver)
```python
from neo4j import GraphDatabase

# Connect to ClickGraph
driver = GraphDatabase.driver("bolt://localhost:7687")

def run_query(query, parameters=None):
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return [record.data() for record in result]

# Example queries
users = run_query("MATCH (u:User) RETURN u.name, u.age")
print(users)

# Parameterized query
friends = run_query(
    "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = $name RETURN f.name",
    {"name": "Alice"}
)
print(friends)

driver.close()
```

#### JavaScript (neo4j-driver)
```javascript
const neo4j = require('neo4j-driver');

const driver = neo4j.driver('bolt://localhost:7687');

async function runQuery(query, parameters = {}) {
    const session = driver.session();
    try {
        const result = await session.run(query, parameters);
        return result.records.map(record => record.toObject());
    } finally {
        await session.close();
    }
}

// Example usage
(async () => {
    const users = await runQuery('MATCH (u:User) RETURN u.name, u.age');
    console.log(users);
    
    const friends = await runQuery(
        'MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = $name RETURN f.name',
        { name: 'Alice' }
    );
    console.log(friends);
    
    await driver.close();
})();
```

#### Java (Neo4j Java Driver)
```java
import org.neo4j.driver.*;

public class ClickGraphExample {
    public static void main(String[] args) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687");
        
        try (Session session = driver.session()) {
            // Simple query
            Result result = session.run("MATCH (u:User) RETURN u.name, u.age");
            while (result.hasNext()) {
                Record record = result.next();
                System.out.println(record.get("u.name") + " - " + record.get("u.age"));
            }
            
            // Parameterized query  
            Result friends = session.run(
                "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = $name RETURN f.name",
                Values.parameters("name", "Alice")
            );
            
            while (friends.hasNext()) {
                Record record = friends.next();
                System.out.println("Friend: " + record.get("f.name"));
            }
        }
        
        driver.close();
    }
}
```

#### .NET (Neo4j.Driver)
```csharp
using Neo4j.Driver;

class Program
{
    static async Task Main(string[] args)
    {
        var driver = GraphDatabase.Driver("bolt://localhost:7687");
        
        var session = driver.AsyncSession();
        try
        {
            // Simple query
            var cursor = await session.RunAsync("MATCH (u:User) RETURN u.name, u.age");
            await cursor.ForEachAsync(record =>
            {
                Console.WriteLine($"{record["u.name"]} - {record["u.age"]}");
            });
            
            // Parameterized query
            var friendsCursor = await session.RunAsync(
                "MATCH (u:User)-[:FOLLOWS]->(f:User) WHERE u.name = $name RETURN f.name",
                new { name = "Alice" }
            );
            
            await friendsCursor.ForEachAsync(record =>
            {
                Console.WriteLine($"Friend: {record["f.name"]}");
            });
        }
        finally
        {
            await session.CloseAsync();
            await driver.CloseAsync();
        }
    }
}
```

## Data Types

### Node Format
```json
{
  "id": 123,
  "labels": ["User", "Person"],
  "properties": {
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
  }
}
```

### Edge Format
```json
{
  "id": 456,
  "type": "FOLLOWS", 
  "start": 123,
  "end": 789,
  "properties": {
    "since": "2023-01-15",
    "weight": 0.8
  }
}
```

### Path Format
```json
{
  "length": 2,
  "start": {"id": 123, "labels": ["User"], "properties": {"name": "Alice"}},
  "end": {"id": 789, "labels": ["User"], "properties": {"name": "Charlie"}},
  "nodes": [
    {"id": 123, "labels": ["User"], "properties": {"name": "Alice"}},
    {"id": 456, "labels": ["User"], "properties": {"name": "Bob"}},
    {"id": 789, "labels": ["User"], "properties": {"name": "Charlie"}}
  ],
  "edges": [
    {"id": 111, "type": "FOLLOWS", "start": 123, "end": 456},
    {"id": 222, "type": "FOLLOWS", "start": 456, "end": 789}
  ]
}
```

## Error Codes

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `CYPHER_SYNTAX_ERROR` | Invalid Cypher syntax | 400 |
| `CYPHER_TYPE_ERROR` | Type mismatch in query | 400 |
| `SCHEMA_ERROR` | Graph schema validation error | 400 |
| `CONSTRAINT_VIOLATION` | Data constraint violation | 400 |
| `CLICKHOUSE_ERROR` | ClickHouse execution error | 500 |
| `INTERNAL_ERROR` | Internal server error | 500 |
| `TIMEOUT_ERROR` | Query execution timeout | 500 |

## Performance Considerations

### Query Optimization Tips
1. **Use LIMIT clauses** to avoid large result sets
2. **Create indexes** on frequently queried properties
3. **Use parameters** instead of string concatenation
4. **Optimize traversal depth** for performance
5. **Use EXPLAIN** to analyze query plans

### Bulk Operations
For large data operations, consider:
- Batch multiple queries in transactions
- Use CSV import for initial data loading
- Optimize ClickHouse table structures
- Configure appropriate memory limits

### Connection Management
- **HTTP**: Stateless, no persistent connections needed
- **Bolt**: Use connection pooling for high-throughput applications
- Close connections properly to avoid resource leaks
- Configure appropriate timeouts for long-running queries

## Monitoring & Debugging

### Query Logging
Enable query logging by setting log level to DEBUG:
```bash
RUST_LOG=debug cargo run --bin brahmand
```

### Performance Metrics
Monitor ClickGraph performance through:
- Query execution times in response stats
- ClickHouse query logs and metrics
- System resource usage (CPU, memory)
- Connection counts and throughput

### Health Checks
```bash
# HTTP API health check
curl -f http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "RETURN 1 as health"}'

# Bolt protocol connectivity test (using cypher-shell)
echo "RETURN 1 as health;" | cypher-shell -a bolt://localhost:7687
```


