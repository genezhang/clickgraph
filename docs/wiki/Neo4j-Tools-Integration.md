# Neo4j Tools Integration Guide

Connect Neo4j Browser, Neodash, and other Neo4j-compatible tools to ClickGraph using the Bolt protocol.

## Overview

ClickGraph implements the Neo4j Bolt protocol v5.8, enabling connection from:
- **Neo4j Browser** - Visual query interface and schema exploration
- **Neodash** - Dashboard and visualization builder
- **Neo4j Desktop** - Desktop application
- **Cypher Shell** - Command-line interface
- **Neo4j Drivers** - Python, Java, JavaScript, Go, .NET
- **Graph-Notebook** - Jupyter notebook visualization (see [Graph-Notebook Compatibility Guide](Graph-Notebook-Compatibility.md))

> **Note**: For AWS graph-notebook and Jupyter visualization, see the dedicated **[Graph-Notebook Compatibility Guide](Graph-Notebook-Compatibility.md)** which includes setup instructions, compatibility mode configuration, and troubleshooting.

## Quick Start

### 1. Start ClickGraph with Bolt Enabled

```bash
# Load your graph schema
export GRAPH_CONFIG_PATH="schemas/examples/social_network.yaml"

# Start server (Bolt protocol enabled by default)
cargo run --release --bin clickgraph

# Or with custom ports
cargo run --release --bin clickgraph -- --http-port 8080 --bolt-port 7687
```

Default ports:
- **HTTP**: 8080
- **Bolt**: 7687 (Neo4j compatible)

### 2. Connect Neo4j Browser

1. Open Neo4j Browser
2. Connect to: `bolt://localhost:7687`
3. Username: `neo4j`
4. Password: `password` (or any password)

### 3. Explore Schema

Once connected, try these procedures:

```cypher
// Get all node labels
CALL db.labels()

// Get all relationship types
CALL db.relationshipTypes()

// Get all property keys
CALL db.propertyKeys()

// Get database info
CALL dbms.components()
```

## Schema Selection

### Understanding Bolt vs HTTP

**HTTP (Stateless):**
- Each request can specify a different schema via `schema_name` parameter
- Example: `{"query": "MATCH (n) RETURN n", "schema_name": "ldbc_snb"}`

**Bolt (Stateful Connection):**
- Connection is bound to ONE schema for its lifetime
- Schema determined at connection time (not per query)
- Cannot switch schemas mid-connection (must disconnect and reconnect)

### How Schema Selection Works

ClickGraph determines which schema to use in this priority order:

#### Option 1: Explicit Default in Config (Recommended)

Specify `default_schema` in your YAML configuration:

```yaml
default_schema: social_benchmark  # Tools will use this schema

schemas:
  - name: social_benchmark
    nodes:
      - name: User
        # ...
```

**When to use:** Single-schema deployments, production environments

#### Option 2: First Loaded Schema

If no `default_schema` specified, ClickGraph uses the first schema in the config file:

```yaml
schemas:
  - name: ldbc_snb          # This becomes default (first in list)
    nodes:
      - name: Person
        # ...
  - name: social_benchmark  # Available but not default
    # ...
```

#### Option 3: Database Parameter (Currently Not Used by Tools)

ClickGraph can extract database from Bolt HELLO/LOGON messages:

```
HELLO {"database": "ldbc_snb", ...}
```

**Note:** Current Neo4j drivers (tested with Python v6.0.3) do **not** send database
parameter in Bolt 5.x. They use connection pooling internally without communicating
the database name in the protocol.

### Multi-Schema Deployments

#### For HTTP API

Load multiple schemas and select per-request:

```yaml
default_schema: social_benchmark

schemas:
  - name: social_benchmark
    # ...
  - name: ldbc_snb
    # ...
  - name: fraud_detection
    # ...
```

HTTP requests can specify any schema:
```bash
curl -X POST http://localhost:8080/query \
  -d '{"query":"MATCH (n) RETURN n", "schema_name":"ldbc_snb"}'
```

#### For Neo4j Tools (Bolt)

**Current behavior:** Tools will use the `default_schema` or first schema in config.

**Workaround for testing different schemas:**

Start separate ClickGraph instances:

```bash
# Terminal 1: Social network on port 7687
export GRAPH_CONFIG_PATH="schemas/examples/social_network.yaml"
cargo run -- --bolt-port 7687

# Terminal 2: LDBC on port 7688
export GRAPH_CONFIG_PATH="schemas/ldbc_snb.yaml"
cargo run -- --bolt-port 7688
```

Connect Neo4j Browser to different ports to access different schemas.

## Available Procedures

### Schema Metadata Procedures

#### db.labels()

Returns all node labels in the current schema.

```cypher
CALL db.labels()
```

**Returns:**
```
╒═══════════════════════════════╕
│ label                         │
╞═══════════════════════════════╡
│ "User"                        │
├───────────────────────────────┤
│ "Post"                        │
└───────────────────────────────┘
```

#### db.relationshipTypes()

Returns all relationship types in the current schema.

```cypher
CALL db.relationshipTypes()
```

**Returns:**
```
╒═══════════════════════════════╕
│ relationshipType              │
╞═══════════════════════════════╡
│ "FOLLOWS"                     │
├───────────────────────────────┤
│ "AUTHORED"                    │
├───────────────────────────────┤
│ "LIKED"                       │
└───────────────────────────────┘
```

#### db.propertyKeys()

Returns all unique property keys across nodes and relationships.

```cypher
CALL db.propertyKeys()
```

**Returns:**
```
╒═══════════════════════════════╕
│ propertyKey                   │
╞═══════════════════════════════╡
│ "name"                        │
├───────────────────────────────┤
│ "email"                       │
├───────────────────────────────┤
│ "created_at"                  │
└───────────────────────────────┘
```

#### dbms.components()

Returns ClickGraph version and edition information.

```cypher
CALL dbms.components()
```

**Returns:**
```
╒════════════╤═══════════╤═════════════╕
│ name       │ versions  │ edition     │
╞════════════╪═══════════╪═════════════╡
│ ClickGraph │ ["0.6.1"] │ "community" │
└────────────┴───────────┴─────────────┘
```

### Graph Algorithm Procedures

#### pagerank()

Computes PageRank centrality scores for nodes.

```cypher
CALL pagerank(
  node_label='User',
  relationship_type='FOLLOWS',
  max_iterations=20
) RETURN node_id, rank
```

See [Cypher Language Reference - Graph Algorithms](Cypher-Language-Reference.md#graph-algorithms) for details.

## Troubleshooting

### "Connection Refused"

**Symptom:** Cannot connect to `bolt://localhost:7687`

**Solutions:**
1. Verify ClickGraph is running: `ps aux | grep clickgraph`
2. Check Bolt port is correct: `--bolt-port 7687` (default)
3. Check firewall: `netstat -an | grep 7687`

### "Empty Database" in Neo4j Browser

**Symptom:** Browser connects but shows no schema or data

**Cause:** No schema loaded or wrong schema selected

**Solutions:**

1. **Verify schema is loaded:**
   ```bash
   # Check server logs for schema loading
   tail -f clickgraph.log | grep -i schema
   
   # Should see: "Loaded graph schema: social_benchmark"
   ```

2. **Check default schema:**
   ```cypher
   CALL dbms.components()  -- Verify server is responding
   CALL db.labels()        -- Check if labels appear
   ```

3. **Set explicit default schema:**
   ```yaml
   # In your schema YAML
   default_schema: social_benchmark  # Add this line at top
   
   schemas:
     - name: social_benchmark
       # ...
   ```

4. **Use single-schema config:**
   ```bash
   # Load only the schema you want to explore
   export GRAPH_CONFIG_PATH="schemas/examples/social_network.yaml"
   cargo run
   ```

### "Schema Not Found" Errors

**Symptom:** Procedures return "Schema not found: xxx"

**Cause:** Requested schema doesn't exist or isn't loaded

**Solutions:**

1. **List loaded schemas:** Check server startup logs
   ```bash
   grep "Loaded graph schema" clickgraph.log
   ```

2. **Verify schema name** matches config:
   ```yaml
   schemas:
     - name: social_benchmark  # Must match exactly
   ```

3. **Check YAML syntax:**
   ```bash
   # Validate YAML
   python3 -c "import yaml; yaml.safe_load(open('schemas/your_schema.yaml'))"
   ```

### Procedures Not Working

**Symptom:** `CALL db.labels()` returns error

**Cause:** Procedure execution not wired up (pre-v0.6.1)

**Solution:** Upgrade to ClickGraph v0.6.1 or later

### Authentication Issues

**Current behavior:** ClickGraph accepts any username/password for Bolt connections.

Future versions may add configurable authentication.

## Performance Notes

### Schema Metadata Procedures

All schema metadata procedures (`db.labels()`, `db.relationshipTypes()`, etc.) execute against in-memory schema metadata:

- **Latency:** < 5ms per call
- **No ClickHouse queries:** Procedures read from loaded schema, not database
- **Safe for frequent calls:** Suitable for UI autocomplete, schema exploration

### Regular Queries

Regular Cypher queries execute as ClickHouse SQL:

- **Latency:** Depends on query complexity and data size
- **Query planning:** ~10-50ms
- **Execution:** Delegated to ClickHouse

## Advanced Configuration

### Custom Ports

```bash
# HTTP on 8081, Bolt on 7688
cargo run -- --http-port 8081 --bolt-port 7688
```

### Disable Bolt Protocol

```bash
# Only HTTP API (no Bolt)
cargo run -- --disable-bolt
```

### Connection Limits

Currently: Unlimited concurrent Bolt connections

Future: Configurable connection pool limits

## Tool-Specific Notes

### Neo4j Browser

- ✅ Connection works
- ✅ Schema exploration via procedures
- ✅ Query execution
- ✅ Session commands (`CALL sys.set()`, `CALL dbms.setConfigValue()`)
- ⚠️ Some visualization features may not work (depends on query results format)

#### Session Commands for Multi-Tenancy

Neo4j Browser cannot pass custom parameters (like `tenant_id`) in Bolt RUN messages.
Use session commands to set parameters that persist for the duration of the browser session:

```cypher
// Set tenant_id for multi-tenancy
CALL sys.set('tenant_id', '1234')

// Browser-friendly alternative (same effect)
CALL dbms.setConfigValue('tenant_id', '1234')

// Arbitrary session parameters
CALL sys.set('custom_param', 'value')
```

After setting `tenant_id`, all subsequent queries in that browser session will use it
for parameterized view resolution. The setting persists until you set a new value or
disconnect.

**Example: Multi-Tenant Workflow in Neo4j Browser**

```cypher
// Step 1: Set tenant context
CALL sys.set('tenant_id', 'acme')

// Step 2: Query — only returns data for tenant 'acme'
MATCH (u:User) RETURN u.name, u.email

// Step 3: Switch to another tenant
CALL sys.set('tenant_id', 'globex')

// Step 4: Same query — now returns data for tenant 'globex'
MATCH (u:User) RETURN u.name, u.email
```

This requires a parameterized view in ClickHouse and a schema that references it:

```yaml
# Schema referencing a parameterized view
nodes:
  - label: User
    table_name: "my_users_by_tenant"
    id_column: user_id
    view_parameters: ["tenant_id"]
    properties:
      - { name: name, column: name }
      - { name: email, column: email }
```

The ClickHouse parameterized view filters by `tenant_id`:

```sql
CREATE VIEW my_users_by_tenant AS
  SELECT * FROM users WHERE tenant_id = {tenant_id:String}
```

**Note on types:** ClickHouse automatically coerces `tenant_id` from String to Integer
if the underlying column is an integer type.

**Note:** The `EXPLAIN` keyword is silently handled — browser autocomplete probes
won't produce error messages.

### Neodash

- ✅ Connection works
- ✅ Schema autocomplete from procedures
- ✅ Dashboard creation
- ✅ Visualization widgets

### Cypher Shell

```bash
cypher-shell -a bolt://localhost:7687 -u neo4j -p password

# Once connected
neo4j@neo4j> CALL db.labels();
neo4j@neo4j> MATCH (n) RETURN n LIMIT 10;
```

### Neo4j Desktop

Add ClickGraph as a remote database:
1. Settings → Add → Remote Connection
2. URL: `bolt://localhost:7687`
3. Username: `neo4j`
4. Password: `password`

## Limitations

### Current Limitations

1. **Read-Only:** Only `MATCH` and `RETURN` queries supported (no `CREATE`, `SET`, `DELETE`, `MERGE`)
2. **Single Schema per Connection:** Bolt connections bound to one schema (cannot switch with `USE` clause)
3. **No Transaction Support:** `BEGIN`, `COMMIT`, `ROLLBACK` not implemented
4. **Limited Procedure Set:** Only 5 procedures currently (4 schema + 1 algorithm)

### Not Supported

- ❌ Write operations
- ❌ Schema modifications (`CREATE INDEX`, `CREATE CONSTRAINT`)
- ❌ User management
- ❌ Role-based access control (planned for future)

## See Also

- [Cypher Language Reference](Cypher-Language-Reference.md) - Query syntax
- [API Reference - HTTP](API-Reference-HTTP.md) - HTTP endpoint documentation
- [Quick Start Guide](Quick-Start-Guide.md) - Getting started
- [Troubleshooting Guide](Troubleshooting-Guide.md) - Common issues
