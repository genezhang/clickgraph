# Troubleshooting Guide

Common issues, solutions, and debugging techniques for ClickGraph.

## Table of Contents
- [Connection Issues](#connection-issues)
- [Schema Loading Issues](#schema-loading-issues)
- [Query Errors](#query-errors)
- [Performance Issues](#performance-issues)
- [Windows-Specific Issues](#windows-specific-issues)
- [Docker Issues](#docker-issues)
- [Debugging Techniques](#debugging-techniques)

---

## Connection Issues

### "Connection refused" to ClickGraph Server

**Symptoms**:
```bash
curl: (7) Failed to connect to localhost port 8080: Connection refused
```

**Causes & Solutions**:

**1. Server not running**
```bash
# Check if server is running
docker-compose ps  # Docker
ps aux | grep clickgraph  # Source build

# Start server
docker-compose up -d  # Docker
cargo run --release --bin clickgraph  # Source
```

**2. Wrong port**
```bash
# Check configured port
docker-compose ps  # See port mappings

# Connect to correct port
curl http://localhost:8080/health  # Default HTTP
curl bolt://localhost:7687  # Default Bolt
```

**3. Server crashed or failed to start**
```bash
# Check logs
docker-compose logs clickgraph  # Docker
# or check terminal output  # Source

# Common errors in logs:
# - "Schema not found" → See Schema Loading Issues
# - "Cannot connect to ClickHouse" → See ClickHouse Connection Issues
```

**4. Firewall blocking**
```bash
# Windows: Allow through firewall
netsh advfirewall firewall add rule name="ClickGraph HTTP" dir=in action=allow protocol=TCP localport=8080
netsh advfirewall firewall add rule name="ClickGraph Bolt" dir=in action=allow protocol=TCP localport=7687

# Linux: Check firewall
sudo ufw status
sudo ufw allow 8080/tcp
sudo ufw allow 7687/tcp
```

---

### "Connection refused" to ClickHouse

**Symptoms**:
```
ERROR: Cannot connect to ClickHouse at http://localhost:8123
```

**Solutions**:

**1. ClickHouse not running**
```bash
# Check if ClickHouse is running
docker ps | grep clickhouse

# Start ClickHouse
docker-compose up -d clickhouse

# Or standalone
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
```

**2. Wrong URL**
```bash
# Check CLICKHOUSE_URL
echo $CLICKHOUSE_URL  # Linux/Mac
echo $env:CLICKHOUSE_URL  # Windows PowerShell

# Correct formats:
export CLICKHOUSE_URL="http://localhost:8123"  # Default
export CLICKHOUSE_URL="http://192.168.1.100:8123"  # Remote
```

**3. Authentication failure**
```bash
# Check credentials
echo $CLICKHOUSE_USER
echo $CLICKHOUSE_PASSWORD

# Default ClickHouse credentials:
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""  # Empty for default user

# Or custom user:
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
```

**4. Test ClickHouse connection**
```bash
# Test directly with curl
curl http://localhost:8123/ping

# Expected: "Ok."

# Test with credentials
curl -u test_user:test_pass http://localhost:8123/ping
```

---

### Neo4j Browser Cannot Connect (Bolt)

**Symptoms**:
```
ServiceUnavailable: Connection to database failed
```

**Solutions**:

**1. Bolt server not enabled**
```bash
# Check if Bolt is running
netstat -an | grep 7687  # Linux/Mac
netstat -an | findstr 7687  # Windows

# If not running, ensure Bolt is not disabled:
cargo run --release --bin clickgraph
# (without --disable-bolt flag)
```

**2. Wrong connection URL**
```
# Correct format in Neo4j Browser:
bolt://localhost:7687

# NOT:
http://localhost:7687  # Wrong protocol
bolt://localhost:8080  # Wrong port
```

**3. Authentication configuration**
```
# In Neo4j Browser:
Username: (leave empty or "none")
Password: (leave empty)
Authentication: None

# Or if using basic auth:
Username: test_user
Password: test_pass
```

**4. Browser version incompatibility**
```
# Neo4j Browser may require specific Bolt versions
# ClickGraph supports Bolt v4.4 and v5.8

# Try with official Neo4j drivers instead:
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687")
```

---

## Schema Loading Issues

### "Schema not found"

**Symptoms**:
```
ERROR: Failed to load graph schema: No such file or directory
```

**Solutions**:

**1. GRAPH_CONFIG_PATH not set**
```bash
# Check environment variable
echo $GRAPH_CONFIG_PATH  # Linux/Mac
echo $env:GRAPH_CONFIG_PATH  # Windows

# Set to schema file location
export GRAPH_CONFIG_PATH="./schemas/demo/users.yaml"  # Linux/Mac
$env:GRAPH_CONFIG_PATH = ".\schemas\demo\users.yaml"  # Windows

# Use absolute path to avoid issues:
export GRAPH_CONFIG_PATH="$(pwd)/schemas/demo/users.yaml"  # Linux/Mac
$env:GRAPH_CONFIG_PATH = "$PWD\schemas\demo\users.yaml"  # Windows
```

**2. Schema file doesn't exist**
```bash
# Verify file exists
ls -la schemas/demo/users.yaml  # Linux/Mac
Get-Item schemas\demo\users.yaml  # Windows

# List available schemas
ls schemas/demo/  # Demo schemas
ls schemas/examples/  # Example schemas
ls benchmarks/schemas/  # Benchmark schemas
```

**3. Invalid YAML syntax**
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('schemas/demo/users.yaml'))"

# Common YAML errors:
# - Incorrect indentation (use 2 or 4 spaces, NOT tabs)
# - Missing colons
# - Unquoted strings with special characters
```

**4. Schema validation errors**
```
# Check ClickGraph logs for specific errors:
[ERROR] Schema validation failed: Missing required field 'id_column'

# Fix schema based on error message
# See: Schema Basics guide
```

---

### "Table not found" Error

**Symptoms**:
```
ERROR: Table 'default.users' doesn't exist
```

**Solutions**:

**1. Table not created**
```bash
# Check if tables exist in ClickHouse
docker exec clickhouse-clickhouse clickhouse-client -q "SHOW TABLES FROM default"

# Create tables (demo data)
docker exec -i clickhouse-clickhouse clickhouse-client < schemas/demo/setup_demo_data.sql
```

**2. Wrong database name**
```bash
# Check CLICKHOUSE_DATABASE
echo $CLICKHOUSE_DATABASE

# Should match database in schema YAML:
# database: "brahmand"  # Schema file
# export CLICKHOUSE_DATABASE="brahmand"  # Environment
```

**3. Wrong table name in schema**
```yaml
# Verify table name in schema matches ClickHouse:
nodes:
  User:
    source_table: users  # Must match actual table name
    
# Check actual table name:
# docker exec clickhouse-clickhouse clickhouse-client -q "SHOW TABLES"
```

**4. Table in different database**
```bash
# List all databases
docker exec clickhouse-clickhouse clickhouse-client -q "SHOW DATABASES"

# List tables in specific database
docker exec clickhouse-clickhouse clickhouse-client -q "SHOW TABLES FROM brahmand"

# Update schema to use correct database:
# Or prefix table name: source_table: "other_db.users"
```

---

## Query Errors

### "Syntax error" in Cypher Query

**Symptoms**:
```
ERROR: Syntax error at line 1, column 15: unexpected token 'WEHRE'
```

**Solutions**:

**1. Typos in keywords**
```cypher
-- ❌ Wrong
MATCH (u:User) WEHRE u.age > 30 RETURN u

-- ✅ Correct
MATCH (u:User) WHERE u.age > 30 RETURN u
```

**2. Missing parentheses or brackets**
```cypher
-- ❌ Wrong (missing parentheses around node)
MATCH u:User RETURN u

-- ✅ Correct
MATCH (u:User) RETURN u

-- ❌ Wrong (missing brackets around relationship)
MATCH (a:User)-:FOLLOWS->(b:User) RETURN a, b

-- ✅ Correct
MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a, b
```

**3. Incorrect string quotes**
```cypher
-- ❌ Wrong (single quotes in JSON/HTTP)
{"query": "MATCH (u:User {name: 'Alice'}) RETURN u"}

-- ✅ Correct (escape quotes or use double quotes)
{"query": "MATCH (u:User {name: \"Alice\"}) RETURN u"}
```

**4. Unsupported Cypher features**
```cypher
-- ❌ Not supported (write operations)
CREATE (u:User {name: 'Alice'})
SET u.age = 30
DELETE u
MERGE (u:User {name: 'Alice'})

-- ✅ Supported (read operations)
MATCH (u:User) RETURN u
MATCH (u:User) WHERE u.name = 'Alice' RETURN u
```

**See**: [Known Limitations](Known-Limitations.md) for complete list

---

### "Property not found" Error

**Symptoms**:
```
ERROR: Property 'name' not found on node type 'User'
```

**Solutions**:

**1. Check property mapping in schema**
```yaml
# Property mappings define Cypher property → ClickHouse column
nodes:
  User:
    property_mappings:
      name: full_name  # Cypher 'name' maps to ClickHouse 'full_name'
      email: email_address
```

```cypher
-- ✅ Use Cypher property name (from schema)
MATCH (u:User) RETURN u.name  -- Queries full_name column

-- ❌ Don't use ClickHouse column name directly
MATCH (u:User) RETURN u.full_name  -- Error!
```

**2. Column doesn't exist in ClickHouse table**
```bash
# Check actual columns
docker exec clickhouse-clickhouse clickhouse-client -q "DESCRIBE TABLE users"

# Add column to ClickHouse:
docker exec clickhouse-clickhouse clickhouse-client -q "ALTER TABLE users ADD COLUMN full_name String"
```

**3. Missing property mapping**
```yaml
# Add mapping to schema YAML:
nodes:
  User:
    property_mappings:
      name: full_name  # Add this mapping
```

---

### "Multi-hop anonymous node" Error

**Symptoms**:
Query with anonymous intermediate nodes in multi-hop patterns generates broken SQL or returns unexpected results.

**Example**:
```cypher
-- ❌ Broken (anonymous intermediate in multi-hop)
MATCH (u1:User)-[:FOLLOWS]->()-[:FOLLOWS]->(u2:User)
WHERE u1.user_id = 1
RETURN u2.name
```

**Root Cause**: SQL generation loses user-provided aliases in nested GraphRel structures.

**Workaround**: Use named intermediate nodes

```cypher
-- ✅ Works (named intermediate)
MATCH (u1:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(u2:User)
WHERE u1.user_id = 1
RETURN u2.name
```

**See**: [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md) for details

**Status**: Low priority (simple workaround, will be fixed in future release)

---

## Performance Issues

### Slow Query Performance

**Symptoms**:
Queries take seconds or minutes when they should be fast.

**Solutions**:

**1. Check if query cache is being used**
```bash
# First run (cold cache): slower
time curl -X POST http://localhost:8080/query -d '{"query":"MATCH (u:User) RETURN count(u)"}'

# Second run (cached): 10-100x faster
time curl -X POST http://localhost:8080/query -d '{"query":"MATCH (u:User) RETURN count(u)"}'
```

**Expected**: Second run should be much faster

**2. Add explicit labels and relationship types**
```cypher
-- ❌ Slow (scans all nodes)
MATCH (n) WHERE n.age > 30 RETURN n

-- ✅ Fast (uses label index)
MATCH (u:User) WHERE u.age > 30 RETURN u

-- ❌ Slow (scans all relationship types)
MATCH (a:User)-[]->(b) RETURN count(*)

-- ✅ Fast (specific relationship type)
MATCH (a:User)-[:FOLLOWS]->(b) RETURN count(*)
```

**3. Optimize ClickHouse indexes**
```sql
-- Check table definition
SHOW CREATE TABLE users;

-- Add ORDER BY key for common filters
CREATE TABLE users (
    user_id UInt64,
    name String,
    age UInt8,
    country String
) ENGINE = MergeTree()
ORDER BY (country, age, user_id);  -- Optimize for country/age queries
```

**4. Use LIMIT to reduce result set size**
```cypher
-- ❌ Processes all results
MATCH (u:User) RETURN u.name ORDER BY u.age DESC

-- ✅ Returns only top 100
MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 100
```

**5. Check ClickHouse query logs**
```bash
# See generated SQL queries
docker exec clickhouse-clickhouse clickhouse-client -q "SELECT query, query_duration_ms FROM system.query_log WHERE type='QueryFinish' ORDER BY event_time DESC LIMIT 10"

# Analyze slow queries
docker exec clickhouse-clickhouse clickhouse-client -q "SELECT query FROM system.query_log WHERE query_duration_ms > 1000 ORDER BY query_duration_ms DESC LIMIT 5"
```

**See**: [Performance Tuning Guide](Performance-Query-Optimization.md)

---

### High Memory Usage

**Symptoms**:
ClickGraph or ClickHouse consuming excessive memory.

**Solutions**:

**1. Limit result set size**
```cypher
-- Always use LIMIT for large result sets
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, friend.name
LIMIT 1000
```

**2. Configure ClickHouse memory limits**
```xml
<!-- config.xml -->
<max_memory_usage>10000000000</max_memory_usage>  <!-- 10 GB -->
<max_bytes_before_external_sort>20000000000</max_bytes_before_external_sort>
```

**3. Use pagination instead of large LIMIT**
```cypher
-- Page 1
MATCH (u:User) RETURN u.name ORDER BY u.user_id LIMIT 100

-- Page 2
MATCH (u:User) RETURN u.name ORDER BY u.user_id SKIP 100 LIMIT 100

-- Page 3
MATCH (u:User) RETURN u.name ORDER BY u.user_id SKIP 200 LIMIT 100
```

**4. Monitor ClickHouse memory usage**
```bash
# Check memory usage
docker stats clickhouse-clickhouse

# Query memory usage details
docker exec clickhouse-clickhouse clickhouse-client -q "SELECT * FROM system.metrics WHERE metric LIKE '%Memory%'"
```

---

## Windows-Specific Issues

### PowerShell Background Job Exits Immediately

**Symptoms**:
Server appears to start but exits when PowerShell script ends.

**Problem**: Running `cargo run` directly in PowerShell script causes server to exit when script completes.

**Solution**: Use `Start-Job` for background processes

```powershell
# ❌ Wrong (exits when script ends)
cargo run --release --bin clickgraph

# ❌ Also wrong (still exits)
Start-Process powershell -ArgumentList "cargo run --release --bin clickgraph"

# ✅ Correct (properly backgrounds the job)
$job = Start-Job -ScriptBlock {
    param($url, $db, $user, $pass, $schema)
    
    # Set environment variables in job context
    $env:CLICKHOUSE_URL = $url
    $env:CLICKHOUSE_DATABASE = $db
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $pass
    $env:GRAPH_CONFIG_PATH = $schema
    
    # Change to project directory
    Set-Location $using:PWD
    
    # Run server
    cargo run --release --bin clickgraph
} -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_DATABASE, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, $env:GRAPH_CONFIG_PATH

# Check output
Receive-Job -Id $job.Id -Keep

# Stop server when done
Stop-Job -Id $job.Id
Remove-Job -Id $job.Id
```

---

### Docker Volume Permission Issues

**Symptoms**:
```
ERROR: Cannot write to /var/lib/clickhouse/data
```

**Problem**: ClickHouse container on Windows cannot write to mounted volumes.

**Solution**: Use `ENGINE = Memory` for tables (acceptable for dev/test)

```sql
-- ❌ Wrong (fails on Windows Docker volumes)
CREATE TABLE users (...) ENGINE = MergeTree() ORDER BY user_id;

-- ✅ Correct (works on Windows)
CREATE TABLE users (...) ENGINE = Memory;
```

**Note**: `Memory` engine doesn't persist data between container restarts, but this is acceptable for development and testing.

**For production**: Use ClickHouse on Linux or deploy without Docker on Windows.

---

### Path Separator Issues

**Symptoms**:
```
ERROR: Cannot find file: ./schemas/demo/users.yaml
```

**Problem**: Unix-style paths don't work on Windows.

**Solutions**:

```powershell
# ✅ Use Windows path separators
$env:GRAPH_CONFIG_PATH = ".\schemas\demo\users.yaml"

# ✅ Or use forward slashes (works on Windows too)
$env:GRAPH_CONFIG_PATH = "./schemas/demo/users.yaml"

# ✅ Best: Use absolute paths
$env:GRAPH_CONFIG_PATH = "$PWD\schemas\demo\users.yaml"
```

---

## Docker Issues

### "Port already in use"

**Symptoms**:
```
ERROR: Cannot start service: port is already allocated
```

**Solutions**:

**1. Check what's using the port**
```bash
# Linux/Mac
lsof -i :8080
lsof -i :8123

# Windows
netstat -ano | findstr :8080
netstat -ano | findstr :8123
```

**2. Stop conflicting service**
```bash
# Stop other ClickGraph instances
docker-compose down

# Or kill specific process (get PID from netstat)
kill <PID>  # Linux/Mac
taskkill /PID <PID> /F  # Windows
```

**3. Use different ports**
```bash
# Modify docker-compose.yaml:
services:
  clickgraph:
    ports:
      - "8081:8080"  # Use 8081 instead of 8080
```

---

### Docker Compose Services Not Starting

**Symptoms**:
```
ERROR: Service 'clickgraph' failed to build
```

**Solutions**:

**1. Check Docker is running**
```bash
docker ps

# If error, start Docker:
# - Windows/Mac: Open Docker Desktop
# - Linux: sudo systemctl start docker
```

**2. Check docker-compose.yaml syntax**
```bash
# Validate YAML
docker-compose config

# Look for syntax errors
```

**3. Rebuild containers**
```bash
# Rebuild all containers
docker-compose build --no-cache

# Restart services
docker-compose up -d
```

**4. Check logs**
```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs clickgraph
docker-compose logs clickhouse
```

---

## Debugging Techniques

### Enable Debug Logging

```bash
# Set log level to debug
export RUST_LOG=debug
cargo run --bin clickgraph

# Or specific modules:
export RUST_LOG=clickgraph::query_planner=debug,clickgraph::clickhouse_query_generator=debug
```

**Output includes**:
- Query parsing details
- Logical plan generation
- SQL generation
- ClickHouse query execution

---

### Inspect Generated SQL

Use `sql_only` parameter to see generated SQL without executing:

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(f) RETURN u.name, f.name",
    "sql_only": true
  }'
```

**Response**:
```json
{
  "sql": "SELECT users_bench.full_name AS u_name, ...",
  "note": "SQL only mode - query not executed"
}
```

---

### Test ClickHouse Queries Directly

```bash
# Test generated SQL in ClickHouse
docker exec clickhouse-clickhouse clickhouse-client -q "SELECT * FROM users LIMIT 5"

# Check query execution plan
docker exec clickhouse-clickhouse clickhouse-client -q "EXPLAIN SELECT * FROM users WHERE age > 30"
```

---

### Check Server Health

```bash
# Health check endpoint
curl http://localhost:8080/health

# Schema info
curl http://localhost:8080/schema

# Version info
curl http://localhost:8080/version
```

---

### Validate Schema YAML

```python
# Python validation script
import yaml

with open('schemas/demo/users.yaml') as f:
    schema = yaml.safe_load(f)
    
# Check required fields
assert 'name' in schema
assert 'views' in schema
assert len(schema['views']) > 0

print("Schema is valid!")
```

---

### Test Queries Incrementally

Start simple and add complexity:

```cypher
-- Step 1: Test basic node match
MATCH (u:User) RETURN count(u)

-- Step 2: Add property
MATCH (u:User) RETURN u.name LIMIT 5

-- Step 3: Add filter
MATCH (u:User) WHERE u.age > 30 RETURN u.name

-- Step 4: Add relationship
MATCH (u:User)-[:FOLLOWS]->(f) RETURN u.name, f.name LIMIT 10

-- Step 5: Add multi-hop
MATCH (u:User)-[:FOLLOWS*2]->(f) RETURN u.name, f.name LIMIT 10
```

---

## Getting More Help

### Community Support

- **[GitHub Issues](https://github.com/genezhang/clickgraph/issues)** - Report bugs
- **[GitHub Discussions](https://github.com/genezhang/clickgraph/discussions)** - Ask questions

### Documentation

- **[Configuration Guide](../configuration.md)** - All configuration options
- **[Known Limitations](Known-Limitations.md)** - Current limitations
- **[KNOWN_ISSUES.md](../../KNOWN_ISSUES.md)** - Detailed issue tracking

### Issue Reporting Template

When reporting issues, include:

```
**Environment**:
- OS: Windows 11 / Ubuntu 22.04 / macOS 14
- ClickGraph version: v0.4.0
- ClickHouse version: 24.1
- Docker version (if applicable): 24.0.7

**Schema**:
- Attach or paste your schema YAML file

**Query**:
```cypher
MATCH (u:User) WHERE u.age > 30 RETURN u
```

**Error Message**:
```
ERROR: ...
```

**Steps to Reproduce**:
1. Start server with...
2. Run query...
3. See error...

**Expected Behavior**:
Should return...

**Actual Behavior**:
Returns error...
```

---

[← Back to Home](Home.md) | [Configuration Reference →](../configuration.md)
