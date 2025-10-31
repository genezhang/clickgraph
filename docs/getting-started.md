# ClickGraph - Getting Started Guide

This guide will help you get ClickGraph up and running quickly for your first graph analysis on ClickHouse data.

## Prerequisites

- **ClickHouse**: Version 21.3+ running locally or accessible via network
- **Rust**: Version 1.85+ for building from source
- **Docker**: (Optional) For running ClickHouse via Docker Compose

## Quick Setup (5 minutes)

### 1. Start ClickHouse

**Option A: Using Docker (Recommended)**
```bash
# Clone ClickGraph repository
git clone https://github.com/genezhang/clickgraph
cd clickgraph

# Start ClickHouse with pre-configured settings
docker-compose up -d
```

**Option B: Existing ClickHouse**
Ensure your ClickHouse instance is accessible and you have credentials.

### 2. Configure Environment
```bash
# Set ClickHouse connection details
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
```

### 3. Build and Run ClickGraph
```bash
# Build ClickGraph
cargo build --release

# Start with default configuration (HTTP:8080, Bolt:7687)
cargo run --bin brahmand
```

You should see output like:
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

🎉 **ClickGraph is now running!**

> **⚠️ Normal Startup Warnings**: You may see warnings like:
> ```
> Warning: Failed to connect to ClickHouse, using empty schema
> Error fetching remote schema: no rows returned by a query
> ```
> These are **expected warnings** about ClickGraph's internal catalog system. They don't affect functionality - your queries will work correctly!

## First Graph Query

### Test with HTTP API
```bash
# Simple test query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "RETURN 1 as test, \"Hello ClickGraph!\" as message"}'
```

Expected response:
```json
{
  "columns": ["test", "message"],
  "data": [
    {"test": 1, "message": "Hello ClickGraph!"}
  ],
  "stats": {
    "execution_time": "2ms"
  }
}
```

### Test with Neo4j Driver
```python
from neo4j import GraphDatabase

# Connect to ClickGraph via Bolt protocol
driver = GraphDatabase.driver("bolt://localhost:7687")

with driver.session() as session:
    result = session.run("RETURN 1 as test, 'Hello ClickGraph!' as message")
    for record in result:
        print(f"Test: {record['test']}, Message: {record['message']}")

driver.close()
```

## Working with Real Data

### Example: Social Network Analysis

#### 1. Create Sample Tables in ClickHouse
```sql
-- Connect to ClickHouse and create sample data
CREATE TABLE users (
    user_id UInt32,
    name String,
    age UInt8,
    country String,
    active UInt8 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE user_follows (
    follower_id UInt32,
    followed_id UInt32,
    created_date Date
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);

-- Insert sample data
INSERT INTO users VALUES 
    (1, 'Alice', 28, 'USA', 1),
    (2, 'Bob', 34, 'Canada', 1),
    (3, 'Charlie', 22, 'UK', 1),
    (4, 'Diana', 31, 'Australia', 1);

INSERT INTO user_follows VALUES
    (1, 2, '2023-01-15'),
    (1, 3, '2023-01-20'),
    (2, 3, '2023-01-25'),
    (3, 4, '2023-02-01'),
    (2, 4, '2023-02-05');
```

#### 2. Configure Graph View
Create `social_network.yaml`:
```yaml
name: social_network
version: "1.0"
description: "Social network analysis"

views:
  - name: main_graph
    nodes:
      User:
        source_table: users
        id_column: user_id
        property_mappings:
          name: name
          age: age
          country: country
        filters:
          - "active = 1"
          
    relationships:
      FOLLOWS:
        source_table: user_follows
        from_column: follower_id
        to_column: followed_id
        property_mappings:
          since: created_date
```

#### 3. Run Graph Queries
```bash
# Find Alice's friends
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (alice:User {name: \"Alice\"})-[:FOLLOWS]->(friend:User) RETURN friend.name, friend.age"
  }'

# Find mutual connections
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) WHERE a.name = \"Alice\" AND b.name = \"Bob\" RETURN mutual.name"
  }'

# Count followers by country
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN f.country, count(u) as follower_count ORDER BY follower_count DESC"
  }'
```

## Configuration Options

### Command-Line Configuration
```bash
# Custom ports
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688

# Disable Bolt protocol (HTTP only)
cargo run --bin brahmand -- --disable-bolt

# Custom host binding
cargo run --bin brahmand -- --http-host 127.0.0.1

# Show all options
cargo run --bin clickgraph -- --help
```

### Environment Variable Configuration
```bash
# Server settings
export CLICKGRAPH_HOST="127.0.0.1"
export CLICKGRAPH_PORT="8081"  
export CLICKGRAPH_BOLT_HOST="127.0.0.1"
export CLICKGRAPH_BOLT_PORT="7688"
export CLICKGRAPH_BOLT_ENABLED="false"

# ClickHouse connection
export CLICKHOUSE_URL="http://your-clickhouse:8123"
export CLICKHOUSE_USER="your_user"
export CLICKHOUSE_PASSWORD="your_password"
export CLICKHOUSE_DATABASE="your_database"
```

## Neo4j Tool Integration

### Neo4j Browser
1. Open Neo4j Browser
2. Connect to `bolt://localhost:7687`
3. Run Cypher queries directly in the browser interface

### Cypher Shell
```bash
# Install Neo4j Cypher Shell
# Connect to ClickGraph
cypher-shell -a bolt://localhost:7687

# Run queries interactively
neo4j> MATCH (n:User) RETURN n.name, n.age;
```

### Programming Language Drivers

**Python**
```bash
pip install neo4j
```

**JavaScript/Node.js**
```bash
npm install neo4j-driver
```

**Java**
```xml
<dependency>
    <groupId>org.neo4j.driver</groupId>
    <artifactId>neo4j-java-driver</artifactId>
    <version>5.x.x</version>
</dependency>
```

## Performance Tips

### Query Optimization
- Use `LIMIT` clauses to avoid large result sets
- Create indexes on frequently queried columns in ClickHouse
- Use parameterized queries for better performance
- Leverage ClickHouse's columnar storage advantages

### Data Modeling
- Denormalize data for better graph query performance
- Create materialized views for complex relationships
- Use appropriate ClickHouse table engines (MergeTree, etc.)
- Consider partitioning large tables by date or category

## Troubleshooting

### Common Issues

**Connection refused errors:**
```bash
# Check if ClickGraph is running
curl http://localhost:8080/query

# Check ClickHouse connectivity
curl http://localhost:8123/ping
```

**ClickHouse authentication errors:**
```bash
# Test ClickHouse connection
curl "http://localhost:8123/?user=test_user&password=test_pass" -d "SELECT 1"
```

**Port conflicts:**
```bash
# Use different ports
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688
```

### Debug Mode
```bash
# Enable debug logging
RUST_LOG=debug cargo run --bin brahmand
```

## Troubleshooting Common Issues

### Schema Warnings (Normal)
**Issue**: Seeing warnings about "Failed to connect to ClickHouse, using empty schema"
```
Warning: Failed to connect to ClickHouse, using empty schema
Error fetching remote schema: no rows returned by a query
```
**Status**: ⚠️ **Expected behavior** - these are cosmetic warnings about ClickGraph's internal catalog.  
**Impact**: None - core functionality works perfectly.  
**Action**: Continue normally - no fix needed.

### Authentication Problems
**Issue**: `401 Unauthorized` or `403 Forbidden` errors
**Cause**: Incorrect ClickHouse credentials  
**Solution**: 
```bash
# Use docker-compose credentials
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"

# Or check your ClickHouse config
```

### Connection Issues
**Issue**: `Unable to connect to the remote server`
**Cause**: ClickGraph server not fully initialized  
**Solution**: Wait 5-10 seconds after seeing "Brahmand server is running"

### File Permission Errors
**Issue**: `filesystem error: in rename: Permission denied`
**Cause**: Docker volume permissions with MergeTree engine tables  
**Solutions**:
1. Use Memory engine for development: `ENGINE = Memory`
2. Fix Docker permissions: `sudo chown -R 101:101 ./clickhouse_data`
3. Recreate Docker volume: `docker volume rm clickgraph_clickhouse_data`

### Memory Engine Data Loss
**Issue**: Data disappears after restart
**Cause**: Memory engine tables are not persistent  
**Solution**: Use MergeTree engine for production:
```sql
CREATE TABLE users (...) ENGINE = MergeTree() ORDER BY id;
```

### Performance Issues
**Issue**: Slow query responses
**Solutions**:
1. Add ClickHouse indexes on frequently queried columns
2. Use appropriate ORDER BY clauses in table definitions
3. Enable ClickGraph query optimization features

### Port Conflicts
**Issue**: `Address already in use`
**Solution**: Use different ports
```bash
cargo run --bin brahmand -- --http-port 8081 --bolt-port 7688
```

## Next Steps

1. **Read the Documentation**: Check out the [Features Guide](docs/features.md) and [API Documentation](docs/api.md)
2. **Configure Graph Views**: Create YAML configurations for your specific data model
3. **Integrate with Applications**: Use HTTP API or Neo4j drivers in your applications
4. **Optimize Performance**: Tune ClickHouse settings and create appropriate indexes
5. **Join the Community**: Contribute to the project and share your use cases

## Need Help?

- **Documentation**: Check the `docs/` folder for comprehensive guides
- **Issues**: Report bugs and feature requests on GitHub
- **Examples**: See `examples/` folder for more complex configurations
- **Community**: Join discussions and share your experiences

Happy graph analyzing! 🎉