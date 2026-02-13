# Graph-Notebook Compatibility Guide

## Overview

ClickGraph supports AWS graph-notebook for Jupyter notebook-based graph visualization and exploration when running in **Neo4j compatibility mode**.

## Enabling Compatibility

### Environment Variable (K8s Recommended)
```bash
export CLICKGRAPH_NEO4J_COMPAT_MODE=true
```

### CLI Flag
```bash
./clickgraph --neo4j-compat-mode
```

### Startup Script
```bash
./scripts/server/start_server_background.sh --neo4j-compat-mode
```

## What Works ✅

### Connection & Authentication
- ✅ Bolt Protocol v5.8
- ✅ Basic authentication (username/password)
- ✅ No authentication mode
- ✅ Connection pooling

### Schema Discovery
- ✅ `CALL db.labels()` - List all node labels
- ✅ `CALL db.relationshipTypes()` - List all relationship types
- ✅ `CALL db.propertyKeys()` - List all property keys
- ✅ `CALL dbms.components()` - Server version info
- ✅ `CALL db.schema.nodeTypeProperties()` - Node property metadata
- ✅ `CALL db.schema.relTypeProperties()` - Relationship property metadata

### Basic Queries
- ✅ `MATCH (n:Label) RETURN n` - Node patterns
- ✅ `MATCH (a)-[r:TYPE]->(b)` - Relationship patterns
- ✅ `WHERE` clause filtering (properties, comparisons)
- ✅ `RETURN` with property access
- ✅ `ORDER BY`, `LIMIT`, `SKIP`
- ✅ `DISTINCT` results
- ✅ Parameterized queries (`$param` syntax)

### Aggregations
- ✅ `count()`, `sum()`, `avg()`, `min()`, `max()`
- ✅ `collect()` to gather values into arrays
- ✅ Implicit `GROUP BY` with aggregations

### Advanced Patterns
- ✅ Multi-hop traversals: `(a)-[]->()-[]->(b)`
- ✅ Variable-length paths: `(a)-[*1..3]->(b)`
- ✅ Shortest path: `shortestPath((a)-[*]->(b))`
- ✅ `OPTIONAL MATCH` for LEFT JOIN semantics
- ✅ `WITH` clause for query composition
- ✅ `UNION` / `UNION ALL` queries
- ✅ Path variables: `p = (a)-[]->(b)` with `nodes(p)`, `relationships(p)`, `length(p)`

### Graph Objects
- ✅ Node objects with labels and properties
- ✅ Relationship objects with type and properties
- ✅ Path objects for visualization
- ✅ Neo4j 5.0+ `elementId` support
- ✅ Legacy `id()` function for compatibility

### Error Handling
- ✅ Neo4j-compatible error codes
- ✅ Syntax error reporting
- ✅ Schema validation errors
- ✅ Query planning errors

## What Doesn't Work ❌

### Write Operations (By Design)
- ❌ `CREATE` nodes/relationships
- ❌ `SET` properties
- ❌ `DELETE` / `REMOVE` nodes/relationships
- ❌ `MERGE` upsert operations
- ❌ Schema modifications (`CREATE INDEX`, `CREATE CONSTRAINT`)

**Reason**: ClickGraph is a **read-only** query engine. Use ClickHouse directly for data modifications.

### APOC Procedures
- ❌ `CALL apoc.*` procedures
- ❌ Virtual graphs, temporal functions, etc.

**Reason**: APOC is Neo4j-specific. Not planned for support.

### GDS (Graph Data Science) Procedures
- ❌ `CALL gds.*` procedures (except `gds.pageRank`)
- ❌ Graph projections, similarity algorithms, etc.

**Status**: ClickGraph has native PageRank. Other algorithms planned for future releases.

### Advanced Cypher Features
- ❌ Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
- ⚠️ `EXISTS` subqueries (partial support)
- ❌ List comprehensions: `[x IN list | x.prop]`
- ❌ Pattern comprehensions: `[(a)-[]->(b) | b.name]`
- ❌ `CALL` ... `YIELD` with multiple queries in subquery
- ❌ Full-text search

**Status**: Some features planned for future releases.

## Known Limitations

### Schema Selection
The Neo4j driver's `database` parameter doesn't reliably pass through to ClickGraph. **Always use the `USE` clause**:

```python
# ✅ WORKS
session.run("USE social_benchmark MATCH (u:User) RETURN u")

# ⚠️ MAY NOT WORK
session.run("MATCH (u:User) RETURN u", database="social_benchmark")
```

### Result Format
- Results are flat JSON rows, not fully hydrated graph objects in some cases
- Path objects may have simplified structure
- Property types are inferred from ClickHouse column types

### Performance
- First query on a pattern may be slower (query translation + SQL execution)
- Subsequent queries benefit from query cache
- Complex queries may timeout (default 30s)

## Testing Compatibility

### Quick Test Script
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    # Test connection
    result = session.run("RETURN 1 as test")
    print("Connection:", result.single()["test"])
    
    # Test schema discovery
    result = session.run("USE social_benchmark CALL db.labels() YIELD label RETURN label LIMIT 5")
    print("Labels:", [r["label"] for r in result])
    
    # Test basic query
    result = session.run("USE social_benchmark MATCH (u:User) RETURN count(u) as total")
    print("Users:", result.single()["total"])

driver.close()
print("✅ All tests passed!")
```

### Full Test Suite
```bash
pytest tests/integration/bolt/test_graph_notebook_compatibility.py -v
```

## Jupyter Notebook Setup

### Installation
```bash
pip install graph-notebook neo4j
jupyter nbextension install --py --sys-prefix graph_notebook.widgets
jupyter nbextension enable  --py --sys-prefix graph_notebook.widgets
```

### Configuration
Create `~/.graph_notebook_config.json`:
```json
{
  "host": "localhost",
  "port": 7687,
  "auth_mode": "DEFAULT",
  "iam_credentials_provider_type": "ROLE",
  "load_from_s3_arn": "",
  "ssl": false,
  "aws_region": "us-west-2",  
  "neptune_service": "neptune-db",
  "protocol": "bolt",
  "neo4j": {
    "username": "neo4j",
    "password": "password",
    "auth": true,
    "database": ""
  }
}
```

### Notebook Magic Commands
```python
%%opencypher
USE social_benchmark
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, collect(friend.name) AS friends
LIMIT 10
```

### Visualization
```python
%%opencypher --store-to results --plan-cache hits
USE social_benchmark  
MATCH p=(u1:User)-[:FOLLOWS]->(u2:User)
WHERE u1.country = 'USA'
RETURN p
LIMIT 50
```

```python
%graph_notebook_vis_options --store-to options
{
  "nodes": {
    "User": {
      "color": "#1f77b4",
      "size": 20,
      "label": "name"
    }
  },
  "edges": {
    "FOLLOWS": {
      "color": "#ff7f0e",
      "width": 2
    }
  }
}
```

## Troubleshooting

### "UnsupportedServerProduct" Error
**Problem**: Neo4j driver rejects connection.  
**Solution**: Enable Neo4j compatibility mode (`--neo4j-compat-mode`).

### Schema Not Found
**Problem**: Queries fail with "Node with label X not found".  
**Solution**: Use explicit `USE <schema_name>` clause in queries.

### Timeout Errors
**Problem**: Complex queries timeout.  
**Solution**: 
- Simplify query patterns
- Add `LIMIT` clauses
- Increase timeout in driver configuration

### Property Not Found
**Problem**: Property access returns null or errors.  
**Solution**: Check schema YAML mapping from Cypher properties to ClickHouse columns.

### Slow Performance
**Problem**: Queries slower than expected.  
**Solution**:
- Enable query cache (enabled by default)
- Add indexes in ClickHouse on join columns
- Use projections for denormalized access

## Reporting Issues

When reporting compatibility issues, include:
1. ClickGraph version (`clickgraph --version`)
2. Neo4j driver version
3. graph-notebook version
4. Complete Cypher query
5. Error message or unexpected behavior
6. Schema YAML (if applicable)

## See Also

- [Bolt Protocol Documentation](docs/wiki/Bolt-Protocol.md)
- [Neo4j Tools Integration](docs/wiki/Neo4j-Tools-Integration.md)
- [Cypher Language Reference](docs/wiki/Cypher-Language-Reference.md)
- [API Reference](docs/api.md)
