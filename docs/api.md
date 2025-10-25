# ClickGraph API Documentation

ClickGraph provides two API interfaces for executing Cypher queries: HTTP REST API and Neo4j Bolt Protocol.

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
  "parameters": {}
}
```

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
  "columns": ["n"],
  "data": [
    {"n": {"id": 1, "properties": {"name": "Alice"}}},
    {"n": {"id": 2, "properties": {"name": "Bob"}}}
  ],
  "stats": {
    "nodes_created": 0,
    "relationships_created": 0,
    "execution_time": "15ms"
  }
}
```

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

### Examples

#### Basic Node Query
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.age > 25 RETURN u.name, u.age LIMIT 5"
  }'
```

#### Relationship Traversal
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

## Neo4j Bolt Protocol

### Connection Details
- **Protocol**: Bolt v4.4
- **Default Port**: 7687
- **URI Format**: `bolt://localhost:7687`

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

### Relationship Format
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
  "relationships": [
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