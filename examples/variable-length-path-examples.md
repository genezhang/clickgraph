# Variable-Length Path Examples

**Quick-start examples for common graph queries**

---

## Setup

First, ensure ClickGraph server is running:

```bash
# Start ClickHouse (if using Docker)
docker-compose up -d

# Start ClickGraph server
./target/release/brahmand
```

Server will be available at `http://localhost:8080`

---

## Example 1: Social Network - Friend Recommendations

### Scenario
Find friends-of-friends for user recommendations.

### Data Model
```
Nodes: User (user_id, name, email)
Relationships: FOLLOWS (follower_id → followee_id)
```

### Query
```cypher
MATCH (me:User {user_id: 1})-[f:FOLLOWS*1..2]->(suggested:User)
WHERE suggested.user_id <> 1
RETURN DISTINCT suggested.name, suggested.email
LIMIT 10
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (me:User {user_id: 1})-[f:FOLLOWS*1..2]->(suggested:User) WHERE suggested.user_id <> 1 RETURN DISTINCT suggested.name, suggested.email LIMIT 10"
  }'
```

### Expected Output
```json
{
  "data": [
    {"suggested.name": "Alice Johnson", "suggested.email": "alice@example.com"},
    {"suggested.name": "Bob Smith", "suggested.email": "bob@example.com"},
    {"suggested.name": "Carol White", "suggested.email": "carol@example.com"}
  ]
}
```

---

## Example 2: Network Reach - Influencer Analysis

### Scenario
Calculate the reach of an influencer (total followers within 2 hops).

### Query
```cypher
MATCH (influencer:User {user_id: 5})-[f:FOLLOWS*1..2]->(reached:User)
RETURN influencer.name,
       COUNT(DISTINCT reached) as total_reach,
       MAX(length(f)) as max_distance
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (influencer:User {user_id: 5})-[f:FOLLOWS*1..2]->(reached:User) RETURN influencer.name, COUNT(DISTINCT reached) as total_reach, MAX(length(f)) as max_distance"
  }'
```

### Expected Output
```json
{
  "data": [
    {
      "influencer.name": "Tech Guru",
      "total_reach": 1247,
      "max_distance": 2
    }
  ]
}
```

---

## Example 3: E-commerce - Product Recommendations

### Scenario
Find products liked by users with similar tastes.

### Data Model
```
Nodes: User, Product
Relationships: LIKED (user_id → product_id)
```

### Query
```cypher
MATCH (me:User {user_id: 10})-[:LIKED]->(product1:Product)
      <-[:LIKED]-(similar:User)-[:LIKED]->(recommended:Product)
WHERE NOT (me)-[:LIKED]->(recommended)
RETURN recommended.name, 
       recommended.category,
       COUNT(DISTINCT similar) as similarity_score
ORDER BY similarity_score DESC
LIMIT 20
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (me:User {user_id: 10})-[:LIKED]->(product1:Product)<-[:LIKED]-(similar:User)-[:LIKED]->(recommended:Product) WHERE NOT (me)-[:LIKED]->(recommended) RETURN recommended.name, recommended.category, COUNT(DISTINCT similar) as similarity_score ORDER BY similarity_score DESC LIMIT 20"
  }'
```

---

## Example 4: Organizational Chart - Management Chain

### Scenario
Find all managers in the reporting chain up to the CEO.

### Data Model
```
Nodes: Employee (employee_id, name, title)
Relationships: REPORTS_TO (employee_id → manager_id)
```

### Query
```cypher
MATCH (employee:Employee {employee_id: 42})-[r:REPORTS_TO*]->(manager:Employee)
RETURN manager.name, 
       manager.title,
       length(r) as levels_up
ORDER BY levels_up
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (employee:Employee {employee_id: 42})-[r:REPORTS_TO*]->(manager:Employee) RETURN manager.name, manager.title, length(r) as levels_up ORDER BY levels_up"
  }'
```

### Expected Output
```json
{
  "data": [
    {"manager.name": "Jane Doe", "manager.title": "Team Lead", "levels_up": 1},
    {"manager.name": "John Smith", "manager.title": "Director", "levels_up": 2},
    {"manager.name": "Sarah Johnson", "manager.title": "VP Engineering", "levels_up": 3},
    {"manager.name": "Mike Wilson", "manager.title": "CEO", "levels_up": 4}
  ]
}
```

---

## Example 5: Knowledge Graph - Concept Relationships

### Scenario
Explore related concepts within semantic distance.

### Data Model
```
Nodes: Concept (concept_id, name, category)
Relationships: RELATED_TO (from_concept → to_concept, strength)
```

### Query
```cypher
MATCH (start:Concept {name: "Machine Learning"})-[r:RELATED_TO*1..3]->(related:Concept)
RETURN DISTINCT related.name, 
       related.category,
       MIN(length(r)) as shortest_distance
ORDER BY shortest_distance, related.name
LIMIT 15
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (start:Concept {name: \"Machine Learning\"})-[r:RELATED_TO*1..3]->(related:Concept) RETURN DISTINCT related.name, related.category, MIN(length(r)) as shortest_distance ORDER BY shortest_distance, related.name LIMIT 15"
  }'
```

---

## Example 6: Shortest Path Discovery

### Scenario
Find the shortest connection between two users.

### Query
```cypher
MATCH path = (user1:User {email: "alice@example.com"})
             -[*1..5]-
             (user2:User {email: "bob@example.com"})
RETURN length(path) as distance,
       [n in nodes(path) | n.name] as connection_names
ORDER BY distance
LIMIT 1
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH path = (user1:User {email: \"alice@example.com\"})-[*1..5]-(user2:User {email: \"bob@example.com\"}) RETURN length(path) as distance ORDER BY distance LIMIT 1"
  }'
```

---

## Example 7: Aggregation with Variable-Length Paths

### Scenario
Count connections at each hop distance.

### Query
```cypher
MATCH (center:User {user_id: 1})-[r:FOLLOWS*1..3]->(connected:User)
RETURN length(r) as hops,
       COUNT(DISTINCT connected) as unique_connections
ORDER BY hops
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (center:User {user_id: 1})-[r:FOLLOWS*1..3]->(connected:User) RETURN length(r) as hops, COUNT(DISTINCT connected) as unique_connections ORDER BY hops"
  }'
```

### Expected Output
```json
{
  "data": [
    {"hops": 1, "unique_connections": 15},
    {"hops": 2, "unique_connections": 87},
    {"hops": 3, "unique_connections": 342}
  ]
}
```

---

## Example 8: Bidirectional Path Search

### Scenario
Find connections in either direction (follower or following).

### Query
```cypher
MATCH (person1:User {name: "Alice"})-[*1..2]-(person2:User {name: "Bob"})
RETURN COUNT(*) as num_paths,
       MIN(length(path)) as shortest_distance
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (person1:User {name: \"Alice\"})-[*1..2]-(person2:User {name: \"Bob\"}) RETURN COUNT(*) as num_paths"
  }'
```

---

## Example 9: Filtered Path Traversal

### Scenario
Find connections only through verified/active users.

### Query
```cypher
MATCH path = (start:User {user_id: 1})-[*1..3]->(end:User)
WHERE ALL(u IN nodes(path) WHERE u.verified = true)
  AND end.active = true
RETURN end.name, end.email
LIMIT 50
```

---

## Example 10: Exact Hop Count (Performance Optimized)

### Scenario
Find connections at exactly 2 hops (uses fast chained JOINs).

### Query
```cypher
MATCH (u1:User {user_id: 1})-[*2]->(u2:User)
RETURN u2.name, u2.email
LIMIT 100
```

### cURL Request
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u1:User {user_id: 1})-[*2]->(u2:User) RETURN u2.name, u2.email LIMIT 100"
  }'
```

**Note**: Exact hop count queries (`*2`, `*3`) are automatically optimized with chained JOINs for 2-5x better performance than range queries.

---

## Testing with Python (Neo4j Driver)

```python
from neo4j import GraphDatabase

# Connect to ClickGraph via Bolt protocol
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("", ""))

def find_connections(user_id, max_hops=2):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (me:User {user_id: $user_id})-[*1..$max_hops]->(connected:User)
            RETURN DISTINCT connected.name, connected.email
            LIMIT 20
            """,
            user_id=user_id,
            max_hops=max_hops
        )
        return [record.data() for record in result]

# Run query
connections = find_connections(user_id=1, max_hops=2)
print(f"Found {len(connections)} connections")
for conn in connections:
    print(f"  - {conn['connected.name']} ({conn['connected.email']})")

driver.close()
```

---

## Testing with JavaScript (Neo4j Driver)

```javascript
const neo4j = require('neo4j-driver');

// Connect to ClickGraph via Bolt protocol
const driver = neo4j.driver(
  'bolt://localhost:7687',
  neo4j.auth.basic('', '')
);

async function findConnections(userId, maxHops = 2) {
  const session = driver.session();
  
  try {
    const result = await session.run(
      `
      MATCH (me:User {user_id: $userId})-[*1..$maxHops]->(connected:User)
      RETURN DISTINCT connected.name, connected.email
      LIMIT 20
      `,
      { userId, maxHops }
    );
    
    return result.records.map(record => ({
      name: record.get('connected.name'),
      email: record.get('connected.email')
    }));
  } finally {
    await session.close();
  }
}

// Run query
findConnections(1, 2)
  .then(connections => {
    console.log(`Found ${connections.length} connections`);
    connections.forEach(conn => {
      console.log(`  - ${conn.name} (${conn.email})`);
    });
  })
  .finally(() => driver.close());
```

---

## Performance Tips

### Use Exact Hop Counts When Possible
```cypher
-- Fast (uses chained JOINs)
MATCH (u1)-[*2]->(u2)

-- Slower (uses recursive CTEs)
MATCH (u1)-[*1..2]->(u2)
```

### Always Use LIMIT for Exploration
```cypher
-- Safe exploration
MATCH (u1)-[*1..3]->(u2)
RETURN u2.name
LIMIT 100
```

### Filter Early
```cypher
-- Efficient: filter at start
MATCH (u1:User {country: "USA"})-[*1..2]->(u2:User)

-- Less efficient: filter at end
MATCH (u1:User)-[*1..2]->(u2:User)
WHERE u1.country = "USA"
```

### Use DISTINCT to Reduce Duplicates
```cypher
-- Remove duplicate paths
MATCH (u1)-[*1..3]->(u2)
RETURN DISTINCT u2.name
```

---

## Configuration for Performance

```bash
# For small graphs (< 1K nodes)
export BRAHMAND_MAX_CTE_DEPTH=50

# For medium graphs (1K-100K nodes)
export BRAHMAND_MAX_CTE_DEPTH=100

# For large graphs (> 100K nodes)
export BRAHMAND_MAX_CTE_DEPTH=200

# For deep hierarchies
export BRAHMAND_MAX_CTE_DEPTH=500
```

---

## Troubleshooting

### Query too slow?
1. Add `LIMIT` clause
2. Reduce hop count
3. Add more specific filters
4. Check graph size

### No results?
1. Verify relationship direction (`->` vs `<-`)
2. Check relationship type spelling
3. Verify starting node exists
4. Try wider hop range

### Out of memory?
1. Reduce hop count
2. Add LIMIT
3. Use DISTINCT
4. Return fewer properties

---

## Next Steps

- Read the full [Variable-Length Paths Guide](variable-length-paths-guide.md)
- Explore [Performance Tuning](variable-length-paths-guide.md#performance-tuning)
- Check [Best Practices](variable-length-paths-guide.md#best-practices)
- See [Troubleshooting](variable-length-paths-guide.md#troubleshooting)

---

**Happy Querying!** 🚀

For issues or questions, see [GitHub Issues](https://github.com/genezhang/clickgraph/issues)
