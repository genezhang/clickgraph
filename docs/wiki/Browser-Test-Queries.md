# Browser Click-to-Expand Test Queries

## Database Setup

**Location**: `scripts/setup/setup_all_db_xxx.sh`

Creates 6 isolated databases for testing different schema patterns:

| Database | Schema File | Pattern Tested |
|----------|------------|----------------|
| `db_standard` | `schemas/dev/social_standard.yaml` | Standard nodes+edges (User, Post, FOLLOWS, AUTHORED) |
| `db_fk_edge` | `schemas/dev/orders_customers_fk.yaml` | FK-based edges (Order→Customer via FK) |
| `db_denormalized` | `schemas/dev/flights_denormalized.yaml` | Denormalized (Airport in flights table) |
| `db_polymorphic` | `schemas/dev/social_polymorphic.yaml` | Polymorphic edge (interactions table) |
| `db_composite_id` | `schemas/examples/composite_node_id_test.yaml` | Composite node IDs (Account=[bank_id, account_number]) |
| `db_multi_tenant` | `schemas/dev/social_multi_tenant.yaml` | Parameterized views with tenant_id |

**Combined schema**: `schemas/dev/all_browser_test.yaml` (loads all 6 at once)

## Setup Commands

```bash
# Create all databases with test data
./scripts/setup/setup_all_db_xxx.sh

# Start ClickGraph with all schemas
export GRAPH_CONFIG_PATH="./schemas/dev/all_browser_test.yaml"
cargo run --bin clickgraph
```

## Browser Click-to-Expand Queries

The Neo4j Browser sends these queries when you click a node to expand its neighbors:

### 1. Initial Node Fetch (on search)
```cypher
MATCH (n:User) WHERE n.user_id = 1 RETURN n
```

### 2. Neighbor Expansion (click node)

**What browser sends** (complex path query):
```cypher
MATCH path = (a)-[r]-(b)
WHERE id(a) = 140737488355328
OPTIONAL MATCH (b)-[r2]-()
RETURN path,
       [x IN nodes(path) WHERE x <> b | x] AS hidden,
       CASE WHEN r2 IS NULL THEN 0 ELSE count(DISTINCT r2) END AS degrees
ORDER BY degrees DESC
LIMIT 50
```

**What ClickGraph transforms it to** (in Bolt handler):
```cypher
MATCH (a)-[r]-(o)
WHERE id(a) = 140737488355328
OPTIONAL MATCH (o)-[r2]-()
RETURN o,
       type(r) AS relType,
       id(r) AS relId,
       CASE WHEN r2 IS NULL THEN 0 ELSE count(DISTINCT r2) END AS degrees
ORDER BY degrees DESC
LIMIT 50
```

### 3. Relationship Fetch (draw edges)
```cypher
MATCH (a)-[r]->(b)
WHERE id(a) IN [140737488355328, 140737488355343, ...]
  AND id(b) IN [140737488355329, 140737488355330, ...]
RETURN DISTINCT r, id(a) AS fromId, id(b) AS toId
```

## Test Queries by Schema Type

### db_standard (Standard Schema)
```cypher
USE social_standard

// Initial fetch
MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email

// Expand User → Posts + Friends
MATCH (u:User)-[r]-(o) WHERE u.user_id = 1
RETURN o, type(r)

// Multi-hop
MATCH (u:User {user_id: 1})-[:FOLLOWS*1..2]->(friend:User)
RETURN friend.name
```

### db_fk_edge (FK-Based Edges)
```cypher
USE orders_customers_fk

// Expand Customer → Orders
MATCH (c:Customer)-[r:PLACED]-(o)
WHERE c.customer_id = 101
RETURN o, type(r)
```

### db_denormalized (Denormalized)
```cypher
USE flights_denormalized

// Expand Airport → Flights
MATCH (a:Airport)-[f:FLIGHT]-(dest)
WHERE a.code = 'JFK'
RETURN dest, f.flight_number
```

### db_polymorphic (Polymorphic Edges)
```cypher
USE social_polymorphic

// Expand User via polymorphic interactions
MATCH (u:User)-[r]-(o) WHERE u.user_id = 1
RETURN o, type(r)

// Type-specific expansion
MATCH (u:User)-[:LIKES]-(p:Post) WHERE u.user_id = 1
RETURN p
```

### db_composite_id (Composite IDs)
```cypher
USE composite_node_id

// Expand Account → Customer
MATCH (a:Account)-[r:OWNED_BY]-(c)
WHERE a.bank_id = 'BANK001' AND a.account_number = 'ACC1001'
RETURN c, type(r)

// Expand Account → Transfers
MATCH (a:Account)-[t:TRANSFER]-(other)
WHERE a.bank_id = 'BANK001' AND a.account_number = 'ACC1001'
RETURN other, t.amount
```

### db_multi_tenant (Multi-Tenancy)
```cypher
USE social_multi_tenant

// Set tenant scope
CALL sys.set('tenant_id', 'acme')

// Expand User (scoped to tenant)
MATCH (u:User)-[r]-(o) WHERE u.user_id = 1
RETURN o, type(r)

// Switch tenant
CALL sys.set('tenant_id', 'globex')

MATCH (u:User)-[r]-(o) WHERE u.user_id = 1
RETURN o, type(r)  // Returns different results
```

## Known Performance Issues

### Slow Queries (>1s)

1. **Untyped node expansion with many property checks**
   ```cypher
   MATCH (n)-[r]-(o) WHERE id(n) = 123456 RETURN o
   ```
   - Triggers `PatternResolver` UNION ALL across all node/edge types
   - Each branch checks if properties exist (10+ CTEs generated)
   - **Workaround**: Use typed patterns: `MATCH (u:User)-[r]-(o)`

2. **Multi-type VLP with large graphs**
   ```cypher
   MATCH (u)-[:FOLLOWS|FRIENDS_WITH*1..3]-(friend) RETURN friend
   ```
   - Recursive CTE with UNION ALL for each hop
   - **Workaround**: Use bounded hops `*1..2` or single type `[:FOLLOWS*1..3]`

3. **Click-to-expand on high-degree nodes (1000+ neighbors)**
   - ClickHouse scans full edge table for `id(a) IN (...)` lookups
   - No indexes on `_clickgraph_id` pseudo-column (computed)
   - **Workaround**: Add LIMIT 50 (browser already does this)

## Performance Testing

```bash
# Time queries via HTTP
time curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE social_standard MATCH (u:User)-[r]-(o) WHERE u.user_id = 1 RETURN o"}'

# Via neo4j driver (includes Bolt overhead)
time python3 -c "
from neo4j import GraphDatabase
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))
with driver.session(database='social_standard') as session:
    result = session.run('MATCH (u:User)-[r]-(o) WHERE u.user_id = 1 RETURN o')
    print(len(list(result)))
"
```

## Debugging Slow Queries

1. **Use `sql_only` parameter** (via HTTP):
   ```bash
   curl -X POST http://localhost:8080/query \
     -H "Content-Type: application/json" \
     -d '{"query":"MATCH (u:User)-[r]-(o) WHERE u.user_id = 1 RETURN o", "sql_only": true}'
   ```

2. **Check ClickHouse query log**:
   ```bash
   docker exec clickhouse clickhouse-client -q "
   SELECT query, query_duration_ms
   FROM system.query_log
   WHERE type = 'QueryFinish'
   ORDER BY event_time DESC
   LIMIT 10
   "
   ```

3. **Enable debug logging**:
   ```bash
   export RUST_LOG=clickgraph=debug
   cargo run --bin clickgraph 2>&1 | tee server_debug.log
   ```
