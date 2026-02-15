# ClickGraph + Neo4j Browser Demo

**Live graph visualization of ClickGraph queries using Neo4j Browser**

Connect the Neo4j 5.15 community edition to ClickGraph server via Bolt protocol for interactive graph exploration.

## Quick Start (5 minutes)

### 1. One-Command Setup

Run the automated setup script:

```bash
cd /home/gz/clickgraph/demos/neo4j-browser
bash setup.sh
```

This will:
- ✅ Start Neo4j Browser (standalone Docker)
- ✅ Start ClickGraph server
- ✅ Open browser automatically
- ✅ Provide next steps

**Skip setup.sh?** Use manual steps below.

### 2. Manual Setup (If Preferred)

```bash
# Terminal 1: Start Neo4j (simple standalone)
docker run --rm -d --name neo4j-clickgraph \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/test_password \
  neo4j:latest

# Wait 30 seconds for Neo4j to start
sleep 30

# Terminal 2: Start ClickGraph (ensure ClickHouse is running first)
cd /home/gz/clickgraph
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
./target/release/clickgraph --http-port 8080 --bolt-port 7687
```

### 3. Launch Neo4j Browser

Open in your browser:
```
http://localhost:7474
```

**First Login:**
- Username: `neo4j`
- Password: `test_password`

### 4. Connect to ClickGraph

In Neo4j Browser:

1. Click **Database** dropdown (top right)
2. Click **Connect to another database**
3. Enter connection details:
   - **URI**: `bolt://localhost:7687`
   - **Username**: (leave empty)
   - **Password**: (leave empty)
4. Click **Connect**

Or paste into URI bar:
```
bolt://localhost:7687
```

**Note**: Leave auth fields empty - ClickGraph uses Neo4j's auth mechanism

---

**Prefer detailed setup?** See [CONNECTION_GUIDE.md](CONNECTION_GUIDE.md)

## What You Can Do

### Explore the Graph

- **Visual queries**: Enter Cypher queries in the command bar (`:`)
- **Click to expand**: Click on nodes to see relationships
- **Explore neighbors**: Double-click relationships to expand
- **Beautiful graphs**: Auto-layout of nodes and edges

### Sample Queries

Try these in the Neo4j Browser query bar:

```cypher
# Find a user and their followers
MATCH (u:User {user_id: 1})-[:FOLLOWS*1..2]->(x)
RETURN u, x
LIMIT 100

# Posts and likes (multi-hop pattern)
MATCH (u:User {user_id: 1})-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker)
RETURN u, p, liker
LIMIT 50

# Followed users and their posts
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(friend)-[:AUTHORED]->(post)
RETURN u, friend, post
LIMIT 30
```

See [SAMPLE_QUERIES.md](SAMPLE_QUERIES.md) for more examples.

## Architecture

```
Neo4j Browser (http://7474)
         ↓
   Neo4j Bolt Driver
         ↓
  ClickGraph Server (8080)
         ↓
  ClickHouse (18123)
         ↓
   Benchmark Database (954.9M rows)
```

- **Neo4j**: Browser UI + Bolt protocol connector
- **ClickGraph**: Translates Cypher to SQL + Bolt message handler
- **ClickHouse**: Executes SQL on benchmark data

## Features Supported

### Query Execution ✅
- Basic MATCH patterns
- WHERE clauses with properties
- Multi-hop traversals
- Variable-length paths (*1..3)
- Parameter binding ($param)
- ORDER BY, LIMIT, SKIP

### Visualization ✅
- Node styling by type
- Relationship coloring by type
- Auto-layout with physics
- Expand/collapse nodes
- Pan and zoom
- Property inspection

### Browser Functions ✅
- Query history
- Parameter suggestions
- Results table view
- Graph view
- Code editor

## Known Limitations

### What Works
- ✅ Query execution via Bolt protocol
- ✅ Result visualization as graph/table
- ✅ Basic node/relationship exploration
- ✅ Parameter binding
- ✅ All 18 benchmark queries

### What Doesn't Work Yet
- ❌ Neo4j Desktop (WebSocket connection)
- ❌ NeoDash (dashboard integration)
- ❌ Transactions (read-only mode)
- ❌ Streaming results

### Query Limitations
- 2 complex nested WITH queries not supported
- Anonymous nodes require UNION expansion
- Cyclic aliases need special handling

See [../../BENCHMARK_VALIDATION.md](../../BENCHMARK_VALIDATION.md) for details.

## Troubleshooting

### Neo4j Container Won't Start (Docker Compose Issue)

**Problem**: `neo4j-dev` fails to start via docker-compose

**Solution**: Use standalone Docker instead:

```bash
# Stop docker-compose version if running
docker-compose -f docker-compose.dev.yaml down

# Remove broken container
docker rm neo4j-dev 2>/dev/null || true

# Start standalone (more reliable)
docker run --rm -d --name neo4j-clickgraph \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/test_password \
  neo4j:5.15-community
```

**Why?** Docker compose networking can be finicky. Standalone works reliably.

### Connection Refused (neo4j-clickgraph not running)

```bash
# Check container status
docker ps | grep neo4j

# If not running, start it
docker run --rm -d --name neo4j-clickgraph \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/test_password \
  neo4j:5.15-community

# Check logs
docker logs neo4j-clickgraph
```

### ClickGraph Connection Error

```bash
# Verify ClickGraph server is running
curl http://localhost:8080/health

# Check ClickGraph logs
docker logs clickgraph-dev

# Verify ClickHouse is available
curl http://localhost:18123/ping
```

### "Bolt connection failed"

1. Ensure ClickGraph server is running (port 8080)
2. Check firewall: `netstat -tlnp | grep 7687`
3. Verify Neo4j can reach ClickGraph: `docker network ls`

### Slow Performance

- Use LIMIT to reduce results (default 100, max 10000)
- Avoid unbounded variable-length paths
- Try with smaller hop ranges (*1..3 instead of *)

### Browser Doesn't Show Results

- Check the **Query Plan** tab for errors
- Review **Parameters** panel
- Check **Server Settings** in Neo4j Browser
- Reload page and try again

## Manual Setup (If Docker Not Available)

If you prefer to run services manually:

```bash
# Terminal 1: Start ClickHouse (on custom ports)
# Setup: See ../../../PERSISTENT_SETUP.md

# Terminal 2: Generate benchmark data
cd /home/gz/clickgraph
python3 setup_unified_direct.py --scale 5000

# Terminal 3: Start ClickGraph
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
./target/release/clickgraph --http-port 8080 --bolt-port 7687

# Terminal 4: Start Neo4j (installed locally or docker run)
docker run --rm -d \
  --name neo4j-demo \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/test_password \
  neo4j:5.15-community

# Open browser: http://localhost:7474
```

## Demo Walkthrough

### Step 1: First Query (30s)

```cypher
# Simple node lookup
MATCH (u:User {user_id: 1}) RETURN u
```

**What to see:**
- Single User node
- All properties visible in panel
- Click to inspect

### Step 2: Direct Relationships (1m)

```cypher
# Find followers
MATCH (u:User {user_id: 1})<-[:FOLLOWS]-(f:User)
RETURN u, f
LIMIT 10
```

**What to see:**
- Central user connected to 10 follower nodes
- FOLLOWS relationships shown as edges
- Click nodes to expand properties

### Step 3: Multi-Hop Pattern (2m)

```cypher
# Friends of friends
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f)-[:FOLLOWS]->(fof)
RETURN u, f, fof
LIMIT 30
```

**What to see:**
- Multiple graph layers (user → followers → second-degree)
- Relationship chains visualized
- Use layout controls to arrange

### Step 4: Multiple Edge Types (2m)

```cypher
# Posts and their engagement
MATCH (u:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker)
WHERE u.user_id < 10
RETURN u, p, liker
LIMIT 50
```

**What to see:**
- Mix of User, Post nodes
- Different colored edges (AUTHORED, LIKED)
- Dense subgraph of engagement

### Step 5: Complex Pattern (3m)

```cypher
# Followers' favorite posts
MATCH (me:User {user_id: 1})-[:FOLLOWS]->(f:User)-[:LIKED]->(p:Post)
RETURN me, f, p
LIMIT 40
```

**What to see:**
- 3-hop pattern with multiple node types
- What posts your followers like
- Discover trending content

## Next Steps

### Testing

1. Try all 18 benchmark queries with visualization
2. Compare performance: Browser vs raw HTTP requests
3. Test parameter binding with different scales
4. Explore relationship counts

### Integration

1. Run NeoDash with custom dashboard (if supported)
2. Create Neo4j app with ClickGraph backend
3. Build visualization tools on Bolt protocol
4. Compare with Neo4j native database

### Optimization

1. Profile slow queries using EXPLAIN
2. Identify bottleneck relationships
3. Add query caching for frequent patterns
4. Test scaling to larger datasets

## Files in This Demo

- `README.md` - This file, setup instructions
- `SETUP.sh` - One-command startup script
- `SAMPLE_QUERIES.md` - 20+ example queries
- `CONNECTION_GUIDE.md` - Detailed connection steps
- `BENCHMARK_QUERIES.md` - All 18 benchmark queries for Browser

## Support

Try these resources for help:

- [ClickGraph STATUS.md](../../STATUS.md)
- [ClickGraph KNOWN_ISSUES.md](../../KNOWN_ISSUES.md)
- [Benchmark Validation Results](../../BENCHMARK_VALIDATION.md)
- [Query Examples](../../QUERY_EXAMPLES.md)
- [Neo4j Browser Docs](https://neo4j.com/developer/neo4j-browser/)

## Architecture Notes

### Why This Works

ClickGraph implements the Neo4j Bolt protocol (v5.8), which allows:
1. Any Bolt-compatible client to connect
2. Neo4j Browser to execute queries
3. Results formatted as Neo4j result objects
4. Graph visualization of Cypher results

### Under the Hood

```
User Query: MATCH (u:User {user_id: 1}) RETURN u
   ↓
Neo4j Browser: Encodes as Bolt message
   ↓
ClickGraph Server: Decodes Bolt message
   ↓
Cypher Parser: Converts to AST
   ↓
Query Planner: Builds logical plan
   ↓
SQL Generator: Creates ClickHouse SQL
   ↓
ClickHouse Client: Executes SQL
   ↓
Result Handler: Formats for Bolt protocol
   ↓
Neo4j Browser: Displays graph visualization
```

Each step fully transparent to the user.

## Contributing

If you find issues or have suggestions:

1. First check [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md)
2. Review query limitations in [BENCHMARK_VALIDATION.md](../../BENCHMARK_VALIDATION.md#limitations)
3. Try the query with HTTP API first to isolate issue
4. Report pattern/query in issue tracker

## Questions?

- Query not working? → See [SAMPLE_QUERIES.md](SAMPLE_QUERIES.md)
- Server not starting? → See Troubleshooting section
- Performance slow? → Check browser console and server logs
- Feature request? → See [STATUS.md](../../STATUS.md) for roadmap
