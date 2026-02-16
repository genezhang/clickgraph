# ClickGraph + Neo4j Browser Quick Start

**Explore graph queries with visual results in 5 minutes**

This is the **end-user quick start** for visualizing ClickGraph queries using Neo4j Browser.

For **performance testing with large benchmark data**, see `tests/neo4j-browser/` instead.

---

## 🚀 Quick Start (5 minutes)

### Prerequisites

- Docker and Docker Compose installed
- 2GB free disk space

### One-Command Setup

```bash
cd demos/neo4j-browser
bash setup.sh
```

This will:
- ✅ Start ClickHouse database
- ✅ Start Neo4j Browser UI
- ✅ Start ClickGraph server
- ✅ Load sample data (30 users, 50 posts, relationships)
- ✅ Open browser automatically

**Done!** Neo4j Browser opens at `http://localhost:7474`

### Manual Setup (If Preferred)

```bash
cd demos/neo4j-browser
docker-compose up -d
bash setup_demo_data.sh
```

Then open: http://localhost:7474

---

## 📊 Connect Neo4j Browser to ClickGraph

1. **Login to Neo4j Browser** (http://localhost:7474)
   - Username: `neo4j`
   - Password: `test_password`

2. **Connect to ClickGraph**
   - Click **Database** dropdown (top right)
   - Click **Connect to another database**
   - Enter URI: `bolt://localhost:7687`
   - Leave username/password empty
   - Click **Connect**

3. **Try a query**
   ```cypher
   MATCH (u:User) RETURN u LIMIT 5
   ```

---

## 📝 Sample Queries

### Basic Node Lookup
```cypher
# Find a user
MATCH (u:User {user_id: 1}) 
RETURN u
```

### Simple Relationships
```cypher
# Find users someone follows
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(followed)
RETURN u, followed
LIMIT 10
```

### Multi-Hop Patterns
```cypher
# Friends of friends
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f)-[:FOLLOWS]->(fof)
RETURN u, f, fof
LIMIT 20
```

### Multiple Edge Types
```cypher
# Users and their posts they liked
MATCH (u:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker)
RETURN u, p, liker
LIMIT 30
```

### Variable-Length Paths
```cypher
# Follow chain up to 3 hops
MATCH (u:User {user_id: 1})-[:FOLLOWS*1..3]->(x)
RETURN u, x
LIMIT 50
```

---

## 🎨 Using Neo4j Browser

### Visualization Features
- **Nodes** are shown as circles, colored by type
- **Relationships** are shown as edges, labeled by type
- **Click** a node to see its properties
- **Drag** nodes to rearrange layout
- **Zoom** and pan to explore large graphs
- **Results** tab shows table view of data

### Query Tips
- Use **LIMIT** to see results faster (try: LIMIT 10)
- Use **WHERE** to filter results
- Use **RETURN** to see specific properties
- Use **ORDER BY** to sort results

### Keyboard Shortcuts
- **Ctrl+Enter** - Execute query
- **Up/Down arrows** - Navigate query history
- **Ctrl+L** - Clear editor

---

## 📚 Sample Data

The demo includes small tables loaded automatically:

- **Users** - 30 users with names and emails
- **Posts** - 50 posts created by users
- **Follows** - 60 follow relationships between users
- **Likes** - 80 likes on posts
- **Authored** - 50 authorship relationships

Total: ~270 rows (loads in seconds)

---

## 🛠️ Managing Services

### Stop Everything
```bash
cd demos/neo4j-browser
docker-compose down
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker logs clickhouse-demo      # Database
docker logs neo4j-demo           # Browser UI
docker logs clickgraph-demo      # Query engine
```

### Restart a Service
```bash
docker-compose restart clickgraph-demo
```

### Reset Data (Delete Volumes)
```bash
docker-compose down -v
docker-compose up -d
```

---

## ⚠️ Troubleshooting

### "Connection refused" for ClickGraph

**Problem**: Can't connect Neo4j Browser to ClickGraph (port 7687)

**Solution**:
```bash
# Check if ClickGraph is running
docker ps | grep clickgraph

# If not running, start it
docker-compose up -d clickgraph-demo

# View logs
docker logs clickgraph-demo
```

### Neo4j Browser not responding

**Problem**: Can't open http://localhost:7474

**Solution**:
```bash
# Wait longer for Neo4j to start
sleep 30

# Check if running
docker ps | grep neo4j

# View logs
docker logs neo4j-demo
```

### ClickHouse connection error

**Problem**: ClickGraph can't connect to database

**Solution**:
```bash
# Check ClickHouse status
docker logs clickhouse-demo

# Verify it's running
docker ps | grep clickhouse

# Wait for healthcheck
sleep 20
```

### Port Already in Use

**Problem**: Error: "port 7474 already allocated"

**Solution**:
```bash
# Find what's using port 7474
lsof -i :7474

# Or change port in docker-compose.yml
# Change: "7474:7474" to "7475:7474"
```

### Queries Run Slowly

**Problem**: Results take more than a few seconds

**Solution**:
- Use **LIMIT** to reduce results
- Avoid unbounded variable-length paths (use `*1..3` instead of `*`)
- Filter with **WHERE** before traversing

---

## 📖 Learn More

### ClickGraph Documentation
- [README](../../README.md) - Project overview
- [Cypher Language Reference](../../docs/wiki-versions/v0.6.0/wiki/Cypher-Language-Reference.md) - All supported syntax
- [Schema Basics](../../docs/wiki-versions/v0.6.0/wiki/Schema-Basics.md) - Understanding schemas

### Neo4j Browser Help
- [Neo4j Browser Docs](https://neo4j.com/developer/neo4j-browser/) - Official documentation
- [Cypher Guide](https://neo4j.com/developer/cypher/) - Learn Cypher queries

---

## 🐛 Known Limitations

- Read-only queries (no CREATE, UPDATE, DELETE)
- Neo4j Browser may cache query plans - refresh if queries seem slow
- Very large result sets (>10,000 rows) may be slow to visualize
- Some Neo4j-specific functions not supported

---

## 🎓 Next Steps

1. **Try all sample queries** above
2. **Modify queries** to explore different patterns
3. **Add more data** to social_demo.yaml if needed
4. **Read** docs for advanced features
5. **Performance testing?** See `tests/neo4j-browser/`

---

## 💬 Need Help?

- Query not working? → Check sample queries above
- Server not starting? → See Troubleshooting section
- More complex examples? → See README in ClickGraph root
- Report issues? → Check [Known Issues](../../KNOWN_ISSUES.md)

**Happy graphing! 🎉**
