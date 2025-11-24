# Neo4j Semantics Testing - Quick Setup

## Prerequisites

1. **Docker** (for Neo4j)
2. **Python 3.7+** with pip

## Step 1: Start Neo4j

```powershell
# Windows PowerShell
docker run -d `
  --name neo4j-test `
  -p 7474:7474 -p 7687:7687 `
  -e NEO4J_AUTH=neo4j/testpassword `
  neo4j:latest

# Wait for Neo4j to start (about 30 seconds)
Start-Sleep -Seconds 30

# Verify it's running
docker logs neo4j-test
```

Or access Neo4j Browser: http://localhost:7474
- Username: `neo4j`
- Password: `testpassword`

## Step 2: Install Python Dependencies

```powershell
pip install neo4j
```

## Step 3: Run Tests

```powershell
python scripts\test\neo4j_semantics_verification.py
```

## Step 4: Review Results

The script will output:
- ✅ Each test result with data
- 📊 Summary analysis
- 🎯 Recommendations for ClickGraph

Expected runtime: **2-3 minutes**

## Step 5: Cleanup (Optional)

```powershell
# Stop and remove Neo4j container
docker stop neo4j-test
docker rm neo4j-test
```

---

## Troubleshooting

### Neo4j connection fails
```powershell
# Check if Neo4j is running
docker ps | Select-String neo4j

# Check logs for errors
docker logs neo4j-test

# Wait longer for startup
Start-Sleep -Seconds 60
```

### Port already in use
```powershell
# Use different ports
docker run -d `
  --name neo4j-test `
  -p 7475:7474 -p 7688:7687 `
  -e NEO4J_AUTH=neo4j/testpassword `
  neo4j:latest

# Update script NEO4J_URI to bolt://localhost:7688
```

### Python neo4j driver not found
```powershell
pip install --upgrade neo4j
```

---

## Expected Output

```
======================================================================
Neo4j Semantics Verification
Testing cycle prevention and node uniqueness behavior
======================================================================
✅ Connected to Neo4j

======================================================================
Setting Up Test Data
======================================================================
✅ Cleared existing data
✅ Created 4 users
✅ Created FOLLOWS relationships
   Topology: 1 -> 2 -> 3 -> 1 (cycle)
             1 -> 3 (direct)
   Total: 4 users, 4 relationships

======================================================================
Test 1: Directed Variable-Length (*2)
======================================================================
❓ Question: Does Neo4j allow (a)-[:FOLLOWS*2]->(a) (returning to start)?
📖 Expected: If prevents cycles: No (1,1). If allows: May have (1,1) or (1,3)

🔍 Query:
MATCH (a:User)-[:FOLLOWS*2]->(c:User)
WHERE a.user_id = 1
RETURN a.user_id, c.user_id
ORDER BY c.user_id

📊 Results (X rows):
   1. {'a.user_id': 1, 'c.user_id': Y}
   ...

[... 9 more tests ...]

======================================================================
SUMMARY OF FINDINGS
======================================================================

📊 Results Analysis:

1. Directed *2: ✅ PREVENTS cycles / ❌ ALLOWS cycles
2. Explicit 2-hop: ✅ PREVENTS cycles / ❌ ALLOWS cycles
3. Undirected 1-hop: ✅ ENFORCES a!=b
4. Friends-of-Friends: ✅ Excludes self / ❌ Returns self (BUG)
5. Undirected *2: ✅ PREVENTS cycles
...

======================================================================
CLICKGRAPH COMPATIBILITY RECOMMENDATIONS
======================================================================

🎯 Based on these findings, ClickGraph should:

✅ KEEP cycle prevention for directed variable-length (*2)
✅ ADD cycle prevention for explicit directed patterns
✅ FIX friends-of-friends to exclude start node (OpenCypher spec)
...
```

---

## What This Tests

| Test | Pattern | Question |
|------|---------|----------|
| 1 | `(a)-[:FOLLOWS*2]->(c)` | Cycles in directed variable-length? |
| 2 | `(a)-[:F]->(b)-[:F]->(c)` | Cycles in explicit patterns? |
| 3 | `(a)-[:F]-(b)` | Undirected a != b? |
| 4 | `(u)-[]-(f)-[]-(fof)` | Friends-of-friends excludes self? |
| 5 | `(a)-[:F*2]-(c)` | Undirected variable-length cycles? |
| 6 | `(a)-[:F]->(b)-[:F]-(c)` | Mixed direction behavior? |
| 7 | `(a)-[]-(b)-[]-(c)` | All nodes unique? |
| 8 | Two MATCH clauses | Cross-clause uniqueness? |
| 9 | `(a)-[:F*1..]->(c)` | Max recursion depth? |
| 10 | Relationship IDs | Relationship uniqueness? |

---

## Timeline

- **Setup**: 5 minutes (Docker + pip)
- **Test run**: 2-3 minutes
- **Analysis**: 10-15 minutes
- **Implementation**: 2-4 hours (based on findings)

**Total**: ~3-4 hours for complete Neo4j compatibility

---

## Next Steps After Testing

1. **Document findings** in comparison table
2. **Implement changes** to match Neo4j exactly
3. **Add tests** for each verified behavior
4. **Update documentation** with Neo4j compatibility notes
