# ClickGraph + AWS Graph-Notebook Demo

**Interactive Jupyter notebooks for graph exploration and visualization**

This demo shows how to use AWS graph-notebook with ClickGraph for interactive graph analytics in Jupyter notebooks.

---

## 📚 What is AWS Graph-Notebook?

[AWS graph-notebook](https://github.com/aws/graph-notebook) is a Jupyter notebook extension for visualizing and exploring graph data. Originally built for AWS Neptune, it works with any Bolt-compatible graph database including ClickGraph.

**Key Features**:
- 🎨 Interactive graph visualization
- 📊 Schema discovery and exploration
- 🔍 Query building and testing
- 📈 Result visualization with charts
- 💻 Python SDK integration

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+ with pip
- Jupyter Notebook or JupyterLab
- ClickGraph server running (see [main README](../../README.md))

### 1. Install graph-notebook

```bash
pip install graph-notebook
jupyter nbextension install --py --sys-prefix graph_notebook.widgets
jupyter nbextension enable --py --sys-prefix graph_notebook.widgets
```

### 2. Start ClickGraph with Neo4j Compatibility Mode

ClickGraph requires Neo4j compatibility mode for graph-notebook integration:

```bash
# Option 1: Environment variable
export CLICKGRAPH_NEO4J_COMPAT_MODE=true
cargo run --bin clickgraph

# Option 2: CLI flag
cargo run --bin clickgraph -- --neo4j-compat-mode

# Option 3: Using startup script
./scripts/server/start_server_background.sh --neo4j-compat-mode
```

### 3. Open the Demo Notebook

```bash
cd demos/graph-notebook
jupyter notebook clickgraph-demo.ipynb
```

---

## 📖 Tutorial: Getting Started

### Connect to ClickGraph

```python
from graph_notebook.configuration.generate_config import Configuration
from graph_notebook.notebooks.nbclient import get_client

# Configure connection
config = Configuration(
    host='localhost',
    port=7687,
    auth_mode='NONE',  # Or 'BASIC' with username/password
    iam_credentials_provider_type='NONE',
    load_from_s3_arn='',
    ssl=False,
    aws_region='us-east-1'
)

# Create client
client = get_client(config)
print("✓ Connected to ClickGraph")
```

### Run Your First Query

```python
%%oc
MATCH (u:User)
RETURN u.name, u.email
LIMIT 5
```

### Visualize Relationships

```python
%%oc
MATCH (u:User)-[r:FOLLOWS]->(f:User)
RETURN u, r, f
LIMIT 20
```

The graph visualization will automatically render!

---

## 📋 What Works with ClickGraph

### ✅ Core Features

**Connection & Authentication**
- ✅ Bolt Protocol v5.8
- ✅ Basic authentication (username/password)
- ✅ No authentication mode
- ✅ Connection pooling

**Schema Discovery**
- ✅ `CALL db.labels()` - List all node labels
- ✅ `CALL db.relationshipTypes()` - List relationship types
- ✅ `CALL db.propertyKeys()` - List all property keys
- ✅ `CALL db.schema.nodeTypeProperties()` - Node property metadata
- ✅ `CALL db.schema.relTypeProperties()` - Relationship property metadata

**Query Features**
- ✅ Node and relationship patterns
- ✅ Property filtering with WHERE
- ✅ Aggregations (count, sum, avg, min, max)
- ✅ ORDER BY, LIMIT, SKIP
- ✅ DISTINCT results
- ✅ Parameterized queries
- ✅ Variable-length paths (`*`, `*1..3`)
- ✅ Path functions (length, nodes, relationships)

**Visualization**
- ✅ Graph rendering with nodes and edges
- ✅ Property display on hover
- ✅ Result tables
- ✅ Chart visualizations

### ⚠️ Known Limitations

**Database Selection**
- Graph-notebook sends `USE database` commands
- Workaround: Set database via `GRAPH_CONFIG_PATH` environment variable
- Multi-database switching within notebook not supported yet

**Write Operations**
- ClickGraph is read-only (by design)
- CREATE, DELETE, MERGE, SET not supported

---

## 📂 Files in This Demo

- **README.md** - This file (setup and usage guide)
- **clickgraph-demo.ipynb** - Interactive tutorial notebook
- **compatibility-guide.md** - Detailed compatibility reference
- **requirements.txt** - Python dependencies

---

## 🔧 Troubleshooting

### Connection Refused

**Problem**: `ConnectionRefusedError: [Errno 111] Connection refused`

**Solution**:
1. Verify ClickGraph is running: `curl http://localhost:7475/health`
2. Check Bolt port: `netstat -tlnp | grep 7687`
3. Ensure Neo4j compatibility mode is enabled

### Schema Discovery Fails

**Problem**: `CALL db.labels()` returns empty

**Solution**:
1. Verify schema is loaded: Check `GRAPH_CONFIG_PATH` environment variable
2. Restart ClickGraph with valid schema YAML
3. Test with basic query: `MATCH (n) RETURN count(n)`

### Visualization Not Rendering

**Problem**: Queries work but graph doesn't display

**Solution**:
1. Check Jupyter extensions: `jupyter nbextension list`
2. Reinstall graph_notebook widgets: `jupyter nbextension install --py graph_notebook.widgets`
3. Restart Jupyter kernel

### "USE database" Errors

**Problem**: Graph-notebook sends `USE social` which fails

**Known Issue**: Database switching not implemented yet

**Workaround**: 
- Set `GRAPH_CONFIG_PATH` to point to your schema YAML
- Ignore USE command warnings (queries still work)

---

## 📚 Additional Resources

- **[Graph-Notebook Compatibility Guide](./compatibility-guide.md)** - Complete feature list
- **[ClickGraph Documentation](../../docs/wiki/Home.md)** - Full documentation
- **[AWS Graph-Notebook GitHub](https://github.com/aws/graph-notebook)** - Upstream project
- **[Cypher Language Reference](../../docs/wiki/Cypher-Language-Support.md)** - Supported Cypher features

---

## 🎯 Next Steps

1. **Explore the Tutorial** - Open `clickgraph-demo.ipynb`
2. **Try Your Data** - Load your own schema and data
3. **Build Dashboards** - Create visualization notebooks
4. **Share Insights** - Export notebooks with results

---

## 💡 Tips

- Use `%%oc` magic command for Cypher queries in cells
- Press `Shift+Enter` to run cells and see results
- Graph visualizations support zoom and pan
- Export results to pandas DataFrames for analysis
- Use `LIMIT` to keep visualizations manageable

---

## 📝 Example Queries

### Find Influencers
```cypher
MATCH (u:User)<-[:FOLLOWS]-(f)
RETURN u.name, count(f) as followers
ORDER BY followers DESC
LIMIT 10
```

### Community Detection
```cypher
MATCH (u:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
WHERE u <> fof
RETURN u.name, collect(DISTINCT fof.name) as mutual_friends
LIMIT 20
```

### Content Engagement
```cypher
MATCH (u:User)-[:AUTHORED]->(p:Post)<-[:LIKED]-(liker)
RETURN p.content, u.name as author, count(liker) as likes
ORDER BY likes DESC
LIMIT 10
```

Happy graphing! 🚀
