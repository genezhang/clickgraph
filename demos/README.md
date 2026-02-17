# ClickGraph Demos

**Interactive demonstrations for exploring ClickGraph capabilities**

This directory contains end-user focused demos for different use cases and tools.

---

## 🎯 Available Demos

### 1. [Neo4j Browser](./neo4j-browser/)
**Visual graph exploration with Neo4j Browser UI**

- ✨ One-command setup with Docker Compose
- 🎨 Interactive graph visualization
- 📊 Real-time query results
- 🚀 Self-contained with sample data (270 rows)

**Best for**: Quick start, visual exploration, demos

```bash
cd neo4j-browser
bash setup.sh
# Open http://localhost:7474
```

---

### 2. [Graph-Notebook](./graph-notebook/)
**Jupyter notebooks for analytics and visualization**

- 📓 Interactive Python notebooks
- 📈 Chart visualizations and data analysis
- 🔍 Schema exploration and discovery
- 💻 Programmatic query building

**Best for**: Data analysis, reporting, tutorials, documentation

```bash
cd graph-notebook
bash setup.sh
# Open http://localhost:8888
```

---

## 📋 Demo Comparison

| Feature | Neo4j Browser | Graph-Notebook |
|---------|--------------|----------------|
| **Setup Time** | 2 minutes | 3 minutes |
| **Interface** | Web UI | Jupyter Notebook |
| **Use Case** | Visual exploration | Analytics & Reports |
| **Learning Curve** | Easy | Moderate |
| **Data Export** | Limited | Full (CSV, JSON, pandas) |
| **Visualization** | Graph only | Graphs + Charts |
| **Automation** | Manual queries | Python scripting |
| **Best For** | Demos, exploration | Analysis, documentation |

---

## 🚀 Quick Start

### Prerequisites

Both demos require:
- Docker and Docker Compose
- 2GB free disk space
- Linux/macOS/WSL (for bash scripts)

### Choose Your Demo

**Want visual exploration?** → Use Neo4j Browser  
**Want analytics/notebooks?** → Use Graph-Notebook  
**Want both?** → Run both! (different ports)

---

## 📂 Demo Structure

Each demo folder contains:
- `README.md` - Detailed setup and usage guide
- `setup.sh` - One-command setup script
- `docker-compose.yml` - Service configuration
- Sample data and queries
- Troubleshooting guides

---

## 🎓 Learning Path

**Beginner**: Start with Neo4j Browser demo
1. Basic node queries (`MATCH (n) RETURN n`)
2. Relationship traversals (`MATCH (a)-[r]->(b)`)
3. Filtering with WHERE
4. Visual graph exploration

**Intermediate**: Move to Graph-Notebook
1. Programmatic query building
2. Data export and analysis
3. Result visualization with charts
4. Integration with Python tools

**Advanced**: Combine both
1. Explore in Neo4j Browser
2. Analyze in Graph-Notebook
3. Build production queries
4. Create reporting dashboards

---

## 📚 Additional Resources

- **[ClickGraph Documentation](../docs/wiki/Home.md)** - Complete documentation
- **[Cypher Language Support](../docs/wiki/Cypher-Language-Support.md)** - Query syntax
- **[Examples](../examples/)** - Code examples and patterns
- **[Main README](../README.md)** - Project overview

---

## 🐛 Troubleshooting

### Port Conflicts

If ports are in use:
```bash
# Check what's using ports
netstat -tlnp | grep -E '7474|7687|8080|8888'

# Change ports in docker-compose.yml
# Example: "7475:7474" instead of "7474:7474"
```

### Services Not Starting

```bash
# View logs
docker-compose logs -f

# Restart services
docker-compose down
docker-compose up -d
```

### Data Not Loading

```bash
# Neo4j Browser demo
cd neo4j-browser
bash setup_demo_data.sh

# Graph-Notebook demo (uses same data)
cd ../neo4j-browser
bash setup_demo_data.sh
```

---

## 🤝 Contributing

Found an issue or have an improvement?
1. Check existing issues on GitHub
2. Submit a pull request
3. Update documentation

---

## 📝 License

See [LICENSE](../LICENSE) file in project root.

---

Happy graphing! 🚀
