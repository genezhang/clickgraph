# GitHub Release Description for v0.1.0

**Copy and paste this into the GitHub release form at:**
https://github.com/genezhang/clickgraph/releases/new?tag=v0.1.0

---

## ClickGraph v0.1.0 - First Official Release :tada:

**Enterprise-scale graph analytics on ClickHouse with Neo4j compatibility.**

### Release Highlights

- :white_check_mark: **Successfully tested on 5 million users and 50 million relationships** (90% success rate)
- :white_check_mark: **Full Neo4j Bolt protocol v4.4 support** for seamless driver integration
- :white_check_mark: **USE clause syntax** matching Neo4j 4.0+ conventions
- :white_check_mark: **318/318 tests passing** (100% success rate) - Production-ready quality
- :white_check_mark: **Windows native support** - HTTP and Bolt protocols fully functional
- :white_check_mark: **Query performance monitoring** with built-in metrics and HTTP headers

### :rocket: Major Features

#### 1. USE Clause for Database Selection
Neo4j 4.0+ compatible database selection directly in Cypher queries.

```cypher
USE social_network
MATCH (u:User)-[:FOLLOWS]->(friend)
RETURN u.name, collect(friend.name) AS friends
```

- Three-way precedence: USE clause > session/request parameter > default schema
- Case-insensitive (USE/use/Use)
- Qualified names (`USE neo4j.database`)

#### 2. Bolt Protocol Multi-Database Support
Full Neo4j 4.0+ compatibility via Bolt protocol.

```python
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session(database="social_network") as session:
    result = session.run("MATCH (u:User) RETURN u.name")
```

#### 3. Path Variables & Functions
Complete path capture and analysis.

```cypher
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN length(p), nodes(p), relationships(p)
```

#### 4. Query Performance Metrics
Built-in monitoring for production deployments.

```bash
curl -i http://localhost:8080/query
# Returns headers:
# X-Query-Total-Time: 45.23ms
# X-Query-Execution-Time: 35.66ms
```

#### 5. CASE Expressions
Full conditional expression support.

```cypher
MATCH (u:User)
RETURN CASE WHEN u.age > 30 THEN 'Senior' ELSE 'Junior' END
```

### :bar_chart: Benchmark Results

| Scale | Dataset | Success Rate | Status |
|-------|---------|--------------|--------|
| **Large** | 5M users, 50M relationships | 90% (9/10) | :white_check_mark: Enterprise-scale validated |
| **Medium** | 10K users, 50K relationships | 100% (10/10) | :white_check_mark: Production-ready |
| **Small** | 1K users, 5K relationships | 100% (10/10) | :white_check_mark: Fully tested |

### :warning: Breaking Changes

**YAML Schema Field Rename:**
- `from_column` → `from_id`
- `to_column` → `to_id`

See [UPGRADING.md](https://github.com/genezhang/clickgraph/blob/main/UPGRADING.md) for migration instructions and automated scripts.

### :package: Installation

**Docker (Recommended):**
```bash
git clone https://github.com/genezhang/clickgraph
cd clickgraph
docker-compose up -d
```

**Native Build:**
```bash
cargo build --release
export CLICKHOUSE_URL="http://localhost:8123"
cargo run --bin clickgraph
```

### :link: Platform Support

| Platform | HTTP | Bolt | Status |
|----------|------|------|--------|
| Linux (Docker/Native) | :white_check_mark: | :white_check_mark: | Fully functional |
| macOS | :white_check_mark: | :white_check_mark: | Fully functional |
| **Windows (Native)** | :white_check_mark: | :white_check_mark: | **Fixed in v0.1.0!** |
| WSL 2 | :white_check_mark: | :white_check_mark: | Fully functional |

### :books: Documentation

- **[RELEASE_NOTES_v0.1.0.md](https://github.com/genezhang/clickgraph/blob/main/RELEASE_NOTES_v0.1.0.md)** - Complete release notes
- **[UPGRADING.md](https://github.com/genezhang/clickgraph/blob/main/UPGRADING.md)** - Migration guide
- **[README.md](https://github.com/genezhang/clickgraph/blob/main/README.md)** - Project overview
- **[docs/api.md](https://github.com/genezhang/clickgraph/blob/main/docs/api.md)** - API documentation

### :dart: Known Limitations

- Pattern comprehensions (`[(a)-[]->(b) | b.name]`) - Not yet implemented
- Subqueries (`CALL { ... }`) - Not yet implemented
- Write operations - Out of scope (read-only engine by design)

See [KNOWN_ISSUES.md](https://github.com/genezhang/clickgraph/blob/main/KNOWN_ISSUES.md) for details.

### :test_tube: Testing

- **Unit Tests:** 318/318 passing (100%)
- **End-to-End:** Comprehensive validation
- **Benchmarks:** 3-tier validation (small, medium, large)

### :pray: Acknowledgments

ClickGraph is a fork of the Brahmand project with significant enhancements including Bolt protocol support, multi-database capabilities, path variables, performance monitoring, and Windows native support.

---

**Full Changelog:** https://github.com/genezhang/clickgraph/blob/main/CHANGELOG.md

**Thank you for using ClickGraph!** :tada:
