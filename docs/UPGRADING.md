# Upgrading to ClickGraph v0.1.0

This guide helps you upgrade from pre-release versions to ClickGraph v0.1.0.

## Breaking Changes

### 1. YAML Schema Field Rename (Relationship Definitions)

**What Changed:**  
Relationship field names have been renamed for improved semantic clarity and consistency.

| Old Field Name | New Field Name | Purpose |
|----------------|----------------|---------|
| `from_column` | `from_id` | Column identifying the source node |
| `to_column` | `to_id` | Column identifying the target node |

**Why the Change:**
- Improved semantic clarity - "id" indicates identity/key semantics
- Consistency with node schemas (which use `id_column`)
- Prepares for future composite key support
- No logic changes - pure field rename refactoring

### Migration Steps

#### Step 1: Update YAML Configuration Files

Find all relationship definitions in your YAML schema files and update the field names.

**Before (v0.0.x):**
```yaml
views:
  - name: social_network
    version: "1.0"
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
    relationships:
      follows:
        source_table: user_follows
        from_node: user
        to_node: user
        from_column: follower_id    # OLD
        to_column: followed_id      # OLD
```

**After (v0.1.0):**
```yaml
views:
  - name: social_network
    version: "1.0"
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
    relationships:
      follows:
        source_table: user_follows
        from_node: user
        to_node: user
        from_id: follower_id        # NEW
        to_id: followed_id          # NEW
```

#### Step 2: Automated Migration Script (PowerShell)

For Windows users, use this PowerShell script to update all YAML files:

```powershell
# update-yaml-schema.ps1
Get-ChildItem -Path . -Filter *.yaml -Recurse | ForEach-Object {
    $content = Get-Content $_.FullName -Raw
    $updated = $content -replace 'from_column:', 'from_id:' -replace 'to_column:', 'to_id:'
    
    if ($content -ne $updated) {
        Write-Host "Updating: $($_.FullName)"
        Set-Content -Path $_.FullName -Value $updated -NoNewline
    }
}
Write-Host "Migration complete!"
```

Run with:
```powershell
.\update-yaml-schema.ps1
```

#### Step 3: Automated Migration Script (Bash)

For Linux/macOS users:

```bash
#!/bin/bash
# update-yaml-schema.sh

find . -name "*.yaml" -type f -exec sed -i.bak \
  -e 's/from_column:/from_id:/g' \
  -e 's/to_column:/to_id:/g' {} \;

echo "Migration complete! Backup files created with .bak extension"
```

Run with:
```bash
chmod +x update-yaml-schema.sh
./update-yaml-schema.sh
```

#### Step 4: Verify Your Changes

1. **Check syntax:** Ensure all YAML files are still valid YAML
2. **Test loading:** Start ClickGraph and verify schemas load without errors
3. **Run queries:** Test your existing queries to ensure they still work

```bash
# Start ClickGraph
cargo run --bin clickgraph

# In another terminal, test a simple query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN n LIMIT 1"}'
```

## New Features in v0.1.0

### USE Clause for Database Selection

You can now select databases using the `USE` clause in Cypher queries:

```cypher
USE social_network
MATCH (u:User) RETURN u.name LIMIT 10
```

**Precedence order:**
1. USE clause (highest priority)
2. Session/request parameter (`schema_name` for HTTP, `database` for Bolt)
3. Default schema

### Bolt Protocol Multi-Database Support

Neo4j drivers can now specify the database when creating sessions:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")

# Select database via session parameter
with driver.session(database="social_network") as session:
    result = session.run("MATCH (u:User) RETURN u.name")
```

### Path Variables and Functions

Capture and analyze entire paths:

```cypher
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN 
  length(p) AS path_length,
  nodes(p) AS path_nodes,
  relationships(p) AS path_rels
```

### Query Performance Metrics

Monitor query performance via HTTP headers:

```bash
curl -i -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN n LIMIT 1"}'

# Response includes:
# X-Query-Total-Time: 45.23ms
# X-Query-Parse-Time: 1.12ms
# X-Query-Execution-Time: 35.66ms
```

## Compatibility

### Supported Platforms

| Platform | HTTP API | Bolt Protocol | Status |
|----------|----------|---------------|--------|
| Linux (Docker) | ✅ | ✅ | Fully functional |
| Linux (Native) | ✅ | ✅ | Fully functional |
| macOS | ✅ | ✅ | Fully functional |
| Windows (Native) | ✅ | ✅ | **Fixed in v0.1.0!** |
| WSL 2 | ✅ | ✅ | Fully functional |

### ClickHouse Version

- **Minimum:** ClickHouse 21.3+
- **Recommended:** ClickHouse 23.8+ for best performance
- **Tested:** ClickHouse 24.x

### Neo4j Driver Compatibility

- **Python:** neo4j-driver 5.x
- **JavaScript:** neo4j-driver 5.x
- **Java:** neo4j-java-driver 5.x
- **Go:** neo4j-go-driver 5.x

All drivers supporting Bolt protocol v4.4 should work.

## Rollback Instructions

If you need to rollback to a pre-release version:

### Step 1: Revert YAML Changes

```powershell
# PowerShell
Get-ChildItem -Path . -Filter *.yaml.bak -Recurse | ForEach-Object {
    $original = $_.FullName -replace '\.bak$', ''
    Copy-Item $_.FullName $original -Force
    Write-Host "Restored: $original"
}
```

```bash
# Bash
find . -name "*.yaml.bak" -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \;
```

### Step 2: Downgrade ClickGraph

```bash
# Check out previous commit
git checkout <previous-commit-hash>

# Rebuild
cargo build --release
```

## Getting Help

If you encounter issues during the upgrade:

1. **Check documentation:** See [docs/](docs/) for detailed guides
2. **Review known issues:** Check [KNOWN_ISSUES.md](KNOWN_ISSUES.md)
3. **Search existing issues:** https://github.com/genezhang/clickgraph/issues
4. **Create new issue:** Include your YAML schema and error messages

## Additional Resources

- **[RELEASE_NOTES_v0.1.0.md](RELEASE_NOTES_v0.1.0.md)** - Complete release notes
- **[README.md](README.md)** - Project overview
- **[docs/api.md](docs/api.md)** - API documentation
- **[STATUS.md](STATUS.md)** - Current capabilities and status



