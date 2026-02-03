# Neodash Integration Guide

## Overview

ClickGraph now supports Neo4j's extended schema procedures used by Neodash for rich metadata discovery. This enables Neodash's autocomplete, property selection, and schema visualization features.

## Supported Procedures

### db.schema.nodeTypeProperties()

Returns detailed property metadata for each node type (label).

**Output Columns:**
- `nodeType`: String in Neo4j format (e.g., `":\`User\`"`)
- `nodeLabels`: Array of label names (e.g., `["User"]`)
- `propertyName`: Property name (e.g., `"name"`)
- `propertyTypes`: Array of type names (e.g., `["String"]`)
- `mandatory`: Boolean - whether property always exists (always `true` in ClickGraph)

**Example:**
```cypher
CALL db.schema.nodeTypeProperties()
```

**Sample Output:**
```json
{
  "count": 36,
  "records": [
    {
      "nodeType": ":`User`",
      "nodeLabels": ["User"],
      "propertyName": "name",
      "propertyTypes": ["String"],
      "mandatory": true
    },
    {
      "nodeType": ":`User`",
      "nodeLabels": ["User"],
      "propertyName": "user_id",
      "propertyTypes": ["String"],
      "mandatory": true
    }
  ]
}
```

### db.schema.relTypeProperties()

Returns detailed property metadata for each relationship type.

**Output Columns:**
- `relType`: String in Neo4j format (e.g., `":\`FOLLOWS\`"`)
- `propertyName`: Property name (e.g., `"since"`)
- `propertyTypes`: Array of type names (e.g., `["DateTime"]`)
- `mandatory`: Boolean - whether property always exists (always `true` in ClickGraph)

**Example:**
```cypher
CALL db.schema.relTypeProperties()
```

**Sample Output:**
```json
{
  "count": 8,
  "records": [
    {
      "relType": ":`FOLLOWS`",
      "propertyName": "follow_date",
      "propertyTypes": ["String"],
      "mandatory": true
    }
  ]
}
```

## Neodash Setup

### Quick Start

1. **Start ClickGraph server:**
   ```bash
   export CLICKHOUSE_URL="http://localhost:8123"
   export CLICKHOUSE_USER="test_user"
   export CLICKHOUSE_PASSWORD="test_pass"
   export CLICKHOUSE_DATABASE="brahmand"
   export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
   
   ./target/release/clickgraph --http-port 8086 --bolt-port 7687
   ```

2. **Connect Neodash to ClickGraph:**
   - Open Neodash in your browser
   - Create new dashboard
   - Connect to database:
     - **Protocol**: `bolt://`
     - **Host**: `localhost`
     - **Port**: `7687`
     - **Database**: `brahmand` (or your configured database)
     - **Username**: Your ClickHouse username
     - **Password**: Your ClickHouse password

3. **Verify connection:**
   - Neodash will automatically call `db.schema.nodeTypeProperties()` and `db.schema.relTypeProperties()`
   - You should see node labels and properties in the autocomplete menus

### Neodash Features Enabled

With these procedures, Neodash can now:

✅ **Property Autocomplete**: When typing Cypher queries, Neodash shows available properties for each node/relationship type

✅ **Smart Filters**: Property selection dropdowns in report builders are populated with actual schema properties

✅ **Type Validation**: Neodash knows which properties exist on which types, preventing invalid queries

✅ **Schema Visualization**: Dashboard designers can see the full property schema

## Type Mapping

Currently, all properties are reported as type `"String"` since ClickGraph schemas don't store column type information. This doesn't affect functionality - Neodash will still display and filter values correctly.

**Future Enhancement**: Query ClickHouse system tables to get actual column types and map them:
- `UInt64`, `Int64` → `"Long"`
- `String` → `"String"`
- `DateTime`, `DateTime64` → `"DateTime"`
- `Date` → `"Date"`
- `Float64` → `"Double"`
- `Bool` → `"Boolean"`

## Comparison with Neo4j Browser

| Tool | Procedure Used | Property Source Info |
|------|---------------|---------------------|
| **Neo4j Browser** | `db.propertyKeys()` | ❌ Property names only |
| **Neodash** | `db.schema.nodeTypeProperties()` | ✅ Type + properties + types |
| **ClickGraph** | Both supported | ✅ Full compatibility |

Neo4j Browser still uses the simpler `db.propertyKeys()` procedure (which ClickGraph also supports), so it sends generic `UNION ALL` queries to find which entities have a given property. Neodash's richer procedures avoid this inefficiency.

## Troubleshooting

### "Unknown procedure" error

If you get `Unknown procedure: db.schema.nodeTypeProperties`, ensure you're running the latest ClickGraph build:

```bash
git pull origin main
cargo build --release
```

### Empty results

If procedures return 0 records:
1. Check that your schema YAML is loaded: `CALL db.labels()` should return node labels
2. Verify schema has property mappings defined
3. Check server logs for schema loading errors

### Connection refused

If Neodash can't connect:
1. Verify Bolt server is running: Check for `"Bolt server loop starting"` in logs
2. Test Bolt connection: `telnet localhost 7687` should connect
3. Check firewall/network settings

## Example Neodash Dashboard

Here's a simple dashboard query that leverages schema metadata:

```cypher
// Neodash will autocomplete properties based on db.schema.nodeTypeProperties()
MATCH (u:User)
WHERE u.country = $neodash_country  // Neodash knows 'country' exists on User
RETURN u.name, u.city, u.registration_date
ORDER BY u.registration_date DESC
LIMIT 10
```

Neodash will:
- Suggest `country`, `name`, `city`, `registration_date` in autocomplete
- Create parameter input for `$neodash_country`
- Validate that these properties exist on `User` nodes

## References

- [Neodash Official Site](https://neo4j.com/labs/neodash/)
- [Neodash GitHub](https://github.com/neo4j-labs/neodash)
- [Neo4j Schema Procedures Documentation](https://neo4j.com/docs/operations-manual/current/reference/procedures/)
