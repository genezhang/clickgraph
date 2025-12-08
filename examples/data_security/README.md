# Data Security Graph Example

A comprehensive example demonstrating ClickGraph's capabilities for security and data governance analysis using graph queries.

## Files

| File | Description |
|------|-------------|
| `data_security.yaml` | Graph schema mapping ClickHouse tables to nodes/edges |
| `setup_schema.sql` | SQL to create tables and load sample data |
| `security-graph-queries.md` | Example Cypher queries for security analysis |

## Quick Start

1. **Create the database and tables:**
   ```bash
   clickhouse-client < examples/data_security/setup_schema.sql
   ```

2. **Start ClickGraph with this schema:**
   ```bash
   GRAPH_CONFIG_PATH="./examples/data_security/data_security.yaml" \
   CLICKHOUSE_URL="http://localhost:8123" \
   cargo run --release
   ```

3. **Run queries** from `security-graph-queries.md`

## Graph Model

### Nodes
- **DataAsset** - Tables, files, databases (with sensitivity levels)
- **User** - Users who can access data
- **Role** - Permission groups
- **Policy** - Governance policies

### Relationships
- `HAS_ACCESS` - User → DataAsset (with permission level)
- `MEMBER_OF` - User → Role
- `GOVERNS` - Policy → DataAsset
- `CONTAINS` - DataAsset → DataAsset (hierarchy via `parent_id`)

## Use Cases

- **Access path analysis**: Who can access sensitive data?
- **Risk assessment**: Which high-sensitivity assets lack governance?
- **Compliance**: Are PII assets properly governed?
- **Data lineage**: Trace data flow through asset hierarchy

See `security-graph-queries.md` for detailed query examples.
