# cg-schema - ClickGraph Schema Designer

ML-powered schema discovery tool for ClickGraph. Analyzes your database schema and suggests graph schema definitions using GLiNER NER.

## Installation

```bash
pip install cg-schema
```

## Quick Start

```bash
# Introspect database via ClickGraph server
cg-schema introspect --server localhost:8080 --database mydb

# Save to file
cg-schema introspect -s localhost:8080 -d mydb -o schema.yaml

# Push to server
cg-schema push schema.yaml -s localhost:8080
```

## Features

- **GLiNER Integration**: Uses zero-shot NER to classify tables and columns
- **Pattern Detection**: Automatically detects schema patterns (node, edge, fk-edge, denormalized)
- **Fallback Heuristics**: Works without GLiNER model using rule-based analysis
- **YAML Output**: Generates ClickGraph-compatible schema files

## Requirements

- ClickGraph server running (for introspection)
- Python 3.9+
- GLiNER model (auto-downloaded on first use, ~200MB)

## How It Works

1. **Connect to ClickGraph server** - Calls `/schemas/introspect` endpoint
2. **Analyze table names** - Uses GLiNER to classify as node/edge
3. **Analyze columns** - Extracts entity references, detects PK/FK
4. **Determine pattern** - Detects schema pattern (standard_node, standard_edge, etc.)
5. **Generate YAML** - Outputs ClickGraph-compatible schema

## GLiNER Labels

The tool uses these entity labels for classification:

| Label | Description | Example |
|-------|-------------|---------|
| node entity | Noun-like, thing | users, posts |
| relationship | Verb-like, action | follows, likes |
| event | Action/happening | logs, events |
| dimension | Lookup table | date_dim |

## Schema Patterns

| Pattern | Description | Detection |
|---------|-------------|-----------|
| standard_node | Entity table | Single PK, no FKs |
| standard_edge | Relationship table | Multiple FKs |
| fk_edge | FK on node table | Single PK + FK |
| denormalized_edge | Edge with embedded properties | origin_/dest_ columns |
| polymorphic_edge | Type-discriminated edge | type column + FKs |
| flat_table | No PK/FK detected | Requires review |

## Examples

### Introspect and view suggestions

```bash
$ cg-schema introspect -s localhost:8080 -d mydb

Connecting to ClickGraph server: http://localhost:8080
✓ Found 5 tables

┌────────────┬───────────────┬───────────────┬──────────┬──────────┬────────────────────┐
│ Table      │ Classification│ Pattern      │ PKs      │ FKs      │ Reason            │
├────────────┼───────────────┼───────────────┼──────────┼──────────┼────────────────────┤
│ users      │ node         │ standard_node │ user_id  │ -        │ Entity table      │
│ follows    │ edge         │ standard_edge │ follow_id│ user_id  │ Edge with 2 FKs   │
│ posts      │ node         │ fk_edge       │ post_id  │ user_id  │ FK-edge pattern   │
│ likes      │ edge         │ standard_edge │ like_id  │ user_id  │ Edge with 2 FKs   │
│ logs       │ unknown      │ flat_table    │ -        │ -        │ Cannot determine   │
└────────────┴───────────────┴───────────────┴──────────┴──────────┴────────────────────┘
```

### Generate and save schema

```bash
cg-schema introspect -s localhost:8080 -d mydb -o schema.yaml
```

Output `schema.yaml`:

```yaml
nodes:
  - label: User
    table: users
    id:
      column: user_id
    properties:
      name: name
      email: email

relationships:
  - type: FOLLOWS
    from:
      node: User
      id: user_id
    to:
      node: User
      id: user_id
    properties:
      created_at: created_at
```

### Push to server

```bash
cg-schema push schema.yaml -s localhost:8080
```

## Development

```bash
# Clone and install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black cg_schema/
ruff check cg_schema/
```

## License

Apache 2.0
