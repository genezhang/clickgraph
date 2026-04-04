---
description: Generate a ClickGraph schema YAML from ClickHouse table metadata using an LLM
---

Generate a ClickGraph graph schema from an existing ClickHouse database. The LLM inspects your table structure and produces a ready-to-use schema YAML — no manual YAML writing required.

The user's input (if any): $ARGUMENTS

## Step 1 — Gather connection details

Collect from environment, config file (`~/.config/cg/config.toml`), or ask the user:

| Setting | Source |
|---------|--------|
| ClickHouse URL | `CG_CLICKHOUSE_URL` or `clickhouse.url` in config |
| Username | `CG_CLICKHOUSE_USER` or `clickhouse.user` in config (default: `default`) |
| Password | `CG_CLICKHOUSE_PASSWORD` or `clickhouse.password` in config |
| Database | `CG_CLICKHOUSE_DATABASE` or from `$ARGUMENTS` |
| Output file | From `$ARGUMENTS` or default to `./<database>_schema.yaml` |
| LLM key | `ANTHROPIC_API_KEY` (default) or `OPENAI_API_KEY` with `CG_LLM_PROVIDER=openai` |

If the ClickHouse URL or database name are missing, ask the user before proceeding.

## Step 2 — Discover the schema

```bash
cg schema discover \
  --clickhouse "$CG_CLICKHOUSE_URL" \
  --user "$CG_CLICKHOUSE_USER" \
  --password "$CG_CLICKHOUSE_PASSWORD" \
  --database <database> \
  --out <output_file>
```

This will:
1. Introspect all tables in the database (column names, types, PKs, sample rows)
2. Send the metadata to the LLM for analysis
3. Write the generated schema YAML to the output file

If the command fails, report the error. Common issues:
- ClickHouse unreachable — check URL and credentials
- No API key — set `ANTHROPIC_API_KEY` or `OPENAI_API_KEY`
- Empty database — check the database name with `cg schema discover --help`

## Step 3 — Show the generated schema

```bash
cg schema show --schema <output_file>
```

Display the schema in Cypher-native format so the user can review it:
- Node labels and their properties
- Relationship types and their directionality
- Any undirected relationships

## Step 4 — Validate

```bash
cg schema validate <output_file>
```

Report validation results. A valid schema is required before running queries.

## Step 5 — Next steps

Tell the user:
- Set `CG_SCHEMA=<output_file>` (or add to `~/.config/cg/config.toml`) to use this schema by default
- Use the `/cypher` skill to query the graph with natural language
- Edit the YAML manually for fine-tuning (property name mappings, relationship directions, etc.)

If the schema has issues or looks wrong, offer to re-run discovery with a different database or to compare against the previous version:

```bash
cg schema diff <old_file> <new_file>
```

## Notes

- Discovery is a one-time operation per database. Re-run when your schema changes significantly.
- The LLM may occasionally misclassify a table or miss an edge — always review before use.
- For databases with 40+ tables, `cg schema discover` batches automatically.
- Property names in the generated schema may differ from ClickHouse column names — this is intentional (e.g., `full_name` → `name`). Check the `property_mappings` section.
