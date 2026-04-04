---
description: Show the ClickGraph graph schema — node labels, relationship types, and properties
---

Show the graph schema for the ClickGraph database connected to this project.

## Step 1 — Locate the schema

Find the schema file in this priority order:
1. `$CG_SCHEMA` environment variable
2. `schema.yaml` or `schemas/*.yaml` in the current working directory
3. `schema.path` in `~/.config/cg/config.toml`

If $ARGUMENTS is non-empty, treat it as the schema file path.

## Step 2 — Display the schema

```bash
cg --schema <path> schema show
```

This prints a compact Cypher-native view:
- Node labels with their properties and types
- Relationship types with directionality and endpoints
- Which relationships are undirected

Summarise the schema for the user in plain language: what entities exist, how they are connected, and which properties are available for querying.

## Step 3 — Validate (always run this)

```bash
cg --schema <path> schema validate
```

Report any validation issues. A valid schema is required before running queries.

## Step 4 — Suggest queries (optional)

Based on the schema, offer 2–3 example Cypher queries the user could run, matched to their apparent use case. Use the `/cypher` skill (or `cg nl`) to execute them.
