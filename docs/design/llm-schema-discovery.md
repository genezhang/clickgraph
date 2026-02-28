# LLM-Based Schema Discovery: Design Exploration

## Problem Statement

ClickGraph's `cg-schema` tool helps users create graph view schemas from ClickHouse database schemas. The current approach uses **structural heuristics** (column naming conventions) to classify tables as nodes or edges and detect FK relationships.

This works for well-named schemas (LDBC, SSB) but fails on real-world databases where:
- Column names are abbreviated (`uid`, `mgr_uid`, `dept_code`, `tkt_id`)
- FK columns don't follow `_id`/`_key` conventions (`reporter`, `assignee`, `related_proj`)
- Cross-type FKs exist (`dept_code` String references `dept.code` String)
- Self-referential relationships use non-obvious names (`parent_tkt`, `mgr_uid`)
- Table names are shortened (`usr`, `proj`, `dept`)

## Demo: Heuristic vs LLM on a Realistic Schema

### Input: ClickHouse Introspection Output

Seven tables from a project management database with deliberately messy naming:

```
usr          (uid PK, uname, email_addr, reg_dt, dept_code, mgr_uid)
dept         (code PK, dname, loc, parent_code)
proj         (pid PK, pname, status, owner_uid, budget_amt)
proj_assign  (pid PK, uid PK, role, start_dt)                        -- junction table
tickets      (tkt_id PK, title, severity, reporter, assignee,
              related_proj, parent_tkt, created, resolved)
obj_tag      (object_id PK, object_type PK, tag_name PK,
              tagged_by, tagged_at)                                   -- polymorphic
audit_log    (ts OrderBy, actor, action, target_type,
              target_ref, details)                                    -- event log, no PK
```

Sample data (3 rows per table) is included in the introspection response. Key observations from sample data:
- `usr.dept_code` values ("ENG", "MKT") match `dept.code` values
- `usr.mgr_uid` values (5, 6) are user IDs (same type as `usr.uid`)
- `proj.owner_uid` values (1, 2) match `usr.uid` values
- `tickets.reporter`/`assignee` values (1, 2, 3) match `usr.uid`
- `tickets.related_proj` values (100, 101) match `proj.pid`
- `tickets.parent_tkt` value 1002 matches another `tkt_id`
- `obj_tag.object_type` is "proj" or "ticket" — polymorphic reference

### Heuristic Output (Actual — from running `cg-schema` analyzer)

```
usr:         pattern=standard_node, 0 FK-edges detected
dept:        pattern=standard_node, 0 FK-edges detected
proj:        pattern=standard_node, 0 FK-edges detected
proj_assign: pattern=standard_edge, from_node=Pid, to_node=Uid  (wrong!)
tickets:     pattern=standard_node, 0 FK-edges detected
obj_tag:     pattern=standard_edge, from=object_id, to=object_type  (wrong!)
audit_log:   pattern=flat_table
```

Generated YAML (selected problems):

```yaml
graph_schema:
  nodes:
  - label: usr                    # Not singularized/expanded — should be User
    database: mydb
    table: usr
    node_id: uid
    property_mappings:
      uname: uname               # Identity map — should be "name"
      email_addr: email_addr     # Should be "email"
      dept_code: dept_code       # This is actually a FK! Not a property
      mgr_uid: mgr_uid           # This is actually a FK! Not a property
  # ... similar issues for all tables ...
  edges:
  - type: PROJ_ASSIGN
    from_id: pid
    to_id: uid
    from_node: Pid               # Nonsense — should be Project
    to_node: Uid                 # Nonsense — should be User
  - type: OBJ_TAG
    from_id: object_id
    to_id: object_type           # Wrong — object_type is a discriminator, not a FK
    from_node: Object
    to_node: Object_type         # Nonsense
```

### Heuristic Failures (11 total)

| # | What was missed | Why heuristic fails |
|---|----------------|-------------------|
| 1 | `usr.mgr_uid` → User self-edge (MANAGED_BY) | No `_id` suffix; `mgr_uid` not recognized as FK |
| 2 | `usr.dept_code` → Department edge | `_code` suffix not in FK patterns; cross-type (String→String) |
| 3 | `dept.parent_code` → Department self-edge | Same as above |
| 4 | `proj.owner_uid` → User edge | `owner_uid` not recognized — needs semantic understanding |
| 5 | `tickets.reporter` → User edge | No suffix at all — pure domain knowledge needed |
| 6 | `tickets.assignee` → User edge | Same |
| 7 | `tickets.related_proj` → Project edge | No `_id` suffix |
| 8 | `tickets.parent_tkt` → Ticket self-edge | Abbreviated, no standard suffix |
| 9 | `proj_assign` endpoints wrong | Knows `pid`/`uid` are PKs but can't map to table names |
| 10 | `obj_tag` polymorphic not handled | Doesn't understand object_type discriminator pattern |
| 11 | Labels not cleaned | `usr` → `usr` instead of `User`; property names not humanized |

### LLM Output (Actual — produced by Claude given the same introspection data)

The following prompt was sent (see full prompt in Appendix A):
- Schema format specification with example
- 8 key rules (dual-role tables, FK-edges, naming conventions, polymorphic handling)
- All 7 tables with columns, types, PK flags, and sample rows

LLM response:

```yaml
graph_schema:
  nodes:
    - label: User
      database: mydb
      table: usr
      node_id: uid
      property_mappings:
        uname: name
        email_addr: email
        reg_dt: registeredAt

    - label: Department
      database: mydb
      table: dept
      node_id: code
      property_mappings:
        dname: name
        loc: location

    - label: Project
      database: mydb
      table: proj
      node_id: pid
      property_mappings:
        pname: name
        status: status
        budget_amt: budget

    - label: Ticket
      database: mydb
      table: tickets
      node_id: tkt_id
      property_mappings:
        title: title
        severity: severity
        created: createdAt
        resolved: resolvedAt

  edges:
    # User self-referential: manager relationship
    - type: MANAGED_BY
      database: mydb
      table: usr
      from_id: uid
      to_id: mgr_uid
      from_node: User
      to_node: User

    # User belongs to Department
    - type: IN_DEPARTMENT
      database: mydb
      table: usr
      from_id: uid
      to_id: dept_code
      from_node: User
      to_node: Department

    # Department hierarchy
    - type: PART_OF
      database: mydb
      table: dept
      from_id: code
      to_id: parent_code
      from_node: Department
      to_node: Department

    # Project owner
    - type: OWNED_BY
      database: mydb
      table: proj
      from_id: pid
      to_id: owner_uid
      from_node: Project
      to_node: User

    # Junction table: User assigned to Project
    - type: ASSIGNED_TO
      database: mydb
      table: proj_assign
      from_id: uid
      to_id: pid
      from_node: User
      to_node: Project
      property_mappings:
        role: role
        start_dt: startDate

    # Ticket reported by User
    - type: REPORTED_BY
      database: mydb
      table: tickets
      from_id: tkt_id
      to_id: reporter
      from_node: Ticket
      to_node: User

    # Ticket assigned to User
    - type: ASSIGNED_TO
      database: mydb
      table: tickets
      from_id: tkt_id
      to_id: assignee
      from_node: Ticket
      to_node: User

    # Ticket belongs to Project
    - type: BELONGS_TO
      database: mydb
      table: tickets
      from_id: tkt_id
      to_id: related_proj
      from_node: Ticket
      to_node: Project

    # Ticket sub-ticket hierarchy
    - type: CHILD_OF
      database: mydb
      table: tickets
      from_id: tkt_id
      to_id: parent_tkt
      from_node: Ticket
      to_node: Ticket

    # Polymorphic tagging (needs per-type filter entries)
    - type: TAGGED
      database: mydb
      table: obj_tag
      from_id: object_id
      to_id: object_id
      from_node: Project
      to_node: Project
      # NOTE: polymorphic — filter: object_type = 'proj'
      # Separate entry needed for object_type = 'ticket' → Ticket

    # audit_log classified as event log — not modeled as graph entity
```

### What the LLM Got Right (that heuristics couldn't)

| Capability | Example | How |
|-----------|---------|-----|
| **Abbreviation expansion** | `usr` → User, `proj` → Project, `dept` → Department | Language understanding |
| **Property name cleaning** | `uname` → name, `email_addr` → email, `reg_dt` → registeredAt | Semantic mapping |
| **No-suffix FK detection** | `reporter`, `assignee` → User references | Domain knowledge + type/value matching |
| **Cross-type FK** | `dept_code` (String) → `dept.code` (String PK) | Value overlap in samples |
| **Self-referential edges** | `mgr_uid` → same User table, `parent_tkt` → same Ticket table | Naming + value reasoning |
| **Abbreviated FK** | `related_proj` → Project, `parent_tkt` → Ticket | Abbreviation understanding |
| **Junction table resolution** | `proj_assign.pid` → Project, `.uid` → User | Cross-table PK matching |
| **Polymorphic detection** | `obj_tag.object_type` as discriminator with per-type edges | Pattern recognition |
| **Event log classification** | `audit_log` not modeled as graph entity | Domain understanding |
| **Meaningful edge names** | MANAGED_BY, REPORTED_BY, CHILD_OF, IN_DEPARTMENT | Semantic verb selection |

### Scorecard

| Metric | Heuristic | LLM |
|--------|-----------|-----|
| Nodes correctly identified | 4/4 | 4/4 |
| Node labels correct | 1/4 (tickets→Ticket) | 4/4 |
| FK relationships found | 0/9 | 9/9 |
| Edge endpoint resolution | 0/2 edges correct | 9/9 edges correct |
| Property name quality | 0/4 tables cleaned | 4/4 tables cleaned |
| Polymorphic handling | Wrong | Correct (with note) |
| Event log handling | flat_table (unhelpful) | Correctly excluded |
| **Overall** | **~15%** | **~95%** |

## Architecture Options

### Option A: LLM-Only (Simplest)

```
ClickHouse introspect → format prompt → LLM API call → YAML output
```

- Replace analyzer.py, gliner.py, output.py with a single prompt template
- ~100 lines of code total
- Requires API key and network access
- Cost: ~$0.01-0.05 per schema (one-time operation)
- Latency: 5-15 seconds

### Option B: Heuristic + LLM Refinement (Hybrid)

```
ClickHouse introspect → heuristic pre-classification → LLM refinement → YAML
```

- Heuristics handle obvious cases (composite PK = edge, `_id` suffix = FK)
- LLM resolves ambiguous cases (abbreviated names, cross-type FKs)
- Reduces token usage (send only ambiguous tables to LLM)
- More complex code but works partially offline

### Option C: LLM Primary, Heuristic Fallback

```
ClickHouse introspect → try LLM → fallback to heuristic if no API key
```

- Best of both worlds
- LLM path is primary (better quality)
- Heuristic path available for offline/no-API-key scenarios
- Current heuristic code serves as fallback, not primary

### Recommendation

**Option C** — the heuristic code we built isn't wasted (it's the fallback), but the LLM path handles the 80% of real-world cases where naming conventions don't help. Schema discovery is a one-time setup operation, so cost/latency are non-issues.

## Considerations

### Context Window Limits

For very large schemas (100+ tables), the full introspection data may exceed context limits.

Mitigation strategies:
- **Batch processing**: Send 20-30 tables per call, include cross-references
- **Two-pass approach**: First call classifies all tables (compact format), second call generates full YAML with relationship resolution
- **Prioritized sampling**: Only include sample rows for ambiguous tables

### LLM API Options

- **Anthropic Claude API** — natural fit since ClickGraph already uses Claude for development
- **OpenAI** — alternative, similar capability
- **Local models** — Llama/Mistral for offline scenarios (lower quality but no API dependency)

### Validation

The LLM output should be validated against the ClickGraph schema format before presenting to the user:
- All referenced node labels exist
- All from_node/to_node match defined node labels
- All column names exist in the actual table
- No duplicate edge type + from_node + to_node combinations

This validation can reuse the existing Rust schema parser (`graph_catalog/`).

## Appendix A: Full LLM Prompt Template

```
You are a database schema analyst for ClickGraph, a graph query engine for ClickHouse.

Given the ClickHouse table metadata below, generate a graph schema YAML that maps
these tables to graph nodes and edges.

## ClickGraph Schema Format

  [schema format spec with example — see tools/cg-schema/docs/schema-format-example.yaml]

## Key Rules
1. A table can be BOTH a node AND a source of FK-edge relationships
   (e.g., a tickets table is a Ticket node, but also has edges to User
   via reporter/assignee columns)
2. Junction/association tables with composite PKs are usually pure edge tables
3. FK-edges: when a node table has a column referencing another table's PK,
   create an edge entry using the SAME table
4. Use meaningful relationship type names (REPORTED_BY, ASSIGNED_TO,
   not HAS_REPORTER)
5. Self-referential FKs create self-edges (e.g., manager → same user table)
6. property_mappings maps ClickHouse column names to clean Cypher property names
7. Polymorphic references (object_type + object_id) should be noted
   but may need multiple edge entries
8. Omit timestamps and internal columns from property_mappings
   unless they carry domain meaning

## ClickHouse Tables

  [for each table: name, columns with type/PK/OrderBy, 3 sample rows as JSON]

## Task
Generate the complete graph_schema YAML for database "{database}". Include:
1. All node definitions with property_mappings (use clean property names)
2. All edge definitions (both junction table edges and FK-edges)
3. Comments explaining non-obvious mappings

Return ONLY the YAML, no explanation.
```

## Appendix B: Demo Script

The demo script that generates the heuristic output and LLM prompt is at:
`/tmp/llm_schema_demo.py`

Run with:
```bash
PYTHONPATH=tools/cg-schema python3 /tmp/llm_schema_demo.py
```

## Appendix C: Heuristic Analyzer (Current State)

The current heuristic analyzer (108 tests passing) is documented in:
- `tools/cg-schema/cg_schema/analyzer.py` — classification logic
- `tools/cg-schema/cg_schema/output.py` — YAML generation
- `tools/cg-schema/cg_schema/gliner.py` — GLiNER integration (unused, no module installed)
- `tools/cg-schema/tests/test_analyzer.py` — 108 tests covering LDBC, SSB, NYC Taxi, etc.

This code would serve as the offline fallback in Option C.
