# Outcome-Loop Open Contracts

Framework-agnostic interfaces for the outcome-learning loop. Any language,
any agent harness, any framework: validate payloads against the JSON Schemas,
speak the wire formats below, and you interoperate — no SDK required.

Compatibility rule: contracts are **additive and versioned** (`*.v1.json`).
Readers ignore unknown fields and never reject them. New versions add files;
old files are never edited in place.

## Files

| Contract | Validates |
|---|---|
| `decision-event.v1.json` | Agent decisions (**propensity required** — no propensity, no off-policy evaluation) |
| `outcome-event.v1.json` | Measured results joining back on `rec_id` |
| `feedback-event.v1.json` | Explicit +1/-1 signals (maps to Langfuse `user_feedback` boolean scores) |
| `score-row.v1.json` | Serving rows: `(cohort_id, action_id, score, ci_low, ci_high, n)` |
| `strategy-experiment.v1.json` | Variant windows; `promote` verdicts require `approved_by` (human) |

## Input compatibility

| Input | How |
|---|---|
| Cypher | Native (`/query`, Bolt, `cg`) — decisions/outcomes are graph edges via schema |
| JSON events | Validate against `*.v1.json`, INSERT into the backing tables directly |
| CSV / Parquet bulk | Same columns as the schemas; bulk-load into `decision_log` / outcome tables |
| OpenAI function calling | Tool wrappers below; validate args against the schemas first |
| Webhooks | Any producer that POSTs schema-valid JSON (feedback widgets, support tools) |

## Agent / framework compatibility

| Framework | Integration |
|---|---|
| Claude Code / OpenCode / Cursor / any harness reading `SKILL.md` | `skills/` (open Agent Skills standard): `/cypher`, `/graph-schema`, `/schema-discover` for queries; contributor skills for loop work |
| MCP clients | Tool definitions below; pair with the ClickGraph MCP server |
| LangChain / AutoGen / CrewAI | MCP tools or subprocess (`cg`), payloads validated against these schemas |
| OTEL-native harnesses | Emit the span mapping below; Langfuse ingests OTEL directly |
| deepseek-harness (`dsh`) style plugins | Each loop piece (schema, skill, hook, contract) is a separable plugin; no core fork needed |

## MCP tool definitions (v1)

```json
{
  "name": "get_recommendation_effectiveness_v1",
  "description": "Outcome-weighted top actions for cohorts. Reads score_table; never walks the graph live.",
  "inputSchema": {
    "type": "object",
    "required": ["cohort_ids"],
    "properties": {
      "cohort_ids": { "type": "array", "items": { "type": "string" }, "maxItems": 10 },
      "limit": { "type": "integer", "default": 10, "maximum": 50 },
      "min_n": { "type": "integer", "default": 30, "description": "Minimum supporting outcomes; smaller samples are withheld" }
    }
  }
}
```

```json
{
  "name": "log_decision_v1",
  "description": "Record an agent decision with propensity for later outcome attribution.",
  "inputSchema": { "$ref": "decision-event.v1.json" }
}
```

```json
{
  "name": "get_cohort_guidance_v1",
  "description": "Top evolved instinct clusters for a cohort as agent instructions.",
  "inputSchema": {
    "type": "object",
    "required": ["cohort_id"],
    "properties": {
      "cohort_id": { "type": "string" },
      "limit": { "type": "integer", "default": 3, "maximum": 10 }
    }
  }
}
```

## OTEL span mapping (for OTEL-native harnesses)

No ClickGraph code needed: emit spans with these attributes and any
OTEL→Langfuse pipeline picks them up.

| Span | Attributes |
|---|---|
| `clickgraph.query` | `cypher.redacted`, `dialect`, `schema.name`, `timings.planning_s`, `timings.render_s`, `timings.sqlgen_s`, `rows` |
| `loop.decision` | `user.id_hash`, `cohort.id`, `action.id`, `policy.propensity`, `policy.version`, `trace.id` |
| `loop.outcome` | `action.id`, `outcome.weight`, `decision.rec_id`, `resolved.at` |

PII rule (mirrors `src/server/langfuse.rs`): hash identifiers, redact message
bodies best-effort by default.

## Per-framework snippets

Python (validate before insert):

```python
import json, jsonschema  # pip install jsonschema
schema = json.load(open("contracts/decision-event.v1.json"))
jsonschema.validate(event, schema)  # raises on violation
```

TypeScript (validate before insert):

```ts
import Ajv from "ajv"; // npm i ajv ajv-formats
const ajv = new Ajv(); require("ajv-formats")(ajv);
const validate = ajv.compile(require("./contracts/decision-event.v1.json"));
if (!validate(event)) throw new Error(JSON.stringify(validate.errors));
```

cURL (feedback → score pipeline):

```bash
curl -s -X POST "$HOST/events" -H 'Content-Type: application/json' \
  -d @feedback.json  # validated against feedback-event.v1.json first
```
