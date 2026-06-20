# DeltaGraph — Beta & GA Readiness

Engineering port is code-complete through Phase 4 (engineering PRs #316–#338
per `CHANGELOG.md`, plus follow-ups). Since then the local-validation +
resilience work below has landed, raising the floor from "code-complete,
unvalidated" toward a defensible **beta**. The bump to **0.7.0 GA** still
requires a live Databricks workspace for the irreducible items.

## Three validation environments

Most of the remaining work is doable without a real warehouse, against two
local stand-ins. Know which environment owns which concern:

| Environment | Owns | Setup |
|---|---|---|
| **Spark/Delta docker** (`deltaio/delta-docker:4.1.0`) | Spark-runtime dialect fidelity (recursive CTE, `explode`, VLP/BFS, array CAST, lateral aliases), result-set parity vs ClickHouse | `tests/spark_smoke/` |
| **zeta-databricks** (`zeta-server-bin --features wire-databricks`) | Executor wiring, REST submit/poll, JSON decode, EXTERNAL_LINKS, fault-injection, flat/agg dialect | `tests/zeta_integration/` |
| **Live Databricks** | Real perf, OAuth vs real IdP, real cloud-storage EXTERNAL_LINKS, cold-start/auto-stop/429 at the warehouse, soak/concurrency | a workspace |

## Beta exit criteria — status

| Item | Status |
|---|---|
| Dialect translation (flat/agg/string/OPTIONAL/VLP/BFS) | ✅ |
| Spark execution of the LDBC sweep on Delta docker | ✅ `tests/spark_smoke` |
| Result-set parity vs ClickHouse (mini dataset) | ✅ 22/22, `test_ldbc_parity.py` |
| Cross-stack transport (executor↔REST↔engine) | ✅ `tests/zeta_integration` |
| EXTERNAL_LINKS (large results) | ✅ impl + tests (live staging pending) |
| Transient-failure resilience (429/503/timeout retry, 401→Auth) | ✅ wiremock + fault-injection |
| OAuth M2M auth path | ✅ impl + tests (live IdP pending) |
| Per-query observability (statement_id + timing) | ✅ |

Beta-blocking gaps remaining: none of the above. The known **non-blocker** is
that VLP/ordered queries don't yet run on zeta-databricks (two Zeta engine
gaps — array CAST + lateral column alias); VLP fidelity is covered on the
Spark docker container, so this only limits zeta as a VLP stand-in, not beta
itself. See `ZETA_FIDELITY.md`.

## Irreducible GA items — need a live Databricks workspace

These cannot be closed by either local stand-in:

- **Performance baseline** (§2) — real per-query latency on ≥2 warehouse
  shapes; cold-vs-warm. Zeta perf ≠ Databricks perf by construction.
- **Endurance / soak** (§3) — ≥24h Bolt session, ≥100 concurrent sessions,
  `reqwest` pool + polling loop under sustained real traffic.
- **Real failure modes** (§4) — actual serverless cold-start timing,
  warehouse auto-stop mid-query, real 429s, >25 MB cloud-storage staging.
- **OAuth against a real IdP** — token format/scopes/refresh under load.
- **First-class metrics** — Prometheus/OTel histograms (today: log-derived).
- **External user run-through.**

These remain the parking spot below. Pick them up when a workspace is available.

---

## Gating items (must pass before GA)

### 1. Correctness against a live warehouse

- LDBC SNB sweep on Delta tables: `bi-1..15`, `bi-17..18`, `complex-1..14`,
  `short-1..7` (skipping `bi-16` — blocked on `CALL` subqueries, see
  out-of-scope section below)
- Result-set diff vs ClickGraph on equivalent seed data — same rows, any
  ordering allowed
- Locally reproducible with the same seed across both backends

**Reference:** `docs/design/DELTAGRAPH_PLAN.md` §5 (test strategy), Phase 2.5

### 2. Performance baseline

- LDBC bi / complex query timings on at least two warehouse shapes
  (e.g. 2X-Small serverless + a Pro cluster)
- Published per-query latency table
- Validate or refute the plan's prediction: "VLP 2–10× slower than
  ClickHouse, flat queries competitive" (DELTAGRAPH_PLAN.md §2 VLP notes)
- Cold-vs-warm-warehouse numbers — the 30–90s warmup is user-visible

### 3. Endurance / soak

- Long-running Bolt session (≥24h)
- Sustained query load — never been exercised past smoke tests
- Memory profile and `reqwest` connection-pool behavior over hours
- Statement Execution API polling loop under sustained traffic

### 4. Failure-mode coverage

The executor now retries transient failures (HTTP 429/503, connect/timeout)
with exponential backoff honoring `Retry-After`, and surfaces 401 as a
dedicated `ExecutorError::Auth` (never retried). Covered by wiremock unit
tests (429-then-success, 401-surfaced, retries-exhausted) and, end-to-end,
by the zeta-databricks fault-injection hook (`POST /_test/inject`, env-gated)
which arms a status for the next submit so the full stack drives its
retry/re-auth path without a live warehouse.

Status per case:

- Rate-limit / HTTP 429 — ✅ retried with backoff + `Retry-After`
- 401 / expired PAT mid-session — ✅ surfaced as `Auth` (re-auth is the
  caller's job for PAT; auto-refresh lands with OAuth M2M)
- Network drop during the poll loop — ✅ connect/timeout retried
- Warehouse cold-start (30–90s) — ✅ 503 retried (capped 30s backoff);
  real serverless cold-start timing still needs live validation
- Warehouse auto-stop mid-query — ⏳ injectable locally; real mid-query
  auto-stop behavior needs a live warehouse
- Oversize result (>25 MB) — ✅ addressed by EXTERNAL_LINKS (below)

### 5. Concurrency

- Multiple Bolt clients against one `deltagraph` process
- Shared executor state under contention
- No cross-session leakage of `QueryContext` task-locals

---

## Should-land-before-GA features

### OAuth M2M auth — IMPLEMENTED (live validation pending)

The executor now supports OAuth 2.0 client-credentials (service-principal)
auth alongside PAT (`DatabricksConfig::oauth`). It exchanges
client_id/client_secret at `{host}/oidc/v1/token`, caches the access token
with a 60s pre-expiry refresh margin, and surfaces token-endpoint rejection
as `ExecutorError::Auth`. Wired through `clickgraph-embedded` and the `cg`
CLI (`CG_DATABRICKS_CLIENT_ID` / `CG_DATABRICKS_CLIENT_SECRET`). Covered by
wiremock tests (token fetch + caching, bad-credential rejection).
**Remaining for GA:** validate against a real Databricks identity provider
(token format, scopes, expiry/refresh under load) — needs a live workspace.

### EXTERNAL_LINKS result disposition — IMPLEMENTED (live validation pending)

The executor now speaks both `INLINE` and `EXTERNAL_LINKS` dispositions
(`DatabricksConfig::disposition`). Under EXTERNAL_LINKS it downloads the
presigned chunk URLs (JSON arrays-of-arrays) and follows `next_chunk_index`
across chunks — no 25 MB ceiling. Covered by unit + wiremock tests
(single-chunk and multi-chunk pagination) and an end-to-end round-trip
against the zeta-databricks emulator, which serves `external_links` and a
chunk-data endpoint. **Remaining for GA:** validate against a live
warehouse's real cloud-storage staging (presigned-URL expiry, large
multi-chunk results, ARROW vs JSON staging) — only a live workspace
exercises the real object store.

### Observability — partial (per-query logging done; aggregated metrics GA)

The executor logs each statement on the `deltagraph::databricks` target with
the Databricks `statement_id` (so an oncall can pivot from a ClickGraph log
line to the warehouse query history), plus `duration_ms`, `polls`, and `rows`
— failures log `state` + error + `statement_id`. A log pipeline can derive
p50/p95 latency and polling overhead from these per-query lines.

**Remaining for GA:**
- First-class metrics (Prometheus/OTel histograms for latency p50/p95,
  warehouse wait time, per-statement bytes) rather than log-derived
- Propagate the Bolt session id into the log line for full
  session→log→query-history correlation (today the statement_id is the pivot)

### `MERGE` (write support)

`STATUS.md` and `CHANGELOG.md` both list `MERGE` as pending before
Databricks GA — i.e. writes are part of the GA scope, planned for v0.7.x.
(`QUICKSTART.md` now states writes against Delta are out of the beta
iteration but in GA scope (v0.7.x), consistent with this doc and
`STATUS.md`.) The embedded ClickHouse path already has
`CREATE` / `SET` / `DELETE` / `REMOVE`; `MERGE` plus relationship
`CREATE`, edge-alias `DELETE`, `SET a += {…}` map-merge, and
`REMOVE a:Label` are the remaining write gaps.

---

## GA exit criteria (the actual bar)

1. LDBC SNB suite passes on Databricks with documented per-query times
   (excluding `bi-16` — see CALL subquery note below)
2. 24h soak with ≥100 concurrent Bolt sessions — no leaks, no degradation
3. OAuth M2M + EXTERNAL_LINKS shipped
4. Documented failure-mode behavior for §4 above
5. At least one external (or fresh-eyes internal) user run-through

---

## What we can validate WITHOUT Databricks (local Spark stand-in)

Apache Spark + Delta Lake OSS, run locally, can substitute for a real
warehouse for most of §1 (correctness) and parts of §2 (relative perf).
It cannot substitute for the Databricks executor, auth, EXTERNAL_LINKS,
warehouse lifecycle, or rate limiting — those need a live workspace.

### What local Spark covers

- ✅ Dialect emitter correctness — every translated query parses and runs
- ✅ Recursive CTE behavior on Spark (the VLP shape is the biggest risk
  the plan flagged)
- ✅ `LATERAL VIEW explode` vs `arrayJoin` structural differences
- ✅ FunctionMapper coverage — every row in DELTAGRAPH_PLAN.md §2
  function-translation table
- ✅ LDBC SNB result-set diff vs ClickGraph
- ✅ Relative performance shape (within an order of magnitude of Databricks)

### What it does NOT cover

- ❌ Statement Execution REST client (different transport entirely)
- ❌ PAT / OAuth M2M auth paths
- ❌ EXTERNAL_LINKS disposition
- ❌ Warehouse cold-start, auto-stop, scaling behavior
- ❌ Rate limiting / 429 handling
- ❌ Photon acceleration (OSS Spark has none)
- ❌ Unity Catalog 3-tier resolution (local uses 2-tier or single-tier)

### Recommended local setup

**A baseline harness is in-tree at `tests/spark_smoke/`** (PR #343). It
runs `cg --dialect databricks sql "..."`, ships the SQL into a
`deltaio/delta-docker:4.1.0` container, executes against seeded Delta
tables, and asserts on the result rows. Gated by
`CLICKGRAPH_SPARK_TESTS=1`; skips cleanly without it.

```bash
cargo build --release -p clickgraph-tool --features databricks
CLICKGRAPH_SPARK_TESTS=1 pytest tests/spark_smoke/ -v
```

Five smokes ship covering the highest-risk surfaces (flat JOINs,
`WITH RECURSIVE` VLP, `collect()`→`collect_list`, `OPTIONAL MATCH`
NULL-safe filter, string-function translation), in ~66s. Growing this
into the full LDBC sweep called out in §1 is the next concrete step.

**Why Spark 4.x rather than DBR-matched 3.5**: upstream Apache Spark 3.5
doesn't have `WITH RECURSIVE` — only DBR's backport on top of its 3.5
fork does. The `deltaio/delta-docker:4.1.0` image ships Spark 4.1.1,
which has recursive CTE natively. Version skew vs DBR is a known harness
limitation; "passes locally" ≠ "passes on Databricks" and the real
warehouse remains the source of truth.

If we later want out-of-process / network-hop coverage (still not
Databricks-fidelity), two alternatives stay on the table:

- **Docker Compose with Spark Thrift Server** — exercises a network hop
  but still not the Statement Execution API.
- **Spark Connect (Spark 3.4+)** — gRPC client/server, closer in spirit
  to Databricks' remote model but needs a thin executor variant to drive.

### Seeding the local dataset

The smoke + parity harnesses seed a tiny LDBC slice (5 `Person`, etc.) from
`tests/spark_smoke/mini_delta_seed.sql` (Delta) and
`benchmarks/ldbc_snb/data/mini_dataset.sql` (ClickHouse). On this mini
dataset, the result-set parity gate passes 22/22 (`test_ldbc_parity.py`) —
but many bi/complex queries filter to zero rows, so much of that is
empty-set agreement rather than content parity (see `LOCAL_TESTING_RESULTS.md`).

**Full-scale LDBC datagen (GA-tier, parked):** a generator that materializes
LDBC SNB sample data (SF1+) as both ClickHouse and Delta tables off
`benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml`, so the same Cypher diff
runs on both backends at scale. This is what turns the parity gate from
"executes + empty-set agreement" into real content + scale validation, and
overlaps the performance baseline. Needs the LDBC datagen toolchain (not
in-repo); deferred to the GA push alongside the live-warehouse work.

(`benchmarks/social_network/schemas/social_benchmark.yaml` is a much
smaller social-graph schema useful for sanity checks during development,
but does not cover the LDBC label set and so is not sufficient for the
correctness gate above.)

(`benchmarks/social_network/schemas/social_benchmark.yaml` is a much
smaller social-graph schema useful for sanity checks during development,
but does not cover the LDBC label set and so is not sufficient for the
correctness gate above.)

---

## Out of scope for GA (explicitly)

- **SQL AST layer (Phase 6)** — optional simplification project, 2–3 weeks,
  not on the GA path.
- **`CALL` subqueries** — same gap as ClickGraph (LDBC `bi-16`), inherited
  from the shared planner. This is why `bi-16` is excluded from the
  correctness gate above; closing it is a planner-level project, not a
  DeltaGraph-specific item.

---

## Pointers

- Plan: [`docs/design/DELTAGRAPH_PLAN.md`](../design/DELTAGRAPH_PLAN.md)
- User-facing setup: [`docs/deltagraph/QUICKSTART.md`](QUICKSTART.md)
- Release state: [`STATUS.md`](../../STATUS.md), [`CHANGELOG.md`](../../CHANGELOG.md)
