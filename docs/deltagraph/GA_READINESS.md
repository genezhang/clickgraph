# DeltaGraph — GA Readiness Checklist

Engineering port is code-complete through Phase 4 (engineering PRs #316–#338
per `CHANGELOG.md`, plus follow-ups #339 / #340 / #341 for CHANGELOG sync,
optional `catalog:` YAML field, and README mention — all on main). Version
stays at **0.6.7-dev**. The bump to **0.7.0 GA** is gated on the validation
and feature work below — none of which is doable autonomously without
either a live Databricks workspace or a local Spark stand-in.

This doc is the parking spot for that work. Pick it up when an environment
is available.

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

Documented behavior + tests for:

- Warehouse cold-start (30–90s on serverless)
- Warehouse auto-stop mid-query
- 401 / expired PAT mid-session
- Rate-limit / HTTP 429
- Network drop during the poll loop
- Oversize result (>25 MB) — surfaces the EXTERNAL_LINKS gap below

### 5. Concurrency

- Multiple Bolt clients against one `deltagraph` process
- Shared executor state under contention
- No cross-session leakage of `QueryContext` task-locals

---

## Should-land-before-GA features

### OAuth M2M auth

PAT-only is a non-starter for enterprise deployment — Databricks itself is
steering customers toward service principals. Plan listed this as Phase 2.4,
deferred to v1.1; for GA it needs to be in v1.0.

### EXTERNAL_LINKS result disposition

Today the executor uses `INLINE` / `JSON_ARRAY` only. Anything beyond a
demo dataset hits the 25 MB API ceiling and fails. Switching to
`EXTERNAL_LINKS` for large results was deferred as a "Phase 5 deliverable"
in QUICKSTART; for GA it must be in.

### Observability

- Query-id correlation between Bolt session → ClickGraph log → Databricks
  query history (so an oncall can trace a slow user query end-to-end)
- Basic metrics: latency p50/p95, warehouse wait time, polling overhead,
  per-statement bytes

### `MERGE` (write support)

`STATUS.md` and `CHANGELOG.md` both list `MERGE` as pending before
Databricks GA — i.e. writes are part of the GA scope, planned for v0.7.x.
(Note: `QUICKSTART.md` currently says writes are "not on the current
roadmap"; that wording predates the GA-scope decision and should be
reconciled when MERGE lands.) The embedded ClickHouse path already has
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

Three viable paths, in increasing order of fidelity:

1. **`pyspark` in a pytest harness** — simplest. Spin up a `SparkSession`
   with Delta jars, submit the SQL produced by
   `cg --dialect databricks sql "..."`, compare result rows.
   - Pros: zero infra, runs in CI, integrates with the existing
     `tests/integration/` pytest layout
   - Cons: in-process only, no networked-client testing

2. **Docker Compose: Spark + Delta + a thin SQL gateway** — `apache/spark`
   or `bitnami/spark` image, Delta jars wired in, Spark Thrift Server
   exposing HiveServer2/JDBC.
   - Pros: out-of-process, exercises a network hop
   - Cons: Thrift is not the Databricks Statement Execution API; you're
     testing the SQL, not the executor

3. **Spark Connect (Spark 3.4+)** — gRPC client/server split, closer in
   spirit to Databricks' remote model.
   - Pros: most "remote-like" of the three
   - Cons: still not the Databricks REST API; would need a thin executor
     variant to drive it

For unblocking GA correctness gates, **option 1 (pyspark + pytest) gets us
the most coverage per hour of work.** A second pass with option 2 would
add the network-hop dimension if we want it before paying for Databricks
time.

### Seeding the local dataset

The LDBC SNB schemas live under `benchmarks/ldbc_snb/schemas/` —
`ldbc_snb_complete.yaml` is the canonical superset used by the bi /
complex / short query suites and covers `Person`, `Comment`, `Forum`,
`Tag`, `Place`, etc. A seed script that materializes the same data
as both ClickHouse tables and Delta tables off that schema would let
us run the diff in CI without external dependencies.

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
