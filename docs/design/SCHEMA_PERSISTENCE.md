# Spec: Persistent Schema Registry

Status: DRAFT v1 — awaiting human review
Owner: hari2353
Grounding: codebase at `2baec91c` (codebase map in the PR description thread), external precedents: Kafka Connect `config.storage.topic` (Apache Kafka ≥ 3.0 docs), Debezium `schema.history.internal` + recovery mode (Debezium reference docs).

---

## Objective

A schema registered into a running ClickGraph must still be there after a restart or
an upgrade. Today it is not: `POST /schemas/load` writes only to in-memory registries,
and the registered YAML text is discarded after load — so every deploy re-registration
(by hand) is required. `GRAPH_CONFIG_PATH` already survives restarts; this feature
gives the *runtime* registration path the same durability, the way Kafka Connect gives
connector configs durability by writing them to a durable store and replaying it at
boot.

**Who is the user:** anyone running ClickGraph as a service (binary or container)
against a remote ClickHouse, who registers schemas through the HTTP API — including
the LLM discovery workflow (`cg schema discover` → push YAML via `POST /schemas/load`).
**DDL-created tables are not covered by this registry** (Semantics §2): they keep
the pre-existing recovery from the legacy `graph_catalog` table.

**Success looks like:** register once → restart or upgrade at will → the schema is
still served, still queryable, still listed in `GET /schemas`, without any manual
step. Nothing is written anywhere unless the operator opts in.

**Decision: single capability.** This req is one subsystem (a registry store behind
the existing load path) plus its lifecycle (write, restore, delete); the pieces are
not independently shippable — restore without write storage is untestable, and write
without restore changes nothing observable. No Phase-0 capability map required; the
build slices inside are listed under Project Structure.

## Tech Stack

Unchanged stack, no new dependencies:

- Rust (the toolchain CI uses)
- `clickhouse` client (already non-optional; `try_get_client()`
  `src/server/clickhouse_client.rs:9`)
- `serde_yaml`/`serde_json` (already present)
- Tests: Rust `#[test]`/integration binaries + python e2e in the existing layout

## Commands

```bash
# format + lint (must pass before any push)
cargo fmt --check
cargo clippy --all-targets --no-default-features -- -D warnings

# unit + integration (no live ClickHouse needed)
cargo test --no-default-features

# live-ClickHouse integration (registry tests need the test instance)
export CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_USER=test_user CLICKHOUSE_PASSWORD=test_pass
# The live tests read CLICKHOUSE_DATABASE too: every probe table and registry
# row lands in that database (CI's step below points it at `test_integration`,
# one shared job database, instead of a fixed name).
export CLICKHOUSE_DATABASE=it_registry
cargo test --no-default-features --test integration schema_registry
# (CI runs exactly this immediately after "Wait for ClickHouse to be ready"
# — ci.yml "Schema registry live tier (rust)" — so the store tier fails fast
# on a broken regression before the server-level steps even start.)

# regression ratchet + corpus census (locked at 1476 of the 1548 corpus
# queries, both dialects - `LOWERED` in binder_corpus.rs pins it; unchanged
# by this feature, and rerun here as part of the section above)
cargo test --no-default-features --test ratchet
cargo test --no-default-features --test integration the_corpus_lowers

# E2E: register → SIGKILL → re-register nothing → restored + query succeeds →
# DELETE stays deleted → off mode writes nothing (the spec's acceptance acts)
# (the script spawns its own server on 7475; CLICKGRAPH_E2E_PORT=<port> moves
# it, and CI sets 17475 because the compose stack publishes 7475)
python tests/e2e/test_schema_persistence_e2e.py
# same run under pytest (collectable; skips when CLICKHOUSE_URL/CLICKGRAPH_BIN
# are unset) — this is how CI drives it:
pytest --noconftest tests/e2e/test_schema_persistence_e2e.py -v
```

## Project Structure

```
src/server/schema_registry.rs          ← NEW. Store enum + ClickHouse impl +
      │                                    bounded replay fetch + precedence
      │                                    logic + serialization format v1.
      │                                    SRP: only life of a persisted
      │                                    registration; does not touch translator.
src/server/graph_catalog.rs            ← schema lifecycle CRUD; remove_schema
                                         (DELETE /schemas/{name} in-memory
                                         half; the store delete is serialized
                                         with re-registrations by one write
                                         gate — Semantics §4)
src/server/mod.rs                      ← boot: after initialize_global_schema,
                                         call replay (config-file names win);
                                         env plumbing (ServerConfig fields)
src/server/handlers.rs                 ← DELETE /schemas/{name} route; existing
                                         POST /schemas/load response unchanged
tests/rust/integration/schema_registry_tests.rs ← NEW unit+integration
tests/e2e/test_schema_persistence_e2e.py        ← NEW restart-cycle e2e
docs/design/SCHEMA_PERSISTENCE.md      ← this file (committed with the PR)
```

Build order (each slice independently testable):

1. **store** — closed enum + CH impl + encode/decode + table create (`upsert`,
   bounded replay fetch, `delete`, `source` tagging).
2. **write-path** — persist on successful `POST /schemas/load`.
3. **boot-replay** — restore after config load with precedence; row + payload
   budgets; degrade-to-warn when ClickHouse is unreachable.
4. **lifecycle API** — `DELETE /schemas/{name}`(+ store delete); docs.

## Interface contract

All behavior below is the spec. Names marked `v1` are the shipped shape.

### Storage (format `registry-v1`)

One row per registered schema name, in the **configured ClickHouse database**
(the same value `CLICKHOUSE_DATABASE` resolves to), table
`clickgraph_schema_registry` (created on first use):

| column | type | meaning |
|---|---|---|
| `name` | `String` | schema name exactly as registered (the `POST` body's `schema_name`) |
| `source` | `LowCardinality(String)` | `api-load` (only value written today) |
| `format` | `LowCardinality(String)` | `yaml` (only value written today) |
| `payload` | `String` | **the original YAML** the caller posted |
| `registry_version` | `UInt16` | 1; read side rejects unknown future versions with a warn (upgrade-safe: new fields newly defaulting paths, never crashes) |
| `registered_at_ms` | `UInt64` | last write, UTC milliseconds (written by the server, server-side, **monotonically** — see below) |
| `payload_len` | `UInt64` MATERIALIZED `length(payload)` | byte length of `payload`, computed by ClickHouse (never written, never stored — the fetch phase-1's budget currency) |

- Engine `ReplacingMergeTree(registered_at_ms) ORDER BY name`: the version
  column makes the last-write-wins collapse deterministic, cf.
  Kafka Connect config-topic semantics. The write path stamps
  `registered_at_ms` **monotonically**: at table-ensure time the store reads
  the table's high-water mark (`SELECT max(registered_at_ms)`) as a CAS
  floor, and each write returns `max(now, floor + 1)` under a CAS loop — the
  increment saturates, so the u64 stamp can never wrap to below the floor —
  so two same-name writes in one millisecond resolve last-write-wins, and a
  host clock that steps back (NTP correction, VM migration) cannot hand the
  collapse to a stale row: stamps this server writes only ever rise. Works
  on any table layout, no migration: legacy tables get the same guarantees
  through the same server-side stamping. Reads always go through `FINAL`.
  Deletes use ClickHouse **lightweight deletes** (server ≥ 25.x confirmed in
  deployment images); fallback `ALTER TABLE … DELETE` if unavailable.
- **Read-side gate on stored rows** (`gate_for_replay`): a row is restorable
  only when its `registry_version` is this server's version AND its `source`
  is exactly `api-load` — every other combination is refused into a counted
  bucket with a per-name WARN, never guessed at (a hand-edited or foreign
  row's payload is not this server's contract). The version check runs
  first: a row that is simultaneously future-versioned and foreign-sourced
  belongs to the strictly safer future-version bucket. The same gate refuses,
  into a fourth counted bucket (`invalid-name`), a row of this server's own
  kind whose **name the admin API would answer 400** (empty, control or
  invisible formatting characters, over 512 bytes): such a row can only be a
  hand-written one, restoring it would register a schema
  `DELETE /schemas/{name}` can never reach, and its name would reach the log.
  The per-row WARN names it (scrubbed) and gives the SQL that removes it.
  `format` stays checked at restore time (a current-version `api-load` row
  with a non-yaml format is a failed restore, not a silent skip).
- **Table preparation is once per process, with probe tolerance.** The
  store issues `CREATE TABLE IF NOT EXISTS` at most once per boot (guarded
  by an in-store mutex; a *failed* ensure is retried by the next operation,
  so a least-privilege boot still succeeds as soon as the table appears or a
  role-privileged write arrives). The successful preparation — and only the
  successful preparation — is cached for the process's lifetime. The failure
  is never cached: the next operation re-runs the entire ensure. The
  high-water seed inside it (`SELECT max(registered_at_ms)`) is load-bearing:
  a write stamped below the table's actual high-water mark loses the
  `ReplacingMergeTree` collapse to that stale row, and the next restart serves
  the old schema — so a seed that cannot be read fails the ensure. The
  registration answers `persisted: false` with a WARN, no row is written,
  and the retried preparation re-attempts everything (`CREATE`, both probes,
  the seed) rather than trusting a partial one. After the CREATE attempt —
  succeeded or not — a two-probe read (column then existence) answers "does the table
  exist (and can we read it)" and "which layout". A user without CREATE
  over a DBA-provisioned table therefore works end to end: the CREATE fails
  loudly, the probes see an existing table, and no CREATE grant is needed
  again until the process restarts. Both probe outcomes merge into one
  honest error when the table is genuinely missing.
- **Two fetch shapes, chosen by the probed layout.** Tables created by this
  version carry `payload_len` and boot replay fetches in two phases: phase 1
  reads only metadata + `payload_len` — **no payload crosses the wire** —
  and spends the byte budget server-side (a `sum(payload_len) OVER (ORDER
  BY registered_at_ms, name)` window; rows past the never-pulled), then the
  gate runs on the metadata, and phase 2 fetches payloads **only for the
  rows that can actually restore**, keyed by the bound
  `(name, registered_at_ms)` pair of each admitted row
  (`WHERE (name, registered_at_ms) IN ((?, ?), …)`): the timestamp is part
  of every key, so a name re-registered between the two reads no longer
  matches its own newer row — it is dropped with the exact
  "vanished or was rewritten" WARN, and the next boot replays the rewrite
  (a name-only key here would instead attach a rewrite's payload to the
  stale row's registration state).
  Boot reads payload bytes only where anyone reads them: on a 300-row ×
  900 KiB table the single-phase read peaked at ~1.13 GiB server-side;
  the phased shape peaked at ~180 MiB at a 64 MiB budget (phase 2 reads
  whole parts for the selected names, `FINAL`: a small multiple of the
  budget, not the budget itself). Budget spend stays uniform: every fetched row kind's bytes
  count in phase 1, and payloads still transfer nothing for rows the gate
  refuses. Tables provisioned without `payload_len` (older server,
  hand-written DDL) keep the single-phase select of round-one semantics,
  byte-identical, through the same budgeted window — and the upgrade is one
  statement, no rebuild:
  `ALTER TABLE clickgraph_schema_registry ADD COLUMN payload_len UInt64 MATERIALIZED length(payload)`.
  Between the two phases a row deleted under a racing rewrite vanishes from
  the payload set; it is dropped with an exact WARN ("changed between the
  replay's metadata and payload reads"), never restored empty.
- The default-alias duplication behavior of `initialize_global_schema` is NOT
  persisted: the store holds exactly the names that were explicitly registered at
  runtime. On replay, restore goes through the *same* `load_schema_from_content`
  path the HTTP handler uses — so alias/hash/cache semantics are by construction
  identical to a live registration (single code path = no drift).

### Configuration (opt-in, backward compatible)

- `CLICKGRAPH_SCHEMA_PERSISTENCE` ∈ {`off`, `clickhouse`}, **default `off`**.
  Parsed strictly (unknown value = startup fails loudly — a config typo should
  never silently disable durability).
- No other env var in v1 besides the two replay-budget knobs below. (Deliberate:
  the ClickHouse connection and database already come from the standard vars; a
  second knob to point persistence elsewhere is a maintenance trap — migrations
  would need two stores.)
- `off` (and default-on-absence of the flag): the process never touches the
  registry table. Byte-for-byte today's behavior — no reads, no writes, no DDL.
- Embedded (`chdb`) and Databricks modes have no ClickHouse client
  (`src/server/mod.rs:149/253`): `clickhouse` mode logs one clear line and stays
  inert there. (A file-backed store for those modes is deliberately out of scope
  v1; the closed two-variant enum keeps the door open.)
- Replay budget: `CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS` (default 500) and
  `CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES` (default 64 MiB) bound boot replay — a
  durable, API-writable store must not convert a one-time write burst into an
  unbounded per-restart cost. Rows left un-restored are counted exactly in a
  WARN naming the knob. Budget note against the threat should the question
  arise: the registry is also an API-writable, *durable* surface —
  `POST /schemas/load` stores whatever an authorized caller posts, and the
  budgets bound the per-boot read cost of that growth, not the storage
  growth itself. The endpoints' rejection classes are validation and
  reservation only; rate limiting and quota are deployment work (auth
  fronting / reverse proxy), exactly as for the pre-existing schema API.
- **Privileges.** The store runs as the ClickHouse user the server is
  configured with, on the configured database. Three provisioning shapes,
  all first-class:

  - **Self-provisioning server** (the common case): the server user needs
    `CREATE TABLE` on the database plus `SELECT`, `INSERT`, and
    `ALTER DELETE` on `clickgraph_schema_registry` (the lightweight
    `DELETE FROM` and its `ALTER TABLE … DELETE` fallback both need it).
    Table preparation is once per process; the per-operation
    `CREATE TABLE` of earlier versions is gone, so the grant is checked
    only on boots that actually prepare the table.
  - **Role-hardened server**: keep the *default user read-only*, and put
    the write privilege on a ClickHouse role the server can act as — the
    same `RoleConnectionPool` resolution `/query` uses for reads:

    ```sql
    CREATE ROLE IF NOT EXISTS schema_writer;
    GRANT SELECT ON db.clickgraph_schema_registry TO schema_writer;
    GRANT INSERT, ALTER DELETE ON db.clickgraph_schema_registry TO schema_writer;
    -- only if the role (not the DBA) provisions the table:
    GRANT CREATE TABLE ON db.clickgraph_schema_registry TO schema_writer;
    GRANT schema_writer TO clickgraph_user;
    ```

    Both durability endpoints take the role: `POST /schemas/load` carries
    `"role": "schema_writer"` in its body; `DELETE /schemas/{name}?role=schema_writer`
    carries it in a query parameter (a DELETE has no body).
    Omitted or empty ⇒ default client, exactly as before. **Boot replay
    never resolves a role and never writes**: a read-only default user
    boots, replays, and serves — write privilege is consumed only by
    durability requests, and only through requests that carry the role.
  - **DBA-provisioned table** (no `CREATE` grant for anyone): provision the
    table by hand (the DDL above — or the one-statement upgrade for
    pre-existing tables, the `ALTER TABLE … ADD COLUMN payload_len …` in
    Storage) and grant `SELECT`, `INSERT`, `ALTER DELETE`. A boot's
    ensure-once then finds the table through its read probe after the
    CREATE attempt fails, and every operation works; nothing ever needs
    the CREATE grant as long as the table exists. (This is why a *failed*
    ensure is retried by the next operation: a boot may degrade its first
    writes, and the first role-privileged or post-provisioning request
    recovers the store without a server restart.)

  With any relevant grant missing, a registration still succeeds in memory
  and reports `persisted: false` — the honest result, not an error. A
  server that must not hold write grants keeps persistence `off`.
- **Store-call timeouts** — no new env knob, deliberately. Every
  round-trip against the store — DDL, insert, delete, existence probe —
  runs under a fixed 10 s bound (`call_bound` / the round-two
  `STORE_CALL_TIMEOUT` value in `schema_registry.rs`); the boot replay's
  fetches run under a separate, longer fixed 60 s bound
  (`REPLAY_READ_TIMEOUT`): the one workload a server *must* finish before
  serving, and the one whose reads legitimately scan the most. Both bounds
  come from one engine, `with_timeout_within(bound, operation, future)` —
  every store call site — DDL, insert, delete, existence probe, replay
  read — wraps its own future through it (the store's `bound_call`
  helper for the write side, `replay_bound` for the replay side), so a
  wedged statement can hold nothing else. A slow or wedged ClickHouse now
  degrades along the already-shipped paths instead of hanging anything: a
  registration answers `persisted: false` plus a WARN naming the
  operation; a boot replay lands in the `store unreachable` branch (`0
  restored …` knockout line). The values bound a resource health check,
  not workload — knobs belong to budgets, and the two budget knobs already
  exist. Honored detail: a timeout abandons the in-flight call on the
  client, dropping its connection; it does not cancel ClickHouse's work —
  the abandoned statement may still run server-side, as §4 states for a
  timed-out DELETE.

### Semantics

1. **Write path** (`POST /schemas/load`, persistence `clickhouse`): after the
   in-memory load succeeds, the raw `config_content` is stored as
   `format=yaml, source=api-load` keyed by the request's `schema_name`.
   - Persistence failure ⇒ **HTTP 200 still** (the registration worked) with a
     `persisted: false` field added to the response body and a WARN log naming the
     ClickHouse error. Rationale: a flaky store must not make the API flaky; the
     restart guarantee degrades but the live guarantee holds. (Debezium's
     `snapshot.mode=recovery` and Kafka's `config` topic treat store health as
     backend health, not request health.)
   - The request body may carry `role: <clickhouse role>`: the store write
     (CREATE/INSERT/delete work) resolves through the role pool exactly like
     `/query`'s `role`; omitted (or empty) keeps the default client. Boot
     replay never resolves a role — the default client must merely read.
   - **Restorability honesty — additive fields, only when true.** The store
     knows one deterministic never-restorable shape: a payload whose own
     bytes already exceed `CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES` spends more
     than the whole budget by itself (the cumulative window sums over its
     own row), so *no* boot's fetch can admit it. For such a row — and only
     for it, and only when the row actually landed — the response gains
     `restorable: false` next to `persisted: true`, plus a
     `restorable_reason` naming the knob and both numbers. Config-shadowing
     of a configured name is deliberately NOT predicted here: the config
     file can change between now and the next boot, and boot replay's
     `config-shadowed` bucket is the authoritative, documented resolution;
     the response states what this server guarantees, not a guess about
     future operator edits. Every other body is byte-identical to the
     previous shipped shape, and old clients ignore new fields by
     construction (serde-default additive keys).
   - Every round-trip against the store runs under the fixed 10 s
     `STORE_CALL_TIMEOUT` (Configuration): a wedged store answers
     `persisted: false` with a WARN naming the operation — it never hangs
     the request.
   - Same name re-registered ⇒ overwrite row (content-hash mismatch warns, exactly
     like the in-memory #463 warning path).
2. **DDL path — future work, deliberately not wired in v1.** DDL-created tables
   (CREATE NODE-style legacy mutations) are NOT persisted through this registry:
   `ddl_handler` is unreachable from query dispatch (every non-read statement is
   refused with 400 — the translate seam produces no DDL SQL), so nothing writes
   `source=ddl` rows and there is nothing to replay. DDL-created schemas keep the
   recovery they already had on the base commit: the lossy `graph_catalog` table
   that `add_to_schema` feeds unconditionally (its serializations are
   `#[serde(skip)]`-lossy: vector/fulltext indexes, engine info, denormalized
   metadata — `graph_schema.rs`). A structural migration to the registry needs a
   DDL write seam first — recorded as future work, not a claim of this feature.
3. **Boot replay** (after `initialize_global_schema`, only when enabled and a CH
   client exists):
   - Load rows **bounded, in two phases on current tables**: the fetch
     itself caps at `CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS` oldest-first rows
     and a cumulative payload of `CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES`. On
     tables with `payload_len` (created by this server), phase 1 reads only
     metadata + the byte lengths — **no payload crosses the wire** — and a
     `sum(payload_len) OVER (ORDER BY registered_at_ms, name)` window
     enforces the byte budget server-side; the replay gate then runs on the
     metadata, and phase 2 fetches payloads by bound name for the rows that
     can actually restore. Tables without `payload_len` keep the
     single-phase shape (`sum(length(payload)) …`), outward-identical.
     Either way the cap bounds boot work, not only restoration (every
     fetched row kind spends the byte budget in phase 1; payloads transfer
     only for rows anyone reads), and rows left out are counted exactly in
     a WARN naming both knobs, never silently dropped. The historical
     client-side re-check in `replay()` stays as a belt-and-suspenders net
     (normally a no-op after a budget-honoring fetch).
   - Restore order and the `ReplacingMergeTree` collapse rest on the
     monotonic `registered_at_ms` stamping (Storage): a clock that stepped
     back between boots cannot resurrect a stale row, and same-millisecond
     writes still resolve last-write-wins.
   - The replay read shares the 10 s `STORE_CALL_TIMEOUT`: an unreachable
     store degrades into the documented knockout line instead of wedging
     the boot.
   - For each name **already present** from the config file ⇒ skip with an INFO
     line (`config-defined schema <name> shadows persisted registration <name>`).
     Determinism over recency — a config file is declared intent, and an upgrade
     that edits the file must preempt stale runtime state. (cf. k8s: declarative
     spec beats what's running.)
   - Remaining rows replayed in `registered_at_ms` order through the normal load
     path (CH client available ⇒ `auto_discover_columns` re-resolve against live
     metadata; a restore re-validates reality instead of trusting a stale
     snapshot).
   - A row that fails to replay (its tables vanished, YAML no longer parses under
     a stricter new parser, an unknown format): WARN with name + error, continue
     with the others. One bad row may not kill the boot — same convention as
     ClickGraph's YAML-load fall back to DB discovery. (Refused rows — newer
     `registry_version`, unknown `source` — are not failures: they are never
     attempted and are counted in their own summary segments below.)
   - Knockout summary: `n restored / m config-shadowed / k failed / b skipped
     over replay budget / v newer-version skipped / u unknown-source skipped /
     i invalid-name skipped / l lost between reads` in one INFO line — the
     eight segments account for every row (`total_rows = restored +
     config-shadowed + failed + budget-skipped + newer-version +
     unknown-source + invalid-name + lost-between-reads`). A row *lost
     between reads* was returned by the metadata read and then deleted or
     re-registered before the payload read (Current tables): it is counted as
     such, never as one the budget left out. When the store is unreachable:
     `0 restored / 0 config-shadowed / 0 failed (store unreachable at boot)`.
4. **Delete**: `DELETE /schemas/{name}` removes the in-memory registration +
   query-cache invalidation (the same call the load path makes) and deletes the
   store row ⇒ 200 `{"message": ..., "schema_name": ..., "persisted": true}`
   (`persisted` appears only when persistence is configured — additive).
   Name classes, stated:
   - **store-only name** (a row that failed boot replay: in the store, never
     loaded into memory): the endpoint is the remedy boot replay's WARN
     advertises — the row is deleted even though `remove_schema` reported
     not-found, and the response carries `"persisted": true, "store_only": true`
     (nothing in memory ever existed for the name this boot). A failing store
     on this path is an honest 500: nothing was deleted, so neither a fake 200
     nor the 404 would be truthful — and the 500's body is a fixed message
     ("stored row for schema … could not be deleted (the registry store
     answered with a failure; the server log names the cause)"): a
     ClickHouse error text carries the server's user, database, grants and
     version, none of which belongs in an API body. The detail is WARNed in
     the server log, with the name already log-safe.
   - **config-defined name**: DELETE clears memory and its row; the config
     file re-registers the schema on the next boot regardless, so the
     in-memory deletion of such a name is transient by design (`default`
     included).
   - **unknown name** (no memory entry, no stored row — includes persistence
     off): pre-feature 404, same body shape as `GET /schemas/{name}`; the
     store-only probe has no write side effect (it never creates the table).
   **Delete vs re-register (and POST vs POST) is serialized across the
   server by one write gate** (`AppState.schema_registry_gate`): both
   admin handlers hold the gate across BOTH halves of their lifecycle —
   the in-memory registration change and the store change — so the halves
   can no longer interleave into `memory = A, store = B`, the divergent
   state a restart then surfaced (reproduced 1-in-60 with concurrent
   POST/DELETE; this spec previously documented the race as
   last-writer-wins under memory-side guarding only, and the gate outlaws
   it). Queuing is the whole cost: an admin-rate endpoint makes one global
   gate effectively free, and holding it across the store write is exactly
   the price of the guarantee — serializing per name would hold a keyed
   lock across a network round-trip for no additional property.
   **The gate exists only where there is a store to keep in step.** With
   persistence off (the default) neither handler takes it: loads and deletes
   behave exactly as before the feature, and a slow load blocks only itself.
   With persistence on, the *store* half of a call is bounded (it answers
   within its call bound, 10 s on the write path, so a wedged store drains
   the queue into degraded answers — N queued calls answer after N × 10 s at
   worst), but the *memory* half of a load (column discovery in
   `load_schema_from_content`) has no client-side bound of its own: a
   ClickHouse call that hangs inside a load holds the gate until the request
   times out (`--query-timeout-secs`, default 300 s; unbounded at `0`).
   **A dropped request does not abandon its store half.** After the schema
   went live in memory (POST) or left it (DELETE), the store write runs as
   its own task and owns the gate until the store answers or its bound
   elapses; a client that disconnects (or a request that times out) in that
   window no longer abandons a store call that was still being prepared or
   sent — verified against a real server whose store was slow at first use:
   the round-three server left the schema live in memory with no stored row
   and no log line (a restart then lost it), the detached half stores it.
   A store that never answers still leaves the registration memory-only
   (`persisted: false`, which the dropped client never sees) — but that is now
   a WARN in the log, and the gate stays held until the call's bound, so a
   late-landing statement cannot interleave with the next admin call. The
   task is bounded by the store's call bounds, so it cannot hold the gate for
   long. Boot
   replay takes no turn in the gate: it runs before the listener binds,
   so there is nothing concurrently racing it. Within the store-only
   path, probe→delete is protected by the same gate (both call sites hold
   it; nothing can re-add a row between probe and delete).
   If the store delete fails, the schema is still removed from memory,
   the response is 200 with `persisted: false` and the stored row remains
   (a retry of the DELETE removes it as a store-only row). A store call
   that exceeds its bound is abandoned by the client, not cancelled: the
   statement may still run in ClickHouse afterwards, so a timed-out DELETE
   can later remove a row that was re-registered in between. The DELETE
   accepts `?role=` for its store write, the same resolution as the POST
   body's `role` (Configuration §Privileges). New endpoint ⇒ additive,
   backward compatible.
5. **Concurrency**: two instances replaying simultaneously is out of scope v1
   (documented; single-instance per the deployment profile). Within one
   server: both admin endpoints serialize their full lifecycle through
   `schema_registry_gate` (§4); the in-memory maps are mutated only under
   the gate or by boot replay, which owns the not-yet-bound server
   outright. The monotonic `registered_at_ms` floor is per-process, seeded
   once at table-ensure time from the table's high-water mark;
   cross-process ordering rests on the same single-writer profile as
   everything else here. For the replication question that comes next, the
   answer is already in the engine: a `Replicated` `ReplacingMergeTree`
   (or any replicated-table shape) collapses per replica by the same
   version column, so concurrent writes to one name still resolve
   last-write-wins by `registered_at_ms` exactly as a single table does;
   each server stamps with its own monotonic floor, so stamps from two
   servers interleave by wall clock, and whichever stamp is higher is
   exactly what replay's `FINAL` read takes. No coordination beyond the
   engine is added for the store's guarantees; multi-writer ClickGraph
   servers stay out of scope v1 per the single-writer deployment profile.
   Reads are safe everywhere (`FINAL` always).
   **With `CLICKHOUSE_CLUSTER` set, use a replicated registry table (or do
   not pass `role`).** The server creates the table once, on one node, as a
   plain `ReplacingMergeTree` (no `ON CLUSTER`). Writes with a `role` go
   through the role pool, which round-robins over the cluster nodes, while
   the default client — and boot replay — use the seed URL: on a cluster, a
   role-scoped write can reach a node that has no such table or a different
   node's local copy that replay never reads. Provision the table as a
   `Replicated` engine on every node (the DBA-provisioned profile, see
   Configuration › Privileges) before enabling persistence on a cluster; the
   server's own `CREATE` does not do that for you.
6. **Reserved names.** `POST /schemas/load` refuses `load`, `draft`,
   `introspect`, `discover-prompt` with a 400 naming the reservation —
   before any gate acquisition, registry, or store work: these spellings
   belong to the routes (`load`) and the CLI discovery workflow (`draft`,
   `introspect`, `discover-prompt`). DELETE is untouched, but the four
   spellings are routes of their own: `DELETE /schemas/load` is answered by
   another route (405), so a manually seeded row of such a name restores at
   boot and is removed from the store directly
   (`DELETE FROM clickgraph_schema_registry WHERE name = '…'`).
7. **Identifier validation.** Beyond the four reserved spellings, both
   durability endpoints validate their identifiers at the door, with a 400
   naming the problem **before any gate acquisition, memory work, or store
   contact** (the reservation check above shares that property): a name that
   is empty, carries a control character or an invisible formatting
   character, or exceeds 512 **bytes**, and a role that carries one of those
   characters or exceeds 256 bytes, are all refused. "Control" is Unicode
   `Cc` (`\n`, `\r`, NUL, DEL, the C1 range); "invisible formatting" is the
   line/paragraph separators (U+2028/U+2029), the zero-width characters
   (U+200B–U+200F, U+2060–U+2064, U+FEFF) and the bidirectional controls
   (U+061C, U+202A–U+202E, U+2066–U+2069) — characters that forge a log
   line or make one name read as another. Boot replay applies the same name
   check to every stored row (a hand-written row the API would refuse is
   skipped into the `invalid-name` bucket, not restored). Rationale: these
   are operator-supplied bytes that otherwise ride
   into log lines (a `\n` in a name could forge entries after the prompt) —
   the store's own logging prefers to route them through `log_safe`, and
   refusing up front keeps the raw byte away from every downstream surface.
   The caps are outright refusals, not truncations: a 513-byte name is
   no deployment's spelling. An **empty role is not an error**: both
   endpoints normalize `""` to no-role (default client), exactly as
   round-two shipped, and that normalization runs before the validator —
   so the validator never sees the empty string from either endpoint's
   normal path.

## Code Style

Follows the house rules already in `src/server/AGENTS.md` and
`src/open_cypher_parser/AGENTS.md`, plus what the 3 merged PRs established:

- Comment density: adversarial commentary explaining "why", with issue/PR refs, esp.
  on precedence rules and the lossy-DDL caveat.
- Errors: this codebase logs-and-continues on store failures (resilient startup),
  but strict-fails on config errors (see `parse_env_var` vs `parse_env_flag`).
  Mirror that split: bad enum ⇒ fail; unreachable store ⇒ warn+degrade.
- No `unsafe`, no new deps, `unwrap()` only in tests.

```rust
// The shipped boundary (DIP: handlers/boot never touch ClickHouse client
// plumbing directly): a closed two-variant enum — no dyn trait, no
// async-trait dependency. A third backend (file) is a new variant.
pub enum SchemaStore {
    Off,
    Clickhouse(Box<ClickhouseSchemaStore>),
}
```


## Testing Strategy

Four station levels and a mutation-red tier, each with a named concern:

1. **Unit (no live deps)** — `tests/rust/integration/schema_registry_tests.rs`,
   built from the corpus_sweep pattern (the orphaned
   `schema_validation_tests.rs` proves that pattern's failure mode: never wire an
   undeclared test file):
   - encode/decode round-trips (`registry-v1`), unknown-version rejection,
   - precedence filter (config names subtract correctly incl. alias `default`),
   - the off-mode contract on the enum (`Off` degrades every op locally — the
     closed enum keeps tests monomorphic, no trait),
   - the monotonic `registered_at_ms` floor: seeded from a table's high-water
     mark, strictly increasing across writes, and past a stepped-back wall
     clock (`max(now, floor + 1)` under CAS) — three unit tests over the
     floor machinery itself;
   - pure helpers: `is_reserved_schema_name` (exact match only), and
     `unrestorable_reason_if_any` (over-cap ⇒ the knob-and-numbers reason;
     within-cap ⇒ `None`).
2. **Integration (live ClickHouse, env-gated)** — same file:
   - table auto-create idempotence; save → bounded replay fetch → delete round-trip;
   - the row budget: a fetch capped below the table's row count returns the cap
     and states the overage exactly (replay's WARN feeds on this shape);
   - last-write-wins on same name; lightweight-delete path taken;
   - boot replay through `replay()`: a seeded row restores through the live
     load path (`GET /schemas/{name}` counts equal a fresh live registration)
     and a name already registered in memory is shadowed, not overwritten;
   - **the gate regression knife**: raw-INSERTed `registry_version`-bumped and
     foreign-`source` rows land in their exact skip buckets (`0 restored` with
     both names WARNed) and are absent after replay — a test that goes red if
     `gate_for_replay` ever stops checking, while every older live test would
     stay green (they seed only current-version, `api-load` rows). Seeds are
     the test's own `INSERT` with quoted literal tokens (an operator hole the
     production path deliberately does NOT have: production writes go through
     bound parameters); cleanup asserts the table is empty afterwards.
     CI runs the live tier (`ci.yml`: "Schema registry live tier (rust)",
     immediately after "Wait for ClickHouse to be ready", env
     `CLICKHOUSE_URL`/`CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`/
     `CLICKHOUSE_DATABASE`), so this knife cannot stay unexercised;
   - database is parametrized (`CLICKHOUSE_DATABASE`, CI's
     `test_integration`): a shared CI job database works because every probe
     table and registry row is name-namespaced to the run.
3. **Server-level (rust, router-driven)** — POST/DELETE handlers against
   `StateRouter` like `databricks_introspect_tests.rs`: response shapes (incl.
   `persisted: false` degradation field), 404 shape, old-payload compat (a
   pre-feature client that ignores extra fields keeps working); the reserved
   name 400 arrives **before any gate acquisition or store contact** (a
   reserved name must stay a pure-router rejection); an optional `role` key is
   accepted on both endpoints' requests and ignored downstream when it is
   absent or empty (shape + backward compat, no live CH needed).
4. **Mutation-red (offline, no ClickHouse at all)** —
   `schema_registry::mutation_tests` drives a tiny fake ClickHouse HTTP
   server that speaks the exact wire shapes the client puts out (fetch =
   GET with the SQL in `query=`; writes = POST; INSERT = RowBinary body
   with `FORMAT RowBinary`; binds already substituted as literals; faults
   steered per request class), so every review item that used to be good
   intentions is pinned at its real call site:
   - the **gate race**: a load's INSERT held mid-flight by the fake; the
     DELETE spawned concurrently must answer 200 with *zero* DELETE-shaped
     store traffic while the INSERT is held — remove either handler's
     `schema_registry_gate.lock()` and the test goes red (verified);
   - **table preparation once per process** (CREATE count == 1 across two
     loads and a delete) and the **failed-CREATE fall-through** (a
     least-privilege registration still stores);
   - the **bound**: a wedged CREATE (hang fault) degrades in 20 ms into
     the mapped `store unreachable` error naming the operation;
   - item-1's repro: a **seed that cannot be read fails the ensure** —
     `persisted: false`, zero INSERTs, and the retried next op answers
     true with CREATE count == 2 (no caching past a failed seed);
   - the **floor**: a stamp must carry the seeded high-water (a no-op
     seed leaves the stamp below it, losing the collapse);
   - the **(name, registered_at_ms) phase-2 binds** and the
     **legacy-layout fetch** both read lines the fake returns;
   - item-4: the store-only delete answers the **fixed 500 message**
     whose body provably carries no ClickHouse fault token;
   - the **validators' refusal matrix** (empty/control/oversize name and
     role; empty role stays a default write);
   - maintainer round four, each verified red by its mutation: the
     **handlers** refuse unsafe identifiers before any store contact (the
     fake sees zero requests — removing a handler's validator, name or role,
     turns it red); the **DELETE `role`** rides on the memory-path and the
     store-only delete; **every store op is bounded at its own call site**
     (`exists`, `delete`, the replay read each wedged on their own statement)
     and the **replay read runs under `replay_bound`** (a slow-but-healthy
     link must not read as an outage); the **`ALTER … DELETE` fallback**; a
     failing store delete after the memory removal stays a 200 with
     `persisted: false`; an unreadable exists probe is a 404, never a
     DELETE; a rejected INSERT answers `persisted: false`; a row **lost
     between the replay reads** is counted and not restored empty; rows with
     **names the API refuses** are skipped; the **timestamp stamp saturates**
     at `u64::MAX`; **a dropped POST / DELETE does not abandon its store
     half** (the write still lands, the gate is held until it does); and the
     **gate is taken only with a store** (with persistence off a held gate
     blocks nothing; with a store it queues the call).
5. **E2E (python, pytest-collectable)** — the restart cycle as CI runs it
   (`pytest tests/e2e/test_schema_persistence_e2e.py`, skipping when
   `CLICKHOUSE_URL` / `CLICKGRAPH_BIN` are unset; plain-python `python …`
   keeps exit 77 as the skip indicator): register → `persisted: true` →
   **SIGKILL** (no graceful shutdown — POSIX `SIGKILL`, Windows
   `TerminateProcess`) → restart with the same env and no re-registration →
   `GET /schemas/{name}` 200 and **a query against the restored schema reads
   seeded data** (query-after-restore, per-run-unique probe so a crashed
   earlier run cannot fake it) → DELETE lifecycle (200 then 404) → restart ⇒
   deleted stays deleted → persistence-off re-register with **no `persisted`
   key** AND zero writes (the `system.tables` count for the database is
   unchanged and the registry holds no row for the name). The server
   lifecycle is the script's (spawn/reap of the real binary), and this step
   runs in CI after the integration job's smoke subset (`ci.yml`: 
   "Schema-persistence restart-cycle e2e"), where the test ClickHouse
   container is already up. Config-file shadowing is pinned at the Rust
   level (`boot_replay_restores_persisted_rows_and_shadows_registered_ones`).

Backward-compat proof is a test class of its own: with the flag unset, a full
`/schemas/load` + `/query` session must produce identical responses to `main`
(precisely how the OTLP PR proved HTTP parity).

## Boundaries

- **Always:** run the four gates (`fmt`, `clippy -D warnings`, full no-default
  suite, ratchet + census 1476) before any push; keep the offline (no-CH) suite
  green without ClickHouse; commit the spec alongside the code that satisfies it.
- **Ask first:** flipping the default to on; introducing a second store backend
  (file); touching `GLOBAL_SCHEMA_CONFIGS` semantics beyond additive use;
  lightweight-delete fallbacks; any new crate dependency (avoid `async-trait` if
  feasible).
- **Never:** persist without opt-in; write to a database the operator didn't
  configure; let a store failure fail a registration request; shadow a
  config-defined schema with persisted state (boot replay always skips it);
  swallow unknown `registry_version`s silently (warn, skip row). A
  config-defined name re-registered through the API is persisted like any
  other runtime registration: boot replay shadows it (config wins), and
  `DELETE /schemas/{name}` clears both memory and the row.

## Success Criteria

1. `POST /schemas/load` (persistence `clickhouse`) ⇒ row in
   `clickgraph_schema_registry` before the response returns; kill -9; restart ⇒
   `GET /schemas` lists the name; a query against it returns rows with **no
   re-registration**. (E2E script is the acceptance runner.)
2. Persistence `off`/unset ⇒ zero CH writes attributable to the new code, and
   live-parity with `main` on all schema endpoints (pinned by server-level tests).
3. Config-file + persisted collision ⇒ config wins; log states the skip; count of
   skipped = expectation in the knockout line.
4. Boot replay bounded: seeding more rows than the budget allows restores the
   oldest-first prefix and the WARN names the exact overage; the byte budget
   is enforced in the fetch itself (rows that would tip it are never pulled
   over the wire), with the default budgets (500 rows / 64 MiB) making a
   boot's restore work a stated, dialable cost, never an unbounded read.
5. `DELETE /schemas/{name}` ⇒ memory + store + query cache all drop it; 404 on
   the gone name (both GET and DELETE), same body shape. A store-only row
   (failed replay) is retirable through the same endpoint: 200
   `{"persisted": true, "store_only": true}` and the row really leaves the
   store; a second DELETE 404s.
6. A registry read that fails at boot with persistence on (the table is
   unreadable, the grant is missing, the query errors) ⇒ the server still
   boots (WARN, `0 restored / 0 config-shadowed / 0 failed (store unreachable
   at boot)`). A ClickHouse that cannot be reached at all stops the server in
   `initialize_global_schema`, with persistence on or off, as before.
7. Ratchet + census unchanged (1476 both dialects — `LOWERED` in
   `binder_corpus.rs` is not touched); no golden churn; no new deps.
8. Unit+integration counts added and green on Windows `--no-default-features`
   toolchain and in CI (default features).
9. **Round-two review items.** Every store round-trip runs under a fixed 10 s
   bound: a wedged store degrades a registration to `persisted: false` (WARN
   names the operation — pinned by the `with_timeout` unit tests) and a boot
   replay to the `store unreachable` knockout line. `POST` vs `POST` and
   `DELETE` vs re-register are serialized by one write gate held across both
   the memory and store halves in both admin handlers (the 1-in-60
   memory/store divergence repro this closes; the divergence was reproduced
   once by hand and never automated — a racy flake is not shippable, the gate
   is structural). Table preparation happens once per process with a
   least-privilege read-probe fallback (the 26.7 grant probe verified it). The
   replay gate is regression-knifed by the live foreign-sourced /
   future-versioned bucket test that CI now runs.
10. **Role-hardening (optional item).** `role` is accepted on both durability
    endpoints and resolves through the same `RoleConnectionPool::get_client`
    resolution `/query` has used since #836; boot replay never resolves a
    role; a read-only default user + `schema_writer` role deployment works end
    to end (Integration tier covers the default-client path; the role path
    shares that resolution, so no separate live grant is invented here).
11. **Round-three review items.** A failed high-water seed fails the ensure
    (no write, no caching of the failure — the round-two repro where a
    stored row with a future stamp plus a failing `max(registered_at_ms)`
    answered `persisted: true` is now a red test). Store operations run
    under `with_timeout_within` with a 10 s write bound and a 60 s replay
    bound. Both durability endpoints refuse empty/control/oversize
    identifiers with a 400 before any gate or store work; an empty role
    stays a normalized default write. The store-only delete's 500 body is
    the fixed message, never a ClickHouse error text. Phase 2 fetch keys
    are `(name, registered_at_ms)` pairs. All of it is pinned by the
    offline mutation-red tier above, plus the round-two live tier.

12. **Maintainer round four.** The admin gate is taken only when there is a
    store (persistence off is the pre-feature behavior again); the store half
    of a POST or DELETE runs as its own task that owns the gate, so a dropped
    request cannot split memory from the store; boot replay refuses rows with
    names the API would refuse and counts rows lost between its two reads
    separately from the budget; unsafe-character checks cover the line/
    paragraph separators and zero-width/bidirectional characters; the cap is
    documented in bytes; the cluster + `role` limitation is stated; and the
    survivors of the round-three mutation run are pinned (see the
    mutation-red tier).

## Open Questions (for maintainer review, not blocking the build)

- Flip default `off → clickhouse` in a follow-up release once field feedback is in?
- Should `GET /schemas/{name}` gain a `?format=yaml` export on the same path
  (cheap once payloads are stored; nice for GitOps round-tripping)?
- Multi-instance concurrent replay (leader election via CH lock or
  `system.replicas`?) — defer until someone actually runs replicas.
