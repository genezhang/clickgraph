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
the LLM discovery workflow (`cg schema discover` → push YAML via `POST /schemas/load`)
and DDL-created tables (`CREATE NODE`/`CREATE RELATIONSHIP`).

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

- Rust (toolchain pinned `1.92.0-x86_64-pc-windows-gnu` locally; CI default)
- `clickhouse` client (already non-optional; `try_get_client()`
  `src/server/clickhouse_client.rs:9`)
- `serde_yaml`/`serde_json` (already present)
- Tests: Rust `#[test]`/integration binaries + python e2e in the existing layout

## Commands

```bash
# format + lint (must pass before any push)
cargo fmt --check
cargo +1.92.0-x86_64-pc-windows-gnu clippy --all-targets --no-default-features -- -D warnings

# unit + integration (no live ClickHouse needed)
cargo +1.92.0-x86_64-pc-windows-gnu test --no-default-features

# live-ClickHouse integration (registry tests need the test instance)
# (one shell block — env does not persist across shells)
$env:CLICKHOUSE_URL='http://127.0.0.1:8123'; $env:CLICKHOUSE_USER='test_user'; $env:CLICKHOUSE_PASSWORD='test_pass'
cargo +1.92.0-x86_64-pc-windows-gnu test --no-default-features --test schema_registry_tests

# regression ratchet + corpus census (unchanged: 1477 queries, both dialects)
$env:CLICKHOUSE_URL='http://127.0.0.1:8123'; $env:CLICKHOUSE_USER='test_user'; $env:CLICKHOUSE_PASSWORD='test_pass'
cargo +1.92.0-x86_64-pc-windows-gnu test --no-default-features --test ratchet
cargo +1.92.0-x86_64-pc-windows-gnu test --no-default-features --test integration the_corpus_lowers

# E2E: register → kill → re-register nothing → query succeeds
python tests/e2e/test_schema_persistence_e2e.py
```

## Project Structure

```
src/graph_catalog/schema_registry.rs   ← NEW. Store trait + ClickHouse impl +
     │                                    replay/precedence logic + serialization
     │                                    format v1. SRP: only life of a persisted
     │                                    registration; does not touch translator.
src/server/graph_catalog.rs            ← load_schema_from_content saves the YAML
                                         after a successful in-memory load;
                                         add_to_schema saves a DDL snapshot
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

1. **store** — trait + CH impl + encode/decode + table create (`save`, `load_all`,
   `delete`, `source` tagging). Unit-testable against the mock ClickHouse client.
2. **write-path** — persist on successful `POST /schemas/load` and on DDL mutations.
3. **boot-replay** — restore after config load with precedence; degrade-to-warn when
   ClickHouse is unreachable.
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
| `source` | `LowCardinality(String)` | `api-load` \| `ddl` |
| `format` | `LowCardinality(String)` | `yaml` \| `graph-schema-json` |
| `payload` | `String` | **the original YAML** for `api-load`; the serialized `GraphSchema` JSON for `ddl` |
| `registry_version` | `UInt16` | 1; read side rejects unknown future versions with a warn (upgrade-safe: new fields newly defaulting paths, never crashes) |
| `registered_at` | `DateTime64(3, 'UTC')` | last write |

- Engine `MergeTree ORDER BY name`. Writes are idempotent per name (last wins, cf.
  Kafka Connect config topic semantics). Deletes use ClickHouse **lightweight
  deletes** (server ≥ 25.x confirmed in deployment images); fallback
  `ALTER TABLE … DELETE` if unavailable.
- The default-alias duplication behavior of `initialize_global_schema` is NOT
  persisted: the store holds exactly the names that were explicitly registered at
  runtime. On replay, restore goes through the *same* `load_schema_from_content`
  path the HTTP handler uses — so alias/hash/cache semantics are by construction
  identical to a live registration (single code path = no drift).

### Configuration (opt-in, backward compatible)

- `CLICKGRAPH_SCHEMA_PERSISTENCE` ∈ {`off`, `clickhouse`}, **default `off`**.
  Parsed strictly (unknown value = startup fails loudly — a config typo should
  never silently disable durability).
- No other env var in v1. (Deliberate: the ClickHouse connection and database
  already come from the standard vars; a second knob to point persistence
  elsewhere is a maintenance trap — migrations would need two stores.)
- `off` (and default-on-absence of the flag): the process never touches the
  registry table. Byte-for-byte today's behavior — no reads, no writes, no DDL.
- Embedded (`chdb`) and Databricks modes have no ClickHouse client
  (`src/server/mod.rs:149/253`): `clickhouse` mode logs one clear line and stays
  inert there. (File-backed store for those modes is deliberately out of scope v1;
  the trait keeps the door open.)

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
   - Same name re-registered ⇒ overwrite row (content-hash mismatch warns, exactly
     like the in-memory #463 warning path).
2. **DDL path**: after `add_to_schema` succeeds, the mutated `GraphSchema` is
   stored as `format=graph-schema-json, source=ddl` keyed `default` (or the
   session schema name in effect). Note honestly in docs: this format inherits the
   known-lossy round-trip (vector/fulltext indexes, engine, denormalized metadata —
   `graph_schema.rs:1678`), same as the legacy `graph_catalog` table. Users who
   need byte-perfect restore re-register the YAML.
3. **Boot replay** (after `initialize_global_schema`, only when enabled and a CH
   client exists):
   - Load all rows; for each name **already present** from the config file ⇒ skip
     with an INFO line (`config-defined schema <name> shadows persisted registration <name>`).
     Determinism over recency — a config file is declared intent, and an upgrade
     that edits the file must preempt stale runtime state. (cf. k8s: declarative
     spec beats what's running.)
   - Remaining rows replayed in `registered_at` order through the normal load
     path (CH client available ⇒ `auto_discover_columns` re-resolve against live
     metadata; a restore re-validates reality instead of trusting a stale
     snapshot).
   - A row that fails to replay (its tables vanished, YAML no longer parses under
     a stricter new parser, …): WARN with name + error, continue with the others. One
     bad row may not kill the boot — same convention as ClickGraph's YAML-load fall
     back to DB discovery.
   - Knockout summary: `n restored / m skipped / k failed` in one INFO line.
4. **Delete**: `DELETE /schemas/{name}` ⇒ remove from memory registries + query
   cache invalidation (same call the load path makes) + delete the store row ⇒ 200
   `{"message": ..., "schema_name": ...}`; unknown name ⇒ 404 same body shape as
   `GET /schemas/{name}`. New endpoint ⇒ additive, backward compatible.
5. **Concurrency**: two instances replaying simultaneously is out of scope v1
   (documented; single-instance per the deployment profile). Writes are safe
   (row-level last-wins); replay is read-only over the store + in-memory writes
   through the existing lock discipline (no new lock ordering: replay takes the
   same two write locks the HTTP load path takes, sequentially, via the same
   function).

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
// sketch of the trait boundary (DIP: handlers/boot code never sees ClickHouse)
#[async_trait::async_trait]  // NOTE: currently no async-trait dep; likely
                             // hand-rolled `fn save(&self, ...) -> Result<..>`
                             // over blocking send to avoid a new dependency —
                             // decided in plan phase
pub trait SchemaStore: Send + Sync {
    fn save(&self, reg: &Registration) -> Result<(), StoreError>;
    fn load_all(&self) -> Result<Vec<Registration>, StoreError>;
    fn delete(&self, name: &str) -> Result<bool, StoreError>;
}
```

## Testing Strategy

Four levels, each with a named concern:

1. **Unit (no live deps)** — `tests/rust/integration/schema_registry_tests.rs`,
   built from the corpus_sweep pattern (the orphaned
   `schema_validation_tests.rs` proves that pattern's failure mode: never wire an
   undeclared test file):
   - encode/decode round-trips (`registry-v1`), unknown-version rejection,
   - precedence filter (config names subtract correctly incl. alias `default`),
   - mock-store save/load/delete contract via the trait (solid principles: tests
     pin the *interface*, not ClickHouse).
2. **Integration (live ClickHouse, env-gated)** — same file:
   - table auto-create idempotence; save → load_all → delete round-trip;
   - last-write-wins on same name; lightweight-delete path taken;
   - replay through `load_schema_from_content` produces
     `GET /schemas/{name}` counts equal to a fresh live registration.
3. **Server-level (rust, router-driven)** — POST/DELETE handlers against
   `StateRouter` like `databricks_introspect_tests.rs`: response shapes (incl.
   `persisted: false` degradation field), 404 shape, old-payload compat (a
   pre-feature client that ignores extra fields keeps working).
4. **E2E (python)** — register → **SIGKILL** → restart → assert `GET /schemas`
   lists it and a query against it returns rows; assert zero-writes on
   persistence=off (count `system.tables` rows before/after); assert shadowing
   (config-file name over persisted name). Kill/restart needs the real binary —
   patterned after `tests/e2e/test_query_cache_e2e.py`.

Backward-compat proof is a test class of its own: with the flag unset, a full
`/schemas/load` + `/query` session must produce identical responses to `main`
(precisely how the OTLP PR proved HTTP parity).

## Boundaries

- **Always:** run the four gates (`fmt`, `clippy -D warnings`, full no-default
  suite, ratchet + census 1477) before any push; keep the offline (no-CH) suite
  green without ClickHouse; commit the spec alongside the code that satisfies it.
- **Ask first:** flipping the default to on; introducing a second store backend
  (file); touching `GLOBAL_SCHEMA_CONFIGS` semantics beyond additive use;
  lightweight-delete fallbacks; any new crate dependency (avoid `async-trait` if
  feasible).
- **Never:** persist without opt-in; write to a database the operator didn't
  configure; let a store failure fail a registration request; shadow a
  config-defined schema with persisted state; swallow unknown
  `registry_version`s silently (warn, skip row); delete a config-defined schema
  from the store behind the operator's back (config schemas are never written to
  the store at all).

## Success Criteria

1. `POST /schemas/load` (persistence `clickhouse`) ⇒ row in
   `clickgraph_schema_registry` before the response returns; kill -9; restart ⇒
   `GET /schemas` lists the name; a query against it returns rows with **no
   re-registration**. (E2E script is the acceptance runner.)
2. Persistence `off`/unset ⇒ zero CH writes attributable to the new code, and
   live-parity with `main` on all schema endpoints (pinned by server-level tests).
3. Config-file + persisted collision ⇒ config wins; log states the skip; count of
   skipped = expectation in the knockout line.
4. `CREATE NODE` (persistence on) ⇒ after restart, the created node label is
   queryable without re-running DDL.
5. `DELETE /schemas/{name}` ⇒ memory + store + query cache all drop it; 404 on
   the gone name (both GET and DELETE).
6. ClickHouse unreachable at boot with persistence on ⇒ server still boots
   healthy (WARN, `0 restored / 0 skipped / 0 failed` or retry-less skip) —
   matches resilient-startup convention.
7. Ratchet + census unchanged (1477 both dialects); no golden churn; no new deps.
8. Unit+integration counts added and green on Windows `--no-default-features`
   toolchain and in CI (default features).

## Open Questions (for maintainer review, not blocking the build)

- Flip default `off → clickhouse` in a follow-up release once field feedback is in?
- Should `GET /schemas/{name}` gain a `?format=yaml` export on the same path
  (cheap once payloads are stored; nice for GitOps round-tripping)?
- Multi-instance concurrent replay (leader election via CH lock or
  `system.replicas`?) — defer until someone actually runs replicas.
