//! Persistent schema registry — the durability behind `POST /schemas/load`
//! (spec: `docs/design/SCHEMA_PERSISTENCE.md`).
//!
//! # The problem it solves
//!
//! `load_schema_from_content` registers into the in-memory `GLOBAL_SCHEMAS` /
//! `GLOBAL_SCHEMA_CONFIGS` maps and drops the YAML text it was given. Net
//! effect: a restart or upgrade loses every runtime registration. Systems
//! that ship this guarantee (Kafka Connect's `config.storage.topic`,
//! Debezium's `schema.history.internal`) externalize the source text to
//! storage owned by the platform itself and replay it at boot; this module is
//! that pattern against ClickHouse, which is already a hard dependency.
//!
//! Not covered here: **DDL-created tables** (`ddl_handler` was never
//! reachable from query dispatch — every non-read statement is refused with
//! 400, so no DDL row was ever written). DDL-created schemas keep bouncing on
//! the pre-existing recovery they always had on `main`: the lossy
//! `graph_catalog` table that `add_to_schema` feeds unconditionally (its
//! serializations lose `#[serde(skip)]` fields — vector/fulltext indexes,
//! engine info, denormalized metadata; `graph_schema.rs`). Wiring real DDL
//! durability through this module is future work recorded in the spec.
//!
//! # What is stored, and why source text (not decoded structs)
//!
//! One row per registered name carries the **original YAML** the caller
//! posted. Decoded structs are lossy today (vector/fulltext indexes, engine
//! info and denormalized metadata are `#[serde(skip)]` on `graph_schema.rs`),
//! so restore-at-boot must start from the source — the YAML also keeps every
//! schema feature the parser grew after the lossy serialization was written.
//!
//! # Guarantees (and their boundaries)
//!
//! - Opt-in: `CLICKGRAPH_SCHEMA_PERSISTENCE=clickhouse`. Unset ⇒ this module
//!   is unreachable — no reads, no writes, no DDL; behavior stays
//!   byte-identical to a server without it.
//! - Registration first: a store failure never fails a registration; the
//!   caller surfaces `persisted: false` instead. Config failures (unknown
//!   enum value) fail startup loudly, like every other `CLICKGRAPH_*` parse.
//! - Config file wins: a name declared through `GRAPH_CONFIG_PATH` is
//!   authoritative; a persisted registration under that name is skipped at
//!   replay with a stated reason. A persisted store is a *runtime* ledger,
//!   never a shadow of declared intent.
//! - One boot, one pass: replay runs before the listener binds and is
//!   read-only against the store. Multi-instance replay is out of scope;
//!   single-instance deployments are the target (see the spec).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

use super::connection_pool::RoleConnectionPool;
use super::graph_catalog;
use crate::config::{SchemaPersistenceMode, ServerConfig};

/// The registry table lives in the client's configured database — the same
/// database the legacy `graph_catalog` table uses, so operators see both
/// durability layers in one place and no second database is ever provisioned.
pub const REGISTRY_TABLE: &str = "clickgraph_schema_registry";

/// Readers refuse rows written by a newer registry format instead of guessing
/// at their shape. Bumping this number is the upgrade contract: old servers
/// skip new rows with a warning, new servers read old rows (`registry_version`
/// stays additive so far).
pub const REGISTRY_VERSION: u16 = 1;

/// Who wrote the row. Only `api-load` rows are produced today (the HTTP
/// registration path); replay skips unknown sources/formats with a warning so
/// an older row left in the table can't confuse a newer (or older) server.
pub const SOURCE_API_LOAD: &str = "api-load";

/// Serialization format of `payload`.
pub const FORMAT_YAML: &str = "yaml";

/// Upper bound on one round-trip against the store (every public op — DDL,
/// insert, delete, probe, replay read — runs under it), mapped to the same
/// outcomes a genuinely unreachable store takes: a registration answers
/// `persisted: false`, a boot replay degrades to the no-restore WARN. This
/// replaces the one way a slow or wedged ClickHouse used to hang the API:
/// the table-preparation mutex used to serialize behind a DDL that never
/// answered, which could hold the first registrations past `/health`'s
/// readiness window. A constant, deliberately not an env knob: the value
/// bounds a resource health check, not workload capacity.
///
/// Dropping a timeout's future also drops its in-flight response, so the
/// store call does not keep consuming a connection after the caller gave
/// up. A CREATE that a timeout abandons is `IF NOT EXISTS` — the next
/// attempt either re-runs it or probes past it.
pub const STORE_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Names the HTTP surface and the Operational CLI reserve. Accepting them
/// would collide with the registry's own routes (`load`) or the CLI's
/// introspection workflow (`draft`, `introspect`, `discover-prompt`), so
/// `POST /schemas/load` answers 400 for them up front — before any memory
/// or store work — and every other endpoint keeps its pre-feature behavior.
pub const RESERVED_SCHEMA_NAMES: [&str; 4] = ["load", "draft", "introspect", "discover-prompt"];

/// Pure (unit-tested) reserved-name check; exact match, like every other
/// schema-name comparison in this module.
pub fn is_reserved_schema_name(name: &str) -> bool {
    RESERVED_SCHEMA_NAMES.contains(&name)
}

/// Generous caps for the two operator-entered identifiers (`schema_name` on
/// `POST /schemas/load`, `role` on both endpoints). Not storage limits —
/// hygiene: both identifiers can reach operator-facing logs, and a tiny
/// ceiling keeps an API abuser from turning log entries or the store into
/// their dustbin. The ClickHouse identifier limit is a few hundred bytes
/// anyway; these sit above it wherever it is.
const MAX_SCHEMA_NAME_LEN: usize = 512;
const MAX_ROLE_NAME_LEN: usize = 256;

/// Pure (unit-tested) schema-name acceptability check, run by both admin
/// endpoints before any gate, registry or store work. `None` = accept.
/// Every rejection is a 400 at the door — an empty name registers (`404`s
/// on GET/DELETE afterwards, unrestorable by construction), and a control
/// character forges log lines ("Cache invalidated for schema: ...\nERROR").
pub fn schema_name_error_if_any(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        Some("schema name must not be empty")
    } else if name.chars().any(|c| c.is_control()) {
        Some("schema name must not contain control characters")
    } else if name.len() > MAX_SCHEMA_NAME_LEN {
        Some("schema name exceeds the 512-byte limit")
    } else {
        None
    }
}

/// Pure (unit-tested) `role` acceptability check for both endpoints. Empty
/// is deliberately not an error here: callers normalize it away before this
/// check, and an accidental `""` must stay a "no role", not a 400 (the
/// round-one contract). Control characters are refused because the pool
/// logs each first-seen role raw; an absurd length keeps a role from
/// becoming a log-line payload.
pub fn role_error_if_any(role: &str) -> Option<&'static str> {
    if role.chars().any(|c| c.is_control()) {
        Some("role must not contain control characters")
    } else if role.len() > MAX_ROLE_NAME_LEN {
        Some("role exceeds the 256-byte limit")
    } else {
        None
    }
}

/// The read side's longer bound: a boot replay carries up to the payload
/// byte budget (64 MiB) over a fetch pair (metadata, then payloads over
/// `FINAL`), and a healthy-but-slow store answering a few seconds per
/// statement would spend half the 10 s one and read as an outage at boot.
/// Published for the docs; constructed into every store alongside
/// [`STORE_CALL_TIMEOUT`] — both constants, never knobs: they bound a
/// degenerate store, not workload capacity.
pub const REPLAY_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Whether an op runs under [`STORE_CALL_TIMEOUT`] or
/// [`REPLAY_READ_TIMEOUT`], the elapsed-outcome maps onto the error path
/// every caller already handles. The op name lands in the message for the
/// log/API trail only — values never do. One engine; unit tests reach it
/// with option durations because a `Duration` is an argument, never a
/// caller-visible knob.
async fn with_timeout_within<T>(
    bound: Duration,
    op: &'static str,
    fut: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    match tokio::time::timeout(bound, fut).await {
        Ok(result) => result,
        Err(_elapsed) => Err(format!(
            "registry store unreachable: '{op}' got no answer \
             within {bound:?}"
        )),
    }
}

/// One persisted registration (`registry-v1`).
///
/// `registered_at_ms` is written by the server (never read back as truth):
/// restore order is "oldest first", so a name re-registered twice ends on its
/// latest registration, the way Kafka's compacted config topic ends on the
/// last write. The write path stamps it from
/// [`ClickhouseSchemaStore::next_registered_at_ms`], which is strictly
/// increasing and never steps back with the host clock — the version column
/// of `ReplacingMergeTree` stays trustworthy through an NTP correction or a
/// VM migration, not just through same-millisecond writes.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct Registration {
    pub name: String,
    pub source: String,
    pub format: String,
    pub payload: String,
    pub registry_version: u16,
    pub registered_at_ms: u64,
}

impl Registration {
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Phase-1 shape of the two-phase replay fetch on Current tables: the
/// bounded metadata read (`payload_len` MATERIALIZED included, payload
/// excluded) that spends the byte budget server-side. The payload itself is
/// fetched only in phase 2 ([`ClickhouseSchemaStore::payloads_for`]), for
/// the rows the gate admits.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct ReplayMetaRow {
    pub name: String,
    pub source: String,
    pub format: String,
    pub registry_version: u16,
    pub registered_at_ms: u64,
    pub payload_len: u64,
}

/// Phase-2 pairing (name → payload) of the two-phase fetch. The row's
/// `registered_at_ms` rides with each payload: phase 2 fetches by
/// (name, timestamp), because a name alone admits a row that was
/// re-registered between the two phases — its new payload landing under
/// the old phase-1 metadata (the client-side budget re-check catches the
/// shape, but the pipe closes at the fetch itself).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct NamePayload {
    name: String,
    registered_at_ms: u64,
    payload: String,
}

/// A replay gate. Three buckets, each conservative:
/// - `restorable`: this server wrote the row (known source) in a version it
///   knows how to read (a `yaml` payload).
/// - `future_versioned`: written by a NEWER ClickGraph — never guessed at,
///   never crashed into.
/// - `unknown_source`: correct version, but produced by a writer this server
///   does not claim (`source != api-load` today). The version check wins over
///   the source check: a row from an unknown future server can be either
///   kind, and "future version" is the strictly safer refusal.
pub struct ReplayGate {
    pub restorable: Vec<Registration>,
    pub future_versioned: Vec<Registration>,
    pub unknown_source: Vec<Registration>,
}

/// Gate rows for boot replay (pure — unit-tested): registry version first,
/// then the writer source.
pub fn gate_for_replay(rows: Vec<Registration>) -> ReplayGate {
    let mut restorable = Vec::new();
    let mut future_versioned = Vec::new();
    let mut unknown_source = Vec::new();
    for row in rows {
        if row.registry_version > REGISTRY_VERSION {
            future_versioned.push(row);
        } else if row.source != SOURCE_API_LOAD {
            unknown_source.push(row);
        } else {
            restorable.push(row);
        }
    }
    ReplayGate {
        restorable,
        future_versioned,
        unknown_source,
    }
}

/// Split replayable rows against the config-defined names (pure —
/// unit-tested).
///
/// `configured_names` are satisfied at boot by `initialize_global_schema`; a
/// persisted row under one of them is deliberately skipped: the config file
/// is declared intent, and after an upgrade that edits the file, stale
/// runtime registrations must not resurrect old shapes. This makes boot
/// resolution deterministic from static inputs alone.
pub fn partition_for_replay(
    rows: Vec<Registration>,
    configured_names: &[String],
) -> (Vec<Registration>, Vec<Registration>) {
    let mut to_restore = Vec::new();
    let mut shadowed = Vec::new();
    for row in rows {
        if configured_names.iter().any(|n| n == &row.name) {
            shadowed.push(row);
        } else {
            to_restore.push(row);
        }
    }
    (to_restore, shadowed)
}

// -------------------------------------------------------------------- store

/// The durable store strategy. A closed enum instead of a trait object: two
/// variants ship in v1, so calls stay concrete and monomorphic (no `dyn`
/// dispatch, no `async-trait` boxing); a third backend (file) is a new
/// variant plus an impl block.
pub enum SchemaStore {
    /// Persistence off — or the flag is set but this server mode has no
    /// ClickHouse client (the no-client server modes), which the spec falls
    /// back to "inert" on. Every method is a no-op through construction.
    Off,
    Clickhouse(Box<ClickhouseSchemaStore>),
}

impl SchemaStore {
    /// The one construction point: mode from the server config, client and
    /// role pool from whatever built `AppState` this boot. Stays log-quiet:
    /// the boot-time "no client in this server mode" notice belongs to
    /// `replay` (once per boot), and per-request constructions must not
    /// spam it. The role pool rides along (`None` when this server mode has
    /// none) so the endpoint-level `role` can resolve through the same
    /// pools `/query` uses; boot replay itself never touches it.
    pub fn from_parts(
        mode: SchemaPersistenceMode,
        clickhouse_client: Option<Client>,
        role_pool: Option<std::sync::Arc<RoleConnectionPool>>,
    ) -> Self {
        if mode == SchemaPersistenceMode::Clickhouse {
            return match clickhouse_client {
                Some(client) => SchemaStore::Clickhouse(Box::new(
                    ClickhouseSchemaStore::with_role_pool(client, role_pool),
                )),
                None => SchemaStore::Off,
            };
        }
        SchemaStore::Off
    }

    /// Persist a YAML registration. `Err` = the store refused — the
    /// registration itself already succeeded in memory (the caller reports
    /// `persisted: false`). `Ok(())` = the row is stored (or the store is
    /// `Off`, which no-ops by construction). `role` names a ClickHouse RBAC
    /// role for the write (resolved through the server's role pool); `None`
    /// or an unwired pool keeps the store's default client.
    pub async fn save_yaml(
        &self,
        name: &str,
        yaml: &str,
        role: Option<&str>,
    ) -> Result<(), String> {
        match self {
            SchemaStore::Off => Ok(()),
            SchemaStore::Clickhouse(store) => {
                store
                    .upsert(name, SOURCE_API_LOAD, FORMAT_YAML, yaml.to_string(), role)
                    .await
            }
        }
    }

    /// Remove a registration row. Returns `Ok(true)` when the row is gone:
    /// the ClickHouse store reports success even when the name never had a
    /// row (`DELETE FROM` matches nothing there) — the end state is the same,
    /// so callers treat both as "not persisted anymore", not as an error.
    /// `role` resolves the same way [`SchemaStore::save_yaml`]'s does; the
    /// `Off` store answers `Ok(false)` (nothing was ever persisted there)
    /// so a memory-path DELETE can still report `persisted: false` honestly.
    pub async fn delete(&self, name: &str, role: Option<&str>) -> Result<bool, String> {
        match self {
            SchemaStore::Off => Ok(false),
            SchemaStore::Clickhouse(store) => store.delete(name, role).await,
        }
    }

    /// Whether a row with this exact name exists. The store-only half of
    /// `DELETE /schemas/{name}`: a failed-at-replay row is remembered by the
    /// store but not loaded into memory, and the boot WARN advertises that
    /// endpoint as its escape hatch — the probe keeps that promise honest
    /// instead of answering 404 for a name that only lives in the store.
    pub async fn exists(&self, name: &str) -> Result<bool, String> {
        match self {
            SchemaStore::Off => Ok(false),
            SchemaStore::Clickhouse(store) => store.exists(name).await,
        }
    }

    /// Boot replay's one read, bounded on BOTH budgets: at most `max_rows`
    /// oldest-first rows whose cumulative payload size also stays within
    /// `max_bytes`. Restoring everything a compartment-compromised or runaway
    /// client ever persisted, unbounded, converts a one-time write burst into
    /// a per-restart cost that can push boot past any readiness window — and
    /// the bound must hold at the fetch itself, not only after the bytes are
    /// already in memory: the select below stops at the cumulative payload
    /// budget inside ClickHouse (a window sum over the replay order), so the
    /// byte budget bounds what a boot restores (the server's peak memory while
    /// fetching is a small multiple of it: phase 2 reads whole parts). Total
    /// rows beyond either cap are counted in [`ReplayFetch`] so the caller
    /// can warn with exact numbers.
    pub async fn load_replay_rows(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<ReplayFetch, String> {
        match self {
            SchemaStore::Off => Ok(ReplayFetch::default()),
            SchemaStore::Clickhouse(store) => store.load_replay_rows(max_rows, max_bytes).await,
        }
    }
}

/// A single bounded replay read.
#[derive(Debug, Default)]
pub struct ReplayFetch {
    /// Current-version, api-load rows, oldest first, within both budgets.
    pub restorable: Vec<Registration>,
    /// Rows whose `registry_version` is newer than this server (never
    /// restored; the store warns with each name at fetch time). Their
    /// payload is not fetched on Current tables (nobody reads it; on Legacy
    /// tables the single-phase select materializes it, as before — see
    /// `load_replay_rows`).
    pub future_versioned: Vec<Registration>,
    /// Current-version rows whose `source` this server does not claim
    /// (`!= api-load`) — never restored either: the writer is unknown, and
    /// unknown writers' payloads are never guessed at. The store warns with
    /// each name and source at fetch time. Same payload note as
    /// `future_versioned`.
    pub unknown_source: Vec<Registration>,
    /// Deduped rows in the table (`FINAL` count) — rows beyond either
    /// budget were never fetched, so this may exceed the vectors' lengths.
    pub total_rows: u64,
    /// Rows actually fetched (`restorable + future_versioned +
    /// unknown_source` lengths).
    pub fetched: usize,
}

/// What the registry's table is capable of (probed once per process):
/// `Current` carries the `payload_len` column (a MATERIALIZED byte length)
/// and gets the two-phase replay fetch; `Legacy` is any table provisioned
/// without it — by an older server or a hand-written DDL — kept fully
/// working through the single-phase fetch (`load_replay_rows_inner`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableLayout {
    Legacy,
    Current,
}

/// ClickHouse-backed store. Statement text is fixed; every operator-supplied
/// value rides in as a bound parameter — schema names are operator input and
/// are never interpolated into SQL text.
pub struct ClickhouseSchemaStore {
    /// The default client: boot replay and every role-less op. Built with
    /// `wait_end_of_query` so its DDL answers before the next step runs.
    client: Client,
    /// Role pools for role-scoped writes (`role` on the two registry
    /// endpoints), resolved exactly like `/query` resolves its role. `None`
    /// keeps every write on the default client.
    role_pool: Option<std::sync::Arc<RoleConnectionPool>>,
    /// One-time table preparation. `None` = not yet ensured; a *failed*
    /// ensure stays `None` and the next op retries it, so a least-privilege
    /// boot (no CREATE grant, table absent) degrades its first ops but
    /// recovers on the first op after the table appears — including a
    /// role-privileged one. The mutex serializes the DDL/probe pair so it
    /// runs once per process on success: issuing a CREATE before every op
    /// (the previous design) both blocked the hot path on its mutex and
    /// raced concurrent first-boot registrations against the metadata.
    table_prepared: tokio::sync::Mutex<Option<TableLayout>>,
    /// Floor for `registered_at_ms`: the table's high-water mark at ensure
    /// time, raised by every write this process makes (see
    /// [`ClickhouseSchemaStore::next_registered_at_ms`]).
    registered_at_floor: AtomicU64,
    /// Per-instance op bounds, defaulting to the module constants: the
    /// production paths run the shipped values; unit tests reach the same
    /// call sites with short bounds instead of a free fn that ignores the
    /// store — a test must go red if a call site ever bypasses the bound.
    call_bound: Duration,
    replay_bound: Duration,
}

impl ClickhouseSchemaStore {
    pub fn new(client: Client) -> Self {
        Self::with_role_pool(client, None)
    }

    /// DDL must not be fire-and-forget here: a store that answers before
    /// its table exists makes the first registration racy.
    pub fn with_role_pool(
        client: Client,
        role_pool: Option<std::sync::Arc<RoleConnectionPool>>,
    ) -> Self {
        Self {
            client: client.clone().with_option("wait_end_of_query", "1"),
            role_pool,
            table_prepared: tokio::sync::Mutex::new(None),
            registered_at_floor: AtomicU64::new(0),
            call_bound: STORE_CALL_TIMEOUT,
            replay_bound: REPLAY_READ_TIMEOUT,
        }
    }

    /// The bounded call a store op makes: the same
    /// [`with_timeout_within`] engine the replay read uses, with this
    /// store's per-call duration. Tests construct a store with short
    /// bounds so a wedged route surfaces in milliseconds instead of the
    /// (deliberate, no-knob) 10 s.
    async fn bound_call<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        with_timeout_within(self.call_bound, op, fut).await
    }

    /// The client a store write runs under. `None` keeps the default client
    /// (round-1 behavior, `wait_end_of_query` included); a role resolves
    /// through the role pool exactly as `/query` does, with the same
    /// `wait_end_of_query` discipline layered on the per-role client so
    /// first-use DDL stays synchronous. Boot replay deliberately never
    /// resolves a role: a read-only default user must be able to restore.
    async fn client_for_write(&self, role: Option<&str>) -> Client {
        match (role, &self.role_pool) {
            (None, _) | (Some(_), None) => self.client.clone(),
            (Some(role), Some(pool)) => pool
                .get_client(Some(role))
                .await
                .with_option("wait_end_of_query", "1"),
        }
    }

    /// Create the registry table once per process (idempotent by virtue of
    /// `IF NOT EXISTS`). `Err` is not fatal by itself — a least-privilege
    /// user may lack the CREATE grant while the table exists.
    async fn try_create_table(&self, client: &Client) -> Result<(), String> {
        // `payload_len` rides on fresh tables: a MATERIALIZED byte length the
        // replay fetch uses to bound its payload transfer without touching
        // the payloads at all (the two-phase fetch below). Appended last so a
        // full-width insert's column order is unchanged; MATERIALIZED
        // columns are never part of an insert.
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (\
             name String, \
             source LowCardinality(String), \
             format LowCardinality(String), \
             payload String, \
             registry_version UInt16, \
             registered_at_ms UInt64, \
             payload_len UInt64 MATERIALIZED length(payload)\
             ) ENGINE = ReplacingMergeTree(registered_at_ms) ORDER BY name"
        );
        client
            .query(&create)
            .execute()
            .await
            .map(|_| ())
            .map_err(|e| format!("registry table create failed: {e}"))
    }

    /// One cheap probe pair answers both "does the table exist (and can we
    /// read it)" and "which layout" without depending on `system.columns`
    /// (some hardened setups restrict system tables). The LIMIT 0 on the
    /// column probe keeps it metadata-only.
    async fn detect_layout(&self, client: &Client) -> Result<TableLayout, String> {
        match client
            .query(&format!("SELECT payload_len FROM {REGISTRY_TABLE} LIMIT 0"))
            .execute()
            .await
        {
            Ok(_) => Ok(TableLayout::Current),
            Err(_) => client
                .query(&format!("SELECT name FROM {REGISTRY_TABLE} LIMIT 0"))
                .execute()
                .await
                .map(|_| TableLayout::Legacy)
                .map_err(|e| format!("registry table probe failed: {e}")),
        }
    }

    /// Prepare the registry table exactly once per process. The CREATE is
    /// attempted first; when it fails (no CREATE grant, in-flight DDL, DDL
    /// contention) the read probe decides whether the table already exists —
    /// the least-privilege path a DBA-provisioned table takes, and the
    /// reason a server whose user cannot CREATE still boots, replays, and
    /// persists. The ensure succeeds only when the table reads AND the
    /// monotonic clock seeds from it: a success is cached for the process's
    /// lifetime, and a failed seed, like a failed create/probe pair, stays
    /// uncached and is retried by the next op — otherwise a floor of 0 (or
    /// whatever a warned seed left) would be frozen into the process and a
    /// re-registration of an old name could stamp a row BELOW the table's
    /// high-water mark with `persisted: true`, which a restart then serves.
    async fn ensure_table(&self, client: &Client) -> Result<TableLayout, String> {
        let mut prepared = self.table_prepared.lock().await;
        if let Some(layout) = *prepared {
            return Ok(layout);
        }

        let create = self.try_create_table(client).await;
        match self.detect_layout(client).await {
            Ok(layout) => match self.seed_registered_at_floor(client).await {
                Ok(()) => {
                    *prepared = Some(layout);
                    Ok(layout)
                }
                Err(seed_err) => Err(format!(
                    "registry registered-at seed failed: {seed_err}\n\
                     (the table answered its probe; the high-water read \
                      did not - the write's registered_at_ms floor must \
                      not be assumed 0)"
                )),
            },
            Err(probe_err) => match create {
                Ok(()) => Err(probe_err),
                Err(create_err) => Err(format!("{create_err} (table probe: {probe_err})")),
            },
        }
    }

    /// Raise the monotonic floor to the table's high-water mark, so values
    /// this process writes are never below what any earlier process wrote —
    /// the fix for a host clock that stepped back between boots. `max` with
    /// the floor is read over all parts (no `FINAL`): any part's value can
    /// only raise it, and a higher floor is always safe. An unreadable
    /// high-water mark is an ERROR, not a warning: the floor is load-bearing
    /// (a row stamped below the table high-water can win `ReplacingMergeTree`'s
    /// collapse against every row written after it), so the ensure that owns
    /// this read must not cache itself past the failure.
    async fn seed_registered_at_floor(&self, client: &Client) -> Result<(), String> {
        client
            .query(&format!(
                "SELECT max(registered_at_ms) FROM {REGISTRY_TABLE}"
            ))
            .fetch_one::<u64>()
            .await
            .map(|high_water| {
                self.registered_at_floor
                    .fetch_max(high_water, Ordering::Relaxed);
            })
            .map_err(|e| {
                format!(
                    "the table's registered-at high-water mark is \
                     unreadable ({e}); timestamps would stay monotonic \
                     within this boot only"
                )
            })
    }

    /// Strictly increasing, even when the host clock steps back: each call
    /// returns `max(now, floor + 1)` under a CAS loop, so a boot whose clock
    /// regressed writes values above the table's high-water mark instead of
    /// randomly losing `ReplacingMergeTree`'s collapse to an old row. Within
    /// one boot the values can only rise (`floor` is carried by every
    /// successful CAS). The `+1` saturates: a hand-edited row with
    /// `registered_at_ms = u64::MAX` must fold the floor to a standstill,
    /// not wrap it to 0 (a release-mode overflow would).
    fn next_registered_at_ms(&self) -> u64 {
        let now = Registration::now_ms();
        let mut current = self.registered_at_floor.load(Ordering::Relaxed);
        loop {
            let next = if now > current {
                now
            } else {
                current.saturating_add(1)
            };
            match self.registered_at_floor.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(actual) => current = actual,
            }
        }
    }

    async fn upsert(
        &self,
        name: &str,
        source: &str,
        format: &str,
        payload: String,
        role: Option<&str>,
    ) -> Result<(), String> {
        self.bound_call("registry upsert", async {
            self.upsert_inner(name, source, format, payload, role).await
        })
        .await
    }

    async fn upsert_inner(
        &self,
        name: &str,
        source: &str,
        format: &str,
        payload: String,
        role: Option<&str>,
    ) -> Result<(), String> {
        let client = self.client_for_write(role).await;
        self.ensure_table(&client).await?;
        let row = Registration {
            name: name.to_string(),
            source: source.to_string(),
            format: format.to_string(),
            payload,
            registry_version: REGISTRY_VERSION,
            registered_at_ms: self.next_registered_at_ms(),
        };
        let mut insert = client
            .insert(REGISTRY_TABLE)
            .map_err(|e| format!("registry insert failed: {e}"))?;
        insert
            .write(&row)
            .await
            .map_err(|e| format!("registry insert failed: {e}"))?;
        insert
            .end()
            .await
            .map_err(|e| format!("registry insert failed: {e}"))?;
        Ok(())
    }

    /// Whether a row with this exact name exists (deduped read via
    /// `FINAL`). Deliberately does **not** `ensure_table`: probing a name on
    /// a database that never registered anything must answer "no", not
    /// create the table — write-on-read would turn every failed DELETE
    /// probe into a DDL side effect. A missing table errors; the caller
    /// treats that as `false` (nothing was ever persisted there).
    pub async fn exists(&self, name: &str) -> Result<bool, String> {
        self.bound_call("registry exists probe", self.exists_inner(name))
            .await
    }

    async fn exists_inner(&self, name: &str) -> Result<bool, String> {
        let count: u64 = self
            .client
            .query(&format!(
                "SELECT count() FROM {REGISTRY_TABLE} FINAL WHERE name = ?"
            ))
            .bind(name.to_string())
            .fetch_one::<u64>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;
        Ok(count > 0)
    }

    /// `FINAL` collapses superseded rows of the same name to the last write
    /// (ReplacingMergeTree's compaction, read live — the same idiom the
    /// legacy `graph_catalog` read uses). The version column
    /// (`registered_at_ms`) makes the collapse deterministic: two same-name
    /// writes can land in the same millisecond, and — with the monotonic
    /// stamping of [`ClickhouseSchemaStore::next_registered_at_ms`] — a
    /// clock that stepped back cannot hand the collapse to an older row.
    ///
    /// Both budgets bound the fetch itself, whatever the table's layout:
    /// the byte cap cuts inside the statement, before any payload
    /// transfer, and a boot with a 64 MiB budget pulls at most 64 MiB of
    /// payload over the wire, whatever the table holds — the cap cannot be
    /// outflanked by a fat table. Rows beyond either cap are counted in
    /// `total_rows` vs `fetched`, never silently dropped.
    ///
    /// Two fetch shapes (layout detected once per process at
    /// [`ensure_table`]):
    /// - Current tables (`payload_len` column): a **two-phase** fetch. Phase
    ///   1 reads only metadata plus `payload_len` — no payload crosses the
    ///   wire — and spends the byte budget server-side with a cumulative
    ///   `sum(payload_len)` window over the replay order. Phase 2 fetches
    ///   payloads (by bound name) for the rows that can actually restore.
    ///   Future-versioned and foreign-sourced rows spend budget but never
    ///   pay the payload transfer, and the server reads payloads only for the
    ///   rows that can restore (a 300-row × 900 KiB table peaked at ~1.13 GiB
    ///   server-side with the single-phase read, ~180 MiB at a 64 MiB budget
    ///   with this one).
    /// - Legacy tables: the single-phase select that reads payload with the
    ///   metadata (`sum(length(payload))` window), exactly as earlier
    ///   versions of this module did — a provisioned table keeps working,
    ///   with the older memory profile.
    ///
    /// Budget spend stays shared by every fetched row kind in both shapes:
    /// the bound is on boot work, not only on restoration.
    pub async fn load_replay_rows(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<ReplayFetch, String> {
        with_timeout_within(
            self.replay_bound,
            "registry replay read",
            self.load_replay_rows_inner(max_rows, max_bytes),
        )
        .await
    }

    async fn load_replay_rows_inner(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<ReplayFetch, String> {
        let layout = self.ensure_table(&self.client).await?;

        // Count first so "N rows skipped over the budget" is an exact
        // statement, not a guess. Two small reads instead of one unbounded
        // one: the count on FINAL is cheap on this table (one row per
        // registration, boot-time only), and the select below then bounds
        // payload transfer regardless of what is in the table.
        let total_rows: u64 = self
            .client
            .query(&format!("SELECT count() FROM {REGISTRY_TABLE} FINAL"))
            .fetch_one::<u64>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;

        // Phase 1 (both layouts): the budgeted, ordered row set. A Current
        // table reads metadata + payload_len only and hands the payloads to
        // phase 2; a Legacy table reads payload along with the metadata —
        // the one shape change between them.
        let mut rows: Vec<Registration> = match layout {
            TableLayout::Current => {
                let select = format!(
                    "SELECT name, source, format, registry_version, \
                     registered_at_ms, payload_len FROM (\
                     SELECT name, source, format, registry_version, \
                            registered_at_ms, payload_len, \
                            sum(payload_len) OVER (ORDER BY registered_at_ms, \
                             name) AS cum_bytes \
                     FROM {REGISTRY_TABLE} FINAL\
                     ) WHERE cum_bytes <= {max_bytes} \
                     ORDER BY registered_at_ms, name LIMIT {max_rows}"
                );
                let meta: Vec<ReplayMetaRow> = self
                    .client
                    .query(&select)
                    .fetch_all::<ReplayMetaRow>()
                    .await
                    .map_err(|e| format!("registry read failed: {e}"))?;
                // Payloads are not known yet (phase 2 fills them, restorable
                // rows only); the gate runs on the metadata columns, which
                // is all it consults.
                meta.into_iter()
                    .map(|meta| Registration {
                        name: meta.name,
                        source: meta.source,
                        format: meta.format,
                        payload: String::new(),
                        registry_version: meta.registry_version,
                        registered_at_ms: meta.registered_at_ms,
                    })
                    .collect()
            }
            TableLayout::Legacy => {
                let select = format!(
                    "SELECT name, source, format, payload, registry_version, \
                     registered_at_ms FROM (\
                     SELECT name, source, format, payload, registry_version, \
                            registered_at_ms, \
                            sum(length(payload)) OVER (ORDER BY registered_at_ms, \
                             name) AS cum_bytes \
                     FROM {REGISTRY_TABLE} FINAL\
                     ) WHERE cum_bytes <= {max_bytes} \
                     ORDER BY registered_at_ms, name LIMIT {max_rows}"
                );
                self.client
                    .query(&select)
                    .fetch_all::<Registration>()
                    .await
                    .map_err(|e| format!("registry read failed: {e}"))?
            }
        };

        let mut gate = gate_for_replay(std::mem::take(&mut rows));
        for row in &gate.future_versioned {
            log::warn!(
                "registry row '{}' was written by registry_version {} \
                 (newer than this server's {REGISTRY_VERSION}); skipping it",
                log_safe(&row.name),
                row.registry_version
            );
        }
        for row in &gate.unknown_source {
            log::warn!(
                "registry row '{}' was written by an unclaimed source '{}' \
                 (this server restores only '{SOURCE_API_LOAD}') — skipping it",
                log_safe(&row.name),
                log_safe(&row.source)
            );
        }

        // Phase 2 (Current tables only): payload transfer for the rows the
        // gate admits — the only rows whose payload anyone reads. A name
        // that vanished between the two reads (a concurrent delete or
        // rewrite) is dropped with an exact WARN rather than restored
        // empty; rows beyond the budget were never in phase 1 at all.
        if matches!(layout, TableLayout::Current) {
            let payloads = self.payloads_for(&gate.restorable).await?;
            let mut with_payload = Vec::with_capacity(gate.restorable.len());
            let mut vanished = 0usize;
            for row in std::mem::take(&mut gate.restorable) {
                match payloads.get(&(row.name.clone(), row.registered_at_ms)) {
                    Some(payload) => {
                        let mut row = row;
                        row.payload = payload.clone();
                        with_payload.push(row);
                    }
                    None => {
                        vanished += 1;
                        log::warn!(
                            "registry row '{}' vanished or was rewritten \
                             between the replay's metadata and payload \
                             reads; not restored this boot",
                            log_safe(&row.name)
                        );
                    }
                }
            }
            gate.restorable = with_payload;
            if vanished > 0 {
                log::warn!(
                    "replay's payload pass lost {vanished} row(s) to \
                     concurrent changes"
                );
            }
        }

        Ok(ReplayFetch {
            fetched: gate.restorable.len()
                + gate.future_versioned.len()
                + gate.unknown_source.len(),
            restorable: gate.restorable,
            future_versioned: gate.future_versioned,
            unknown_source: gate.unknown_source,
            total_rows,
        })
    }

    /// Phase 2 of the two-phase fetch: payloads for exactly these rows, by
    /// bound (name, registered_at_ms) pairs — names and timestamps are
    /// operator input, never interpolated into SQL text. The pair list is
    /// capped by phase 1's `max_rows`, so the statement stays small however
    /// fat the table is. The timestamp in every key is what closes a
    /// re-register between the two phases: a name whose row now carries a
    /// newer timestamp no longer matches, so the vanishing logic below
    /// (drop, WARN, exact count) instead restores nothing, and a later boot
    /// replays the rewrite — never the rewrite's payload under the old
    /// metadata's timestamp.
    async fn payloads_for(
        &self,
        rows: &[Registration],
    ) -> Result<HashMap<(String, u64), String>, String> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }
        let pairs_sql = rows.iter().map(|_| "(?, ?)").collect::<Vec<_>>().join(", ");
        let mut query = self.client.query(&format!(
            "SELECT name, registered_at_ms, payload FROM {REGISTRY_TABLE} FINAL \
             WHERE (name, registered_at_ms) IN ({pairs_sql})"
        ));
        for row in rows {
            query = query.bind(row.name.clone()).bind(row.registered_at_ms);
        }
        let pairs: Vec<NamePayload> = query
            .fetch_all::<NamePayload>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;
        Ok(pairs
            .into_iter()
            .map(|pair| ((pair.name, pair.registered_at_ms), pair.payload))
            .collect())
    }

    /// `DELETE FROM` (lightweight delete) first; the mutation-based
    /// `ALTER TABLE ... DELETE` is the fallback for servers that disable
    /// lightweight deletes. Both are synchronous before returning. Success is
    /// reported even when the name has no row (an idempotent end state), and
    /// note the write-on-read surprise: on a database where nothing was ever
    /// registered, this creates the (empty) registry table. `role` resolves
    /// the write client (see [`ClickhouseSchemaStore::client_for_write`]).
    pub async fn delete(&self, name: &str, role: Option<&str>) -> Result<bool, String> {
        self.bound_call("registry delete", self.delete_inner(name, role))
            .await
    }

    async fn delete_inner(&self, name: &str, role: Option<&str>) -> Result<bool, String> {
        let client = self.client_for_write(role).await;
        self.ensure_table(&client).await?;
        let lightweight = client
            .query(&format!("DELETE FROM {REGISTRY_TABLE} WHERE name = ?"))
            .bind(name.to_string())
            .with_option("wait_end_of_query", "1")
            .execute()
            .await;
        if lightweight.is_ok() {
            return Ok(true);
        }
        client
            .query(&format!(
                "ALTER TABLE {REGISTRY_TABLE} DELETE WHERE name = ?"
            ))
            .bind(name.to_string())
            .with_option("mutations_sync", "1")
            .execute()
            .await
            .map_err(|e| format!("registry delete failed: {e}"))?;
        Ok(true)
    }
}

// -------------------------------------------------------------- write paths

/// What `POST /schemas/load` reports about durability. `persisted` is the
/// round-1 field, unchanged; `unrestorable_reason` is additive (upgrades
/// only ever add fields — old clients ignore it, the off-mode body stays
/// byte-identical): when set, the caller emits `restorable: false` next to
/// `persisted: true`, for a row that stored fine but can never boot-restore
/// under the current configuration.
pub struct PersistOutcome {
    pub persisted: bool,
    /// `None` = ordinary restorable row (no extra response fields).
    pub unrestorable_reason: Option<String>,
}

/// The deterministic part of "can this row ever restore": a single payload
/// whose own bytes already exceed the byte budget spends more than the
/// whole budget by itself (the cumulative window sums over its own row),
/// so no boot's fetch can admit it. Config-shadowing of a configured name
/// is deliberately NOT predicted here — the config file can change between
/// now and the next boot, and boot replay's `config-shadowed` bucket (spec
/// §Replay) is the authoritative, documented resolution for it. The
/// response states what this server guarantees, not a guess about future
/// operator edits.
fn unrestorable_reason_if_any(yaml_len: usize, max_bytes: u64) -> Option<String> {
    (yaml_len as u64 > max_bytes).then(|| {
        format!(
            "payload of {yaml_len} bytes exceeds CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES \
             ({max_bytes}); the stored row can never be fetched at boot under \
             the current configuration"
        )
    })
}

/// Persist after a successful `POST /schemas/load`.
///
/// Takes the server's one shared store (`AppState.schema_store`) — hoisting
/// the instance is what makes the table-preparation ensure per process
/// instead of per request. The config rides along to keep the observable
/// contract exact: with `clickhouse` mode on a server that carries no
/// ClickHouse client (the embedded and other no-client modes — see
/// `run_server`'s `AppState` construction), the answer is
/// `Some(persisted: false)` with a debug trail — the attempt was real, the
/// mode just has nothing to persist into — while a genuinely disabled mode
/// answers `None` so the response body stays byte-identical to a server
/// without the feature.
///
/// `None` = persistence is off (the caller must leave the response body
/// byte-identical to a server without the feature). `Some` = the attempt
/// happened; `persisted` says whether the store took the row. Store
/// failures degrade to `persisted: false` plus a WARN: the schema itself
/// is live either way, and a flaky store must not make the registration
/// API flaky. `role` is the optional ClickHouse RBAC role for the store
/// write (`role` on the request body); boot replay never uses it.
pub async fn persist_yaml_after_load(
    config: &ServerConfig,
    store: &SchemaStore,
    schema_name: &str,
    yaml_content: &str,
    role: Option<&str>,
) -> Option<PersistOutcome> {
    match (config.schema_persistence, store) {
        (SchemaPersistenceMode::Off, _) => None,
        // `clickhouse` mode but no client in this server mode: the shared
        // store is `Off` by construction (`from_parts`). Per-request paths
        // must not re-WARN (boot's `replay` logged the condition once); a
        // persisted:false response is the contract surface, debug keeps the
        // trail for support.
        (SchemaPersistenceMode::Clickhouse, SchemaStore::Off) => {
            log::debug!(
                "schema persistence: no ClickHouse client in this server \
                 mode — '{}' stays memory-only",
                log_safe(schema_name)
            );
            Some(PersistOutcome {
                persisted: false,
                unrestorable_reason: None,
            })
        }
        (SchemaPersistenceMode::Clickhouse, SchemaStore::Clickhouse(clickhouse_store)) => {
            let outcome = clickhouse_store
                .upsert(
                    schema_name,
                    SOURCE_API_LOAD,
                    FORMAT_YAML,
                    yaml_content.to_string(),
                    role,
                )
                .await;
            if let Err(e) = &outcome {
                log::warn!(
                    "schema '{}' could not be persisted for restart ({e}); \
                     the registration is live, it just will not survive one",
                    log_safe(schema_name)
                );
            }
            // The never-restorable note rides only on an actually stored
            // row (a persisted:false answer already says what happened).
            Some(PersistOutcome {
                persisted: outcome.is_ok(),
                unrestorable_reason: outcome
                    .is_ok()
                    .then(|| {
                        unrestorable_reason_if_any(
                            yaml_content.len(),
                            config.schema_replay_max_bytes,
                        )
                    })
                    .flatten(),
            })
        }
    }
}

// --------------------------------------------------------------- boot replay

/// Restore runtime registrations after `initialize_global_schema`.
///
/// Call exactly once, before the listener binds (single site: `run_server`).
/// No-op when the flag is unset, or when the server mode has no ClickHouse
/// client — the guaranteed behavior there is today's behavior.
///
/// Bounded: restores at most `CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS` rows whose
/// cumulative payload stays under `CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES`, in
/// `registered_at_ms` order (oldest first). Every row left un-restored by a
/// budget is named in a WARN with the exact count — bounded work at boot is
/// the point, silent truncation would be a data-loss bug pretending to be a
/// cap.
pub async fn replay(app_state: &super::AppState, config: &ServerConfig) {
    // The boot notice lives here (not on the per-request write paths — they
    // share the hoisted store and never warn about this) so a no-client mode
    // logs it exactly once: `replay` itself runs once per boot.
    if config.schema_persistence == SchemaPersistenceMode::Clickhouse
        && app_state.clickhouse_client.is_none()
    {
        log::warn!(
            "CLICKGRAPH_SCHEMA_PERSISTENCE=clickhouse but this server mode \
             has no ClickHouse client — schema persistence stays off: nothing \
             is stored or replayed (no-client server modes carry none)"
        );
    }

    let store = &app_state.schema_store;
    // Off stays fully silent (byte-for-byte the pre-feature boot).
    if matches!(**store, SchemaStore::Off) {
        return;
    }
    let mut fetch = match store
        .load_replay_rows(
            config.schema_replay_max_rows,
            config.schema_replay_max_bytes,
        )
        .await
    {
        Ok(fetch) => fetch,
        Err(e) => {
            log::warn!(
                "schema persistence: could not read {REGISTRY_TABLE} ({e}) — \
                 booting without restoring runtime registrations"
            );
            // The spec's degraded-boot contract: one knockout line, all
            // zeros, so log scrapers see the same shape they see on a
            // successful pass.
            log::info!(
                "schema persistence: 0 restored / 0 config-shadowed / 0 failed \
                 (store unreachable at boot)"
            );
            return;
        }
    };

    // Rows the budgets never fetched: the count and the bounded select
    // disagree under a concurrent write between the two reads (and the byte
    // cap cuts rows the row cap alone would have admitted) — saturate so a
    // transient disagreement can never wrap into a bogus seven-figure WARN.
    let over_budget = fetch.total_rows.saturating_sub(fetch.fetched as u64);
    if over_budget > 0 {
        log::warn!(
            "registry holds {total} rows but the replay budget allows only \
             {fetched} (CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS={max}, \
             CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES={bytes}) — the {over} \
             newest rows were not fetched and will not be restored; raise \
             the budget or clean {REGISTRY_TABLE}",
            total = fetch.total_rows,
            fetched = fetch.fetched,
            max = config.schema_replay_max_rows,
            bytes = config.schema_replay_max_bytes,
            over = over_budget,
        );
    }

    // Names the config file already satisfied this boot; the replay restores
    // through the *same* load path a live registration uses, so alias and
    // cache semantics are identical by construction.
    let configured_names = graph_catalog::list_available_schemas().await;
    let (to_restore, shadowed) =
        partition_for_replay(std::mem::take(&mut fetch.restorable), &configured_names);
    for row in &shadowed {
        log::info!(
            "config-defined schema '{}' shadows a persisted registration; \
             the config file wins (edit the file to change the schema)",
            log_safe(&row.name)
        );
    }

    let mut restored = 0usize;
    let mut byte_skipped = 0usize;
    let mut used_bytes = 0u64;
    let mut failed_names: Vec<String> = Vec::new();
    for row in &to_restore {
        // Byte budget, re-checked here in replay order — normally a no-op,
        // because the fetch itself stops at the same cumulative budget
        // server-side (a window sum over this exact order); kept as a
        // belt-and-suspenders net against a store that does not honor it.
        // Stop before a row would exceed it and skip the deterministic
        // remainder (a newer registration that fits drops nothing from the
        // budget already used), counting what is left so the WARN tells an
        // exact number.
        let row_bytes = row.payload.len() as u64;
        if used_bytes + row_bytes > config.schema_replay_max_bytes {
            byte_skipped = to_restore.len() - restored - failed_names.len();
            log::warn!(
                "replay payload budget exhausted before '{}': \
                 {} of {} bytes used, {} remaining rows not restored \
                 (CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES={})",
                log_safe(&row.name),
                used_bytes,
                config.schema_replay_max_bytes,
                byte_skipped,
                config.schema_replay_max_bytes
            );
            break;
        }
        used_bytes += row_bytes;

        let outcome = match row.format.as_str() {
            FORMAT_YAML => {
                graph_catalog::load_schema_from_content(
                    &row.name,
                    &row.payload,
                    app_state.clickhouse_client.clone(),
                    false,
                )
                .await
            }
            other => Err(format!(
                "unknown registry format '{other}'; \
                    skipping it — rows written by another source/format \
                    are never guessed at"
            )),
        };
        match outcome {
            Ok(()) => restored += 1,
            Err(e) => {
                failed_names.push(row.name.clone());
                // `e` can embed the stored name ("Failed to load schema
                // '<name>': …"), so it is scrubbed like the name itself.
                let reason = log_safe(&e);
                log::warn!(
                    "persisted schema '{}' could not be restored ({reason}) — \
                     register it again, or drop it with DELETE /schemas/{}",
                    log_safe(&row.name),
                    log_safe(&row.name)
                );
            }
        }
    }

    // The summary accounts for every fetched row — restored, shadowed,
    // failed, over either budget, newer-versioned, unknown-sourced — so
    // nothing crosses the fetch and vanishes between the buckets:
    // total_rows = restored + config-shadowed + failed + skipped over
    // replay budget + newer-version skipped + unknown-source skipped.
    log::info!(
        "schema persistence: {} restored / {} config-shadowed / {} failed \
         / {} skipped over replay budget / {} newer-version skipped \
         / {} unknown-source skipped (failed: {:?})",
        restored,
        shadowed.len(),
        failed_names.len(),
        over_budget + byte_skipped as u64,
        fetch.future_versioned.len(),
        fetch.unknown_source.len(),
        failed_names
    );
}

/// A schema name is operator input routed into log lines: replace control
/// characters so a `\n` in a name cannot forge entries after this one. Names
/// in `Vec/Option` debug output (`{:?}`) are already escaped by the formatter
/// and need no scrubbing.
pub(crate) fn log_safe(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

// --------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, at_ms: u64) -> Registration {
        Registration {
            name: name.to_string(),
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "nodes: {}".to_string(),
            registry_version: REGISTRY_VERSION,
            registered_at_ms: at_ms,
        }
    }

    #[test]
    fn future_registry_version_is_skipped_not_booted() {
        let gate = gate_for_replay(vec![
            row("ok", 1),
            Registration {
                registry_version: REGISTRY_VERSION + 1,
                ..row("from-the-future", 2)
            },
        ]);
        assert_eq!(gate.restorable.len(), 1);
        assert_eq!(gate.restorable[0].name, "ok");
        assert_eq!(gate.future_versioned.len(), 1);
        assert_eq!(gate.future_versioned[0].name, "from-the-future");
    }

    #[test]
    fn unknown_source_rows_are_never_restored() {
        // The write path only ever stamps SOURCE_API_LOAD; anything else in
        // the column is an operator hand-edit or another tool's write, and
        // its payload is never guessed at.
        let gate = gate_for_replay(vec![Registration {
            source: "not-api-load".to_string(),
            ..row("hand-written", 1)
        }]);
        assert!(
            gate.restorable.is_empty(),
            "an unclaimed source must never restore"
        );
        assert_eq!(gate.unknown_source.len(), 1);
        assert_eq!(gate.unknown_source[0].name, "hand-written");
        assert_eq!(gate.unknown_source[0].source, "not-api-load");
        assert!(gate.future_versioned.is_empty());
    }

    #[test]
    fn unknown_version_wins_over_unknown_source() {
        // A row from an unknown future server can be EITHER foreign-sourced
        // or future-versioned; the version refusal is the strictly safer
        // bucket, so the version check runs first.
        let gate = gate_for_replay(vec![Registration {
            source: "not-api-load".to_string(),
            registry_version: REGISTRY_VERSION + 1,
            ..row("doubly-unknown", 1)
        }]);
        assert_eq!(gate.future_versioned.len(), 1);
        assert!(
            gate.unknown_source.is_empty() && gate.restorable.is_empty(),
            "one row, exactly one refusal bucket"
        );
    }

    #[test]
    fn empty_source_is_still_unknown() {
        // Cheap-defect probe: an empty string is a source this server does
        // not claim and must not restore.
        let gate = gate_for_replay(vec![Registration {
            source: String::new(),
            ..row("blank-stamp", 1)
        }]);
        assert_eq!(gate.unknown_source.len(), 1);
        assert!(gate.restorable.is_empty());
    }

    #[test]
    fn config_defined_names_shadow_persisted_ones() {
        let rows = vec![row("from-file", 1), row("from-api", 2), row("default", 3)];
        let (restore, shadowed) = partition_for_replay(rows, &["from-file".to_string()]);
        assert_eq!(restore.len(), 2);
        assert_eq!(shadowed.len(), 1);
        assert_eq!(shadowed[0].name, "from-file");
    }

    #[test]
    fn shadowing_is_by_exact_name() {
        let rows = vec![row("MySchema", 1)];
        let (restore, shadowed) = partition_for_replay(rows, &["myschema".to_string()]);
        assert_eq!(restore.len(), 1);
        assert_eq!(shadowed.len(), 0, "case must match exactly");
    }

    #[test]
    fn serialization_round_trips_through_row_fields() {
        let row = Registration {
            name: "a'b\"c`d".to_string(), // adversarial: a name, never SQL
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "{}".to_string(),
            registry_version: 1,
            registered_at_ms: 42,
        };
        let json = serde_json::to_string(&row).unwrap();
        let back: Registration = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "a'b\"c`d");
        assert_eq!(back.registered_at_ms, 42);
    }

    #[test]
    fn log_safe_strips_newlines_from_operated_names() {
        // Log-forging: the "name" is a second log line smuggled in via \n.
        assert_eq!(
            log_safe("legit\n2024-01-01 ERROR injected"),
            "legit?2024-01-01 ERROR injected"
        );
        assert_eq!(log_safe("carriage-return\r"), "carriage-return?");
        assert_eq!(log_safe("normal-name.0"), "normal-name.0");
    }

    #[test]
    fn replay_fetch_reports_bounds_for_off_store() {
        let fetch = ReplayFetch::default();
        assert_eq!(fetch.total_rows, 0);
        assert_eq!(fetch.fetched, 0);
        assert!(fetch.restorable.is_empty());
        assert!(fetch.future_versioned.is_empty());
        assert!(fetch.unknown_source.is_empty());
    }

    // ------------------------------------------------ registration clock

    #[test]
    fn timestamps_rise_even_in_the_same_millisecond() {
        // Many rapid calls land in one millisecond: wall time alone would
        // tie. The server-side floor makes every stamp strictly higher than
        // the last (the CAS loop carries it).
        let store = ClickhouseSchemaStore::new(clickhouse::Client::default());
        let mut last = 0u64;
        for _ in 0..1_000 {
            let stamp = store.next_registered_at_ms();
            assert!(stamp > last, "timestamps must strictly increase");
            last = stamp;
        }
    }

    #[test]
    fn a_seeded_floor_never_steps_back_below_the_table() {
        // Simulates a boot whose host clock stepped back since the last
        // process: the floor carries the table's high-water mark (seeded at
        // ensure time), so the next stamp is above anything earlier boots
        // wrote — an older row cannot win ReplacingMergeTree's collapse.
        let store = ClickhouseSchemaStore::new(clickhouse::Client::default());
        let high_water = 9_000_000_000_000u64; // far past wall-clock ms today
        store
            .registered_at_floor
            .fetch_max(high_water, Ordering::Relaxed);
        assert!(
            store.next_registered_at_ms() > high_water,
            "the floor must push every stamp past {high_water}"
        );
    }

    #[test]
    fn contended_timestamps_never_repeat() {
        // Eight threads stamping concurrently: the CAS loop must hand out a
        // single strictly-increasing chain across all of them (two writers
        // in the same millisecond is exactly the old tie case).
        use std::sync::Arc;
        let store = Arc::new(ClickhouseSchemaStore::new(clickhouse::Client::default()));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || {
                    (0..250)
                        .map(|_| store.next_registered_at_ms())
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("stamp thread"))
            .collect();
        all.sort_unstable();
        assert!(
            all.windows(2).all(|pair| pair[0] < pair[1]),
            "2000 contended stamps must form one strictly-increasing chain"
        );
    }

    // ------------------------------------------------------- response truth

    #[test]
    fn reserved_names_are_rejected_up_front() {
        for name in RESERVED_SCHEMA_NAMES {
            assert!(is_reserved_schema_name(name));
        }
        assert!(!is_reserved_schema_name("loads"), "exact match only");
        assert!(!is_reserved_schema_name("Load "), "exact match only");
        assert!(!is_reserved_schema_name("my-load"), "exact match only");
    }

    #[test]
    fn unrestorable_reason_only_appears_past_the_byte_budget() {
        assert!(unrestorable_reason_if_any(10, 100).is_none());
        assert!(unrestorable_reason_if_any(100, 100).is_none(), "equal fits");
        let reason = unrestorable_reason_if_any(101, 100).expect("over budget");
        assert!(
            reason.contains("CLICKGRAPH_SCHEMA_REPLAY_MAX_BYTES"),
            "the reason must name the knob: {reason}"
        );
    }

    // -------------------------------------------------- store-call timeout

    #[tokio::test]
    async fn a_store_call_that_never_answers_lands_in_the_unreachable_error() {
        let err = with_timeout_within(
            Duration::from_millis(20),
            "upsert",
            std::future::pending::<Result<(), String>>(),
        )
        .await
        .expect_err("a pending store call must hit the bound");
        assert!(
            err.contains("store unreachable"),
            "the mapped error feeds the shipped persisted:false path: {err}"
        );
        assert!(
            err.contains("'upsert'"),
            "the op name trails for logs: {err}"
        );
    }

    #[tokio::test]
    async fn a_store_call_within_the_bound_passes_through_unchanged() {
        let ok: Result<u32, String> =
            with_timeout_within(Duration::from_secs(10), "exists-probe", async { Ok(7) }).await;
        assert_eq!(ok, Ok(7), "success values are untouched");
        let inner: Result<u32, String> =
            with_timeout_within(Duration::from_secs(10), "replay-read", async {
                Err("inner detail".to_string())
            })
            .await;
        assert_eq!(
            inner.unwrap_err(),
            "inner detail",
            "an inner error must not be rewritten as a timeout"
        );
    }
}

// =====================================================================
// Mutation-red tests for the round-3 review — a fake ClickHouse wire
// server so every mutation the review names turns at least one test red
// at the call site it would corrupt:
//
//   - removing the handlers' registry gate   → racing_load_and_delete…
//   - bypassing `with_timeout_within` at a
//     call site                              → a_wedged_store_statement…
//   - ignoring `role` on the write path      → the_endpoint_role_rides…
//   - issuing CREATE on every operation      → the_registry_table_is_created…
//   - failing the ensure on a failed CREATE  → a_failed_create_falls…
//   - making the floor seed a no-op          → a_floor_left_unseeded…
//   - breaking the legacy-layout select      → the_legacy_layout_restores…
//   - (name-only) phase-2 fetch              → phase_two_fetches_payloads…
//   - caching past a failed seed (item 1)    → a_failed_high_water_seed…
//
// The fake speaks exactly the shapes clickhouse-0.13.3 puts on the wire
// (verified against the crate's sources): fetches are GETs with the SQL in
// the `query=` URL parameter; non-fetch statements are POSTs with the SQL
// as the body; INSERTs are POSTs with the SQL in the `query=` parameter and
// the rows in a RowBinary body; binds are substituted client-side, so
// statements arrive with their literals; RowBinary strings are LEB128
// length-prefixed, numbers little-endian fixed-width; every option,
// including an endpoint's `role`, rides as a URL parameter.
#[cfg(test)]
mod mutation_tests {
    use super::*;

    use crate::executor::{ExecutorError, QueryExecutor};
    use crate::server::handlers::{
        delete_schema_handler, load_schema_handler, DeleteSchemaParams, LoadSchemaRequest,
    };
    use crate::server::AppState;
    use axum::extract::{Path, Query, State};
    use axum::http::{StatusCode, Uri};
    use axum::Json;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    // ------------------------------------------------------------- plumbing

    /// The schema-lifecycle endpoints never invoke the executor.
    struct StubExecutor;

    #[async_trait::async_trait]
    impl QueryExecutor for StubExecutor {
        async fn execute_json(
            &self,
            _sql: &str,
            _role: Option<&str>,
        ) -> Result<Vec<serde_json::Value>, ExecutorError> {
            Ok(vec![])
        }
        async fn execute_text(
            &self,
            _sql: &str,
            _format: &str,
            _role: Option<&str>,
        ) -> Result<String, ExecutorError> {
            Ok(String::new())
        }
    }

    /// The global once-cells a load needs (idempotent, first-initializer
    /// wins; the same pattern the integration suite uses).
    async fn ensure_globals() {
        use crate::server::graph_catalog::initialize_global_schema;
        use crate::server::{GLOBAL_SCHEMAS, GLOBAL_SCHEMA_CONFIGS};
        use tokio::sync::RwLock;
        let _ = GLOBAL_SCHEMAS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let _ = GLOBAL_SCHEMA_CONFIGS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let _ = initialize_global_schema(None, false).await;
    }

    fn persisted_config() -> ServerConfig {
        ServerConfig {
            schema_persistence: SchemaPersistenceMode::Clickhouse,
            ..ServerConfig::default()
        }
    }

    /// A tiny valid schema in the config grammar's list form. The database
    /// is never contacted (`validate_schema` stays off), so the name is a
    /// plain placeholder.
    fn tiny_yaml(name: &str) -> String {
        format!(
            "name: {name}\n\
             graph_schema:\n\
             \x20 nodes:\n\
             \x20   - label: Doc\n\
             \x20     database: mutation_tests\n\
             \x20     table: it_dummy_table\n\
             \x20     node_id: id\n\
             \x20     property_mappings:\n\
             \x20       id: id\n\
             \x20 edges: []",
        )
    }

    /// Process-unique names: two parallel tests must not collide on the
    /// in-memory registries (the integration suite learned this the hard way).
    fn unique_name(tag: &str) -> String {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        format!(
            "mutation_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// The load path auto-discovers columns through a `Some(client)`, so the
    /// test state carries `None` (the offline build's shape) and only the
    /// schema_store points at the fake — exactly the half the gate and the
    /// store write under test use.
    fn app_state(
        store_client: Client,
        role_pool: Option<Arc<RoleConnectionPool>>,
        config: ServerConfig,
    ) -> Arc<AppState> {
        let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::with_role_pool(
            store_client,
            role_pool.clone(),
        )));
        Arc::new(AppState {
            executor: Arc::new(StubExecutor),
            clickhouse_client: None,
            config,
            query_semaphore: None,
            pool: role_pool,
            schema_store: Arc::new(store),
            schema_registry_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    // ------------------------------------------------- RowBinary wire codecs

    /// LEB128 (unsigned, the RowBinary String/variable padding actually on
    /// the wire) length prefix + UTF-8 bytes.
    fn write_string(s: &str, out: &mut Vec<u8>) {
        let mut len = s.len() as u64;
        loop {
            let mut byte = (len & 0x7f) as u8;
            len >>= 7;
            if len != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                break;
            }
        }
        out.extend_from_slice(s.as_bytes());
    }

    fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *bytes.get(*pos)?;
            *pos += 1;
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    fn read_string(bytes: &[u8], pos: &mut usize) -> Option<String> {
        let len = read_varint(bytes, pos)? as usize;
        let end = *pos + len;
        let text = String::from_utf8_lossy(bytes.get(*pos..end)?).into_owned();
        *pos = end;
        Some(text)
    }

    /// One body = exactly one `Registration` row's RowBinary encoding (a
    /// single `write` + `end`), strings LEB-prefixed, numbers LE.
    fn decode_insert_row(body: &[u8]) -> Option<Registration> {
        let mut pos = 0usize;
        let name = read_string(body, &mut pos)?;
        let source = read_string(body, &mut pos)?;
        let format = read_string(body, &mut pos)?;
        let payload = read_string(body, &mut pos)?;
        if pos + 2 > body.len() {
            return None;
        }
        let registry_version = u16::from_le_bytes([body[pos], body[pos + 1]]);
        pos += 2;
        if pos + 8 > body.len() {
            return None;
        }
        let mut ms = [0u8; 8];
        ms.copy_from_slice(&body[pos..pos + 8]);
        let registered_at_ms = u64::from_le_bytes(ms);
        pos += 8;
        if pos != body.len() {
            return None;
        }
        Some(Registration {
            name,
            source,
            format,
            payload,
            registry_version,
            registered_at_ms,
        })
    }

    fn encode_meta_row(row: &Registration) -> Vec<u8> {
        let mut out = Vec::new();
        write_string(&row.name, &mut out);
        write_string(&row.source, &mut out);
        write_string(&row.format, &mut out);
        out.extend_from_slice(&row.registry_version.to_le_bytes());
        out.extend_from_slice(&row.registered_at_ms.to_le_bytes());
        out.extend_from_slice(&(row.payload.len() as u64).to_le_bytes());
        out
    }

    fn encode_legacy_row(row: &Registration) -> Vec<u8> {
        let mut out = Vec::new();
        write_string(&row.name, &mut out);
        write_string(&row.source, &mut out);
        write_string(&row.format, &mut out);
        write_string(&row.payload, &mut out);
        out.extend_from_slice(&row.registry_version.to_le_bytes());
        out.extend_from_slice(&row.registered_at_ms.to_le_bytes());
        out
    }

    fn encode_u64(value: u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    // ------------------------------------------------------- the fake server

    struct Recorded {
        sql: String,
        /// The `role=` URL parameter (an endpoint's role rides there).
        role: Option<String>,
        /// Decoded INSERT row (`None` on every other statement).
        inserted: Option<Registration>,
    }

    struct FakeShared {
        /// The "table" — ReplacingMergeTree+FINAL semantics applied at write:
        /// one row per name, the higher `registered_at_ms` wins. A held
        /// INSERT is NOT here yet (a not-yet-answered write is invisible to
        /// reads, as it is in ClickHouse).
        rows: Mutex<Vec<Registration>>,
        /// Every statement, in wire order.
        requests: Mutex<Vec<Recorded>>,
        create_fails: AtomicBool,
        seed_fails: AtomicBool,
        delete_fails: AtomicBool,
        legacy_layout: AtomicBool,
        hang_create: AtomicBool,
        hold_inserts: AtomicBool,
        /// The `SELECT max(registered_at_ms)` answer (the floor's high-water).
        high_water: AtomicU64,
        /// Steers a held INSERT (and a hung CREATE) back to life — the tests
        /// release once their mid-flight assertions ran.
        release: Notify,
        released: AtomicBool,
    }

    impl FakeShared {
        // The release steering lives in `park_until_released` below.
    }

    /// A fake ClickHouse HTTP endpoint on an ephemeral loopback port, one
    /// per test. Route keys are SQL substrings (mutually exclusive by
    /// construction, asserted implicitly by the tests' greenness).
    struct FakeClickHouse {
        url: String,
        shared: Arc<FakeShared>,
    }

    impl FakeClickHouse {
        async fn spawn() -> Self {
            let shared = Arc::new(FakeShared {
                rows: Mutex::new(Vec::new()),
                requests: Mutex::new(Vec::new()),
                create_fails: AtomicBool::new(false),
                seed_fails: AtomicBool::new(false),
                delete_fails: AtomicBool::new(false),
                legacy_layout: AtomicBool::new(false),
                hang_create: AtomicBool::new(false),
                hold_inserts: AtomicBool::new(false),
                high_water: AtomicU64::new(1_700_000_000_000),
                release: Notify::new(),
                released: AtomicBool::new(false),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("fake bind");
            let addr = listener.local_addr().expect("fake addr");
            let app = axum::Router::new()
                .route("/", axum::routing::get(fake_handle).post(fake_handle))
                .with_state(shared.clone());
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self {
                url: format!("http://{addr}"),
                shared,
            }
        }

        fn client(&self) -> Client {
            Client::default().with_url(&self.url)
        }

        fn set_create_fails(&self, on: bool) {
            self.shared.create_fails.store(on, Ordering::Relaxed);
        }
        fn set_seed_fails(&self, on: bool) {
            self.shared.seed_fails.store(on, Ordering::Relaxed);
        }
        fn set_delete_fails(&self, on: bool) {
            self.shared.delete_fails.store(on, Ordering::Relaxed);
        }
        fn set_legacy_layout(&self, on: bool) {
            self.shared.legacy_layout.store(on, Ordering::Relaxed);
        }
        fn set_hang_create(&self, on: bool) {
            self.shared.hang_create.store(on, Ordering::Relaxed);
        }
        fn set_hold_inserts(&self, on: bool) {
            self.shared.hold_inserts.store(on, Ordering::Relaxed);
        }
        fn set_high_water(&self, value: u64) {
            self.shared.high_water.store(value, Ordering::Relaxed);
        }

        /// Let a held INSERT (or a hung CREATE) finish: the world resumes.
        fn release(&self) {
            self.shared.released.store(true, Ordering::SeqCst);
            self.shared.release.notify_waiters();
        }

        fn rows_snapshot(&self) -> Vec<Registration> {
            self.shared.rows.lock().unwrap().clone()
        }

        fn requests_snapshot(&self) -> Vec<Recorded> {
            // Recorded is not Clone; snapshot to owned values.
            self.shared
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|recorded| Recorded {
                    sql: recorded.sql.clone(),
                    role: recorded.role.clone(),
                    inserted: recorded.inserted.clone(),
                })
                .collect()
        }

        fn create_count(&self) -> usize {
            self.requests_snapshot()
                .iter()
                .filter(|r| r.sql.contains("CREATE TABLE IF NOT EXISTS"))
                .count()
        }

        fn insert_count(&self) -> usize {
            self.requests_snapshot()
                .iter()
                .filter(|r| r.inserted.is_some())
                .count()
        }

        fn inserted_rows(&self) -> Vec<Registration> {
            self.requests_snapshot()
                .iter()
                .filter_map(|r| r.inserted.clone())
                .collect()
        }

        /// DELETE-shaped store traffic: the DELETE statements themselves and
        /// the exists probe a store-only delete runs before them.
        fn delete_traffic_count(&self) -> usize {
            self.requests_snapshot()
                .iter()
                .filter(|r| {
                    r.sql.contains("DELETE FROM")
                        || r.sql.contains("ALTER TABLE")
                        || (r.sql.contains("SELECT count() FROM") && r.sql.contains("WHERE name ="))
                })
                .count()
        }

        async fn wait_for(
            &self,
            what: &str,
            deadline: Duration,
            pred: impl Fn(&[Recorded]) -> bool,
        ) {
            let start = std::time::Instant::now();
            while !pred(&self.requests_snapshot()) {
                assert!(
                    start.elapsed() < deadline,
                    "fake server never observed {what} within {deadline:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        /// Give a row directly to the "table" (bypassing the INSERT route —
        /// the state a hand-written row or a prior boot left behind).
        fn push_row(&self, row: Registration) {
            let mut rows = self.shared.rows.lock().unwrap();
            match rows.iter().position(|r| r.name == row.name) {
                Some(idx) if rows[idx].registered_at_ms <= row.registered_at_ms => {
                    rows[idx] = row;
                }
                Some(_) => {}
                None => rows.push(row),
            }
        }
    }

    /// Park until the test releases the fake, re-checking the flag around
    /// the await so the release can never be lost in the tiny race window.
    async fn park_until_released(shared: &FakeShared) {
        while !shared.released.load(Ordering::SeqCst) {
            let notified = shared.release.notified();
            if shared.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn ok(body: Vec<u8>) -> (StatusCode, Vec<u8>) {
        (StatusCode::OK, body)
    }

    fn fail(message: &str) -> (StatusCode, Vec<u8>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            message.as_bytes().to_vec(),
        )
    }

    fn query_param(uri: &Uri, name: &str) -> Option<String> {
        for pair in uri.query()?.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if key == name {
                return Some(percent_decode(value));
            }
        }
        None
    }

    fn percent_decode(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                out.push(u8::from_str_radix(&text[i + 1..i + 3], 16).unwrap_or(b'%'));
                i += 3;
            } else if bytes[i] == b'+' {
                // form-urlencoded spaces ride as '+'
                out.push(b' ');
                i += 1;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    fn digits_after(sql: &str, needle: &str) -> Option<u64> {
        let start = sql.find(needle)? + needle.len();
        let rest = &sql[start..];
        let len = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..len].parse().ok()
    }

    fn single_quoted_after(sql: &str, needle: &str) -> Option<String> {
        let start = sql.find(needle)? + needle.len();
        let rest = &sql[start..];
        let open = rest.find('\'')?;
        let close = rest[open + 1..].find('\'')?;
        Some(rest[open + 1..open + 1 + close].to_string())
    }

    /// The (name, registered_at_ms) bind pairs of a phase-2 fetch, parsed off
    /// the client-substituted literals: `(('n1', 111), ('n2', 222))`. The
    /// crate suffixes fetched SQL with `FORMAT RowBinaryWithNamesAndTypes`
    /// (verified in the crate's `sql::SqlBuilder::finish`), so everything
    /// from that marker on is stripped first.
    fn phase2_pairs(sql: &str) -> Vec<(String, u64)> {
        let sql = match sql.find(" FORMAT ") {
            Some(cut) => &sql[..cut],
            None => sql,
        };
        let Some(start) = sql.find("IN (") else {
            return Vec::new();
        };
        let inner = sql[start + 4..].trim_start();
        let inner = inner.strip_suffix(')').unwrap_or(inner);
        inner
            .split("), (")
            .map(|piece| {
                let piece = piece.trim().trim_start_matches('(');
                let piece = piece.trim_end_matches(')').trim();
                let (name_part, ms_part) = piece.split_once(',').unwrap_or(("", ""));
                (
                    name_part.trim().trim_matches('\'').to_string(),
                    ms_part.trim().parse().unwrap_or(0),
                )
            })
            .collect()
    }

    /// The one router: answers every statement shape the registry store
    /// emits, honoring the fake's fault flags, and records everything.
    async fn fake_handle(
        State(shared): State<Arc<FakeShared>>,
        uri: Uri,
        body: axum::body::Bytes,
    ) -> (StatusCode, Vec<u8>) {
        let param_sql = query_param(&uri, "query");
        let sql = match param_sql {
            Some(sql) => sql,
            None => String::from_utf8_lossy(&body).into_owned(),
        };
        let role = query_param(&uri, "role");

        // ---------------------------------------------------- INSERT (held?)
        if sql.contains("INSERT INTO") && sql.contains("FORMAT RowBinary") {
            let row = match decode_insert_row(&body) {
                Some(row) => row,
                None => {
                    return fail("fake: undecodable INSERT body (wire shape drift)");
                }
            };
            shared.requests.lock().unwrap().push(Recorded {
                sql: sql.clone(),
                role,
                inserted: Some(row.clone()),
            });
            if shared.hold_inserts.load(Ordering::SeqCst) {
                park_until_released(&shared).await;
            }
            // Apply only after any hold: a not-yet-answered write is
            // invisible to reads, as it is in ClickHouse.
            let mut rows = shared.rows.lock().unwrap();
            match rows.iter().position(|r| r.name == row.name) {
                Some(idx) if rows[idx].registered_at_ms <= row.registered_at_ms => {
                    rows[idx] = row;
                }
                Some(_) => {}
                None => rows.push(row),
            }
            drop(rows);
            return ok(Vec::new());
        }

        shared.requests.lock().unwrap().push(Recorded {
            sql: sql.clone(),
            role,
            inserted: None,
        });

        // ------------------------------------------------ table preparation
        if sql.contains("CREATE TABLE IF NOT EXISTS") {
            if shared.hang_create.load(Ordering::SeqCst) {
                park_until_released(&shared).await;
            }
            if shared.create_fails.load(Ordering::SeqCst) {
                return fail("fake: CREATE rejected - unsigned privilege (CH-CREATE-FAULT)");
            }
            return ok(Vec::new());
        }

        // ---------------------------------------------------------- deletes
        if sql.contains("DELETE FROM") {
            if shared.delete_fails.load(Ordering::SeqCst) {
                return fail("fake: DELETE rejected - CH-FAULT d34dbeef mutations are disabled");
            }
            let name = match single_quoted_after(&sql, "WHERE name =") {
                Some(name) => name,
                None => return fail("fake: unparseable DELETE name"),
            };
            shared.rows.lock().unwrap().retain(|r| r.name != name);
            return ok(Vec::new());
        }
        if sql.contains("ALTER TABLE") {
            if shared.delete_fails.load(Ordering::SeqCst) {
                return fail(
                    "fake: ALTER DELETE rejected - CH-FAULT d34dbeef mutations are disabled",
                );
            }
            let name = match single_quoted_after(&sql, "WHERE name =") {
                Some(name) => name,
                None => return fail("fake: unparseable ALTER DELETE name"),
            };
            shared.rows.lock().unwrap().retain(|r| r.name != name);
            return ok(Vec::new());
        }

        // ------------------------------------------------- layout detection
        if sql.contains("SELECT payload_len FROM") {
            if shared.legacy_layout.load(Ordering::SeqCst) {
                return fail("fake: no column payload_len (column probe fault)");
            }
            return ok(Vec::new());
        }
        if sql.contains("SELECT name FROM") {
            return ok(Vec::new());
        }

        // ------------------------------------------------------- replay read
        if sql.contains("sum(payload_len) OVER") {
            let max_bytes = digits_after(&sql, "cum_bytes <= ").unwrap_or(0);
            let limit = digits_after(&sql, "LIMIT ").unwrap_or(0) as usize;
            let mut rows = shared.rows.lock().unwrap().clone();
            rows.sort_by(|a, b| (a.registered_at_ms, &a.name).cmp(&(b.registered_at_ms, &b.name)));
            let mut out = Vec::new();
            let mut cum = 0u64;
            for row in rows {
                cum += row.payload.len() as u64;
                if cum > max_bytes {
                    break;
                }
                if out.len() == limit {
                    break;
                }
                out.extend_from_slice(&encode_meta_row(&row));
            }
            return ok(out);
        }
        if sql.contains("sum(length(payload)) OVER") {
            let max_bytes = digits_after(&sql, "cum_bytes <= ").unwrap_or(0);
            let limit = digits_after(&sql, "LIMIT ").unwrap_or(0) as usize;
            let mut rows = shared.rows.lock().unwrap().clone();
            rows.sort_by(|a, b| (a.registered_at_ms, &a.name).cmp(&(b.registered_at_ms, &b.name)));
            let mut out = Vec::new();
            let mut cum = 0u64;
            for row in rows {
                cum += row.payload.len() as u64;
                if cum > max_bytes {
                    break;
                }
                if out.len() == limit {
                    break;
                }
                out.extend_from_slice(&encode_legacy_row(&row));
            }
            return ok(out);
        }
        if sql.contains("SELECT name, registered_at_ms, payload") {
            let pairs = phase2_pairs(&sql);
            let rows = shared.rows.lock().unwrap().clone();
            let mut out = Vec::new();
            for (name, at_ms) in pairs {
                for row in &rows {
                    if row.name == name && row.registered_at_ms == at_ms {
                        let mut encoded = Vec::new();
                        write_string(&row.name, &mut encoded);
                        encoded.extend_from_slice(&row.registered_at_ms.to_le_bytes());
                        write_string(&row.payload, &mut encoded);
                        out.extend_from_slice(&encoded);
                    }
                }
            }
            return ok(out);
        }

        // ------------------------------------------------------ floor seed
        if sql.contains("SELECT max(registered_at_ms)") {
            if shared.seed_fails.load(Ordering::SeqCst) {
                return fail("fake: high-water read rejected - CH-FAULT b7adwater");
            }
            return ok(encode_u64(shared.high_water.load(Ordering::Relaxed)));
        }

        // ------------------------------------------------------------ counts
        if sql.contains("SELECT count() FROM") && sql.contains("WHERE name =") {
            let name = match single_quoted_after(&sql, "WHERE name =") {
                Some(name) => name,
                None => return fail("fake: unparseable count name"),
            };
            let count = shared
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.name == name)
                .count() as u64;
            return ok(encode_u64(count));
        }
        if sql.contains("SELECT count() FROM") {
            let count = shared.rows.lock().unwrap().len() as u64;
            return ok(encode_u64(count));
        }

        fail("fake: unmatched statement — the store emitted SQL the fake (and the tests) do not know")
    }

    // ------------------------------------------------------------ ::: tests

    async fn body_json(
        result: Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)>,
    ) -> Result<serde_json::Value, (StatusCode, serde_json::Value)> {
        match result {
            Ok(Json(value)) => Ok(value),
            Err((status, Json(value))) => Err((status, value)),
        }
    }

    #[tokio::test]
    async fn racing_load_and_delete_of_one_name_cannot_leave_memory_and_store_disagreeing() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        fake.set_hold_inserts(true);
        let app = app_state(fake.client(), None, persisted_config());

        let name = unique_name("gate");
        let app_post = app.clone();
        let post_payload = LoadSchemaRequest {
            schema_name: name.clone(),
            config_content: tiny_yaml(&name),
            validate_schema: None,
            role: None,
        };
        let post = tokio::spawn(async move {
            body_json(load_schema_handler(State(app_post), Json(post_payload)).await).await
        });
        // The load is mid-write: the promise is that no concurrent DELETE can
        // interleave a store statement past this point.
        fake.wait_for("the held INSERT", Duration::from_secs(10), |reqs| {
            reqs.iter().any(|r| r.inserted.is_some())
        })
        .await;

        let app_delete = app.clone();
        let name_delete = name.clone();
        let delete = tokio::spawn(async move {
            body_json(
                delete_schema_handler(
                    State(app_delete),
                    Path(name_delete),
                    Query(DeleteSchemaParams { role: None }),
                )
                .await,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            fake.delete_traffic_count(),
            0,
            "no DELETE-shaped store traffic may arrive while the load's \
             INSERT is still held — remove either handler's \
             `schema_registry_gate.lock()` and the DELETE races the held \
             write (this assert is that mutation's half)"
        );

        fake.release();
        let post_body = post.await.expect("post task").expect("post 200");
        let delete_body = delete.await.expect("delete task").expect("delete 200");
        assert_eq!(
            post_body["persisted"],
            serde_json::json!(true),
            "{post_body}"
        );
        assert_eq!(
            delete_body["persisted"],
            serde_json::json!(true),
            "{delete_body}"
        );

        // The money assert, memory against the fake's FINAL-collapsed table:
        // after the DELETE both sides agree the name is gone. The race
        // mutation ends `memory = gone, store = row` here (a restart would
        // resurrect a schema the operator deleted).
        let store_names: Vec<String> = fake.rows_snapshot().into_iter().map(|r| r.name).collect();
        assert!(
            store_names.is_empty(),
            "the store must hold nothing for the deleted name: {store_names:?}"
        );
        assert!(
            graph_catalog::get_graph_schema_by_name(&name)
                .await
                .is_err(),
            "memory must hold nothing for the deleted name"
        );
    }

    #[tokio::test]
    async fn the_endpoint_role_rides_on_the_registry_write() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        let pool = Arc::new(
            RoleConnectionPool::new_with_params(
                &fake.url,
                "fake-user",
                "fake-pass",
                Some("default"),
                None,
                100,
            )
            .await
            .expect("role pool"),
        );
        let app = app_state(fake.client(), Some(pool), persisted_config());

        // Role-less write: the write client must not claim a role.
        let plain_name = unique_name("role-plain");
        let plain_body = body_json(
            load_schema_handler(
                State(app.clone()),
                Json(LoadSchemaRequest {
                    schema_name: plain_name.clone(),
                    config_content: tiny_yaml(&plain_name),
                    validate_schema: None,
                    role: None,
                }),
            )
            .await,
        )
        .await;
        fake.wait_for("the role-less INSERT", Duration::from_secs(10), |reqs| {
            reqs.iter().any(|r| r.inserted.is_some())
        })
        .await;
        assert_eq!(
            plain_body.expect("plain post 200")["persisted"],
            serde_json::json!(true)
        );

        // Role'd write: the endpoint's role must reach the write path.
        let writer_name = unique_name("role-writer");
        let writer_body = body_json(
            load_schema_handler(
                State(app.clone()),
                Json(LoadSchemaRequest {
                    schema_name: writer_name.clone(),
                    config_content: tiny_yaml(&writer_name),
                    validate_schema: None,
                    role: Some("writer".to_string()),
                }),
            )
            .await,
        )
        .await;
        fake.wait_for("the role'd INSERT", Duration::from_secs(10), |reqs| {
            reqs.iter().filter(|r| r.inserted.is_some()).count() >= 2
        })
        .await;
        assert_eq!(
            writer_body.expect("writer post 200")["persisted"],
            serde_json::json!(true)
        );

        let snapshot = fake.requests_snapshot();
        let inserts: Vec<&Recorded> = snapshot.iter().filter(|r| r.inserted.is_some()).collect();
        assert_eq!(inserts.len(), 2, "two plain INSERT statements expected");
        assert!(
            inserts[0].role.is_none(),
            "a registry write without `role` must not claim one (mutation: \
             ignore role → default client): {:?}",
            inserts[0].sql
        );
        assert_eq!(
            inserts[1].role.as_deref(),
            Some("writer"),
            "the write must run as the endpoint's role — a mutation that \
             drops `role` from `client_for_write` turns this red: {:?}",
            inserts[1].role
        );
    }

    #[tokio::test]
    async fn the_registry_table_is_created_once_per_process_not_per_operation() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        let app = app_state(fake.client(), None, persisted_config());

        let names: Vec<String> = vec![unique_name("create-once-a"), unique_name("create-once-b")];
        for name in &names {
            let body = body_json(
                load_schema_handler(
                    State(app.clone()),
                    Json(LoadSchemaRequest {
                        schema_name: name.clone(),
                        config_content: tiny_yaml(name),
                        validate_schema: None,
                        role: None,
                    }),
                )
                .await,
            )
            .await;
            assert_eq!(
                body.expect("post 200")["persisted"],
                serde_json::json!(true)
            );
        }

        // A memory-path delete rides the same cached preparation.
        let delete_name = fake.rows_snapshot()[0].name.clone();
        let delete_body = body_json(
            delete_schema_handler(
                State(app.clone()),
                Path(delete_name.clone()),
                Query(DeleteSchemaParams { role: None }),
            )
            .await,
        )
        .await;
        assert_eq!(
            delete_body.expect("delete 200")["persisted"],
            serde_json::json!(true)
        );

        assert_eq!(
            fake.create_count(),
            1,
            "issuing a CREATE before every operation (the round-1 design the \
             review called out) makes this 3; the process prepares its table \
             exactly once"
        );
        let store_names: Vec<String> = fake.rows_snapshot().into_iter().map(|r| r.name).collect();
        let survivor = names
            .iter()
            .find(|candidate| **candidate != delete_name)
            .expect("exactly one registration survives the delete");
        assert_eq!(
            store_names,
            vec![survivor.clone()],
            "the surviving registration stays stored: {store_names:?}"
        );
        assert!(
            graph_catalog::get_graph_schema_by_name(&delete_name)
                .await
                .is_err(),
            "the deleted schema must be gone from memory"
        );
    }

    #[tokio::test]
    async fn a_failed_create_falls_through_to_the_probe_and_still_stores() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        fake.set_create_fails(true);
        let app = app_state(fake.client(), None, persisted_config());

        let name = unique_name("least-privilege");
        let body = body_json(
            load_schema_handler(
                State(app),
                Json(LoadSchemaRequest {
                    schema_name: name.clone(),
                    config_content: tiny_yaml(&name),
                    validate_schema: None,
                    role: None,
                }),
            )
            .await,
        )
        .await;
        let body = body.expect("post 200");
        assert_eq!(
            body["persisted"],
            serde_json::json!(true),
            "a user without the CREATE grant must still persist (the \
             DBA-provisioned-table path); mutation: a failed CREATE fails \
             the ensure → body: {body}"
        );
        assert_eq!(
            fake.create_count(),
            1,
            "the CREATE was attempted, then the probes settled the layout"
        );
        assert!(
            fake.rows_snapshot().into_iter().any(|r| r.name == name),
            "sanity: the row is stored"
        );
    }

    #[tokio::test]
    async fn a_wedged_store_statement_degrades_within_the_bound() {
        let fake = FakeClickHouse::spawn().await;
        let mut inner = ClickhouseSchemaStore::new(fake.client());
        inner.call_bound = Duration::from_millis(20);
        inner.replay_bound = Duration::from_millis(60);
        // save_yaml lives on the SchemaStore enum (the handlers' door), not
        // on the concrete Clickhouse store.
        let store = SchemaStore::Clickhouse(Box::new(inner));
        fake.set_hang_create(true);

        let saved = tokio::time::timeout(
            Duration::from_secs(5),
            store.save_yaml(&unique_name("wedged"), "name: wedged", None),
        )
        .await;
        let outcome = saved.expect(
            "every store op must be bounded at ITS call site (`bound_call`): \
             save_yaml hung past the harness deadline, meaning the op itself \
             was left unbounded — remove `with_timeout_within` at a call \
             site and this assertion fires instead of the API hanging",
        );
        let outcome =
            outcome.expect_err("a wedged server must surface the bounded-call error, not success");
        assert!(
            outcome.contains("store unreachable"),
            "the wedged CREATE must surface as the mapped error, not the raw \
             one: {outcome}"
        );
        assert!(
            outcome.contains("'registry upsert'"),
            "the op name trails for the log trail: {outcome}"
        );
    }

    #[tokio::test]
    async fn a_failed_high_water_seed_never_caches_the_preparation_and_never_writes() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        fake.set_seed_fails(true);
        let app = app_state(fake.client(), None, persisted_config());

        let first = unique_name("seed-fail");
        let body = body_json(
            load_schema_handler(
                State(app.clone()),
                Json(LoadSchemaRequest {
                    schema_name: first.clone(),
                    config_content: tiny_yaml(&first),
                    validate_schema: None,
                    role: None,
                }),
            )
            .await,
        )
        .await;
        let body = body.expect("post 200");
        assert_eq!(
            body["persisted"],
            serde_json::json!(false),
            "item 1: the ensure fails when the floor's high-water read \
             fails; the write never happens — body: {body}"
        );
        assert_eq!(
            fake.insert_count(),
            0,
            "a seed that cannot be read must stop the write: the floor is \
             load-bearing (a row stamped below the table high-water wins the \
             collapse and a restart serves a stale schema)"
        );

        // The failure was NOT cached: fixing the store makes the very next
        // op succeed, having re-run the whole preparation.
        fake.set_seed_fails(false);
        let second = unique_name("seed-ok");
        let body = body_json(
            load_schema_handler(
                State(app),
                Json(LoadSchemaRequest {
                    schema_name: second.clone(),
                    config_content: tiny_yaml(&second),
                    validate_schema: None,
                    role: None,
                }),
            )
            .await,
        )
        .await;
        assert_eq!(
            body.expect("post 200")["persisted"],
            serde_json::json!(true)
        );
        assert_eq!(
            fake.create_count(),
            2,
            "the failed ensure ran once, its retry ran again — caching past \
             a failed seed (the pre-round-3 bug) answers false retries"
        );
        assert_eq!(fake.insert_count(), 1, "exactly the retry succeeds");
    }

    #[tokio::test]
    async fn a_floor_left_unseeded_cannot_stamp_below_the_table_high_water() {
        let fake = FakeClickHouse::spawn().await;
        let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(fake.client())));
        // 100 seconds past this process's wall clock: a stamp below this can
        // only come from a no-op seed (the mutation).
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let high_water = now_ms + 100_000_000;
        fake.set_high_water(high_water);

        let name = unique_name("seeded-floor");
        store
            .save_yaml(&name, "name: seeded-floor", None)
            .await
            .expect("the store write succeeds on a healthy fake");

        let inserted = fake.inserted_rows();
        assert_eq!(inserted.len(), 1, "exactly one write");
        assert!(
            inserted[0].registered_at_ms >= high_water,
            "the write's stamp ({}) must carry the SEEDED floor (= {high_water} \
             here, then strict-increase): a no-op seed leaves it at wall \
             clock and the row loses ReplacingMergeTree's collapse to an \
             older row",
            inserted[0].registered_at_ms
        );
    }

    #[tokio::test]
    async fn the_legacy_layout_restores_through_the_single_phase_select() {
        let fake = FakeClickHouse::spawn().await;
        fake.set_legacy_layout(true);
        let store = ClickhouseSchemaStore::new(fake.client());

        let first = Registration {
            name: "legacy-alpha".to_string(),
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "alpha-legacy-payload".to_string(),
            registry_version: REGISTRY_VERSION,
            registered_at_ms: 111,
        };
        let second = Registration {
            name: "legacy-bravo".to_string(),
            payload: "bravo-legacy-payload".to_string(),
            registered_at_ms: 222,
            ..first.clone()
        };
        fake.push_row(first);
        fake.push_row(second);

        let fetch = store
            .load_replay_rows(500, 16_777_216)
            .await
            .expect("a legacy table keeps restoring");
        assert_eq!(
            fetch.total_rows, 2,
            "the count read sees both rows before any budget math"
        );
        assert_eq!(fetch.fetched, 2);
        assert_eq!(fetch.restorable.len(), 2, "both rows are restorable");
        let payloads: Vec<&str> = fetch
            .restorable
            .iter()
            .map(|row| row.payload.as_str())
            .collect();
        assert!(
            payloads.contains(&"alpha-legacy-payload")
                && payloads.contains(&"bravo-legacy-payload"),
            "the legacy select carries the payloads themselves (mutation: a \
             legacy-layout select broken → empty payloads or an error): \
             {payloads:?}"
        );

        let snapshot = fake.requests_snapshot();
        let replay_reads: Vec<&str> = snapshot
            .iter()
            .map(|r| r.sql.as_str())
            .filter(|sql| sql.contains("sum("))
            .collect();
        assert_eq!(
            replay_reads.len(),
            1,
            "one layout probe fell through to the legacy shape: \
             {replay_reads:?}"
        );
        assert!(
            replay_reads[0].contains("sum(length(payload))"),
            "the legacy fetch answers through the single-phase select: {}",
            replay_reads[0]
        );
        assert!(
            !replay_reads[0].contains("sum(payload_len)"),
            "the legacy table has no payload_len column: {}",
            replay_reads[0]
        );
    }

    #[tokio::test]
    async fn phase_two_fetches_payloads_by_name_and_registered_at_ms() {
        let fake = FakeClickHouse::spawn().await;
        let store = ClickhouseSchemaStore::new(fake.client());

        fake.push_row(Registration {
            name: "pair-alpha".to_string(),
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "alpha-current-payload".to_string(),
            registry_version: REGISTRY_VERSION,
            registered_at_ms: 123,
        });
        fake.push_row(Registration {
            name: "pair-bravo".to_string(),
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "bravo-current-payload".to_string(),
            registry_version: REGISTRY_VERSION,
            registered_at_ms: 456,
        });

        let fetch = store.load_replay_rows(500, 16_777_216).await.expect("read");
        assert_eq!(fetch.restorable.len(), 2);
        for row in &fetch.restorable {
            let expected = match row.name.as_str() {
                "pair-alpha" => "alpha-current-payload",
                "pair-bravo" => "bravo-current-payload",
                other => panic!("unexpected row {other}"),
            };
            assert_eq!(
                row.payload, expected,
                "the payload must land on ITS (name, registered_at_ms) row"
            );
        }

        let snapshot = fake.requests_snapshot();
        let phase2_sqls: Vec<&str> = snapshot
            .iter()
            .map(|r| r.sql.as_str())
            .filter(|sql| sql.contains("SELECT name, registered_at_ms, payload"))
            .collect();
        assert_eq!(phase2_sqls.len(), 1, "one phase-2 fetch");
        let sql = phase2_sqls[0];
        assert!(
            sql.contains("(name, registered_at_ms) IN ("),
            "the phase-2 fetch keys rows by BOTH columns — reverting to a \
             name-only IN restores a rewritten row under stale metadata \
             (the round-3 note): {sql}"
        );
        assert!(
            sql.contains("('pair-alpha', 123)") && sql.contains("('pair-bravo', 456)"),
            "the binds arrive as client-substituted literals: {sql}"
        );
    }

    #[tokio::test]
    async fn a_failed_store_only_delete_answers_500_without_leaking_the_clickhouse_error() {
        ensure_globals().await;
        let fake = FakeClickHouse::spawn().await;
        fake.set_delete_fails(true);
        // A row in the store, deliberately absent from memory: exactly the
        // state a failed replay leaves behind.
        fake.push_row(Registration {
            name: "store-only-faulty".to_string(),
            source: SOURCE_API_LOAD.to_string(),
            format: FORMAT_YAML.to_string(),
            payload: "rows".to_string(),
            registry_version: REGISTRY_VERSION,
            registered_at_ms: 42,
        });
        let app = app_state(fake.client(), None, persisted_config());

        let attempted = body_json(
            delete_schema_handler(
                State(app),
                Path("store-only-faulty".to_string()),
                Query(DeleteSchemaParams { role: None }),
            )
            .await,
        )
        .await;
        let Err((status, body)) = attempted else {
            panic!("a failing store must not answer 200: {attempted:?}");
        };
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let error = body["error"].as_str().expect("error body");
        assert!(
            error.contains("could not be deleted") && error.contains("registry store answered"),
            "item 4: the fixed message promises nothing about the store \
             internals — body: {error}"
        );
        assert!(
            !error.contains("d34dbeef"),
            "the ClickHouse-side fault detail stays in the log (the WARN), \
             never in an API body: {error}"
        );
    }

    #[test]
    fn the_endpoint_validators_refuse_what_the_registry_cannot_record_safely() {
        // The good cases first: the happy path must not trip anything.
        assert_eq!(schema_name_error_if_any("good-name2"), None);
        assert_eq!(schema_name_error_if_any(&"x".repeat(512)), None, "at cap");
        assert_eq!(role_error_if_any("writer"), None);
        // An empty role is NOT a validator error: both endpoints normalize ''
        // to no-role before the check (the round-one contract), and a stray
        // empty string must stay a default write, not a 400.
        assert_eq!(role_error_if_any(""), None);

        // Names: empty, control characters, over-cap — all refusals.
        assert!(schema_name_error_if_any("").is_some(), "empty name");
        assert!(
            schema_name_error_if_any("forged\nlog-line").is_some(),
            "a control character forges log lines"
        );
        assert!(schema_name_error_if_any("f\u{7}orged").is_some());
        assert!(
            schema_name_error_if_any(&"x".repeat(513)).is_some(),
            "past the cap"
        );

        // Roles: control characters and over-cap — refusals.
        assert!(
            role_error_if_any("candidate\nrole").is_some(),
            "the pool logs a first-seen role verbatim"
        );
        assert!(role_error_if_any(&"x".repeat(257)).is_some());
        assert_eq!(role_error_if_any(&"x".repeat(256)), None, "at cap");
    }
}
