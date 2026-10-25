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

/// Bounds one store round-trip ([`STORE_CALL_TIMEOUT`]) and maps the
/// elapsed-outcome to the error path every caller already handles. The op
/// name lands in the message for the log/API trail only — values never do.
async fn with_timeout<T>(
    op: &'static str,
    fut: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    with_timeout_within(STORE_CALL_TIMEOUT, op, fut).await
}

/// The boundable core of [`with_timeout`]: every store op routes through the
/// public-shaped wrapper's duration, and unit tests pin the mapping with
/// option values (`Duration` is an argument, never a caller-visible knob).
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

/// Phase-2 pairing (name → payload) of the two-phase fetch.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct NamePayload {
    name: String,
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
    /// budget inside ClickHouse (a window sum over the replay order), so a
    /// boot with a 64 MiB budget never materializes more than 64 MiB. Total
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
        }
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
    /// persists. A successful ensure caches the layout for the process's
    /// lifetime; a failed one stays uncached and is retried by the next op.
    async fn ensure_table(&self, client: &Client) -> Result<TableLayout, String> {
        let mut prepared = self.table_prepared.lock().await;
        if let Some(layout) = *prepared {
            return Ok(layout);
        }

        let create = self.try_create_table(client).await;
        match self.detect_layout(client).await {
            Ok(layout) => {
                self.seed_registered_at_floor(client).await;
                *prepared = Some(layout);
                Ok(layout)
            }
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
    /// only raise it, and a higher floor is always safe.
    async fn seed_registered_at_floor(&self, client: &Client) {
        match client
            .query(&format!(
                "SELECT max(registered_at_ms) FROM {REGISTRY_TABLE}"
            ))
            .fetch_one::<u64>()
            .await
        {
            Ok(high_water) => {
                self.registered_at_floor
                    .fetch_max(high_water, Ordering::Relaxed);
            }
            Err(e) => {
                log::warn!(
                    "could not read the registry's registered-at high-water \
                     mark ({e}); timestamps stay monotonic within this boot \
                     only"
                );
            }
        }
    }

    /// Strictly increasing, even when the host clock steps back: each call
    /// returns `max(now, floor + 1)` under a CAS loop, so a boot whose clock
    /// regressed writes values above the table's high-water mark instead of
    /// randomly losing `ReplacingMergeTree`'s collapse to an old row. Within
    /// one boot the values can only rise (`floor` is carried by every
    /// successful CAS).
    fn next_registered_at_ms(&self) -> u64 {
        let now = Registration::now_ms();
        let mut current = self.registered_at_floor.load(Ordering::Relaxed);
        loop {
            let next = if now > current { now } else { current + 1 };
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
        with_timeout(
            "registry upsert",
            self.upsert_inner(name, source, format, payload, role),
        )
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
        with_timeout("registry exists probe", self.exists_inner(name)).await
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
    ///   pay the payload transfer, and the server materializes payloads for
    ///   no row whose bytes exceed the budget (a 300-row × 900 KiB table
    ///   used to materialize ~1.13 GiB server-side before answering).
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
        with_timeout(
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
                match payloads.get(&row.name) {
                    Some(payload) => {
                        let mut row = row;
                        row.payload = payload.clone();
                        with_payload.push(row);
                    }
                    None => {
                        vanished += 1;
                        log::warn!(
                            "registry row '{}' vanished between the replay's \
                             metadata and payload reads; not restored this \
                             boot",
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

    /// Phase 2 of the two-phase fetch: payloads for exactly these rows,
    /// by bound parameters (names are operator input — never interpolated
    /// into SQL text). The bound name list is capped by phase 1's
    /// `max_rows`, so the statement stays small however fat the table is.
    async fn payloads_for(&self, rows: &[Registration]) -> Result<HashMap<String, String>, String> {
        if rows.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = vec!["?"; rows.len()].join(", ");
        let mut query = self.client.query(&format!(
            "SELECT name, payload FROM {REGISTRY_TABLE} FINAL \
             WHERE name IN ({placeholders})"
        ));
        for row in rows {
            query = query.bind(row.name.clone());
        }
        let pairs: Vec<NamePayload> = query
            .fetch_all::<NamePayload>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;
        Ok(pairs
            .into_iter()
            .map(|pair| (pair.name, pair.payload))
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
        with_timeout("registry delete", self.delete_inner(name, role)).await
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
