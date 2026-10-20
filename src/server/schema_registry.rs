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

use std::time::{SystemTime, UNIX_EPOCH};

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

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

/// One persisted registration (`registry-v1`).
///
/// `registered_at_ms` is written by the server (never read back as truth):
/// restore order is "oldest first", so a name re-registered twice ends on its
/// latest registration, the way Kafka's compacted config topic ends on the
/// last write.
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

/// A version gate: current rows to restore, and rows whose `registry_version`
/// was written by a newer ClickGraph (skip + report — never panic, never
/// guess at an unknown shape).
pub struct VersionGate {
    pub restorable: Vec<Registration>,
    pub future_versioned: Vec<Registration>,
}

/// Gate rows by registry version (pure — unit-tested).
pub fn gate_registry_versions(rows: Vec<Registration>) -> VersionGate {
    let mut restorable = Vec::new();
    let mut future_versioned = Vec::new();
    for row in rows {
        if row.registry_version <= REGISTRY_VERSION {
            restorable.push(row);
        } else {
            future_versioned.push(row);
        }
    }
    VersionGate {
        restorable,
        future_versioned,
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
    /// The one construction point: mode from the server config, client from
    /// whatever built `AppState` this boot. Stays log-quiet: the boot-time
    /// "no client in this server mode" notice belongs to `replay` (once per
    /// boot), and per-request constructions must not spam it.
    pub fn from_parts(mode: SchemaPersistenceMode, clickhouse_client: Option<Client>) -> Self {
        if mode == SchemaPersistenceMode::Clickhouse {
            return match clickhouse_client {
                Some(client) => {
                    SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client)))
                }
                None => SchemaStore::Off,
            };
        }
        SchemaStore::Off
    }

    /// Persist a YAML registration. `Ok(false)` = the store refused — the
    /// registration itself already succeeded in memory.
    pub async fn save_yaml(&self, name: &str, yaml: &str) -> Result<(), String> {
        match self {
            SchemaStore::Off => Ok(()),
            SchemaStore::Clickhouse(store) => {
                store
                    .upsert(name, SOURCE_API_LOAD, FORMAT_YAML, yaml.to_string())
                    .await
            }
        }
    }

    /// Remove a registration row. Returns `Ok(true)` when the row is gone:
    /// the ClickHouse store reports success even when the name never had a
    /// row (`DELETE FROM` matches nothing there) — the end state is the same,
    /// so callers treat both as "not persisted anymore", not as an error.
    pub async fn delete(&self, name: &str) -> Result<bool, String> {
        match self {
            SchemaStore::Off => Ok(false),
            SchemaStore::Clickhouse(store) => store.delete(name).await,
        }
    }

    /// Boot replay's one read, bounded: the deduped row count plus (at most)
    /// `max_rows` oldest-first `restorable` rows. Restoring everything a
    /// compartment-compromised or runaway client ever persisted, unbounded,
    /// converts a one-time write burst into a per-restart cost that can push
    /// boot past any readiness window — the row budget makes that worst case
    /// visible and operator-dialable instead of silent. Total rows beyond the
    /// cap are counted in [`ReplayFetch`] so the caller can warn with exact
    /// numbers.
    pub async fn load_replay_rows(&self, max_rows: usize) -> Result<ReplayFetch, String> {
        match self {
            SchemaStore::Off => Ok(ReplayFetch::default()),
            SchemaStore::Clickhouse(store) => store.load_replay_rows(max_rows).await,
        }
    }
}

/// A single bounded replay read.
#[derive(Debug, Default)]
pub struct ReplayFetch {
    /// Current-version rows, oldest first, within the row budget.
    pub restorable: Vec<Registration>,
    /// Rows whose `registry_version` is newer than this server (never
    /// restored; the store warns with each name at fetch time).
    pub future_versioned: Vec<Registration>,
    /// Deduped rows in the table (`FINAL` count) — rows beyond the row
    /// budget were never fetched, so this may exceed both vectors' lengths.
    pub total_rows: u64,
    /// Rows actually fetched (`restorable + future_versioned` lengths).
    pub fetched: usize,
}

/// ClickHouse-backed store. Statement text is fixed; every operator-supplied
/// value rides in as a bound parameter — schema names are operator input and
/// are never interpolated into SQL text.
pub struct ClickhouseSchemaStore {
    client: Client,
    /// Serializes table creation. A CREATE TABLE IF NOT EXISTS per store op is
    /// racy when two first-boot registrations run concurrently (one can lose
    /// the metadata race and fail the op, degrading `persisted: false`); one
    /// write path at a time removes the race without error-string sniffing.
    table_creation: tokio::sync::Mutex<()>,
}

impl ClickhouseSchemaStore {
    pub fn new(client: Client) -> Self {
        // DDL must not be fire-and-forget here: a store that answers before
        // its table exists makes the first registration racy.
        Self {
            client: client.clone().with_option("wait_end_of_query", "1"),
            table_creation: tokio::sync::Mutex::new(()),
        }
    }

    async fn ensure_table(&self) -> Result<(), String> {
        let _guard = self.table_creation.lock().await;
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (\
             name String, \
             source LowCardinality(String), \
             format LowCardinality(String), \
             payload String, \
             registry_version UInt16, \
             registered_at_ms UInt64\
             ) ENGINE = ReplacingMergeTree(registered_at_ms) ORDER BY name"
        );
        self.client
            .query(&create)
            .execute()
            .await
            .map_err(|e| format!("registry table create failed: {e}"))?;
        Ok(())
    }

    async fn upsert(
        &self,
        name: &str,
        source: &str,
        format: &str,
        payload: String,
    ) -> Result<(), String> {
        self.ensure_table().await?;
        let row = Registration {
            name: name.to_string(),
            source: source.to_string(),
            format: format.to_string(),
            payload,
            registry_version: REGISTRY_VERSION,
            registered_at_ms: Registration::now_ms(),
        };
        let mut insert = self
            .client
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

    /// `FINAL` collapses superseded rows of the same name to the last write
    /// (ReplacingMergeTree's compaction, read live — the same idiom the
    /// legacy `graph_catalog` read uses). The version column
    /// (`registered_at_ms`) makes the collapse deterministic: two same-name
    /// writes can land in the same millisecond, and with the version column
    /// the merge keeps the one registered later in the same part cycle.
    pub async fn load_replay_rows(&self, max_rows: usize) -> Result<ReplayFetch, String> {
        self.ensure_table().await?;

        // Count first so "N rows skipped over the budget" is an exact
        // statement, not a guess. Two small reads instead of one unbounded
        // one: the count on FINAL is cheap on this table (one row per
        // registration, boot-time only), and the LIMIT below then bounds
        // payload transfer regardless of what is in the table.
        let total_rows: u64 = self
            .client
            .query(&format!("SELECT count() FROM {REGISTRY_TABLE} FINAL"))
            .fetch_one::<u64>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;

        let select = format!(
            "SELECT name, source, format, payload, registry_version, \
             registered_at_ms FROM {REGISTRY_TABLE} FINAL \
             ORDER BY registered_at_ms, name LIMIT {max_rows}"
        );
        let rows: Vec<Registration> = self
            .client
            .query(&select)
            .fetch_all::<Registration>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;

        let gate = gate_registry_versions(rows);
        for row in &gate.future_versioned {
            log::warn!(
                "registry row '{}' was written by registry_version {} \
                 (newer than this server's {REGISTRY_VERSION}); skipping it",
                log_safe(&row.name),
                row.registry_version
            );
        }
        Ok(ReplayFetch {
            fetched: gate.restorable.len() + gate.future_versioned.len(),
            restorable: gate.restorable,
            future_versioned: gate.future_versioned,
            total_rows,
        })
    }

    /// `DELETE FROM` (lightweight delete) first; the mutation-based
    /// `ALTER TABLE ... DELETE` is the fallback for servers that disable
    /// lightweight deletes. Both are synchronous before returning. Success is
    /// reported even when the name has no row (an idempotent end state), and
    /// note the write-on-read surprise: on a database where nothing was ever
    /// registered, this creates the (empty) registry table.
    pub async fn delete(&self, name: &str) -> Result<bool, String> {
        self.ensure_table().await?;
        let lightweight = self
            .client
            .query(&format!("DELETE FROM {REGISTRY_TABLE} WHERE name = ?"))
            .bind(name.to_string())
            .with_option("wait_end_of_query", "1")
            .execute()
            .await;
        if lightweight.is_ok() {
            return Ok(true);
        }
        self.client
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

/// Persist after a successful `POST /schemas/load`.
///
/// `None` = persistence is off (the caller must leave the response body
/// byte-identical to a server without the feature). `Some(true/false)` = the
/// attempt happened and the field says whether it stuck. Store failures
/// degrade to `Some(false)` plus a WARN: the schema itself is live either
/// way, and a flaky store must not make the registration API flaky.
pub async fn persist_yaml_after_load(
    config: &ServerConfig,
    clickhouse_client: Option<Client>,
    schema_name: &str,
    yaml_content: &str,
) -> Option<bool> {
    if config.schema_persistence != SchemaPersistenceMode::Clickhouse {
        return None;
    }
    // From here on the attempt is real: `Some(ok)` is the honest answer even
    // when a store is unavailable (no client in this server mode) — the
    // registration succeeded in memory, the response reports what persisted.
    let store = SchemaStore::from_parts(config.schema_persistence, clickhouse_client);
    let outcome = match &store {
        SchemaStore::Off => {
            // No-client modes must not re-WARN per request (boot's `replay`
            // logged the condition once); a persisted:false response is the
            // contract surface. Debug keeps the trail for support.
            log::debug!(
                "schema persistence: no ClickHouse client in this server \
                 mode — '{}' stays memory-only",
                log_safe(schema_name)
            );
            return Some(false);
        }
        SchemaStore::Clickhouse(store) => {
            store
                .upsert(
                    schema_name,
                    SOURCE_API_LOAD,
                    FORMAT_YAML,
                    yaml_content.to_string(),
                )
                .await
        }
    };
    if let Err(e) = &outcome {
        log::warn!(
            "schema '{}' could not be persisted for restart ({e}); \
             the registration is live, it just will not survive one",
            log_safe(schema_name)
        );
    }
    Some(outcome.is_ok())
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
    // The boot notice lives here (not in `from_parts`, which the per-request
    // write paths also construct) so a no-client mode logs it exactly once.
    if config.schema_persistence == SchemaPersistenceMode::Clickhouse
        && app_state.clickhouse_client.is_none()
    {
        log::warn!(
            "CLICKGRAPH_SCHEMA_PERSISTENCE=clickhouse but this server mode \
             has no ClickHouse client — schema persistence stays off: nothing \
             is stored or replayed (no-client server modes carry none)"
        );
    }

    let store = SchemaStore::from_parts(
        config.schema_persistence,
        app_state.clickhouse_client.clone(),
    );
    // Off stays fully silent (byte-for-byte the pre-feature boot).
    if matches!(store, SchemaStore::Off) {
        return;
    }
    let mut fetch = match store.load_replay_rows(config.schema_replay_max_rows).await {
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

    let over_row_cap = fetch.total_rows - fetch.fetched as u64;
    if over_row_cap > 0 {
        log::warn!(
            "registry holds {total} rows but the replay budget allows only \
             {fetched} (CLICKGRAPH_SCHEMA_REPLAY_MAX_ROWS={max}) — \
             {over} oldest-first rows were not fetched and will not be \
             restored; raise the budget or clean {REGISTRY_TABLE}",
            total = fetch.total_rows,
            fetched = fetch.fetched,
            max = config.schema_replay_max_rows,
            over = over_row_cap,
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
        // Byte budget, enforced in registered_at_ms order: stop before a row
        // would exceed it and skip the deterministic remainder (a newer
        // registration that fits drops nothing from the budget already used),
        // counting what is left so the WARN tells an exact number.
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
                log::warn!(
                    "persisted schema '{}' could not be restored ({e}) — \
                     register it again, or drop it with DELETE /schemas/{}",
                    log_safe(&row.name),
                    log_safe(&row.name)
                );
            }
        }
    }

    log::info!(
        "schema persistence: {} restored / {} config-shadowed / {} failed \
         / {} skipped over replay budget (failed: {:?})",
        restored,
        shadowed.len(),
        failed_names.len(),
        over_row_cap + byte_skipped as u64,
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
        let gate = gate_registry_versions(vec![
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
    }
}
