//! Persistent schema registry — the durability behind `POST /schemas/load`
//! and DDL-created tables (spec: `docs/design/SCHEMA_PERSISTENCE.md`).
//!
//! # The problem it solves
//!
//! `load_schema_from_content` registers into the in-memory `GLOBAL_SCHEMAS` /
//! `GLOBAL_SCHEMA_CONFIGS` maps and drops the YAML text it was given; the
//! legacy DDL path serializes a lossy `GraphSchema` JSON into the
//! `graph_catalog` table but nothing boots from it unless *no* YAML is
//! configured. Net effect: a restart or upgrade loses every runtime
//! registration. Systems that ship this guarantee (Kafka Connect's
//! `config.storage.topic`, Debezium's `schema.history.internal`) externalize
//! the source text to storage owned by the platform itself and replay it at
//! boot; this module is that pattern against ClickHouse, which is already a
//! hard dependency.
//!
//! # What is stored, and why source text (not decoded structs)
//!
//! One row per registered name carries the **original YAML** for an HTTP
//! registration and the serialized `GraphSchema` JSON for a DDL mutation.
//! Decoded structs are lossy today (vector/fulltext indexes, engine info and
//! denormalized metadata are `#[serde(skip)]` on `graph_schema.rs:1678`), so
//! restore-at-boot must start from the source. The YAML additionally keeps
//! every schema feature the parser grew after the JSON path was written.
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

/// Who wrote the row. `api-load` rows carry replayable YAML; `ddl` rows carry
/// a `GraphSchema` snapshot that inherits the documented round-trip losses.
pub const SOURCE_API_LOAD: &str = "api-load";
pub const SOURCE_DDL: &str = "ddl";

/// Serialization format of `payload`.
pub const FORMAT_YAML: &str = "yaml";
pub const FORMAT_GRAPH_SCHEMA_JSON: &str = "graph-schema-json";

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
    /// whatever built `AppState` this boot.
    pub fn from_parts(mode: SchemaPersistenceMode, clickhouse_client: Option<Client>) -> Self {
        match (mode, clickhouse_client) {
            (SchemaPersistenceMode::Clickhouse, Some(client)) => {
                SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client)))
            }
            (mode, client) => {
                if mode == SchemaPersistenceMode::Clickhouse && client.is_none() {
                    log::warn!(
                        "CLICKGRAPH_SCHEMA_PERSISTENCE=clickhouse but this server \
                         mode has no ClickHouse client — schema persistence stays \
                         off (no-client server modes carry none)"
                    );
                }
                SchemaStore::Off
            }
        }
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

    /// Persist a DDL-mutated schema snapshot. Same never-fail contract.
    pub async fn save_schema_snapshot(&self, name: &str, json: &str) -> Result<(), String> {
        match self {
            SchemaStore::Off => Ok(()),
            SchemaStore::Clickhouse(store) => {
                store
                    .upsert(name, SOURCE_DDL, FORMAT_GRAPH_SCHEMA_JSON, json.to_string())
                    .await
            }
        }
    }

    /// Remove a registration row. `Ok(false)` = nothing was stored under the
    /// name (never persisted, or already deleted).
    pub async fn delete(&self, name: &str) -> Result<bool, String> {
        match self {
            SchemaStore::Off => Ok(false),
            SchemaStore::Clickhouse(store) => store.delete(name).await,
        }
    }

    /// All rows, oldest first; future versions filtered with a warning.
    pub async fn load_oldest_first(&self) -> Result<Vec<Registration>, String> {
        match self {
            SchemaStore::Off => Ok(Vec::new()),
            SchemaStore::Clickhouse(store) => store.load_oldest_first().await,
        }
    }
}

/// ClickHouse-backed store. Statement text is fixed; every operator-supplied
/// value rides in as a bound parameter — schema names are operator input and
/// are never interpolated into SQL text.
pub struct ClickhouseSchemaStore {
    client: Client,
}

impl ClickhouseSchemaStore {
    pub fn new(client: Client) -> Self {
        // DDL must not be fire-and-forget here: a store that answers before
        // its table exists makes the first registration racy.
        Self {
            client: client.clone().with_option("wait_end_of_query", "1"),
        }
    }

    async fn ensure_table(&self) -> Result<(), String> {
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (\
             name String, \
             source LowCardinality(String), \
             format LowCardinality(String), \
             payload String, \
             registry_version UInt16, \
             registered_at_ms UInt64\
             ) ENGINE = ReplacingMergeTree() ORDER BY name"
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

    /// Persist a YAML registration (`api-load` source).
    pub async fn save_yaml(&self, name: &str, yaml: &str) -> Result<(), String> {
        self.upsert(name, SOURCE_API_LOAD, FORMAT_YAML, yaml.to_string())
            .await
    }

    /// Persist a serialized `GraphSchema` snapshot (`ddl` source).
    pub async fn save_schema_snapshot(&self, name: &str, json: &str) -> Result<(), String> {
        self.upsert(name, SOURCE_DDL, FORMAT_GRAPH_SCHEMA_JSON, json.to_string())
            .await
    }

    /// `FINAL` collapses superseded rows of the same name to the last write
    /// (ReplacingMergeTree's compaction, read live — the same idiom the
    /// legacy `graph_catalog` read uses).
    pub async fn load_oldest_first(&self) -> Result<Vec<Registration>, String> {
        self.ensure_table().await?;
        let rows: Vec<Registration> = self
            .client
            .query(&format!(
                "SELECT name, source, format, payload, registry_version, \
                 registered_at_ms FROM {REGISTRY_TABLE} FINAL \
                 ORDER BY registered_at_ms, name"
            ))
            .fetch_all::<Registration>()
            .await
            .map_err(|e| format!("registry read failed: {e}"))?;

        let gate = gate_registry_versions(rows);
        for row in &gate.future_versioned {
            log::warn!(
                "registry row '{}' was written by registry_version {} \
                 (newer than this server's {REGISTRY_VERSION}); skipping it",
                row.name,
                row.registry_version
            );
        }
        Ok(gate.restorable)
    }

    /// `DELETE FROM` (lightweight delete) first; the mutation-based
    /// `ALTER TABLE ... DELETE` is the fallback for servers that disable
    /// lightweight deletes. Both are synchronous before returning.
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
        SchemaStore::Off => Err("no ClickHouse client in this server mode".to_string()),
        SchemaStore::Clickhouse(store) => store.save_yaml(schema_name, yaml_content).await,
    };
    if let Err(e) = &outcome {
        log::warn!(
            "schema '{}' could not be persisted for restart ({e}); \
             the registration is live, it just will not survive one",
            schema_name
        );
    }
    Some(outcome.is_ok())
}
/// Persist after a successful DDL mutation (`add_to_schema`): snapshot the
/// mutated `GraphSchema` so a boot without the config file restores the
/// created tables. Inherits the documented lossy round-trip — the same
/// caveat the legacy `graph_catalog` path carries.
pub async fn persist_ddl_snapshot(
    config: &ServerConfig,
    clickhouse_client: Option<Client>,
    schema_name: &str,
) -> Option<bool> {
    if config.schema_persistence != SchemaPersistenceMode::Clickhouse {
        return None;
    }
    let store = SchemaStore::from_parts(config.schema_persistence, clickhouse_client);
    // Attempt is real: no client (no-client server modes) reports `false`,
    // the same honest answer a store error gets.
    let outcome = match &store {
        SchemaStore::Off => Err("no ClickHouse client in this server mode".to_string()),
        SchemaStore::Clickhouse(_) => match snapshot_registered_schema(schema_name).await {
            Ok(json) => store.save_schema_snapshot(schema_name, &json).await,
            Err(e) => {
                log::warn!("DDL snapshot for '{schema_name}' not persisted ({e})");
                return Some(false);
            }
        },
    };
    if let Err(e) = &outcome {
        log::warn!(
            "schema '{schema_name}' DDL mutation not persisted ({e}); \
             the created table is live, it just will not survive a restart"
        );
        return Some(false);
    }
    Some(true)
}

/// Serialize the current in-memory `GraphSchema` of `schema_name`.
///
/// Point-in-time read: another DDL in flight may mutate the same schema after
/// this snapshot; the store's last-write-wins rows keep it convergent with
/// the in-memory registry either way.
async fn snapshot_registered_schema(schema_name: &str) -> Result<String, String> {
    let schemas_lock = super::GLOBAL_SCHEMAS
        .get()
        .ok_or_else(|| "GLOBAL_SCHEMAS not initialized".to_string())?;
    let schemas = schemas_lock.read().await;
    let schema = schemas
        .get(schema_name)
        .ok_or_else(|| format!("schema '{schema_name}' not registered"))?;
    serde_json::to_string(schema).map_err(|e| format!("DDL snapshot serialization failed: {e}"))
}

// --------------------------------------------------------------- boot replay

/// Restore runtime registrations after `initialize_global_schema`.
///
/// Call exactly once, before the listener binds (single site: `run_server`).
/// No-op when the flag is unset, or when the server mode has no ClickHouse
/// client — the guaranteed behavior there is today's behavior.
pub async fn replay(app_state: &super::AppState, config: &ServerConfig) {
    let store = SchemaStore::from_parts(
        config.schema_persistence,
        app_state.clickhouse_client.clone(),
    );
    if matches!(store, SchemaStore::Off) {
        return;
    }

    let rows = match store.load_oldest_first().await {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!(
                "schema persistence: could not read {REGISTRY_TABLE} ({e}) — \
                 booting without restoring runtime registrations"
            );
            return;
        }
    };
    if rows.is_empty() {
        return;
    }

    // Names the config file already satisfied this boot; the replay restores
    // through the *same* load path a live registration uses, so alias and
    // cache semantics are identical by construction.
    let configured_names = graph_catalog::list_available_schemas().await;
    let (to_restore, shadowed) = partition_for_replay(rows, &configured_names);
    for row in &shadowed {
        log::info!(
            "config-defined schema '{}' shadows a persisted registration; \
             the config file wins (edit the file to change the schema)",
            row.name
        );
    }

    let mut restored = 0usize;
    let mut failed_names: Vec<&str> = Vec::new();
    for row in &to_restore {
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
            FORMAT_GRAPH_SCHEMA_JSON => restore_graph_schema_json(&row.name, &row.payload).await,
            other => Err(format!("unknown registry format '{other}'")),
        };
        match outcome {
            Ok(()) => restored += 1,
            Err(e) => {
                failed_names.push(&row.name);
                log::warn!(
                    "persisted schema '{}' could not be restored ({e}) — \
                     register it again, or drop it with DELETE /schemas/{}",
                    row.name,
                    row.name
                );
            }
        }
    }
    log::info!(
        "schema persistence: {} restored / {} config-shadowed / {} failed \
         (failed: {:?})",
        restored,
        shadowed.len(),
        failed_names.len(),
        failed_names
    );
}

/// Restore a `graph-schema-json` row without the live-ClickHouse re-discovery
/// the YAML rows get: the DDL path has no YAML to re-derive from, so this
/// takes the snapshot at its word — the same trade the legacy
/// `get_graph_catalog` fallback makes. The schema is registered before the
/// query cache is invalidated so stale SQL for a re-created table can never
/// outlive the boot.
async fn restore_graph_schema_json(name: &str, json: &str) -> Result<(), String> {
    let mut schema: crate::graph_catalog::graph_schema::GraphSchema =
        serde_json::from_str(json).map_err(|e| format!("snapshot parse failed: {e}"))?;
    schema.rebuild_derived_indexes();

    let schemas_lock = super::GLOBAL_SCHEMAS
        .get()
        .ok_or_else(|| "GLOBAL_SCHEMAS not initialized".to_string())?;
    let mut schemas = schemas_lock.write().await;
    schemas.insert(name.to_string(), schema);
    drop(schemas);

    if let Some(cache) = super::GLOBAL_QUERY_CACHE.get() {
        cache.invalidate_schema(name);
    }
    Ok(())
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
            source: SOURCE_DDL.to_string(),
            format: FORMAT_GRAPH_SCHEMA_JSON.to_string(),
            payload: "{}".to_string(),
            registry_version: 1,
            registered_at_ms: 42,
        };
        let json = serde_json::to_string(&row).unwrap();
        let back: Registration = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "a'b\"c`d");
        assert_eq!(back.registered_at_ms, 42);
    }
}
