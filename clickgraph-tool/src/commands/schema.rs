use anyhow::{anyhow, Result};
use clickgraph::graph_catalog::{
    config::GraphSchemaConfig,
    llm_prompt, merge_batch_yaml,
    node_property_check::{several_values_sql, Sampling},
    schema_discovery::{IntrospectResponse, SchemaDiscovery},
};
use tokio::io::AsyncBufReadExt;

use crate::{
    commands::query::open_for_translation,
    config::CgConfig,
    llm::{extract_yaml, LlmClient},
    schema_fmt, DialectArg,
};

/// `cg schema show` — print the loaded schema in a compact, agent-friendly format.
pub fn run_show(format: &str, cfg: &CgConfig) -> Result<()> {
    // The Database owns a runtime, so it is opened and dropped off the async worker
    let text = tokio::task::block_in_place(|| -> Result<String> {
        let db = open_for_translation(cfg)?;
        Ok(match format {
            "json" => {
                let json = schema_fmt::format_json(db.schema());
                format!("{}\n", serde_json::to_string_pretty(&json)?)
            }
            _ => schema_fmt::format_text(db.schema()),
        })
    })?;
    print!("{}", text);
    Ok(())
}

/// `cg schema validate <file>` — structural validation without ClickHouse.
pub fn run_validate_schema(path: &str) -> Result<()> {
    let config = GraphSchemaConfig::from_yaml_file(path)
        .map_err(|e| anyhow!("YAML parse error in '{}': {}", path, e))?;

    config
        .validate()
        .map_err(|e| anyhow!("Schema validation failed: {}", e))?;

    // Also try building the GraphSchema to catch type inference errors.
    // Without a connection, columns that auto_discover_columns would add are
    // not read, so only the declared mappings are checked.
    config
        .to_graph_schema_declared_only()
        .map_err(|e| anyhow!("Schema build error: {}", e))?;

    println!("OK — schema '{}' is valid.", path);
    let rel_count = config.graph_schema.relationships.len() + config.graph_schema.edges.len();
    println!(
        "  {} node label(s), {} relationship type(s)",
        config.graph_schema.nodes.len(),
        rel_count
    );
    for target in config.column_discovery_targets() {
        println!(
            "  note: {} discovers the columns of `{}`.`{}` at load; they were not checked",
            target.owner, target.database, target.table
        );
    }
    Ok(())
}

/// `cg schema validate --data` — check, in ClickHouse, that the rows of
/// each node embedded in edge tables hold one value of each property per
/// node id (#531). A query reads any of such a property's values, or fails
/// with `check_node_properties`; this finds such properties first. Sampled,
/// it reads part of the data, so it can miss an id; it never reports a
/// consistent one. The schema is read as the server reads it (engines for
/// FINAL, discovered columns).
pub async fn run_validate_data(
    path: &str,
    sampling: Option<Sampling>,
    cfg: &CgConfig,
) -> Result<()> {
    if matches!(cfg.dialect, DialectArg::Databricks) {
        return Err(anyhow!("--data checks ClickHouse only"));
    }
    let url = cfg.clickhouse_url.clone().ok_or_else(|| {
        anyhow!("--data reads ClickHouse: pass --clickhouse (or CG_CLICKHOUSE_URL)")
    })?;
    let config = GraphSchemaConfig::from_yaml_file(path)
        .map_err(|e| anyhow!("YAML parse error in '{}': {}", path, e))?;
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_user(&cfg.ch_user)
        .with_password(&cfg.ch_password);
    let schema = config
        .to_graph_schema_with_client(&client)
        .await
        .map_err(|e| anyhow!("Schema build error: {}", e))?;
    // The schema the lowering reads (`GraphSchema::lowering_view`).
    let Some(schema) = schema
        .lowering_view()
        .filter(|s| s.node_relations().next().is_some())
    else {
        println!("data: no node is embedded in edge tables; nothing to check");
        return Ok(());
    };
    match &sampling {
        Some(s) => println!(
            "data: checking one node id in {} (at most {} rows of each table)",
            s.one_in, s.max_rows
        ),
        None => println!("data: checking every node id"),
    }
    let mut failed = Vec::new();
    let mut errors = Vec::new();
    for (_, relation) in schema.node_relations() {
        let label = &relation.label;
        let sql = match several_values_sql(relation, sampling.as_ref()) {
            Ok(Some(sql)) => sql,
            Ok(None) => {
                println!("  :{label} has no property besides its id; nothing to check");
                continue;
            }
            Err(skip) => {
                println!("  :{label} not checked: {skip}");
                continue;
            }
        };
        let (checked, found) = match several_values(&client, &sql).await {
            Ok(r) => r,
            Err(e) => {
                println!("  :{label} not checked: {e}");
                errors.push(format!(":{label}"));
                continue;
            }
        };
        match (found.is_empty(), checked, &sampling) {
            (true, 0, Some(_)) => println!(
                "  :{label} not checked: the sample holds no node id (use --sample 1 or --full)"
            ),
            (true, 0, None) => println!("  :{label} has no node; nothing to check"),
            (true, _, _) => println!("  :{label} OK ({checked} node id(s) checked)"),
            _ => {}
        }
        for (property, ids, id) in found {
            println!(
                "  :{label} property {property}: the rows of {ids} node id(s) hold several values (e.g. id {id})"
            );
            failed.push(format!(":{label}.{property}"));
        }
    }
    if !failed.is_empty() {
        return Err(anyhow!(
            "a query reading {} reads any one of the values (an error with \
             --check-node-properties / CLICKGRAPH_CHECK_NODE_PROPERTIES): the property is \
             not the node's (declare it on the relationship), or the node's id columns do \
             not identify it",
            failed.join(", ")
        ));
    }
    if !errors.is_empty() {
        return Err(anyhow!("ClickHouse failed checking {}", errors.join(", ")));
    }
    Ok(())
}

/// Runs a [`several_values_sql`] query: how many ids it checked, and each
/// property the rows of some id disagree on (how many ids, one of them).
async fn several_values(
    client: &clickhouse::Client,
    sql: &str,
) -> Result<(u64, Vec<(String, u64, String)>)> {
    // The client reads `?` as a bound argument (`??` is a `?`).
    let mut lines = client
        .query(&sql.replace('?', "??"))
        .fetch_bytes("JSONEachRow")?
        .lines();
    let mut found = Vec::new();
    let mut checked = 0;
    while let Some(line) = lines.next_line().await? {
        let row: serde_json::Value = serde_json::from_str(&line)?;
        let (Some(property), Some(ids), Some(id)) = (
            row["property"].as_str(),
            row["ids"].as_u64(),
            row["id"].as_str(),
        ) else {
            return Err(anyhow!("unexpected row {line}"));
        };
        match property.is_empty() {
            true => checked = ids,
            false => found.push((property.to_string(), ids, id.to_string())),
        }
    }
    Ok((checked, found))
}

/// `cg schema discover` — LLM-assisted schema discovery.
///
/// Branches on `cfg.dialect`:
/// - **ClickHouse** (default): introspects via `system.tables`/`system.columns`
///   through a `clickhouse::Client`.
/// - **Databricks** (requires the `databricks` build feature on `clickgraph-tool`):
///   introspects via `SHOW TABLES IN catalog.schema` + `DESCRIBE TABLE EXTENDED`
///   through the same `DatabricksSqlExecutor` `cg query --dialect databricks`
///   uses. The `--catalog` flag (or `CG_DATABRICKS_CATALOG` / `DATABRICKS_CATALOG`
///   env per the precedence in `config.rs`) supplies the catalog; `--database`
///   reuses the existing flag as the Databricks "schema" name (Spark's two-level
///   naming inside a catalog).
pub async fn run_discover(
    database: &str,
    catalog: Option<&str>,
    ch_url: &str,
    user: &str,
    password: &str,
    out: Option<&str>,
    cfg: &CgConfig,
) -> Result<()> {
    // Discovery prompts feed downstream LLM batches and care only about the
    // table/column listing — both backends produce the same `IntrospectResponse`
    // shape, so a single LLM path follows.
    let (db_label, introspect) = match cfg.dialect {
        DialectArg::Databricks => run_introspect_databricks(catalog, database, cfg).await?,
        DialectArg::Clickhouse => {
            let ch_client = clickhouse::Client::default()
                .with_url(ch_url)
                .with_user(user)
                .with_password(password);
            eprintln!("Introspecting ClickHouse database '{}'...", database);
            let resp = SchemaDiscovery::introspect(&ch_client, database)
                .await
                .map_err(|e| anyhow!("Introspection failed: {}", e))?;
            (database.to_string(), resp)
        }
    };

    eprintln!(
        "Found {} table(s). Generating schema with LLM...",
        introspect.tables.len()
    );

    // Format the discovery prompt(s)
    let prompt_response = llm_prompt::format_discovery_prompt(&db_label, &introspect.tables);

    let llm = LlmClient::from_config(&cfg.llm)?;
    eprintln!("Using {} model: {}", provider_name(&llm), llm.model);

    let mut all_yaml = String::new();

    for (i, prompt) in prompt_response.prompts.iter().enumerate() {
        if prompt_response.prompts.len() > 1 {
            eprintln!(
                "Processing batch {}/{} ({} tables)...",
                i + 1,
                prompt_response.prompts.len(),
                prompt.table_count
            );
        }

        let result = llm.call(&prompt.system_prompt, &prompt.user_prompt).await?;
        let yaml = extract_yaml(&result);

        if i == 0 {
            all_yaml.push_str(&yaml);
            all_yaml.push('\n');
        } else {
            // Continuation batch: merge nodes/edges into the first batch
            all_yaml = merge_batch_yaml(&all_yaml, &yaml);
        }
    }

    // Write or print
    if let Some(path) = out {
        std::fs::write(path, &all_yaml)
            .map_err(|e| anyhow!("Failed to write '{}': {}", path, e))?;
        eprintln!("Schema written to '{}'.", path);
        eprintln!("Validate with: cg schema validate {}", path);
    } else {
        print!("{}", all_yaml);
    }

    Ok(())
}

/// `cg schema diff <old> <new>` — show the diff between two schema files.
pub fn run_diff(old_path: &str, new_path: &str) -> Result<()> {
    let old_cfg = GraphSchemaConfig::from_yaml_file(old_path)
        .map_err(|e| anyhow!("Failed to load '{}': {}", old_path, e))?;
    let new_cfg = GraphSchemaConfig::from_yaml_file(new_path)
        .map_err(|e| anyhow!("Failed to load '{}': {}", new_path, e))?;

    let old_nodes: std::collections::BTreeSet<String> = old_cfg
        .graph_schema
        .nodes
        .iter()
        .map(|n| n.label.clone())
        .collect();
    let new_nodes: std::collections::BTreeSet<String> = new_cfg
        .graph_schema
        .nodes
        .iter()
        .map(|n| n.label.clone())
        .collect();

    let old_rels: std::collections::BTreeSet<String> = old_cfg
        .graph_schema
        .relationships
        .iter()
        .map(|r| r.type_name.clone())
        .chain(old_cfg.graph_schema.edges.iter().filter_map(edge_type_name))
        .collect();
    let new_rels: std::collections::BTreeSet<String> = new_cfg
        .graph_schema
        .relationships
        .iter()
        .map(|r| r.type_name.clone())
        .chain(new_cfg.graph_schema.edges.iter().filter_map(edge_type_name))
        .collect();

    let mut any_diff = false;

    for label in new_nodes.difference(&old_nodes) {
        println!("+ Node label: {}", label);
        any_diff = true;
    }
    for label in old_nodes.difference(&new_nodes) {
        println!("- Node label: {}", label);
        any_diff = true;
    }
    for rel in new_rels.difference(&old_rels) {
        println!("+ Relationship: {}", rel);
        any_diff = true;
    }
    for rel in old_rels.difference(&new_rels) {
        println!("- Relationship: {}", rel);
        any_diff = true;
    }

    // Property-level diff for nodes present in both
    for label in old_nodes.intersection(&new_nodes) {
        let old_node = old_cfg
            .graph_schema
            .nodes
            .iter()
            .find(|n| &n.label == label);
        let new_node = new_cfg
            .graph_schema
            .nodes
            .iter()
            .find(|n| &n.label == label);
        if let (Some(old), Some(new)) = (old_node, new_node) {
            let old_props: std::collections::BTreeSet<String> =
                old.properties.keys().cloned().collect();
            let new_props: std::collections::BTreeSet<String> =
                new.properties.keys().cloned().collect();
            for p in new_props.difference(&old_props) {
                println!("+ {}  .{}", label, p);
                any_diff = true;
            }
            for p in old_props.difference(&new_props) {
                println!("- {}  .{}", label, p);
                any_diff = true;
            }
        }
    }

    if !any_diff {
        println!("No differences found.");
    }

    Ok(())
}

fn edge_type_name(e: &clickgraph::graph_catalog::config::EdgeDefinition) -> Option<String> {
    use clickgraph::graph_catalog::config::EdgeDefinition;
    match e {
        EdgeDefinition::Standard(s) => Some(s.type_name.clone()),
        EdgeDefinition::Polymorphic(_) => None, // polymorphic edges don't have a fixed type name
    }
}

fn provider_name(llm: &LlmClient) -> &str {
    match llm.provider {
        crate::llm::LlmProvider::Anthropic => "Anthropic",
        crate::llm::LlmProvider::OpenAI => "OpenAI-compatible",
    }
}

// ── Databricks introspect path (Phase 3) ─────────────────────────────────────
//
// Feature-gated mirror of the ClickHouse path in `run_discover`. With the
// feature off, the binary still compiles and `cg schema discover --dialect
// databricks` exits with a clear rebuild error rather than a confusing
// reqwest/connection trace.

#[cfg(feature = "databricks")]
async fn run_introspect_databricks(
    catalog: Option<&str>,
    schema: &str,
    cfg: &CgConfig,
) -> Result<(String, IntrospectResponse)> {
    use clickgraph::executor::databricks_sql::{DatabricksConfig, DatabricksSqlExecutor};
    use clickgraph::graph_catalog::databricks_probe::DatabricksProbe;

    // Catalog precedence: CLI flag > CG_DATABRICKS_CATALOG / DATABRICKS_CATALOG
    // > config.toml > schema YAML's top-level `catalog:` (Phase 3.2). Errors
    // out up front rather than letting `SHOW TABLES IN` fail with a less-clear
    // message from the warehouse.
    let catalog = if let Some(c) = catalog
        .map(str::to_string)
        .or_else(|| cfg.databricks.catalog.clone())
    {
        c
    } else if let Some(c) = schema_catalog_from_yaml(cfg)? {
        c
    } else {
        return Err(anyhow!(
            "Databricks catalog not set. Pass --catalog, set \
             DATABRICKS_CATALOG / CG_DATABRICKS_CATALOG, or add a \
             top-level `catalog:` field to the schema YAML."
        ));
    };
    let host = cfg.databricks.hostname.as_deref().ok_or_else(|| {
        anyhow!("DATABRICKS_HOST not set — see `cg schema discover --help` for env vars.")
    })?;
    let warehouse_id = cfg
        .databricks
        .warehouse_id
        .as_deref()
        .ok_or_else(|| anyhow!("DATABRICKS_WAREHOUSE_ID not set."))?;
    let token = cfg.databricks.token.as_deref().ok_or_else(|| {
        anyhow!(
            "DATABRICKS_TOKEN not set — provide it via env or [databricks].token in config.toml \
             (env-only; never accepted on the command line)."
        )
    })?;

    let mut dbc = DatabricksConfig::new(host, warehouse_id, token);
    dbc.catalog = Some(catalog.clone());
    dbc.schema = Some(schema.to_string());
    dbc.base_url = cfg.databricks.base_url.clone();

    let executor = DatabricksSqlExecutor::new(dbc)
        .map_err(|e| anyhow!("Failed to open Databricks executor: {e}"))?;

    let db_label = format!("{catalog}.{schema}");
    eprintln!("Introspecting Databricks namespace '{db_label}'...");
    let resp = DatabricksProbe::introspect(&executor, &catalog, schema)
        .await
        .map_err(|e| anyhow!("Databricks introspection failed: {e}"))?;
    Ok((db_label, resp))
}

#[cfg(not(feature = "databricks"))]
async fn run_introspect_databricks(
    _catalog: Option<&str>,
    _schema: &str,
    _cfg: &CgConfig,
) -> Result<(String, IntrospectResponse)> {
    Err(anyhow!(
        "`cg schema discover --dialect databricks` requires the `databricks` build feature. \
         Rebuild cg with `cargo install clickgraph-tool --features databricks` \
         (or use `--dialect clickhouse` against a ClickHouse staging copy)."
    ))
}

/// Read the optional top-level `catalog:` field from the loaded schema
/// YAML (DeltaGraph Phase 3.2).
///
/// Returns:
/// - `Ok(None)` if `--schema` wasn't supplied (no YAML to consult).
/// - `Ok(Some(catalog))` if the YAML parsed and carried a `catalog:`.
/// - `Ok(None)` if the YAML parsed but had no `catalog:` field.
/// - `Err(...)` if `--schema` *was* supplied but the file is missing
///   or doesn't parse. We surface the underlying error rather than
///   swallow it — otherwise a typo'd path would silently degrade to
///   "Databricks catalog not set" and the user would never realize
///   their YAML was the problem.
#[cfg(feature = "databricks")]
fn schema_catalog_from_yaml(cfg: &CgConfig) -> Result<Option<String>> {
    let Some(path) = cfg.schema_path.as_deref() else {
        return Ok(None);
    };
    let config = GraphSchemaConfig::from_yaml_file(path).map_err(|e| {
        anyhow!(
            "Failed to read schema YAML '{path}' while resolving Databricks catalog \
             fallback: {e}"
        )
    })?;
    Ok(config.catalog)
}
