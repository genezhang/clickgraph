use anyhow::{anyhow, Result};
use clickgraph::graph_catalog::{
    config::GraphSchemaConfig,
    llm_prompt, merge_batch_yaml,
    schema_discovery::{IntrospectResponse, SchemaDiscovery},
};

use crate::{
    config::CgConfig,
    llm::{extract_yaml, LlmClient},
    schema_fmt, DialectArg,
};

/// `cg schema show` — print the loaded schema in a compact, agent-friendly format.
pub fn run_show(format: &str, cfg: &CgConfig) -> Result<()> {
    let path = cfg.require_schema()?;
    let config = GraphSchemaConfig::from_yaml_file(path)
        .map_err(|e| anyhow!("Failed to load schema '{}': {}", path, e))?;
    let schema = config
        .to_graph_schema()
        .map_err(|e| anyhow!("Failed to build schema: {}", e))?;

    match format {
        "json" => {
            let json = schema_fmt::format_json(&schema);
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            print!("{}", schema_fmt::format_text(&schema));
        }
    }
    Ok(())
}

/// `cg schema validate <file>` — structural validation without ClickHouse.
pub fn run_validate_schema(path: &str) -> Result<()> {
    let config = GraphSchemaConfig::from_yaml_file(path)
        .map_err(|e| anyhow!("YAML parse error in '{}': {}", path, e))?;

    config
        .validate()
        .map_err(|e| anyhow!("Schema validation failed: {}", e))?;

    // Also try building the GraphSchema to catch type inference errors
    config
        .to_graph_schema()
        .map_err(|e| anyhow!("Schema build error: {}", e))?;

    println!("OK — schema '{}' is valid.", path);
    let rel_count = config.graph_schema.relationships.len() + config.graph_schema.edges.len();
    println!(
        "  {} node label(s), {} relationship type(s)",
        config.graph_schema.nodes.len(),
        rel_count
    );
    Ok(())
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
