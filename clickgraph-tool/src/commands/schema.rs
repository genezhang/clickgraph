use anyhow::{anyhow, Result};
use clickgraph::clickhouse_query_generator::qualified_column;
use clickgraph::graph_catalog::{
    config::GraphSchemaConfig,
    graph_schema::NodeRelation,
    llm_prompt, merge_batch_yaml,
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

/// How much of the data `cg schema validate --data` reads (all of it when
/// absent).
pub struct Sampling {
    /// One node id in `one_in`, by a hash of its text: all rows of each id
    /// it takes, so that two rows of one id disagreeing are seen together.
    pub one_in: u64,
    /// At most this many rows of each table.
    pub max_rows: u64,
}

/// `cg schema validate --data` — check, in ClickHouse, that the rows of
/// each node embedded in edge tables hold one value of each property per
/// node id (#531). A query reading a property whose rows disagree is an
/// error; this finds such properties before a query does. Sampled, it reads
/// part of the data, so it can miss an id; it never reports a consistent
/// one.
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
    let schema = config
        .to_graph_schema_declared_only()
        .map_err(|e| anyhow!("Schema build error: {}", e))?;
    let Some(schema) = schema.with_node_relations() else {
        println!("data: no node is embedded in edge tables; nothing to check");
        return Ok(());
    };
    let client = clickhouse::Client::default()
        .with_url(url)
        .with_user(&cfg.ch_user)
        .with_password(&cfg.ch_password);
    match &sampling {
        Some(s) => println!(
            "data: checking one node id in {} (at most {} rows of each table)",
            s.one_in, s.max_rows
        ),
        None => println!("data: checking every node id"),
    }
    let mut failed = Vec::new();
    for (_, relation) in schema.node_relations() {
        let sql = match several_values_sql(relation, sampling.as_ref()) {
            Ok(Some(sql)) => sql,
            Ok(None) => continue,
            Err(skip) => {
                println!("  :{} not checked: {}", relation.label, skip);
                continue;
            }
        };
        let mut lines = client
            .query(&sql)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| anyhow!(":{}: {}", relation.label, e))?
            .lines();
        let mut found = Vec::new();
        let mut checked = 0;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| anyhow!(":{}: {}", relation.label, e))?
        {
            let row: serde_json::Value = serde_json::from_str(&line)?;
            let property = row["property"].as_str().unwrap_or_default().to_string();
            let ids = row["ids"].as_u64().unwrap_or_default();
            let id = row["id"].as_str().unwrap_or_default().to_string();
            match property.is_empty() {
                true => checked = ids,
                false => found.push((property, ids, id)),
            }
        }
        if found.is_empty() {
            println!("  :{} OK ({} node id(s) checked)", relation.label, checked);
        }
        for (property, ids, id) in found {
            println!(
                "  :{} property {}: the rows of {} node id(s) hold several values (e.g. id {})",
                relation.label, property, ids, id
            );
            failed.push(format!(":{}.{}", relation.label, property));
        }
    }
    if !failed.is_empty() {
        return Err(anyhow!(
            "a query reading {} is an error: the property is not the node's \
             (declare it on the relationship), or the node's id columns do not identify it",
            failed.join(", ")
        ));
    }
    Ok(())
}

/// The ClickHouse query listing each property of `relation` that the rows
/// of some node id hold several values of: `property`, how many ids, one
/// of them (`id`, the text of its tuple); and, as the property `''`, how
/// many ids were checked. `None` when the relation has no
/// property besides its id; an error (the reason it is not checked) when a
/// source reads a parameterized view.
///
/// Each source's rows (its `filter:` and FINAL) give the id, as text (the
/// sources' id columns may differ in type), and per property the hash of
/// its value (a source without it: none); the ids are grouped, counting up
/// to two distinct hashes per property, as the lowering's
/// `FunctionMapper::one_value` does.
fn several_values_sql(
    relation: &NodeRelation,
    sampling: Option<&Sampling>,
) -> std::result::Result<Option<String>, String> {
    let properties: Vec<&String> = relation
        .properties
        .iter()
        .filter(|p| !relation.id.contains(p))
        .collect();
    if properties.is_empty() {
        return Ok(None);
    }
    let mut arms = Vec::new();
    for source in &relation.sources {
        if source.view_parameters.is_some() {
            return Err(format!("{} is a parameterized view", source.table));
        }
        let column = |p: &str| {
            source
                .columns
                .iter()
                .find(|(q, _)| q == p)
                .map(|(_, c)| qualified_column("e", c))
        };
        let mut ids = Vec::new();
        for p in &relation.id {
            let c =
                column(p).ok_or_else(|| format!("a source in {} without its id", source.table))?;
            ids.push(c);
        }
        let texts: Vec<String> = ids.iter().map(|c| format!("toString({c})")).collect();
        let mut items = vec![format!("tuple({}) AS id", texts.join(", "))];
        for (j, p) in properties.iter().enumerate() {
            match column(p) {
                Some(c) => items.push(format!("if(isNull({c}), NULL, cityHash64({c})) AS h{j}")),
                None => items.push(format!("CAST(NULL, 'Nullable(UInt64)') AS h{j}")),
            }
        }
        let mut filters: Vec<String> = ids.iter().map(|c| format!("NOT isNull({c})")).collect();
        if let Some(f) = &source.filter {
            filters.push(format!("({})", f.to_sql("e")?));
        }
        let mut limit = String::new();
        if let Some(s) = sampling {
            filters.push(format!(
                "cityHash64({}) % {} = 0",
                texts.join(", "),
                s.one_in
            ));
            limit = format!(" LIMIT {}", s.max_rows);
        }
        let fin = if source.use_final { " FINAL" } else { "" };
        arms.push(format!(
            "SELECT {} FROM {} AS e{fin} WHERE {}{limit}",
            items.join(", "),
            source.table,
            filters.join(" AND ")
        ));
    }
    let counts: Vec<String> = (0..properties.len())
        .map(|j| format!("uniqUpTo(1)(h{j}) > 1"))
        .collect();
    let names: Vec<String> = properties
        .iter()
        .map(|p| format!("'{}'", p.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect();
    Ok(Some(format!(
        "SELECT property, count() AS ids, toString(any(id)) AS id FROM (\
         SELECT id, arrayJoin(arrayConcat([''], arrayFilter((p, several) -> several, [{}], [{}]))) \
         AS property FROM ({}) GROUP BY id) GROUP BY property ORDER BY property",
        names.join(", "),
        counts.join(", "),
        arms.join(" UNION ALL ")
    )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use clickgraph::graph_catalog::{filter_parser::SchemaFilter, graph_schema::NodeSource};

    fn source(table: &str, columns: &[(&str, &str)]) -> NodeSource {
        NodeSource {
            table: table.to_string(),
            view_parameters: None,
            use_final: false,
            filter: None,
            columns: columns
                .iter()
                .map(|(p, c)| (p.to_string(), c.to_string()))
                .collect(),
        }
    }

    fn airport(sources: Vec<NodeSource>) -> NodeRelation {
        NodeRelation {
            label: "Airport".to_string(),
            id: vec!["code".to_string()],
            properties: vec!["city".to_string(), "code".to_string(), "tz".to_string()],
            sources,
        }
    }

    /// #531: each source's rows give the id's text and each property's
    /// hash (none where the source does not hold it); sampled, one id in N
    /// by its text's hash, every row of it, at most M rows of each table.
    #[test]
    fn several_values_query_reads_each_source() {
        let mut dst = source("db.fl", &[("code", "dst"), ("city", "dst_city")]);
        dst.use_final = true;
        dst.filter = Some(SchemaFilter::new("carrier = 'AA'").unwrap());
        let relation = airport(vec![
            source(
                "db.fl",
                &[("code", "org"), ("city", "org_city"), ("tz", "org_tz")],
            ),
            dst,
        ]);
        let sampling = Sampling {
            one_in: 10,
            max_rows: 500,
        };
        let sql = several_values_sql(&relation, Some(&sampling))
            .unwrap()
            .unwrap();
        for part in [
            "SELECT tuple(toString(e.org)) AS id, if(isNull(e.org_city), NULL, cityHash64(e.org_city)) AS h0, \
             if(isNull(e.org_tz), NULL, cityHash64(e.org_tz)) AS h1 FROM db.fl AS e \
             WHERE NOT isNull(e.org) AND cityHash64(toString(e.org)) % 10 = 0 LIMIT 500",
            "CAST(NULL, 'Nullable(UInt64)') AS h1 FROM db.fl AS e FINAL WHERE NOT isNull(e.dst) AND (",
            "['city', 'tz'], [uniqUpTo(1)(h0) > 1, uniqUpTo(1)(h1) > 1]",
            "arrayConcat([''],",
        ] {
            assert!(sql.contains(part), "missing `{part}` in\n{sql}");
        }
        let full = several_values_sql(&relation, None).unwrap().unwrap();
        assert!(!full.contains(" LIMIT "), "{full}");
        assert!(!full.contains("% "), "{full}");
    }

    #[test]
    fn several_values_query_skips_what_it_cannot_read() {
        // Only an id: nothing to check.
        let relation = NodeRelation {
            properties: vec!["code".to_string()],
            ..airport(vec![source("db.fl", &[("code", "org")])])
        };
        assert!(several_values_sql(&relation, None).unwrap().is_none());
        // A parameterized view: not read.
        let mut s = source("db.v", &[("code", "org"), ("city", "c")]);
        s.view_parameters = Some(vec!["tenant".to_string()]);
        let err = several_values_sql(&airport(vec![s]), None).unwrap_err();
        assert!(err.contains("parameterized view"), "{err}");
    }
}
