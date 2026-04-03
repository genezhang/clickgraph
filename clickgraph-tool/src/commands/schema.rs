use anyhow::{anyhow, Result};
use clickgraph::graph_catalog::{
    config::GraphSchemaConfig, llm_prompt, schema_discovery::SchemaDiscovery,
};

use crate::{
    config::CgConfig,
    llm::{extract_yaml, LlmClient},
    schema_fmt,
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

/// `cg schema discover` — LLM-assisted schema discovery from ClickHouse.
pub async fn run_discover(
    database: &str,
    ch_url: &str,
    user: &str,
    password: &str,
    out: Option<&str>,
    cfg: &CgConfig,
) -> Result<()> {
    // Build a clickhouse client directly
    let ch_client = clickhouse::Client::default()
        .with_url(ch_url)
        .with_user(user)
        .with_password(password);

    eprintln!("Introspecting database '{}'...", database);
    let introspect = SchemaDiscovery::introspect(&ch_client, database)
        .await
        .map_err(|e| anyhow!("Introspection failed: {}", e))?;

    eprintln!(
        "Found {} table(s). Generating schema with LLM...",
        introspect.tables.len()
    );

    // Format the discovery prompt(s)
    let prompt_response = llm_prompt::format_discovery_prompt(database, &introspect.tables);

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

// ── Batch YAML merging (adapted from clickgraph-client) ──────────────────────

fn merge_batch_yaml(base: &str, continuation: &str) -> String {
    let cont_nodes = extract_yaml_list_items(continuation, "nodes:");
    let cont_edges = extract_yaml_list_items(continuation, "edges:");

    let mut result = base.trim_end().to_string();

    if !cont_nodes.is_empty() {
        if let Some(pos) = result
            .find("\n  edges:")
            .or_else(|| result.find("\nedges:"))
        {
            result.insert_str(pos, &format!("\n{}", cont_nodes));
        } else {
            result.push('\n');
            result.push_str(&cont_nodes);
        }
    }

    if !cont_edges.is_empty() {
        result.push('\n');
        result.push_str(&cont_edges);
    }

    result.push('\n');
    result
}

fn extract_yaml_list_items(yaml: &str, section: &str) -> String {
    let mut in_section = false;
    let mut items = String::new();

    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("nodes:") || trimmed.starts_with("edges:") {
            in_section = trimmed.starts_with(section.trim());
            continue;
        }
        if !line.starts_with(' ') && !line.is_empty() && !line.starts_with('#') {
            if in_section {
                break;
            }
            continue;
        }
        if in_section {
            items.push_str(line);
            items.push('\n');
        }
    }

    items.trim_end().to_string()
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
