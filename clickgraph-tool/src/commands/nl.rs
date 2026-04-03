use anyhow::Result;

use crate::{
    commands::query::run_query,
    config::CgConfig,
    llm::{extract_code_block, LlmClient},
    schema_fmt,
};

const NL_SYSTEM_PROMPT: &str = r#"You are an expert in openCypher graph query language for ClickGraph.
ClickGraph is a read-only graph query engine that translates Cypher to ClickHouse SQL.

Rules:
- Only use MATCH, WHERE, WITH, RETURN, ORDER BY, LIMIT, SKIP, UNWIND, OPTIONAL MATCH
- No write operations (CREATE, SET, DELETE, MERGE are not supported)
- Use the exact node labels and relationship types from the schema
- Use the exact Cypher property names from the schema (not the ClickHouse column names)
- Variable-length paths use [*min..max] syntax, e.g. [:KNOWS*1..3]
- Output ONLY the Cypher query — no explanation, no markdown fences, no extra text"#;

/// `cg nl` — translate natural language to Cypher using LLM, then optionally execute.
pub async fn run_nl(description: &str, execute: bool, format: &str, cfg: &CgConfig) -> Result<()> {
    let llm = LlmClient::from_config(&cfg.llm)?;

    // Load schema for context
    let schema_context = if cfg.schema_path.is_some() {
        let schema = load_schema_for_nl(cfg)?;
        format!("Schema:\n{}", schema)
    } else {
        "No schema loaded. Generate a general Cypher query.".to_string()
    };

    let user_prompt = format!(
        "{}\n\nNatural language query: {}",
        schema_context, description
    );

    eprintln!("Calling {} ({})...", llm.model, provider_name(&llm));
    let response = llm.call(NL_SYSTEM_PROMPT, &user_prompt).await?;
    let cypher = extract_code_block(&response, "cypher");

    // Print the generated Cypher
    println!("-- Generated Cypher:");
    println!("{}", cypher);

    if execute {
        println!();
        println!("-- Executing...");
        run_query(&cypher, false, format, cfg).await?;
    }

    Ok(())
}

fn load_schema_for_nl(cfg: &CgConfig) -> Result<String> {
    use anyhow::anyhow;
    use clickgraph::graph_catalog::config::GraphSchemaConfig;

    let path = cfg.require_schema()?;
    let config = GraphSchemaConfig::from_yaml_file(path)
        .map_err(|e| anyhow!("Failed to load schema '{}': {}", path, e))?;
    let schema = config
        .to_graph_schema()
        .map_err(|e| anyhow!("Failed to build schema: {}", e))?;
    Ok(schema_fmt::format_text(&schema))
}

fn provider_name(llm: &LlmClient) -> &str {
    match llm.provider {
        crate::llm::LlmProvider::Anthropic => "Anthropic",
        crate::llm::LlmProvider::OpenAI => "OpenAI-compatible",
    }
}
