mod llm;

use clap::Parser;
use reqwest::Client;
use rustyline::{error::ReadlineError, DefaultEditor};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:8080")]
    url: String,
}

fn print_usage() {
    println!("ClickGraph Client Commands:");
    println!("  <query>           - Execute Cypher query (default)");
    println!("  :discover <db>   - LLM-powered schema discovery (needs ANTHROPIC_API_KEY)");
    println!("  :introspect <db> - Show tables/columns in database");
    println!("  :design <db>     - Interactive schema design wizard");
    println!("  :schemas         - List loaded schemas");
    println!("  :load <file>     - Load schema from YAML file");
    println!("  :help            - Show this help");
    println!();
    println!("Examples:");
    println!("  :discover mydb");
    println!("  :introspect lineage");
    println!("  :schemas");
    println!("  MATCH (n:User) RETURN n.name LIMIT 5");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Client::new();

    println!("\nConnected to ClickGraph server at {}.", args.url);
    println!("Type :help for commands.\n");

    let mut rl = DefaultEditor::new()?;

    loop {
        let readline = rl.readline("clickgraph-client :) ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                rl.add_history_entry(input)?;

                if input.starts_with(':') {
                    let parts: Vec<&str> = input.splitn(2, ' ').collect();
                    let cmd = parts[0];
                    let arg = parts.get(1).map(|s| s.to_string());

                    match cmd {
                        ":help" | ":h" => {
                            print_usage();
                        }
                        ":discover" | ":disc" => {
                            if let Some(db) = arg {
                                match run_discover(&client, &args.url, &db, &mut rl).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("Error: {}", e);
                                    }
                                }
                            } else {
                                println!("Usage: :discover <database>");
                            }
                        }
                        ":introspect" | ":i" => {
                            if let Some(db) = arg {
                                match introspect_database(&client, &args.url, &db).await {
                                    Ok(response) => {
                                        print_introspect_result(&response);
                                    }
                                    Err(e) => {
                                        eprintln!("Error: {}", e);
                                    }
                                }
                            } else {
                                println!("Usage: :introspect <database>");
                            }
                        }
                        ":design" | ":d" => {
                            if let Some(db) = arg {
                                match run_design_wizard(&client, &args.url, &db, &mut rl).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("Design error: {}", e);
                                    }
                                }
                            } else {
                                println!("Usage: :design <database>");
                            }
                        }
                        ":schemas" | ":s" => match list_schemas(&client, &args.url).await {
                            Ok(response) => {
                                println!("\n=== Loaded Schemas ===\n");
                                if let Some(schemas) =
                                    response.get("schemas").and_then(|s| s.as_array())
                                {
                                    for schema in schemas {
                                        let name = schema
                                            .get("name")
                                            .and_then(|n| n.as_str())
                                            .unwrap_or("?");
                                        let nodes = schema
                                            .get("node_count")
                                            .and_then(|n| n.as_u64())
                                            .unwrap_or(0);
                                        let edges = schema
                                            .get("relationship_count")
                                            .and_then(|r| r.as_u64())
                                            .unwrap_or(0);
                                        println!("  {}: {} nodes, {} edges", name, nodes, edges);
                                    }
                                }
                                println!();
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                            }
                        },
                        ":load" => {
                            if let Some(file_path) = arg {
                                match load_schema_from_file(&client, &args.url, &file_path).await {
                                    Ok(response) => {
                                        println!(
                                            "\n{}\n",
                                            serde_json::to_string_pretty(&response)
                                                .unwrap_or_default()
                                        );
                                    }
                                    Err(e) => {
                                        eprintln!("Error: {}", e);
                                    }
                                }
                            } else {
                                println!("Usage: :load <file_path>");
                            }
                        }
                        _ => {
                            println!(
                                "Unknown command: {}. Type :help for available commands.",
                                cmd
                            );
                        }
                    }
                    continue;
                }

                // Regular Cypher query
                match run_query(&client, &args.url, input).await {
                    Ok(response) => {
                        print_query_result(&response);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("\nI'll be back:)");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("\nI'll be back:)");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

fn print_introspect_result(response: &Value) {
    let db = response
        .get("database")
        .and_then(|d| d.as_str())
        .unwrap_or("?");
    println!("\n=== Database: {} ===\n", db);

    if let Some(tables) = response.get("tables").and_then(|t| t.as_array()) {
        for table in tables {
            let name = table.get("name").and_then(|n| n.as_str()).unwrap_or("?");
            let row_count = table.get("row_count").and_then(|r| r.as_u64()).unwrap_or(0);
            print!("  {} ({} rows)", name, row_count);

            // Show columns
            if let Some(cols) = table.get("columns").and_then(|c| c.as_array()) {
                let pk_cols: Vec<_> = cols
                    .iter()
                    .filter(|c| {
                        c.get("is_primary_key")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .map(|c| c.get("name").and_then(|n| n.as_str()).unwrap_or("?"))
                    .collect();
                if !pk_cols.is_empty() {
                    print!(" [PK: {}]", pk_cols.join(", "));
                }
            }
            println!();
        }
    }
    println!();

    if let Some(suggestions) = response.get("suggestions").and_then(|s| s.as_array()) {
        if !suggestions.is_empty() {
            println!("=== Suggestions ===\n");
            let mut by_table: HashMap<&str, Vec<(&str, &str)>> = HashMap::new();
            for sugg in suggestions {
                let table = sugg.get("table").and_then(|t| t.as_str()).unwrap_or("?");
                let stype = sugg.get("type").and_then(|t| t.as_str()).unwrap_or("?");
                let reason = sugg.get("reason").and_then(|r| r.as_str()).unwrap_or("");
                by_table.entry(table).or_default().push((stype, reason));
            }
            for (table, suggestions) in by_table {
                println!("  {}:", table);
                for (stype, reason) in suggestions {
                    println!("    - [{}] {}", stype, reason);
                }
            }
            println!();
        }
    }
}

fn print_query_result(response: &Value) {
    if let Some(array) = response.as_array() {
        for item in array {
            if let Some(s) = item.as_str() {
                println!("\n{}\n", s);
            } else {
                println!("\n{}\n", item);
            }
        }
    } else {
        println!("\n{}\n", response);
    }
}

async fn run_discover(
    client: &Client,
    url: &str,
    database: &str,
    rl: &mut DefaultEditor,
) -> Result<(), String> {
    let llm_config = match llm::LlmConfig::from_env() {
        Some(config) => config,
        None => {
            println!("No LLM API key found. Falling back to :introspect.\n");
            println!("To use LLM-powered discovery, set one of:");
            println!("  ANTHROPIC_API_KEY  (default, uses Claude)");
            println!("  OPENAI_API_KEY + CLICKGRAPH_LLM_PROVIDER=openai");
            println!();
            let response = introspect_database(client, url, database).await?;
            print_introspect_result(&response);
            return Ok(());
        }
    };

    let provider_name = match llm_config.provider {
        llm::LlmProvider::Anthropic => "Anthropic",
        llm::LlmProvider::OpenAI => "OpenAI-compatible",
    };

    println!("\n=== LLM Schema Discovery for '{}' ===\n", database);
    println!("Using {} model: {}", provider_name, llm_config.model);
    println!("Fetching table metadata from server...\n");

    // Get discovery prompt from server
    let endpoint = format!("{}/schemas/discover-prompt", url);
    let payload = json!({ "database": database });

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(format!("Server error: {}", text));
    }

    let prompt_response: Value = response.json().await.map_err(|e| e.to_string())?;

    let prompts = prompt_response
        .get("prompts")
        .and_then(|p| p.as_array())
        .ok_or("Invalid response from server: missing prompts")?;

    let total_tables = prompt_response
        .get("total_tables")
        .and_then(|t| t.as_u64())
        .unwrap_or(0);

    println!(
        "Found {} tables. Sending {} prompt(s) to LLM...\n",
        total_tables,
        prompts.len()
    );

    // Process each prompt batch
    let mut all_yaml = String::new();
    for (i, prompt) in prompts.iter().enumerate() {
        let system = prompt
            .get("system_prompt")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let user = prompt
            .get("user_prompt")
            .and_then(|u| u.as_str())
            .unwrap_or("");
        let est_tokens = prompt
            .get("estimated_tokens")
            .and_then(|t| t.as_u64())
            .unwrap_or(0);

        if prompts.len() > 1 {
            println!(
                "  Batch {}/{} (~{} tokens)...",
                i + 1,
                prompts.len(),
                est_tokens
            );
        } else {
            println!("  Sending ~{} tokens...", est_tokens);
        }

        let result = llm::call_llm(client, &llm_config, system, user).await?;
        let yaml = llm::extract_yaml(&result);

        if i == 0 {
            // First batch: use the full YAML (includes name/version/graph_schema wrapper)
            all_yaml.push_str(&yaml);
            all_yaml.push('\n');
        } else {
            // Continuation batches: merge nodes/edges into the first batch's YAML
            all_yaml = merge_batch_yaml(&all_yaml, &yaml);
        }
    }

    println!("\n=== Generated Schema YAML ===\n");
    println!("{}", all_yaml);

    // Offer to save or load
    println!("What would you like to do?");
    println!("  [s] Save to file");
    println!("  [l] Load into server");
    println!("  [b] Both (save + load)");
    println!("  [n] Nothing (just review)");

    let readline = rl.readline("action> ");
    let action = readline.map_err(|e| e.to_string())?;

    match action.trim().to_lowercase().as_str() {
        "s" | "save" | "b" | "both" => {
            let default_path = format!("{}.yaml", database);
            let path_input = rl
                .readline(&format!("Save to [{}]> ", default_path))
                .map_err(|e| e.to_string())?;
            let path = if path_input.trim().is_empty() {
                default_path
            } else {
                path_input.trim().to_string()
            };

            std::fs::write(&path, &all_yaml).map_err(|e| format!("Failed to save: {}", e))?;
            println!("Saved to {}", path);

            if action.trim().to_lowercase().starts_with('b') {
                // Also load
                match load_schema_yaml(client, url, database, &all_yaml).await {
                    Ok(resp) => {
                        println!(
                            "Loaded: {}",
                            serde_json::to_string_pretty(&resp).unwrap_or_default()
                        );
                    }
                    Err(e) => {
                        eprintln!("Load error: {}", e);
                    }
                }
            }
        }
        "l" | "load" => match load_schema_yaml(client, url, database, &all_yaml).await {
            Ok(resp) => {
                println!(
                    "Loaded: {}",
                    serde_json::to_string_pretty(&resp).unwrap_or_default()
                );
            }
            Err(e) => {
                eprintln!("Load error: {}", e);
            }
        },
        _ => {
            println!("OK. You can save this YAML manually or use :load <file> later.");
        }
    }

    Ok(())
}

/// Merge continuation batch YAML into the base YAML.
/// Extracts nodes/edges sections from the continuation and appends them
/// to the corresponding sections in the base YAML.
fn merge_batch_yaml(base: &str, continuation: &str) -> String {
    // Extract nodes and edges entries from the continuation batch.
    // The continuation should contain bare `nodes:` and `edges:` arrays
    // (or partial graph_schema content). We find those sections and append
    // their items to the base YAML.

    let cont_nodes = extract_yaml_list_items(continuation, "nodes:");
    let cont_edges = extract_yaml_list_items(continuation, "edges:");

    let mut result = base.trim_end().to_string();

    // Find the last occurrence of "edges:" or "nodes:" to know where to insert.
    // Strategy: append nodes before edges section, append edges at the end.
    if !cont_nodes.is_empty() {
        // Find the edges: section in base and insert nodes before it
        if let Some(edges_pos) = result.find("\n  edges:") {
            result.insert_str(edges_pos, &format!("\n{}", cont_nodes));
        } else if let Some(edges_pos) = result.find("\nedges:") {
            result.insert_str(edges_pos, &format!("\n{}", cont_nodes));
        } else {
            // No edges section — just append nodes at the end
            result.push('\n');
            result.push_str(&cont_nodes);
        }
    }

    if !cont_edges.is_empty() {
        // Append edges at the very end
        result.push('\n');
        result.push_str(&cont_edges);
    }

    result.push('\n');
    result
}

/// Extract the list items (lines starting with "    - " or "  - ") under a given section header.
fn extract_yaml_list_items(yaml: &str, section: &str) -> String {
    let mut in_section = false;
    let mut items = String::new();

    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("nodes:") || trimmed.starts_with("edges:") {
            in_section = trimmed.starts_with(section.trim());
            continue;
        }
        // Stop if we hit another top-level key
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

async fn load_schema_yaml(
    client: &Client,
    url: &str,
    schema_name: &str,
    yaml_content: &str,
) -> Result<Value, String> {
    let endpoint = format!("{}/schemas/load", url);
    let payload = json!({
        "schema_name": schema_name,
        "config_content": yaml_content
    });

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        response.json().await.map_err(|e| e.to_string())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

async fn run_design_wizard(
    client: &Client,
    url: &str,
    database: &str,
    rl: &mut DefaultEditor,
) -> Result<(), String> {
    println!("\n=== Schema Design Wizard for '{}' ===\n", database);

    // Step 1: Introspect
    println!("Step 1: Discovering tables...\n");
    let response = introspect_database(client, url, database).await?;
    print_introspect_result(&response);

    // Collect tables info
    let tables: Vec<TableInfo> = response
        .get("tables")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    Some(TableInfo {
                        name: t.get("name")?.as_str()?.to_string(),
                        columns: t
                            .get("columns")
                            .and_then(|c| c.as_array())
                            .map(|cols| {
                                cols.iter()
                                    .filter_map(|c| {
                                        Some(ColumnInfo {
                                            name: c.get("name")?.as_str()?.to_string(),
                                            is_pk: c
                                                .get("is_primary_key")
                                                .and_then(|v| v.as_bool())
                                                .unwrap_or(false),
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let suggestions: Vec<SuggestionInfo> = response
        .get("suggestions")
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| {
                    Some(SuggestionInfo {
                        table: s.get("table").and_then(|t| t.as_str())?.to_string(),
                        stype: s.get("type").and_then(|t| t.as_str())?.to_string(),
                        reason: s.get("reason").and_then(|r| r.as_str())?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Step 2: Define nodes
    println!("Step 2: Define nodes");
    println!("  Enter table names to create nodes (comma-separated), or press Enter to skip:");
    println!("  Suggested: ");
    let node_suggestions: Vec<_> = suggestions
        .iter()
        .filter(|s| s.stype == "node_candidate")
        .collect();
    for s in &node_suggestions {
        print!("    {} ", s.table);
    }
    println!("\n");

    let readline = rl.readline("nodes> ");
    let nodes_input = readline.map_err(|e| e.to_string())?;

    let mut nodes: Vec<NodeHint> = Vec::new();
    if !nodes_input.trim().is_empty() {
        for part in nodes_input.split(',') {
            let table = part.trim();
            // Find PK for this table
            let table_info = tables.iter().find(|t| t.name == table);
            let node_id = table_info
                .and_then(|t| t.columns.iter().find(|c| c.is_pk))
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("{}_id", table));

            let label = to_label(table);
            let node_id_for_print = node_id.clone();
            nodes.push(NodeHint {
                table: table.to_string(),
                label,
                node_id,
            });
            println!(
                "  Added node: {} (label: {}, id: {})",
                table,
                to_label(table),
                node_id_for_print
            );
        }
    }
    println!();

    // Step 3: Define edges
    println!("Step 3: Define edges");
    println!("  Enter edges as: <table>:<type>:<from_node>:<to_node>:<from_id>:<to_id>");
    println!("  Example: user_follows:FOLLOWS:User:User:follower_id:followed_id");
    println!("  Suggested: ");
    let edge_suggestions: Vec<_> = suggestions
        .iter()
        .filter(|s| s.stype == "edge_candidate")
        .collect();
    for s in &edge_suggestions {
        println!("    {} ({})", s.table, s.reason);
    }
    println!("\n");

    let readline = rl.readline("edges> ");
    let edges_input = readline.map_err(|e| e.to_string())?;

    let mut edges: Vec<EdgeHint> = Vec::new();
    if !edges_input.trim().is_empty() {
        for part in edges_input.split(',') {
            let parts: Vec<&str> = part.trim().split(':').collect();
            if parts.len() >= 6 {
                edges.push(EdgeHint {
                    table: parts[0].to_string(),
                    edge_type: parts[1].to_string(),
                    from_node: parts[2].to_string(),
                    to_node: parts[3].to_string(),
                    from_id: parts[4].to_string(),
                    to_id: parts[5].to_string(),
                });
                println!(
                    "  Added edge: {} ({} {} -> {})",
                    parts[0], parts[1], parts[4], parts[5]
                );
            }
        }
    }
    println!();

    // Step 4: Define FK edges
    println!("Step 4: Define FK edges (edges that use node table as edge)");
    println!("  Enter as: <table>:<type>:<from_node>:<to_node>:<from_id>:<to_id>");
    println!("  Example: orders:PLACED_BY:Order:User:order_id:customer_id");
    println!("  Suggested: ");
    let fk_suggestions: Vec<_> = suggestions
        .iter()
        .filter(|s| s.stype == "fk_edge_candidate")
        .collect();
    for s in &fk_suggestions {
        println!("    {} ({})", s.table, s.reason);
    }
    println!("\n");

    let readline = rl.readline("fk_edges> ");
    let fk_input = readline.map_err(|e| e.to_string())?;

    let mut fk_edges: Vec<FkEdgeHint> = Vec::new();
    if !fk_input.trim().is_empty() {
        for part in fk_input.split(',') {
            let parts: Vec<&str> = part.trim().split(':').collect();
            if parts.len() >= 6 {
                fk_edges.push(FkEdgeHint {
                    table: parts[0].to_string(),
                    edge_type: parts[1].to_string(),
                    from_node: parts[2].to_string(),
                    to_node: parts[3].to_string(),
                    from_id: parts[4].to_string(),
                    to_id: parts[5].to_string(),
                });
                println!(
                    "  Added FK edge: {} ({} {} -> {})",
                    parts[0], parts[1], parts[4], parts[5]
                );
            }
        }
    }
    println!();

    // Step 5: Generate YAML
    println!("Step 5: Generating YAML...\n");

    let payload = json!({
        "database": database,
        "schema_name": database,
        "nodes": nodes,
        "edges": edges,
        "fk_edges": fk_edges,
        "options": { "auto_discover_columns": true }
    });

    let endpoint = format!("{}/schemas/draft", url);
    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        let result: Value = response.json().await.map_err(|e| e.to_string())?;
        if let Some(yaml) = result.get("yaml").and_then(|y| y.as_str()) {
            println!("=== Generated YAML ===\n");
            println!("{}", yaml);
            println!("\nTo load this schema, use :load command or POST to /schemas/load");
            println!("Or copy the YAML and load manually.\n");
        }
        Ok(())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

fn to_label(table: &str) -> String {
    // Singularize table name → PascalCase label
    // Only strip a single trailing 's' (not all), and handle common English patterns
    let singular = if table == "people" {
        "person".to_string()
    } else if table.ends_with("ies") && table.len() > 3 {
        // categories -> category, companies -> company
        format!("{}y", &table[..table.len() - 3])
    } else if table.ends_with("ses") || table.ends_with("xes") || table.ends_with("zes") {
        // addresses -> address, boxes -> box, quizzes -> quiz (approximate)
        table[..table.len() - 2].to_string()
    } else if table.ends_with('s')
        && !table.ends_with("ss")
        && !table.ends_with("us")
        && !table.ends_with("is")
        && table.len() > 2
    {
        // users -> user, orders -> order
        // but: boss (ends with ss) stays boss, status (ends with us) stays status
        table[..table.len() - 1].to_string()
    } else {
        table.to_string()
    };

    // PascalCase: split on underscores and capitalize each segment
    singular
        .split('_')
        .map(|seg| {
            let mut c = seg.chars();
            match c.next() {
                None => String::new(),
                Some(first) => {
                    let upper: String = first.to_uppercase().collect();
                    upper + c.as_str()
                }
            }
        })
        .collect()
}

async fn introspect_database(client: &Client, url: &str, database: &str) -> Result<Value, String> {
    let endpoint = format!("{}/schemas/introspect", url);
    let payload = json!({ "database": database });

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        response.json().await.map_err(|e| e.to_string())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

async fn run_query(client: &Client, url: &str, query: &str) -> Result<Value, String> {
    let endpoint = format!("{}/query", url);
    let payload = json!({ "query": query, "format": "PrettyCompact" });

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        response.json().await.map_err(|e| e.to_string())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

async fn list_schemas(client: &Client, url: &str) -> Result<Value, String> {
    let endpoint = format!("{}/schemas", url);

    let response = client
        .get(&endpoint)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        response.json().await.map_err(|e| e.to_string())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

async fn load_schema_from_file(
    client: &Client,
    url: &str,
    file_path: &str,
) -> Result<Value, String> {
    let content =
        std::fs::read_to_string(file_path).map_err(|e| format!("Failed to read file: {}", e))?;

    let endpoint = format!("{}/schemas/load", url);
    let schema_name = std::path::Path::new(file_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("schema")
        .to_string();

    let payload = json!({
        "schema_name": schema_name,
        "config_content": content
    });

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        response.json().await.map_err(|e| e.to_string())
    } else {
        let text = response.text().await.unwrap_or_default();
        Err(text)
    }
}

#[derive(Debug, Clone)]
struct TableInfo {
    name: String,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    is_pk: bool,
}

#[derive(Debug, Clone)]
struct SuggestionInfo {
    table: String,
    stype: String,
    reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct NodeHint {
    table: String,
    label: String,
    node_id: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct EdgeHint {
    table: String,
    #[serde(rename = "type")]
    edge_type: String,
    from_node: String,
    to_node: String,
    from_id: String,
    to_id: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct FkEdgeHint {
    table: String,
    #[serde(rename = "type")]
    edge_type: String,
    from_node: String,
    to_node: String,
    from_id: String,
    to_id: String,
}
