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
    println!("  :introspect <db> - Discover tables in database");
    println!("  :design <db>     - Interactive schema design wizard");
    println!("  :schemas         - List loaded schemas");
    println!("  :load <file>     - Load schema from YAML file");
    println!("  :help            - Show this help");
    println!("");
    println!("Examples:");
    println!("  :introspect lineage");
    println!("  :design lineage");
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
                                println!("");
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
            println!("");
        }
    }
    println!("");

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
            println!("");
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
    // Simple singularization: users -> User, orders -> Order
    let mut label = table.trim_end_matches('s').to_string();
    // Handle special cases
    if label == "people" {
        label = "Person".to_string();
    } else if label == "analysis" {
        // keep as is
    } else if label != table {
        // Capitalize first letter
        if let Some(c) = label.get(0..1) {
            label = c.to_uppercase() + &label[1..];
        }
    } else {
        label = table.to_string();
    }
    label
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
