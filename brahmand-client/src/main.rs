use clap::Parser;
use reqwest::Client;
use rustyline::{DefaultEditor, error::ReadlineError};
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:8080")]
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Client::new();

    println!("\nConnected to brahamnd server at {}.\n", args.url);

    let mut rl = DefaultEditor::new()?;

    loop {
        let readline = rl.readline("clickgraph-client :) ");
        match readline {
            Ok(line) => {
                let query = line.trim();
                // Add non-empty lines to history.
                if !query.is_empty() {
                    rl.add_history_entry(query)?;
                } else {
                    continue;
                }

                // Send the query to the server.
                let payload = json!({ "query": query, "format": "PrettyCompact" });
                let endpoint = format!("{}/query", args.url);

                match client.post(&endpoint).json(&payload).send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            if let Some(ct) = response.headers().get("content-type") {
                                let ct = ct.to_str().unwrap_or("");
                                if ct.contains("application/json") {
                                    let json_value: Value = response.json().await?;
                                    if let Some(array) = json_value.as_array() {
                                        for item in array {
                                            if let Some(s) = item.as_str() {
                                                println!("\n{}\n", s);
                                            } else {
                                                println!("\n{}\n", item);
                                            }
                                        }
                                    } else {
                                        println!("\n{}\n", json_value);
                                    }
                                } else {
                                    // no json then string
                                    let text = response.text().await?;
                                    println!("\n{}\n", text);
                                }
                            } else {
                                // no content type fallback to string
                                let text = response.text().await?;
                                println!("\n{}\n", text);
                            }
                        } else {
                            println!("\n{:?}\n", response.text().await?);
                        }
                    }
                    Err(err) => {
                        eprintln!("\nRequest error: {}\n", err);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("I'll be back:)");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("I'll be back:)");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
