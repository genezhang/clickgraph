use anyhow::{anyhow, Result};
use clickgraph_embedded::{
    connection::Connection,
    database::{Database, RemoteConfig},
    value::Value,
};

use crate::config::CgConfig;

/// `cg sql` — translate Cypher to SQL and print it.
pub fn run_sql(cypher: &str, cfg: &CgConfig) -> Result<()> {
    let sql = tokio::task::block_in_place(|| translate(cypher, cfg))?;
    println!("{}", sql);
    Ok(())
}

/// `cg validate` — parse + plan Cypher, report success or error.
pub fn run_validate(cypher: &str, cfg: &CgConfig) -> Result<()> {
    tokio::task::block_in_place(|| translate(cypher, cfg))?;
    println!("OK — Cypher is valid and translates successfully.");
    Ok(())
}

/// Translate Cypher → SQL using sql_only mode (no executor needed).
fn translate(cypher: &str, cfg: &CgConfig) -> Result<String> {
    let path = cfg.require_schema()?;
    let db = Database::sql_only(path).map_err(|e| anyhow!("{}", e))?;
    let conn = Connection::new(&db).map_err(|e| anyhow!("{}", e))?;
    conn.query_to_sql(cypher).map_err(|e| anyhow!("{}", e))
}

/// `cg query` — translate Cypher and optionally execute against ClickHouse.
pub async fn run_query(cypher: &str, sql_only: bool, format: &str, cfg: &CgConfig) -> Result<()> {
    let path = cfg.require_schema()?;

    if sql_only {
        return run_sql(cypher, cfg);
    }

    let ch_url = cfg
        .clickhouse_url
        .as_deref()
        .ok_or_else(|| anyhow!("No ClickHouse URL. Use --clickhouse or CG_CLICKHOUSE_URL."))?;

    let remote = RemoteConfig {
        url: ch_url.to_string(),
        user: cfg.ch_user.clone(),
        password: cfg.ch_password.clone(),
        database: cfg.ch_database.clone(),
        cluster_name: None,
    };
    let schema_path = path.to_string();
    let cypher = cypher.to_string();

    let (col_names, rows) = tokio::task::spawn_blocking(move || {
        let db = Database::new_remote(&schema_path, remote)
            .map_err(|e| anyhow!("Failed to connect to ClickHouse: {}", e))?;
        let conn = Connection::new(&db).map_err(|e| anyhow!("{}", e))?;
        let result = conn.query_remote(&cypher).map_err(|e| anyhow!("{}", e))?;
        let col_names: Vec<String> = result.get_column_names().to_vec();
        let rows: Vec<Vec<Value>> = result.map(|row| row.values().to_vec()).collect();
        Ok::<_, anyhow::Error>((col_names, rows))
    })
    .await??;

    match format {
        "json" => print_json(&col_names, &rows)?,
        "pretty" => print_json_pretty(&col_names, &rows)?,
        _ => print_table(&col_names, &rows),
    }

    Ok(())
}

// ── Output formatters ────────────────────────────────────────────────────────

fn print_table(cols: &[String], rows: &[Vec<Value>]) {
    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    let mut widths: Vec<usize> = cols.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            widths[i] = widths[i].max(value_str(val).len());
        }
    }

    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    println!("+{}+", sep);
    let header: Vec<String> = cols
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!(" {:w$} ", c, w = w))
        .collect();
    println!("|{}|", header.join("|"));
    println!("+{}+", sep);
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!(" {:w$} ", value_str(v), w = w))
            .collect();
        println!("|{}|", cells.join("|"));
    }
    println!("+{}+", sep);
    println!(
        "({} row{})",
        rows.len(),
        if rows.len() == 1 { "" } else { "s" }
    );
}

fn print_json(cols: &[String], rows: &[Vec<Value>]) -> Result<()> {
    for row in rows {
        let obj: serde_json::Map<String, serde_json::Value> = cols
            .iter()
            .zip(row)
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect();
        println!("{}", serde_json::to_string(&obj)?);
    }
    Ok(())
}

fn print_json_pretty(cols: &[String], rows: &[Vec<Value>]) -> Result<()> {
    let arr: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = cols
                .iter()
                .zip(row)
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&arr)?);
    Ok(())
}

fn value_str(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::String(s) | Value::Date(s) | Value::Timestamp(s) | Value::UUID(s) => s.clone(),
        Value::List(items) => format!(
            "[{}]",
            items.iter().map(value_str).collect::<Vec<_>>().join(", ")
        ),
        Value::Map(pairs) => format!(
            "{{{}}}",
            pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", k, value_str(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(n) => serde_json::Value::Number((*n).into()),
        Value::Float64(f) => serde_json::json!(f),
        Value::String(s) | Value::Date(s) | Value::Timestamp(s) | Value::UUID(s) => {
            serde_json::Value::String(s.clone())
        }
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        Value::Map(pairs) => serde_json::Value::Object(
            pairs
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}
