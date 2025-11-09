use std::env;

use clickhouse::Client;

fn read_env_var(key: &str) -> Option<String> {
    env::var(key).ok()
}

pub fn try_get_client() -> Option<Client> {
    let url = read_env_var("CLICKHOUSE_URL")?;
    let user = read_env_var("CLICKHOUSE_USER")?;
    let password = read_env_var("CLICKHOUSE_PASSWORD")?;
    let database = read_env_var("CLICKHOUSE_DATABASE")?;
    
    println!("\n CLICKHOUSE_URL {}\n", url);
    Some(Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password)
        .with_database(database)
        .with_option("join_use_nulls", "1")  // Return NULL for unmatched LEFT JOIN columns
        .with_option("allow_experimental_json_type", "1")
        .with_option("input_format_binary_read_json_as_string", "1")
        .with_option("output_format_binary_write_json_as_string", "1"))
}

pub fn get_client() -> Client {
    try_get_client().expect("ClickHouse environment variables should be set")
}
