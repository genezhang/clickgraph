use std::env;

use clickhouse::Client;

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

pub fn get_client() -> Client {
    println!("\n CLICKHOUSE_URL {}\n", read_env_var("CLICKHOUSE_URL"));
    Client::default()
        .with_url(read_env_var("CLICKHOUSE_URL"))
        .with_user(read_env_var("CLICKHOUSE_USER"))
        .with_password(read_env_var("CLICKHOUSE_PASSWORD"))
        .with_database(read_env_var("CLICKHOUSE_DATABASE"))
        .with_option("allow_experimental_json_type", "1")
        .with_option("input_format_binary_read_json_as_string", "1")
        .with_option("output_format_binary_write_json_as_string", "1")
}
