use std::env;

use clickhouse::Client;

fn read_env_var(key: &str) -> Option<String> {
    env::var(key).ok()
}

pub fn try_get_client() -> Option<Client> {
    let url = read_env_var("CLICKHOUSE_URL")?;
    let user = read_env_var("CLICKHOUSE_USER")?;
    let password = read_env_var("CLICKHOUSE_PASSWORD")?;
    // Database is optional - defaults to "default". All queries use fully-qualified table names anyway.
    let database = read_env_var("CLICKHOUSE_DATABASE").unwrap_or_else(|| "default".to_string());

    log::info!(
        "✓ Connecting to ClickHouse at {} as user {} with password {} (database: {})",
        url,
        user,
        if password.is_empty() { "[EMPTY]" } else { "[SET]" },
        database
    );
    Some(
        Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(database)
            .with_option("join_use_nulls", "1") // Return NULL for unmatched LEFT JOIN columns
            .with_option("allow_experimental_json_type", "1")
            .with_option("input_format_binary_read_json_as_string", "1")
            .with_option("output_format_binary_write_json_as_string", "1")
            // Query safety limits to prevent hanging/OOM on large result sets
            .with_option("max_execution_time", "60") // 60 second query timeout
            .with_option("max_result_rows", "1000000") // Max 1M rows per query
            .with_option("max_result_bytes", "1073741824") // Max 1GB result size
            .with_option("result_overflow_mode", "throw"), // Throw error instead of truncating
    )
}

/// Get ClickHouse client with default configuration
/// Reserved for direct client access use cases
#[allow(dead_code)]
pub fn get_client() -> Client {
    try_get_client().expect("ClickHouse environment variables should be set")
}

/// Set ClickHouse role for RBAC enforcement via SET ROLE command
///
/// Requires:
/// - Database-managed user (not users.xml)
/// - Role must be granted to the user
///
/// Example usage:
/// ```sql
/// -- Setup (by admin):
/// CREATE ROLE admin_role;
/// CREATE ROLE viewer_role;
/// CREATE USER app_user IDENTIFIED WITH plaintext_password BY 'password';
/// GRANT admin_role TO app_user;
/// GRANT viewer_role TO app_user;
///
/// -- View definition:
/// CREATE VIEW secure_data AS
/// SELECT * FROM data
/// WHERE required_role IN (SELECT role_name FROM system.current_roles);
/// ```
pub async fn set_role(client: &Client, role: &str) -> Result<(), clickhouse::error::Error> {
    log::debug!("Setting ClickHouse role: {}", role);
    client.query(&format!("SET ROLE {}", role)).execute().await
}
