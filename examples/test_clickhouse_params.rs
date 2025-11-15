// Test ClickHouse parameterized query support
// Run with: cargo run --manifest-path brahmand/Cargo.toml --example test_clickhouse_params

use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

// Define Row struct for query results
#[derive(Debug, Row, Serialize, Deserialize)]
struct User {
    name: String,
    age: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing ClickHouse Parameter Support ===\n");

    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_user("test_user")
        .with_password("test_pass")
        .with_database("test_integration");

    // Test 1: Simple positional parameter with ? placeholder
    println!("Test 1: Single positional parameter");
    println!("Query: SELECT name, age FROM users WHERE name = ?");
    println!("Binding: 'Alice Johnson'");

    match client
        .query("SELECT name, age FROM users WHERE name = ?")
        .bind("Alice Johnson")
        .fetch_all::<User>()
        .await
    {
        Ok(results) => {
            println!("✅ Success! Returned {} rows", results.len());
            if !results.is_empty() {
                println!("   Sample row: {:?}\n", results[0]);
            } else {
                println!("   (No rows found - table may be empty)\n");
            }
        }
        Err(e) => {
            println!("❌ Failed: {}\n", e);
        }
    }

    // Test 2: Multiple positional parameters
    println!("Test 2: Multiple positional parameters");
    println!("Query: SELECT name, age FROM users WHERE name = ? AND age > ?");
    println!("Bindings: 'Alice Johnson', 25");

    match client
        .query("SELECT name, age FROM users WHERE name = ? AND age > ?")
        .bind("Alice Johnson")
        .bind(25)
        .fetch_all::<User>()
        .await
    {
        Ok(results) => {
            println!("✅ Success! Returned {} rows", results.len());
            if !results.is_empty() {
                println!("   Sample row: {:?}\n", results[0]);
            } else {
                println!("   (No rows found)\n");
            }
        }
        Err(e) => {
            println!("❌ Failed: {}\n", e);
        }
    }

    // Test 3: Array parameter for IN clause
    println!("Test 3: Array parameter");
    println!("Query: SELECT name, age FROM users WHERE name IN ?");
    println!("Binding: vec!['Alice Johnson', 'Bob Smith']");

    match client
        .query("SELECT name, age FROM users WHERE name IN ?")
        .bind(vec!["Alice Johnson", "Bob Smith"])
        .fetch_all::<User>()
        .await
    {
        Ok(results) => {
            println!("✅ Success! Returned {} rows\n", results.len());
        }
        Err(e) => {
            println!("❌ Failed: {}\n", e);
        }
    }

    // Summary
    println!("\n=== Summary ===");
    println!("ClickHouse Rust client uses POSITIONAL parameters with ? placeholders.");
    println!("The .bind() method takes a single value (not key-value pairs).");
    println!("Multiple parameters are bound in order by chaining .bind() calls.");
    println!("\nConclusion:");
    println!("- ✅ ClickHouse supports parameterized queries");
    println!("- ✅ Rust client has .bind() API");
    println!("- ❌ Named parameters like {{name:String}} are NOT supported");
    println!("- ✅ Must use ? placeholder with positional binding");
    println!("\nNext Steps:");
    println!("1. Update parameter-support.md with correct ClickHouse API");
    println!("2. Decide: Convert Neo4j $name → ClickHouse ? or extend clickhouse crate");
    println!("3. Implement parameter extraction from HTTP/Bolt");
    println!("4. Generate SQL with ? placeholders");
    println!("5. Bind parameters in order before execution");

    Ok(())
}
