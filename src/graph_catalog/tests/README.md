# Graph Catalog Testing Guide

This directory contains tests for the graph catalog functionality, particularly focused on view definitions and schema validation.

## Test Structure

- `mock_clickhouse.rs`: Mock ClickHouse client implementation
- `schema_validator_tests.rs`: Schema validation tests
- `config_tests.rs`: Configuration loading and validation tests

## Mock ClickHouse Schema

The mock client provides predefined schemas for common test tables:

### Users Table
```sql
CREATE TABLE users (
    user_id UInt64,
    full_name String,
    email_address String,
    registration_date DateTime,
    is_active UInt8
)
```

### Posts Table
```sql
CREATE TABLE posts (
    post_id UInt64,
    author_id UInt64,
    post_title String,
    post_content String,
    post_date DateTime
)
```

## Running Tests

### Without ClickHouse
Most tests use the mock client and can run without a ClickHouse instance:

```bash
cargo test
```

### With ClickHouse
To test against a real ClickHouse instance:

1. Start ClickHouse:
   ```bash
   docker-compose up -d
   ```

2. Create test tables:
   ```sql
   CREATE TABLE users (
       user_id UInt64,
       full_name String,
       email_address String,
       registration_date DateTime,
       is_active UInt8
   ) ENGINE = MergeTree()
   ORDER BY user_id;

   CREATE TABLE posts (
       post_id UInt64,
       author_id UInt64,
       post_title String,
       post_content String,
       post_date DateTime
   ) ENGINE = MergeTree()
   ORDER BY post_id;
   ```

3. Set environment variables:
   ```bash
   export CLICKHOUSE_URL=http://localhost:8123
   export CLICKHOUSE_USER=default
   export CLICKHOUSE_PASSWORD=
   export CLICKHOUSE_DATABASE=default
   ```

4. Run integration tests:
   ```bash
   cargo test --features integration-tests
   ```

## Adding New Tests

1. Add mock schema to `mock_clickhouse.rs` if needed
2. Create test configurations in temporary files
3. Use `SchemaValidator` with mock client
4. Assert expected validation results

