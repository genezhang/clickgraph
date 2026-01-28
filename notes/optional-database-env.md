# Optional CLICKHOUSE_DATABASE Environment Variable

**Date**: December 20, 2025  
**Status**: ✅ Complete  
**Type**: Breaking Change (Simplification)

## Summary

Made `CLICKHOUSE_DATABASE` environment variable **optional** with a default value of `"default"`. This simplifies ClickGraph configuration by eliminating a redundant required variable.

## Motivation

**Problem**: The `CLICKHOUSE_DATABASE` environment variable was marked as required, but it's actually redundant because:

1. **All SQL queries use fully-qualified table names** (`database.table`) generated from the schema config
2. **Each table in the schema specifies its database** (e.g., `database: brahmand` in YAML)
3. **Multi-database support**: A single graph view can span multiple databases
4. **Only used for connection context**: The variable only sets the ClickHouse client's default database, which isn't actually used in queries

**User Pain Point**: Having to set `CLICKHOUSE_DATABASE` even when it doesn't matter for query execution added unnecessary configuration complexity.

## Implementation

### Code Changes

**1. `/home/gz/clickgraph/src/server/clickhouse_client.rs`**
```rust
// BEFORE:
let database = read_env_var("CLICKHOUSE_DATABASE")?;  // Required - fails if not set

// AFTER:
let database = read_env_var("CLICKHOUSE_DATABASE")
    .unwrap_or_else(|| "default".to_string());  // Optional - defaults to "default"
```

**2. `/home/gz/clickgraph/src/server/connection_pool.rs`**
```rust
// BEFORE:
database: env::var("CLICKHOUSE_DATABASE")
    .map_err(|_| "CLICKHOUSE_DATABASE not set".to_string())?,  // Required

// AFTER:
database: env::var("CLICKHOUSE_DATABASE")
    .unwrap_or_else(|_| "default".to_string()),  // Optional with default
```

### Documentation Updates

Updated all documentation to reflect the optional nature:

1. **docs/wiki/Quick-Start-Guide.md** - Marked as optional with comments
2. **docs/wiki/Docker-Deployment.md** - Added explanation and commented out example
3. **docs/docker-deployment.md** - Updated table to show as optional
4. **README.md** - Removed from required examples, added clarifying note
5. **docs/configuration.md** - Marked as optional in quick start
6. **.github/copilot-instructions.md** - Updated setup examples with comments
7. **CHANGELOG.md** - Added breaking change entry with migration guide

### Test Script

Created `scripts/test/test_optional_database_env.sh` to verify the server starts without `CLICKHOUSE_DATABASE` set.

## Impact

### ✅ Benefits

1. **Simpler configuration**: One less required environment variable
2. **Clearer semantics**: Makes it obvious that database context comes from schema config
3. **No functionality loss**: All queries use fully-qualified names anyway
4. **Multi-database clarity**: Reinforces that each table specifies its own database

### ⚠️ Breaking Change Classification

**Technical**: Yes - changes from required to optional  
**Practical Impact**: **None** - existing configurations continue to work

**Migration**:
```bash
# BEFORE (still works):
export CLICKHOUSE_DATABASE="brahmand"

# AFTER (also works):
# (Just omit the variable - it defaults to "default")

# Or explicitly set default:
export CLICKHOUSE_DATABASE="default"
```

### Examples

**Minimal Configuration** (new simplified approach):
```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph
```

**With Explicit Database** (still supported):
```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"  # Optional - can specify if needed
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph
```

## Why This Works

**Key Architectural Detail**: ClickGraph's SQL generator **always** uses fully-qualified table names:

```rust
// From render_expr.rs, query_planner, etc:
format!("{}.{}", node_schema.database, node_schema.table_name)
//       ^^^^^^^^^                      ^^^^^^^^^^^^^^^^^^^^^^^
//       from YAML config               from YAML config
```

**Example Schema**:
```yaml
graph_schema:
  nodes:
    - label: User
      database: brahmand        # ← This is used in SQL generation
      table: users_bench
      node_id: user_id
```

**Generated SQL**:
```sql
SELECT u.user_id, u.full_name 
FROM brahmand.users_bench AS u  -- ← Fully-qualified from schema config
```

The `CLICKHOUSE_DATABASE` environment variable only sets the ClickHouse client's default context, which is never actually used because all table references in generated SQL are fully-qualified.

## Verification

### Build Status
```bash
$ cargo build --release
✅ Finished `release` profile [optimized] target(s) in 25.80s
```

### Test Results
- ✅ All existing tests pass
- ✅ Server starts without `CLICKHOUSE_DATABASE` set
- ✅ All SQL generation uses fully-qualified names

### Documentation Consistency
- ✅ 7 documentation files updated
- ✅ All examples now show variable as optional
- ✅ Migration guide in CHANGELOG

## Related Architecture

This change reinforces ClickGraph's **multi-schema, multi-database architecture**:

1. **Schema-driven database selection**: Each table in the schema config specifies its database
2. **Cross-database graph views**: A single graph can span multiple ClickHouse databases
3. **Fully-qualified SQL**: All generated queries use `database.table` format
4. **Connection pooling**: The default database is only used for connection context, not query execution

## Future Considerations

**Potential Further Simplification**: Consider making `GRAPH_CONFIG_PATH` the **only** required configuration, with ClickHouse connection details optionally specified in the schema YAML file itself:

```yaml
# Future possibility - schema config with embedded connection
clickhouse:
  url: "http://localhost:8123"
  user: "test_user"
  password: "test_pass"

graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users_bench
```

This would enable "single-file configuration" for simple use cases while maintaining flexibility for complex deployments.

## Conclusion

Making `CLICKHOUSE_DATABASE` optional removes unnecessary configuration complexity while maintaining full backwards compatibility. The change better reflects ClickGraph's architecture where database context comes from the schema config, not environment variables.

**Status**: ✅ Ready for next release (v0.5.5 or v0.6.0)
