# Auto-Schema Discovery

**Status**: Complete  
**Feature**: Phase 2 - Task #4  
**Date**: November 17, 2025

## Summary

Auto-schema discovery automatically detects table columns and engine types from ClickHouse metadata, eliminating manual YAML configuration for wide tables. Users can selectively exclude columns and override property mappings as needed.

## How It Works

### Configuration

Add two optional fields to node/relationship definitions:

```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    
    # Enable auto-discovery
    auto_discover_columns: true
    
    # Optional: Naming convention (snake_case or camelCase)
    naming_convention: camelCase
    
    # Optional: Exclude columns
    exclude_columns: [_version, _internal]
    
    # Optional: Override mappings
    property_mappings:
      email_address: email
```

### Discovery Process

1. **Column Query**: Query `system.columns` for table metadata
2. **Apply Naming Convention**: Convert column names (snake_case → camelCase if specified)
3. **Apply Exclusions**: Remove columns listed in `exclude_columns`
4. **Manual Overrides**: Merge `property_mappings` (manual wins)
5. **Engine Detection**: Query `system.tables` for engine type
6. **Auto FINAL**: Set `use_final = true` for ReplacingMergeTree tables

### Naming Conventions

**snake_case (default)**:
- Keep original column names: `user_id → user_id`
- No conversion applied

**camelCase**:
- Convert to camelCase: `user_id → userId`, `email_address → emailAddress`
- Common for GraphQL/JavaScript frontends

### Data Flow

```
YAML Config
  ↓
NodeDefinition.auto_discover_columns = true
  ↓
query_table_columns(client, database, table)
  ↓
Filter excluded columns
  ↓
Build identity mappings
  ↓
Merge with manual property_mappings
  ↓
detect_table_engine(client, database, table)
  ↓
NodeSchema with auto-discovered properties + engine
```

## Key Files

### Core Implementation

- `src/graph_catalog/config.rs`
  * Added fields: `auto_discover_columns: bool`, `exclude_columns: Vec<String>`
  * New method: `to_graph_schema_with_client(&self, client: &Client)`
  * Logic: Queries columns, builds mappings, detects engines

- `src/graph_catalog/column_info.rs`
  * Function: `query_table_columns(client, database, table) -> Vec<String>`
  * Queries: `SELECT name FROM system.columns WHERE ... ORDER BY position`

- `src/server/graph_catalog.rs`
  * Updated: `load_schema_and_config_from_yaml()` to accept client
  * Calls: `to_graph_schema_with_client()` when client available

### Example Schemas

- `schemas/examples/auto_discovery_demo.yaml` - Demo with all features

### Tests

- `tests/integration/test_auto_discovery.py` - Full integration test suite

## Design Decisions

### 1. Manual Override Priority

**Decision**: Manual `property_mappings` always override auto-discovered mappings.

**Rationale**: Gives users full control. They can:
- Auto-discover 50 columns with identity mappings
- Manually rename 2-3 columns with special requirements
- Example: `full_name → name`, `email_address → email`

### 2. No Auto-Detection of Tables

**Decision**: Users must explicitly list nodes/relationships in YAML.

**Rationale**:
- Avoids ambiguity (which tables are nodes vs edges?)
- User intent is clear
- Simpler implementation

### 3. Naming Convention Support

**Decision**: Support both `snake_case` (default) and `camelCase` naming conventions.

**Rationale**:
- ClickHouse uses snake_case: `user_id`, `email_address`
- GraphQL/JavaScript prefer camelCase: `userId`, `emailAddress`
- Users can choose what fits their API style
- Simple conversion function (snake_to_camel_case)

### 4. Graceful Fallback

**Decision**: If engine detection fails, continue without FINAL.

**Rationale**:
- Non-critical feature
- Better to work without FINAL than fail entirely
- User can manually set `use_final: true` if needed

## Limitations

### Current

1. **No composite keys**: `id_column` is still a single string (future: array support)
2. **No table discovery**: Users must list all tables in YAML
3. **No relationship inference**: Can't auto-detect relationships from foreign keys
4. **No type mapping**: Column types not exposed to Cypher (all treated as generic properties)

### Future Enhancements

1. **Primary key detection**: Auto-set `id_column` from ClickHouse PK
2. **Foreign key hints**: Detect relationships from naming patterns (`*_id` columns)
3. **Type awareness**: Expose ClickHouse types to Cypher (for type checking)
4. **Additional naming conventions**: kebab-case, PascalCase, etc.

## Gotchas

### 1. Excluded Columns Are Inaccessible

If you exclude a column, you **cannot** query it:

```yaml
exclude_columns: [internal_hash]
```

```cypher
MATCH (u:User) RETURN u.internal_hash  # ERROR: Property not found
```

### 2. Manual Overrides Must Be Correct

If you override a mapping incorrectly, queries will fail:

```yaml
property_mappings:
  wrong_name: email  # If wrong_name doesn't exist in table
```

### 3. Schema Changes Require Reload

Auto-discovery happens at schema load time. If you add columns to ClickHouse:
- Without auto-discovery: No change (manual YAML unchanged)
- With auto-discovery: Reload schema to pick up new columns

### 4. Client Required

Auto-discovery only works when ClickHouse client is available. Without client:
- Falls back to manual mode
- Uses `property_mappings` from YAML as-is
- No engine detection

## Examples

### Minimal (Identity Mapping)

```yaml
nodes:
  - label: WideTable
    table: analytics_events
    id_column: event_id
    auto_discover_columns: true
```

Result: All 50 columns become properties (snake_case).

### With camelCase Naming

```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    auto_discover_columns: true
    naming_convention: camelCase
```

Result: `user_id → userId`, `email_address → emailAddress`, etc.
    auto_discover_columns: true
```

Result: All 50 columns become properties (identity mapping).

### With Exclusions

```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    auto_discover_columns: true
    exclude_columns:
      - _version
      - _shard_num
      - raw_metadata
```

Result: All columns except 3 excluded ones.

### With Overrides

```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    auto_discover_columns: true
    property_mappings:
      full_name: name           # Rename 1
      email_address: email      # Rename 2
      # Other 20 columns: identity mapping
```

Result: 22 properties total (20 identity + 2 renamed).

### Mixed Schema (Some Auto, Some Manual)

```yaml
nodes:
  - label: User
    auto_discover_columns: true  # Auto
    
  - label: Admin
    auto_discover_columns: false  # Manual
    property_mappings:
      user_id: id
      # ... explicit mappings
```

Result: User auto-discovered, Admin manually configured.

## Testing

### Unit Tests

Schema parsing tests already cover new fields (serde default behavior).

### Integration Tests

`tests/integration/test_auto_discovery.py`:
- ✅ Basic query with auto-discovered properties
- ✅ All non-excluded columns accessible
- ✅ Relationship property auto-discovery
- ✅ Manual overrides work
- ✅ Exclusion prevents access
- ✅ Engine detection and FINAL generation
- ✅ Backward compatibility (manual schemas still work)

Run: `pytest tests/integration/test_auto_discovery.py -v`

### Manual Testing

```bash
# 1. Start server with auto-discovery schema
export GRAPH_CONFIG_PATH="schemas/examples/auto_discovery_demo.yaml"
cargo run --bin clickgraph

# 2. Query auto-discovered properties
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.email, u.country LIMIT 1"
  }'

# 3. Check SQL generation (FINAL should be present)
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) WHERE u.user_id = 1 RETURN u.name LIMIT 1",
    "sql_only": true
  }'
```

## Migration Guide

### Before (Manual - 50 columns)

```yaml
nodes:
  - label: Event
    table: analytics_events
    id_column: event_id
    property_mappings:
      event_id: event_id
      event_type: event_type
      user_id: user_id
      timestamp: timestamp
      # ... 46 more columns
```

### After (Auto-Discovery)

```yaml
nodes:
  - label: Event
    table: analytics_events
    id_column: event_id
    auto_discover_columns: true
    exclude_columns: [_internal_version]
```

**Benefit**: 48 lines → 5 lines (90% reduction).

## Future Work

1. **Composite Keys**: Support `id_column: [tenant_id, user_id]`
2. **Type Mapping**: Expose ClickHouse types to Cypher
3. **Smart Naming**: Auto-convert snake_case → camelCase
4. **Relationship Inference**: Detect edges from foreign key patterns
5. **Incremental Refresh**: Detect schema changes without full reload
6. **Schema Diff**: Show what changed between reloads

## Related Features

- **Engine Detection** (`engine_detection.rs`) - Detects table engines
- **FINAL Support** - Auto-applied for ReplacingMergeTree tables
- **Parameterized Views** - Works with auto-discovery
- **Multi-Schema** - Auto-discovery per schema
