# Multi-Schema Architecture Design

## Overview
ClickGraph currently supports a single global schema loaded at startup. To enable dynamic schema switching for benchmarking and multi-tenant scenarios, we need to support multiple schemas that can be loaded and switched dynamically without server restarts.

## Current Architecture
- Single `GLOBAL_GRAPH_SCHEMA: OnceCell<RwLock<GraphSchema>>`
- Schema loaded from `GRAPH_CONFIG_PATH` environment variable at startup
- All queries use the same global schema
- Schema switching requires server restart

## Proposed Architecture

### 1. Global Schema Storage
```rust
pub static GLOBAL_SCHEMAS: OnceCell<RwLock<HashMap<String, GraphSchema>>> = OnceCell::const_new();
pub static GLOBAL_VIEW_CONFIGS: OnceCell<RwLock<HashMap<String, GraphViewConfig>>> = OnceCell::const_new();
```

### 2. Query Request Enhancement
```rust
#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub format: Option<OutputFormat>,
    pub sql_only: Option<bool>,
    pub schema_name: Option<String>, // NEW: Specify which schema to use
}
```

### 3. Schema Management API
- `POST /api/schemas` - Load a new schema
- `GET /api/schemas` - List available schemas
- `DELETE /api/schemas/{name}` - Remove a schema
- `GET /api/schemas/{name}` - Get schema details

### 4. Schema Loading
- **Startup**: Load default schema from `GRAPH_CONFIG_PATH` with name "default"
- **Dynamic**: Load additional schemas via API with custom names
- **Validation**: Ensure schema names are unique and valid

### 5. Query Processing Changes
- Extract `schema_name` from request (default to "default" if not specified)
- Look up schema by name from `GLOBAL_SCHEMAS`
- Pass specific schema to query planner instead of global schema

### 6. Backward Compatibility
- If no `schema_name` specified, use "default" schema
- Existing single-schema deployments continue to work unchanged

## Implementation Plan

### Phase 1: Core Infrastructure
1. Replace global schema storage with HashMap-based storage
2. Update schema loading to support named schemas
3. Modify query handler to accept schema_name parameter

### Phase 2: API Endpoints
1. Implement schema management REST API
2. Add schema validation and error handling
3. Update startup logic to load default schema

### Phase 3: Benchmark Integration
1. Update benchmark scripts to use schema_name parameter
2. Remove need for server restarts between datasets
3. Add schema switching commands to benchmark suite

### Phase 4: Testing & Documentation
1. Update unit tests for multi-schema support
2. Add integration tests for schema switching
3. Update documentation and examples

## Benefits
- **Benchmarking**: Switch between datasets without server restarts
- **Multi-tenancy**: Support multiple isolated graph schemas
- **Development**: Easier testing of different schema configurations
- **Production**: Dynamic schema updates without downtime

## Migration Path
1. Deploy multi-schema version alongside existing single-schema
2. Gradually migrate benchmarking to use schema_name parameter
3. Eventually deprecate single-schema mode (with backward compatibility)

## Schema Name Conventions
- Use lowercase alphanumeric names with hyphens/underscores
- Reserved names: "default", "system"
- Maximum length: 64 characters
- Must be unique across all loaded schemas</content>
<parameter name="filePath">c:\Users\GenZ\clickgraph\MULTI_SCHEMA_DESIGN.md