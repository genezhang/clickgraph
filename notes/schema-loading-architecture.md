# Schema Loading Architecture - Dual Registration Explained

## Two Different Loading Paths

### 1. Startup Schema Loading (via `GRAPH_CONFIG_PATH`)

**Function**: `initialize_global_schema()` in `graph_catalog.rs`

**When**: Server startup

**Registration Logic**:
```rust
// Always register as "default"
schemas.insert("default".to_string(), schema.clone());

// Also register with schema name if provided in YAML
if let Some(ref schema_name) = config.name {
    schemas.insert(schema_name.clone(), schema.clone());
}
```

**Result**: Dual-key registration
- `GLOBAL_SCHEMAS["default"]` → Schema instance
- `GLOBAL_SCHEMAS["test_integration"]` → Same schema instance (if `name: test_integration` in YAML)

**Purpose**: The startup schema is the **default schema** - accessible by:
- No schema specified → uses "default"
- `USE default` → explicit default
- `USE test_integration` → actual schema name
- `schema_name=default` → parameter
- `schema_name=test_integration` → parameter

---

### 2. API Schema Loading (via `/api/schemas/load`)

**Function**: `load_schema_by_name()` in `graph_catalog.rs`

**When**: Runtime, via HTTP API

**Registration Logic**:
```rust
// Register with ONLY the schema_name from API request
schemas_guard.insert(schema_name.to_string(), schema.clone());

// Does NOT touch "default" - preserves startup schema
```

**Result**: Single-key registration
- `GLOBAL_SCHEMAS["custom_schema"]` → New schema instance
- Does NOT update `GLOBAL_SCHEMAS["default"]`

**Purpose**: Load additional schemas without affecting the default

---

## Why This Design is Correct

### ❌ What We DON'T Want

API loading a new schema should **NOT**:
1. Overwrite the "default" schema
2. Change what queries without `USE` clause access
3. Affect concurrent queries using the default schema

### ✅ What We DO Want

API loading a new schema **SHOULD**:
1. Make the schema available by its specific name
2. Allow queries to use it via `USE schema_name`
3. Coexist with the default schema
4. Support multiple API-loaded schemas

---

## Example Usage Scenarios

### Scenario 1: Single Startup Schema
```bash
# Server started with:
GRAPH_CONFIG_PATH=test_integration.yaml

# YAML contains: name: test_integration

# Result:
GLOBAL_SCHEMAS = {
    "default": test_integration_schema,
    "test_integration": test_integration_schema  # same instance
}

# All these work:
MATCH (n:User) RETURN n                    # uses "default"
USE default MATCH (n:User) RETURN n        # explicit
USE test_integration MATCH (n:User) RETURN n  # by name
```

### Scenario 2: Startup + API-Loaded Schema
```bash
# Server started with:
GRAPH_CONFIG_PATH=test_integration.yaml  # name: test_integration

# Then via API:
POST /schemas/load
{
    "schema_name": "social_network",
    "config_path": "social_network.yaml"
}

# Result:
GLOBAL_SCHEMAS = {
    "default": test_integration_schema,
    "test_integration": test_integration_schema,  # same instance
    "social_network": social_network_schema      # different instance
}

# Usage:
MATCH (n:User) RETURN n                        # uses test_integration (default)
USE social_network MATCH (n:User) RETURN n     # uses social_network
USE test_integration MATCH (n:User) RETURN n   # uses test_integration
```

### Scenario 3: Multiple API-Loaded Schemas
```bash
# Server started with minimal/empty schema

# Then via API:
POST /schemas/load {"schema_name": "ecommerce", ...}
POST /schemas/load {"schema_name": "social", ...}
POST /schemas/load {"schema_name": "analytics", ...}

# Result:
GLOBAL_SCHEMAS = {
    "default": empty_schema,
    "ecommerce": ecommerce_schema,
    "social": social_schema,
    "analytics": analytics_schema
}

# Usage - must specify schema:
USE ecommerce MATCH (p:Product) RETURN p
USE social MATCH (u:User) RETURN u
USE analytics MATCH (e:Event) RETURN e
```

---

## Key Architectural Points

### 1. "default" is Special
- Only set once at startup
- Never changed by API loading
- Represents the primary/default schema

### 2. API Loading is Additive
- Adds new schemas to `GLOBAL_SCHEMAS`
- Does not modify existing entries
- Does not overwrite "default"

### 3. YAML `name` Field Usage

**Startup Loading**:
```yaml
name: test_integration  # Used for dual registration
graph_schema: ...
```
→ Registered as both "default" and "test_integration"

**API Loading**:
```yaml
name: anything  # IGNORED!
graph_schema: ...
```
→ Registered with name from API request parameter, not YAML

### 4. Schema Isolation
- Each schema is independent
- No cross-schema queries (by design)
- `USE` clause selects which schema to query

---

## Testing Strategy

### Test Startup Schema (test_schema_access_patterns.py)
✅ Tests dual-key registration for startup schema
✅ Verifies "default" and actual name both work
✅ Validates USE clause, parameters, and implicit default

### Test API-Loaded Schema (test_api_loaded_schema.py)
✅ Tests API loading endpoint
✅ Verifies schema accessible by API-provided name
✅ Validates default schema NOT affected
✅ Confirms multiple schemas can coexist

### Integration Tests (tests/integration/conftest.py)
✅ Use API loading path
✅ Validate production code path
✅ Test multi-schema scenarios

---

## Common Misconceptions

### ❌ WRONG: "API-loaded schemas should also be dual-registered"
**Why wrong**: This would create confusion about which schema is "default"

### ❌ WRONG: "The YAML `name` field is always used"
**Why wrong**: For API loading, the API parameter determines the name

### ❌ WRONG: "All schemas should have a 'default' alias"
**Why wrong**: Only ONE schema should be default - the startup one

### ✅ CORRECT: "API loading adds schemas, startup sets the default"
**Why correct**: Clear separation of concerns - startup = default, API = additional

---

## Future Enhancements

### Possible (if needed):
1. **API endpoint to change default schema**
   - `POST /schemas/set-default {"schema_name": "social_network"}`
   - Would update `GLOBAL_SCHEMAS["default"]` to point to different instance

2. **Schema aliasing**
   - Allow multiple names to point to same schema
   - `POST /schemas/alias {"from": "social", "to": "social_network"}`

3. **Schema unloading**
   - `DELETE /schemas/{schema_name}`
   - Remove from `GLOBAL_SCHEMAS` (except "default")

### Not Recommended:
- ❌ Automatic dual-registration for API-loaded schemas
- ❌ Allowing YAML `name` to override API parameter
- ❌ Swapping GLOBAL_GRAPH_SCHEMA (race condition risk)

---

## Summary

The dual-key registration architecture is **intentionally asymmetric**:

- **Startup schema**: Dual registration ("default" + name)
- **API schemas**: Single registration (API-provided name only)

This design:
✅ Prevents race conditions
✅ Preserves default schema stability
✅ Supports multi-schema workloads
✅ Maintains clear semantics

It's **working as designed**, not a bug!
