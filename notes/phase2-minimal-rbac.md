# Phase 2: Minimal RBAC via Parameterized Views

**Date**: November 15, 2025  
**Purpose**: Minimal, elegant multi-tenancy + row-level security via ClickHouse parameterized views  
**Status**: Design Approved - Ready for Implementation  
**Scope**: v0.5.0 (4 weeks)

---

## Design Philosophy: Maximum Power, Minimum Code

**Core Principle**: **ClickHouse parameterized views do it all!**

### The Insight

ClickHouse parameterized views already provide:
1. ✅ **Row-level security** - `WHERE tenant_id = {tenant_id:String}`
2. ✅ **Per-tenant encryption** - `decrypt(..., (SELECT key WHERE tenant_id = ...))`
3. ✅ **Custom filters** - Any SQL logic users want (time, region, role, etc.)
4. ✅ **Performance** - ClickHouse optimizes view queries natively

**ClickGraph's job**: Just pass `tenant_id` parameter to views. That's it!

### What We're NOT Building (and why)

❌ **No row filter table** - ClickHouse views already do this  
❌ **No priority-based filter injection** - Views handle composition  
❌ **No callback plugins** - Defer to Phase 3 (query rewrite for materialized view optimization)  
❌ **No complex privilege storage** - ClickHouse RBAC already exists  

**Result**: ~200 lines of code instead of ~2000 lines, delivered in 4 weeks instead of 6!

### Multi-Tenancy as View Parameter

**Key Insight**: `tenant_id` is just a view parameter (optional, not special)

```sql
-- Users create views with ANY logic they want
CREATE VIEW my_secure_data AS
SELECT * FROM base_table
WHERE tenant_id = {tenant_id:String}  -- Multi-tenancy
  AND region IN ('US', 'CA')          -- Custom filter
  AND created_at >= today() - 30      -- Time-based
  AND decrypt(...) AS pii_column;     -- Encryption
```

ClickGraph passes `tenant_id`, ClickHouse does everything else!

---

## Minimal Implementation Design

### What ClickGraph Does (Simple!)

```rust
// 1. Accept tenant_id from request
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    parameters: Option<HashMap<String, Value>>,
    tenant_id: Option<String>,  // ← Only this new field!
}

// 2. Detect parameterized views from schema
pub struct ViewConfig {
    pub table: String,
    pub view_parameters: Option<Vec<String>>,  // ← New: ["tenant_id", "region", etc.]
}

// 3. Pass parameters to view
impl ViewResolver {
    pub fn resolve(&self, 
        view: &ViewConfig, 
        context: &QueryContext,
        params: &HashMap<String, Value>
    ) -> Result<String> {
        if let Some(view_params) = &view.view_parameters {
            let mut param_pairs = Vec::new();
            
            for param_name in view_params {
                // Check context first (tenant_id, user_id, etc.)
                let value = if param_name == "tenant_id" {
                    context.tenant_id.as_ref()
                        .ok_or(SecurityError::MissingParameter("tenant_id"))?
                } else if let Some(val) = params.get(param_name) {
                    val
                } else {
                    return Err(SecurityError::MissingParameter(param_name));
                };
                
                param_pairs.push(format!("{} = {}", param_name, value));
            }
            
            // Generate: table_name(tenant_id = 'acme', region = 'US')
            Ok(format!("{}({})", view.table, param_pairs.join(", ")))
        } else {
            Ok(view.table.clone())
        }
    }
}
```

**That's the entire feature!** ~200 lines of code.

### Schema Configuration (Flexible!)

```yaml
# schemas/multi_tenant.yaml
nodes:
  - label: User
    table: users_secure        # Parameterized view in ClickHouse
    view_parameters:           # ← List of parameters to pass
      - tenant_id              # Required from request context
      - region                 # Optional from query parameters
    properties:
      user_id: user_id
      name: name
      email: email  # Decrypted by view if needed

# Or simple case (just tenant_id)
  - label: Customer
    table: customers_secure
    view_parameters: [tenant_id]  # ← Shorthand syntax
    properties:
      customer_id: customer_id
```

### What Users Do in ClickHouse (Their Choice!)

Users create parameterized views with **any security logic they want**:

**Pattern 1: Simple Tenant Isolation**
```sql
CREATE VIEW users_secure AS
SELECT * FROM users 
WHERE tenant_id = {tenant_id:String};
```

**Pattern 2: Tenant + Role-Based Access**
```sql
CREATE VIEW users_secure AS
SELECT * FROM users 
WHERE tenant_id = {tenant_id:String}
  AND (
    getSetting('user_role') = 'admin'  -- Admins see all
    OR owner_id = getSetting('user_id')  -- Others see their own
  );
```

**Pattern 3: Per-Tenant Encryption (Your Use Case!)**
```sql
CREATE VIEW users_secure AS
SELECT 
    user_id,
    tenant_id,
    name,
    -- Decrypt using tenant-specific key
    decrypt('aes-256-gcm', email_encrypted,
            (SELECT encryption_key FROM tenant_keys 
             WHERE tenant_id = {tenant_id:String})) AS email,
    decrypt('aes-256-gcm', phone_encrypted,
            (SELECT encryption_key FROM tenant_keys 
             WHERE tenant_id = {tenant_id:String})) AS phone
FROM users_encrypted
WHERE tenant_id = {tenant_id:String};
```

**Pattern 4: Time-Based + Regional Access**
```sql
CREATE VIEW events_secure AS
SELECT * FROM events 
WHERE tenant_id = {tenant_id:String}
  AND event_date >= {start_date:Date}           -- Parameterized!
  AND event_date <= {end_date:Date}             -- Parameterized!
  AND region = {region:String};                 -- Parameterized!
```

**Usage**:
```yaml
# Schema
nodes:
  - label: Event
    table: events_secure
    view_parameters: [tenant_id, start_date, end_date, region]
```

```json
// Query
{
  "query": "MATCH (e:Event) RETURN e",
  "tenant_id": "acme",
  "parameters": {
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "region": "US"
  }
}
```

**Generated SQL**:
```sql
SELECT * FROM events_secure(
    tenant_id = 'acme',
    start_date = '2025-01-01',
    end_date = '2025-12-31',
    region = 'US'
)
```

**Pattern 5: Hierarchical Tenants**
```sql
-- Parent sees child data via recursive CTE in view
CREATE VIEW data_secure AS
WITH RECURSIVE tenant_tree AS (
    SELECT tenant_id FROM tenants WHERE tenant_id = {tenant_id:String}
    UNION ALL
    SELECT t.tenant_id FROM tenants t
    JOIN tenant_tree tt ON t.parent_id = tt.tenant_id
)
SELECT d.* FROM data d
WHERE d.tenant_id IN (SELECT tenant_id FROM tenant_tree);
```

**ClickGraph doesn't know or care about these rules!** It just passes `tenant_id`.

---

## Usage Examples

### Example 1: Multi-Tenant SaaS Query

**ClickHouse View Setup** (done once by user):
```sql
CREATE VIEW customers_secure AS
SELECT * FROM customers 
WHERE tenant_id = {tenant_id:String};
```

**ClickGraph Schema**:
```yaml
nodes:
  - label: Customer
    table: customers_secure
    view_parameters: [tenant_id]  # Simple case - one parameter
    properties:
      customer_id: customer_id
      name: name
```

**Query**:
```cypher
POST /query
{
  "query": "MATCH (c:Customer) WHERE c.name CONTAINS 'Acme' RETURN c",
  "tenant_id": "acme-corp"
}
```

**Generated SQL**:
```sql
SELECT customer_id, name
FROM customers_secure(tenant_id = 'acme-corp')  -- Parameterized view!
WHERE name LIKE '%Acme%'
```

### Example 2: Healthcare with Per-Tenant Encryption

**ClickHouse Setup**:
```sql
-- Encryption keys table
CREATE TABLE tenant_keys (
    tenant_id String,
    encryption_key String,
    PRIMARY KEY tenant_id
) ENGINE = MergeTree();

-- Encrypted patient data
CREATE TABLE patients_encrypted (
    patient_id UInt64,
    tenant_id String,
    name String,
    ssn_encrypted String,
    diagnosis_encrypted String
) ENGINE = MergeTree()
ORDER BY (tenant_id, patient_id);

-- Parameterized view with decryption
CREATE VIEW patients_secure AS
SELECT 
    patient_id,
    tenant_id,
    name,
    decrypt('aes-256-gcm', ssn_encrypted,
            (SELECT encryption_key FROM tenant_keys 
             WHERE tenant_id = {tenant_id:String})) AS ssn,
    decrypt('aes-256-gcm', diagnosis_encrypted,
            (SELECT encryption_key FROM tenant_keys 
             WHERE tenant_id = {tenant_id:String})) AS diagnosis
FROM patients_encrypted
WHERE tenant_id = {tenant_id:String};
```

**ClickGraph Schema**:
```yaml
nodes:
  - label: Patient
    table: patients_secure
    view_parameters: [tenant_id]  # Encryption handled in view
    properties:
      patient_id: patient_id
      ssn: ssn          # Decrypted by view
      diagnosis: diagnosis  # Decrypted by view
```

**Query**:
```cypher
POST /query
{
  "query": "MATCH (p:Patient {patient_id: 12345}) RETURN p.ssn, p.diagnosis",
  "tenant_id": "hospital_a"
}
```

**Generated SQL**:
```sql
SELECT ssn, diagnosis
FROM patients_secure(tenant_id = 'hospital_a')  -- Uses hospital_a's key
WHERE patient_id = 12345
```

**Security Properties**:
- ✅ ClickGraph never sees encryption keys
- ✅ Each hospital has unique key in ClickHouse
- ✅ Cross-tenant queries automatically fail (wrong key)
- ✅ HIPAA/GDPR compliant by design

---

## Implementation Plan (4 Weeks)

### Week 1: tenant_id Context Propagation

**HTTP API**:
```rust
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    parameters: Option<HashMap<String, Value>>,
    tenant_id: Option<String>,  // ← New field
}
```

**Bolt Protocol**:
```rust
// Extract tenant_id from HELLO metadata
impl BoltConnection {
    fn extract_tenant_id(&self, hello_msg: &HelloMessage) -> Option<String> {
        hello_msg.metadata.get("tenant_id").cloned()
    }
}
```

**Thread Through Execution**:
```rust
pub struct QueryContext {
    pub schema_name: String,
    pub tenant_id: Option<String>,  // ← New field
}
```

**Deliverable**: `tenant_id` flows from request → query context → SQL generation

### Week 2: Parameterized View Support

**Schema Extension**:
```yaml
# Add view_parameters field to ViewConfig
nodes:
  - label: User
    table: users_secure
    view_parameters: [tenant_id, region]  # ← Multiple parameters!
```

**ViewResolver Update**:
```rust
impl ViewResolver {
    pub fn resolve(&self, 
        view: &ViewConfig, 
        context: &QueryContext,
        params: &HashMap<String, Value>
    ) -> Result<String> {
        if let Some(view_params) = &view.view_parameters {
            let mut param_pairs = Vec::new();
            
            for param_name in view_params {
                let value = if param_name == "tenant_id" {
                    context.tenant_id.as_ref()
                        .ok_or(SecurityError::MissingParameter("tenant_id"))?
                } else if let Some(val) = params.get(param_name) {
                    val
                } else {
                    return Err(SecurityError::MissingParameter(param_name));
                };
                
                param_pairs.push(format!("{} = {}", param_name, value));
            }
            
            Ok(format!("{}({})", view.table, param_pairs.join(", ")))
        } else {
            Ok(view.table.clone())
        }
    }
}
```

**SQL Generator Integration**:
```rust
// In SQL generation, pass both context and parameters
let table_ref = view_resolver.resolve(&view_config, &query_context, &parameters)?;
// Generates: users_secure(tenant_id = 'acme', region = 'US')
```

**Deliverable**: Parameterized view syntax in generated SQL

### Week 3: Testing

**Unit Tests**:
- Schema parsing with `view_parameters: [tenant_id, region]`
- ViewResolver with single/multiple parameters
- Error handling (missing required parameters)
- Parameter priority (context vs query parameters)

**Integration Tests**:
- Simple tenant isolation (single parameter)
- Multi-parameter views (tenant + region + date range)
- Per-tenant encryption (with test ClickHouse setup)
- Multi-tenant queries (different tenants, different results)
- Performance (overhead < 1ms)

**Deliverable**: 100% test coverage for new code

### Week 4: Documentation & Examples

**User Guide**: `docs/multi-tenancy.md`
- How to create parameterized views in ClickHouse
- Schema configuration with `view_parameters`
- 5 patterns with complete SQL examples (including multi-parameter!)
- Security best practices
- Parameter resolution order (context → query params)

**Example Schemas**:
- `schemas/examples/multi_tenant_simple.yaml`
- `schemas/examples/multi_tenant_encrypted.yaml`

**Migration Guide**:
- How to add multi-tenancy to existing schema
- ClickHouse view creation steps
- Testing checklist

**Deliverable**: Complete documentation suite

**Total: 4 weeks** (down from 6 weeks!)

---

## Why This Design Wins

### ✅ **Benefits**

1. **Simple**: ~200 lines of code vs ~2000 lines
2. **Fast**: 4 weeks vs 6+ weeks development
3. **Flexible**: Users define ANY security logic in views (not limited by our imagination)
4. **Performant**: ClickHouse optimizes view queries natively (< 1ms overhead)
5. **Maintainable**: Less code = fewer bugs = easier upgrades
6. **Powerful**: Per-tenant encryption, hierarchical tenants, time-based access - all possible!
7. **True to Philosophy**: ClickGraph translates, ClickHouse enforces

### 📊 **Comparison**

| Approach | Lines of Code | Dev Time | Flexibility | Maintenance |
|----------|--------------|----------|-------------|-------------|
| **Parameterized Views (Chosen)** | ~200 | 4 weeks | ∞ (Any SQL) | Low |
| Row Filter Table + Injection | ~1500 | 5 weeks | Limited | High |
| Callback Plugins | ~2000 | 6+ weeks | High | Very High |

### 🎯 **What Users Get**

Without writing complex filter configs or callback plugins, users can:
- ✅ Simple tenant isolation
- ✅ Per-tenant encryption keys
- ✅ Role-based access within tenants
- ✅ Time-based data access
- ✅ Regional restrictions
- ✅ Hierarchical tenant trees
- ✅ Custom business logic (any SQL they want!)

All by creating ClickHouse views with standard SQL!

---

## Design Decisions

### ✅ **Decision 1: Parameterized Views Only**
- **Why**: Covers 100% of use cases with 10% of the code
- **Benefit**: Users have unlimited flexibility (any SQL logic)
- **Trade-off**: Users must learn ClickHouse views (but they're simple!)

### ✅ **Decision 2: Multi-Parameter Support from Day 1**
- **Why**: Users need more than just tenant_id (region, date ranges, etc.)
- **Benefit**: Full flexibility without waiting for Phase 3
- **Implementation**: `view_parameters: [tenant_id, region, start_date]`
- **Complexity**: Minimal (same code path, just iterate parameters)

### ✅ **Decision 3: No Filter Injection in ClickGraph**
- **Why**: ClickHouse views are more powerful and safer
- **Benefit**: No SQL parsing/rewriting bugs, no security holes
- **Performance**: ClickHouse query optimizer handles views natively

### ✅ **Decision 4: Defer Callback Plugins to Phase 3**
- **Why**: Validate need with real users first
- **Use Case**: Query rewriting for materialized view optimization
- **Note**: ClickHouse's built-in rewrite logic is limited, callbacks can help

### ✅ **Decision 5: Encryption in ClickHouse, Not ClickGraph**
- **Why**: ClickGraph stays stateless, never sees keys
- **Benefit**: Security by design, compliance-ready
- **Implementation**: Parameterized views with `decrypt()` function

---

## Open Questions & Future Enhancements

### Phase 2 (Current - 4 weeks)
- ✅ Parameterized views with multiple parameters
- ✅ Schema config: `view_parameters: [tenant_id, region, date_range]`
- ✅ HTTP and Bolt protocol support
- ✅ Documentation with 5 powerful patterns
- ✅ Parameter resolution: context fields (tenant_id) + query params (region, dates)

### Phase 3 (Future - Based on User Feedback)

1. **Query Rewrite Callbacks** (if CH rewrite is insufficient)
   - HTTP webhook for SQL query rewriting
   - Use case: Materialized view substitution
   - **Complexity**: Medium
   - **Timeline**: 2 weeks

2. **Connection Pooling** (if performance matters)
   - Per-tenant ClickHouse connections
   - True ClickHouse RBAC enforcement
   - **Complexity**: High (connection management)
   - **Timeline**: 3 weeks

**Decision Criteria**: Wait for user feedback after v0.5.0 release!

---

## Migration Path for Existing Users

**No breaking changes!** Multi-tenancy is opt-in:

```yaml
# Existing schemas work unchanged
nodes:
  - label: User
    table: users  # Regular table (no parameters)
    properties:
      user_id: user_id

# New multi-parameter schemas
nodes:
  - label: User
    table: users_secure  # Parameterized view
    view_parameters: [tenant_id, region]  # ← Multiple parameters!
    properties:
      user_id: user_id
```

**Adoption Path**:
1. Deploy v0.5.0 (no changes needed)
2. Create parameterized views in ClickHouse (with any parameters you want!)
3. Update schema YAML with `view_parameters: [tenant_id, region, ...]`
4. Start passing parameters in requests
5. Done!

**Example - Adding region filtering**:
```sql
-- ClickHouse: Add region parameter to existing view
CREATE VIEW users_secure_v2 AS
SELECT * FROM users 
WHERE tenant_id = {tenant_id:String}
  AND region = {region:String};  -- New parameter!
```

```yaml
# Schema: Add region to parameters
nodes:
  - label: User
    table: users_secure_v2
    view_parameters: [tenant_id, region]  # Was just [tenant_id]
```

```json
// Query: Pass region parameter
{
  "query": "MATCH (u:User) RETURN u",
  "tenant_id": "acme",
  "parameters": { "region": "US" }  // New parameter!
}
```

---

## Success Metrics

**Phase 2 Complete When**:
- ✅ Multiple parameters accepted: tenant_id (from context) + custom params (from request)
- ✅ `view_parameters: [...]` parsed from schema YAML
- ✅ Parameterized view syntax generated with all parameters
- ✅ All tests passing (unit + integration + multi-param scenarios)
- ✅ Documentation published with 5 patterns (including multi-parameter examples)
- ✅ Example schemas available (simple + complex)

**v0.5.0 Release Ready When**:
- ✅ No performance regression (< 1ms overhead)
- ✅ Backward compatible (existing schemas still work)
- ✅ Security tested (tenant isolation verified)
- ✅ Beta customers validate encryption use case

---

## Next Steps

1. ✅ **Review design** (this document) - DONE
2. 🔄 **Commit design** to `notes/phase2-minimal-rbac.md`
3. 🔄 **Update ROADMAP.md** with 4-week timeline
4. 🔄 **Create implementation tasks** in todo list
5. 🔄 **Week 1: Start coding!**

**Target**: v0.5.0 release in January 2026 (4 weeks from now)
