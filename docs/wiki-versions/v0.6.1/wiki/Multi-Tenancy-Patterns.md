> **Note**: This documentation is for ClickGraph v0.6.1. [View latest docs →](../../wiki/Home.md)
# Multi-Tenancy with Parameterized Views

**Feature**: Phase 2 Multi-Tenancy  
**Version**: v0.5.0+  
**Status**: Production Ready

## Overview

ClickGraph supports multi-tenant architectures through **ClickHouse parameterized views**. This approach provides:

- ✅ **Row-level security** - Filter data by tenant, region, time period, etc.
- ✅ **Per-tenant encryption** - Each tenant can have unique encryption keys
- ✅ **Flexible filtering** - Any SQL logic you can express in a view
- ✅ **Native performance** - ClickHouse optimizes parameterized views natively
- ✅ **Efficient caching** - Single cache entry shared across all tenants

**Key Principle**: ClickHouse does the heavy lifting. ClickGraph just passes parameters to your views.

---

## Quick Start

### 1. Create a Parameterized View in ClickHouse

```sql
-- Base table with tenant data
CREATE TABLE users (
    user_id UInt64,
    tenant_id String,
    name String,
    email String,
    country String
) ENGINE = MergeTree()
ORDER BY (tenant_id, user_id);

-- Parameterized view for tenant isolation
CREATE VIEW users_by_tenant AS
SELECT 
    user_id,
    tenant_id,
    name,
    email,
    country
FROM users
WHERE tenant_id = {tenant_id:String};
```

### 2. Configure ClickGraph Schema

```yaml
# schemas/my_schema.yaml
graph_schema:
  database: my_database
  
  nodes:
    - label: User
      table: users_by_tenant          # Reference the parameterized view
      view_parameters: [tenant_id]     # Declare required parameters
      node_id: user_id
      properties:
        user_id: user_id
        name: name
        email: email
        country: country
```

### 3. Query with Tenant Context

**HTTP API**:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u.name, u.email",
    "schema_name": "default",
    "view_parameters": {
      "tenant_id": "acme-corp"
    }
  }'
```

**Bolt Protocol** (Neo4j drivers):
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("user", "pass"))
with driver.session(database="brahmand") as session:
    result = session.run(
        "MATCH (u:User) RETURN u.name, u.email",
        tenant_id="acme-corp"  # Passed as parameter
    )
    for record in result:
        print(record["u.name"], record["u.email"])
```

**Generated SQL** (behind the scenes):
```sql
-- ClickGraph generates SQL template with placeholder:
SELECT name AS `u.name`, email AS `u.email`
FROM users_by_tenant(tenant_id = $tenant_id) AS u

-- At runtime, $tenant_id is substituted:
-- For ACME: tenant_id = 'acme-corp'
-- For GLOBEX: tenant_id = 'globex-inc'
```

---

## Multi-Tenant Patterns

### Pattern 1: Simple Tenant Isolation

**Use Case**: SaaS application where each customer has isolated data.

```sql
CREATE VIEW users_by_tenant AS
SELECT * FROM users
WHERE tenant_id = {tenant_id:String};

CREATE VIEW orders_by_tenant AS
SELECT * FROM orders
WHERE tenant_id = {tenant_id:String};
```

```yaml
nodes:
  - label: User
    table: users_by_tenant
    view_parameters: [tenant_id]
  
  - label: Order
    table: orders_by_tenant
    view_parameters: [tenant_id]

relationships:
  - type: PLACED
    table: user_orders_by_tenant
    view_parameters: [tenant_id]
    from_node_id: user_id
    to_node_id: order_id
```

**Query**:
```cypher
// Tenant A sees only their data
MATCH (u:User)-[:PLACED]->(o:Order)
RETURN u.name, o.order_id

// Pass tenant_id via view_parameters
{
  "view_parameters": {"tenant_id": "tenant-a"}
}
```

### Pattern 2: Multi-Parameter Views

**Use Case**: Filter by tenant + region + date range.

```sql
CREATE VIEW events_secure AS
SELECT * FROM events
WHERE tenant_id = {tenant_id:String}
  AND region = {region:String}
  AND event_date BETWEEN {start_date:Date} AND {end_date:Date};
```

```yaml
nodes:
  - label: Event
    table: events_secure
    view_parameters: [tenant_id, region, start_date, end_date]
    properties:
      event_id: event_id
      event_type: event_type
      event_date: event_date
```

**Query**:
```json
{
  "query": "MATCH (e:Event) WHERE e.event_type = 'login' RETURN e",
  "schema_name": "default",
  "view_parameters": {
    "tenant_id": "acme",
    "region": "US",
    "start_date": "2025-01-01",
    "end_date": "2025-12-31"
  }
}
```

### Pattern 3: Per-Tenant Encryption

**Use Case**: Healthcare/financial apps with per-tenant encryption keys.

```sql
-- Table storing per-tenant encryption keys
CREATE TABLE tenant_keys (
    tenant_id String,
    encryption_key String
) ENGINE = MergeTree()
ORDER BY tenant_id;

-- Encrypted patient data
CREATE TABLE patients_encrypted (
    patient_id UInt64,
    tenant_id String,
    name String,
    ssn_encrypted String,
    diagnosis_encrypted String
) ENGINE = MergeTree()
ORDER BY (tenant_id, patient_id);

-- Parameterized view with automatic decryption
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

```yaml
nodes:
  - label: Patient
    table: patients_secure
    view_parameters: [tenant_id]
    properties:
      patient_id: patient_id
      ssn: ssn           # Auto-decrypted by view
      diagnosis: diagnosis  # Auto-decrypted by view
```

**Security Properties**:
- ✅ ClickGraph never sees encryption keys
- ✅ Each hospital/tenant has unique key in ClickHouse
- ✅ Cross-tenant queries automatically fail (wrong key)
- ✅ HIPAA/GDPR compliant by design

### Pattern 4: Hierarchical Tenants

**Use Case**: Parent accounts can access child account data.

```sql
CREATE TABLE tenant_hierarchy (
    tenant_id String,
    parent_id Nullable(String)
) ENGINE = MergeTree()
ORDER BY tenant_id;

CREATE VIEW data_secure AS
WITH RECURSIVE tenant_tree AS (
    -- Start with requested tenant
    SELECT tenant_id FROM tenant_hierarchy 
    WHERE tenant_id = {tenant_id:String}
    
    UNION ALL
    
    -- Include all child tenants
    SELECT t.tenant_id 
    FROM tenant_hierarchy t
    JOIN tenant_tree tt ON t.parent_id = tt.tenant_id
)
SELECT d.* FROM data d
WHERE d.tenant_id IN (SELECT tenant_id FROM tenant_tree);
```

**Result**: Parent tenant sees their data + all children's data.

### Pattern 5: Role-Based Access (Combining with SET ROLE)

**Use Case**: Viewer role sees anonymized data, Admin sees full data.

```sql
-- Parameterized view with conditional anonymization
CREATE VIEW users_by_role AS
SELECT 
    user_id,
    name,
    -- Admin sees real email, Viewer sees anonymized
    multiIf(
        current_roles() LIKE '%admin%', email,
        concat(substring(email, 1, 2), '***@***')
    ) AS email,
    country
FROM users
WHERE tenant_id = {tenant_id:String};
```

```yaml
nodes:
  - label: User
    table: users_by_role
    view_parameters: [tenant_id]
```

**Query with Role**:
```json
{
  "query": "MATCH (u:User) RETURN u.email",
  "view_parameters": {"tenant_id": "acme"},
  "role": "viewer"  // Sets role via SET ROLE
}
```

---

## How It Works

### SQL Generation with Placeholders

ClickGraph generates SQL templates with `$paramName` placeholders instead of literal values:

```
Query: MATCH (u:User) RETURN u.name
view_parameters: {"tenant_id": "acme"}

Generated SQL Template:
SELECT name AS `u.name`
FROM users_by_tenant(tenant_id = $tenant_id) AS u

Runtime Substitution:
SELECT name AS `u.name`
FROM users_by_tenant(tenant_id = 'acme') AS u
```

### Efficient Caching

**Cache Key**: `(query, schema)` - **excludes view_parameters**

**Result**: All tenants share the same cached SQL template!

```
ACME query  → Cache MISS → Generate template → Execute with tenant_id='acme'
                           ↓ (template cached)
GLOBEX query → Cache HIT  → Reuse template   → Execute with tenant_id='globex'
```

**Performance**: 2x faster for cache hits (measured: 18ms → 9ms)

**Memory**: 99% reduction for 100-tenant scenario (1 entry vs 100)

---

## Parameter Resolution

Parameters can come from multiple sources. **Priority order**:

1. **Query parameters** (highest priority) - `{"parameters": {"userId": 123}}`
2. **View parameters** - `{"view_parameters": {"tenant_id": "acme"}}`
3. **Default values** (if defined in ClickHouse view)

**Example**:
```json
{
  "query": "MATCH (u:User) WHERE u.user_id = $userId RETURN u",
  "parameters": {"userId": 5},           // For WHERE clause
  "view_parameters": {"tenant_id": "acme"}  // For parameterized view
}
```

---

## Best Practices

### Security

1. **Always validate tenant_id at the API gateway**
   - Don't trust client-provided tenant_id
   - Use JWT claims or session data to determine tenant

2. **Use ClickHouse roles for additional security**
   ```json
   {
     "view_parameters": {"tenant_id": "acme"},
     "role": "read_only"  // Enforces column-level permissions
   }
   ```

3. **Log tenant_id for audit trails**
   ```sql
   -- Add audit logging in ClickHouse
   INSERT INTO audit_log (tenant_id, user_id, query, timestamp)
   VALUES ({tenant_id:String}, {user_id:UInt64}, {query:String}, now());
   ```

### Performance

1. **Create indexes on tenant_id**
   ```sql
   CREATE TABLE users (
       tenant_id String,
       user_id UInt64,
       ...
   ) ENGINE = MergeTree()
   ORDER BY (tenant_id, user_id);  -- tenant_id first!
   ```

2. **Use ReplacingMergeTree for mutable tenant data**
   ```sql
   CREATE TABLE users (
       tenant_id String,
       user_id UInt64,
       name String,
       version UInt64
   ) ENGINE = ReplacingMergeTree(version)
   ORDER BY (tenant_id, user_id);
   ```

3. **Partition large tables by tenant_id**
   ```sql
   CREATE TABLE events (
       tenant_id String,
       event_id UInt64,
       ...
   ) ENGINE = MergeTree()
   PARTITION BY tenant_id
   ORDER BY (tenant_id, event_id);
   ```

### Schema Design

1. **Keep view_parameters in sync with ClickHouse views**
   ```yaml
   # If ClickHouse view expects: tenant_id, region
   # Schema must declare:
   view_parameters: [tenant_id, region]
   ```

2. **Use descriptive parameter names**
   ```yaml
   # Good
   view_parameters: [tenant_id, start_date, end_date]
   
   # Bad
   view_parameters: [p1, p2, p3]
   ```

3. **Document required parameters**
   ```yaml
   nodes:
     - label: User
       table: users_by_tenant
       view_parameters: [tenant_id]  # REQUIRED: tenant_id must be provided
   ```

---

## Migration Guide

### Adding Multi-Tenancy to Existing Schema

**Step 1**: Create parameterized views in ClickHouse
```sql
-- Existing table
CREATE TABLE users (...) ENGINE = MergeTree() ORDER BY user_id;

-- New parameterized view (non-breaking)
CREATE VIEW users_by_tenant AS
SELECT * FROM users
WHERE tenant_id = {tenant_id:String};
```

**Step 2**: Update ClickGraph schema (backward compatible)
```yaml
nodes:
  - label: User
    table: users_by_tenant           # Changed from 'users'
    view_parameters: [tenant_id]      # Added
    node_id: user_id
    # ... rest unchanged
```

**Step 3**: Update application queries
```diff
  POST /query
  {
    "query": "MATCH (u:User) RETURN u",
-   "schema_name": "default"
+   "schema_name": "default",
+   "view_parameters": {"tenant_id": "acme"}
  }
```

**Step 4**: Deploy and test
- Old queries without `view_parameters` will generate SQL without tenant filtering
- New queries with `view_parameters` get tenant isolation
- No downtime required!

---

## Troubleshooting

### Query returns empty results

**Problem**: Query succeeds but returns no data.

**Solution**: Check that `view_parameters` match your data:
```bash
# Check what tenant_id values exist in your data
clickhouse-client --query "SELECT DISTINCT tenant_id FROM users"

# Verify view works manually
clickhouse-client --query "SELECT * FROM users_by_tenant(tenant_id = 'acme')"
```

### "Table not found" error

**Problem**: ClickHouse can't find the parameterized view.

**Solution**: Verify view exists and has correct syntax:
```sql
-- List all views
SHOW TABLES LIKE '%by_tenant%';

-- Check view definition
SHOW CREATE TABLE users_by_tenant;

-- Correct syntax requires curly braces for parameters:
WHERE tenant_id = {tenant_id:String}  ✅
WHERE tenant_id = $tenant_id          ❌ (wrong, use curly braces)
```

### Slow queries after enabling multi-tenancy

**Problem**: Queries are slower with parameterized views.

**Solution**: Ensure proper indexing:
```sql
-- Add tenant_id to ORDER BY (first position)
CREATE TABLE users (
    tenant_id String,
    user_id UInt64,
    ...
) ENGINE = MergeTree()
ORDER BY (tenant_id, user_id);  -- tenant_id must be first!

-- Check query plan
EXPLAIN SELECT * FROM users_by_tenant(tenant_id = 'acme');
```

### Parameters not substituted

**Problem**: SQL contains `$tenant_id` instead of actual value.

**Solution**: Check parameter name spelling in view_parameters:
```yaml
# Schema declares:
view_parameters: [tenant_id]

# Query must match exactly:
view_parameters: {"tenant_id": "acme"}  ✅
view_parameters: {"tenantId": "acme"}   ❌ (wrong case)
```

---

## API Reference

### HTTP API

**Endpoint**: `POST /query`

**Request Body**:
```typescript
{
  query: string;                        // Cypher query
  schema_name?: string;                 // Schema to use (default: "default")
  parameters?: Record<string, any>;     // Query parameters ($userId, etc.)
  view_parameters?: Record<string, any>; // View parameters (tenant_id, etc.)
  role?: string;                        // ClickHouse role (optional)
  sql_only?: boolean;                   // Return SQL without executing
}
```

**Example**:
```json
{
  "query": "MATCH (u:User) WHERE u.user_id = $userId RETURN u",
  "schema_name": "default",
  "parameters": {"userId": 123},
  "view_parameters": {"tenant_id": "acme"},
  "role": "viewer"
}
```

### Bolt Protocol (Neo4j Drivers)

**Python**:
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run(
        "MATCH (u:User) RETURN u",
        tenant_id="acme",  # Passed as view parameter
        userId=123         # Passed as query parameter
    )
```

**JavaScript**:
```javascript
const neo4j = require('neo4j-driver');
const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();

const result = await session.run(
  'MATCH (u:User) RETURN u',
  { tenant_id: 'acme', userId: 123 }
);
```

---

## Examples

Complete example schemas are available in:
- `schemas/examples/multi_tenant_simple.yaml` - Basic tenant isolation
- `schemas/examples/multi_tenant_encrypted.yaml` - Per-tenant encryption
- `schemas/test/multi_tenant.yaml` - Test schema with sample data

---

## FAQ

**Q: Can I use multiple parameters in one view?**  
A: Yes! Declare them all in `view_parameters: [tenant_id, region, start_date]`

**Q: What happens if I don't provide required parameters?**  
A: ClickGraph generates SQL without the parameter substitution. Query may return unexpected results or error.

**Q: Can I use view_parameters with non-parameterized views?**  
A: Yes. If schema doesn't declare `view_parameters`, they're ignored (no error).

**Q: How does this compare to application-level filtering?**  
A: ClickHouse views are faster (native optimization) and more secure (can't be bypassed).

**Q: Can I combine with ClickHouse roles (SET ROLE)?**  
A: Yes! Use `role` field for column-level permissions + `view_parameters` for row-level filtering.

**Q: Does caching work with multiple tenants?**  
A: Yes! All tenants share the same cached SQL template (99% memory reduction).

**Q: Can I use this for time-based access control?**  
A: Yes! Create a view with date parameters and use `toDate(now())` comparisons.

---

## Next Steps

- **Tutorial**: [Building a Multi-Tenant SaaS App](./tutorials/multi-tenant-saas.md)
- **Security Guide**: [Securing Multi-Tenant Deployments](./security-guide.md)
- **Performance**: [Optimizing Large-Scale Multi-Tenancy](./performance-tuning.md)
- **Examples**: Browse `schemas/examples/` for more patterns

---

**Questions?** Open an issue on GitHub or join our Discord community.
