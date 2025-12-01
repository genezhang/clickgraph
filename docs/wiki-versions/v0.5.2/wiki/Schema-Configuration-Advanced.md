> **Note**: This documentation is for ClickGraph v0.5.2. [View latest docs →](../../wiki/Home.md)
# Schema Configuration: Advanced Guide

Comprehensive guide to advanced schema configuration, optimization, and best practices for ClickGraph.

## Table of Contents
- [Schema Architecture](#schema-architecture)
- [Advanced Property Mappings](#advanced-property-mappings)
- [Multi-Schema Management](#multi-schema-management)
- [Dynamic Schema Features](#dynamic-schema-features)
- [Schema Validation](#schema-validation)
- [Performance Optimization](#performance-optimization)
- [Advanced Use Cases](#advanced-use-cases)
- [Migration Strategies](#migration-strategies)

---

## Schema Architecture

### Understanding ClickGraph's View-Based Model

ClickGraph uses a **view-based architecture** - it doesn't require special graph tables. Instead, it maps your existing ClickHouse tables to graph entities.

```mermaid
graph TD
    A[ClickGraph Schema YAML] -->|Defines| B[Nodes, Edges, Properties]
    B -->|Translates| C[Query Planner & Optimizer]
    C -->|Generates| D[ClickHouse SQL]
    D -->|Executes on| E[ClickHouse Tables]
    E -->|MergeTree, ReplacingMT| F[Standard Tables]
    
    style A fill:#e1f5ff
    style C fill:#fff4e1
    style E fill:#e8f5e9
```

**Key Principles**:
1. **Non-invasive**: Works with existing tables
2. **Flexible**: Multiple views of same data
3. **Optimizable**: Leverage ClickHouse performance
4. **Multi-tenant**: Schema-level isolation

### Schema File Structure

```yaml
# Complete schema structure
name: production_graph
version: "1.0"
description: "Production social network graph"

graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users_bench
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        email: email_address
        registration_date: registration_date
        is_active: is_active
        country: country
        city: city
    
    - label: Post
      database: brahmand
      table: posts_bench
      id_column: post_id
      property_mappings:
        post_id: post_id
        title: post_title
        content: post_content
        date: post_date
  
  edges:
    - type: FOLLOWS
      database: brahmand
      table: user_follows_bench
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User
      property_mappings:
        follow_date: follow_date
    
    - type: AUTHORED
      database: brahmand
      table: posts_bench
      from_id: author_id
      to_id: post_id
      from_node: User
      to_node: Post
      property_mappings:
        post_date: post_date
```

---

## Advanced Property Mappings

### 1. Computed Properties

**Scenario**: Derive properties from expressions.

```yaml
nodes:
  - label: User
    database: brahmand
    table: users
    id_column: user_id
    property_mappings:
      user_id: user_id
      name: full_name
      
      # Computed: Full name from first + last
      display_name: "concat(first_name, ' ', last_name)"
      
      # Computed: Age from birth_date
      age: "dateDiff('year', birth_date, today())"
      
      # Computed: Account age in days
      account_age: "dateDiff('day', registration_date, today())"
      
      # Computed: Active status
      # ⚠️ Conditionals not yet supported - use query time:
      # is_active: "last_login_date >= today() - INTERVAL 30 DAY"
```

**Usage**:
```cypher
MATCH (u:User)
WHERE u.age >= 18 AND u.is_active = true
RETURN u.display_name, u.account_age
```

**Generated SQL**:
```sql
SELECT 
    concat(first_name, ' ', last_name) AS display_name,
    dateDiff('day', registration_date, today()) AS account_age
FROM users
WHERE dateDiff('year', birth_date, today()) >= 18
  AND (last_login_date >= today() - INTERVAL 30 DAY) = 1
```

### 2. Type Conversions

```yaml
nodes:
  User:
    property_mappings:
      user_id: "user_id"
      
      # String to Date
      registration_date: "toDate(registration_date_str)"
      
      # String to Number
      age: "toUInt8(age_str)"
      
      # JSON parsing
      metadata: "JSONExtractString(metadata_json, 'key')"
      
      # Array from comma-separated
      tags: "splitByChar(',', tags_str)"
```

### 3. Filters on Nodes and Edges

**Feature**: Apply static SQL filters to node or edge definitions to pre-filter data at the schema level.

```yaml
nodes:
  - label: ActiveUser
    database: brahmand
    table: users
    id_column: user_id
    filter: "is_active = 1 AND deleted_at IS NULL"  # Only active, non-deleted users
    property_mappings:
      user_id: user_id
      name: full_name

  - label: RecentPost
    database: brahmand
    table: posts
    id_column: post_id
    filter: "post_date >= today() - INTERVAL 30 DAY"  # Only posts from last 30 days
    property_mappings:
      post_id: post_id
      title: title

edges:
  - type: RECENT_FOLLOWS
    database: brahmand
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    from_node: User
    to_node: User
    filter: "follow_date >= today() - INTERVAL 7 DAY"  # Only follows from last week
    property_mappings:
      follow_date: follow_date
```

**Usage**:
```cypher
-- Query only returns active users (filter applied automatically)
MATCH (u:ActiveUser)
RETURN u.name
```

**Generated SQL**:
```sql
SELECT full_name AS name
FROM users
WHERE is_active = 1 AND deleted_at IS NULL
```

**Benefits**:
- ✅ Pre-filter data at schema level (no need to repeat in every query)
- ✅ Improves query performance (filter applied at table scan)
- ✅ Create multiple "views" of same table with different filters

### 4. Edge Properties

```yaml
edges:
  - type: FOLLOWS
    database: brahmand
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    from_node: User
    to_node: User
    property_mappings:
      since: follow_date
      duration: "dateDiff('day', follow_date, today())"
      # Mathematical expressions are supported:
      strength: "interaction_count / 100.0"
```

**Usage**:
```cypher
MATCH (u1:User)-[r:FOLLOWS]->(u2:User)
WHERE r.is_recent = true AND r.strength > 0.5
RETURN u1.name, u2.name, r.duration
```

---

## Multi-Schema Management

### 1. Multiple Schemas in Production

**Use Cases**:
- Multi-tenancy (per-tenant schemas)
- Different data environments (staging, production)
- Multiple graph models (social, commerce, knowledge)

**Architecture**:
```yaml
# Schema 1: Social Network (schemas/social.yaml)
name: social_graph
graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users
      id_column: user_id
      property_mappings:
        name: full_name
    - label: Post
      database: brahmand
      table: posts
      id_column: post_id

---
# Schema 2: E-commerce (schemas/commerce.yaml)
name: commerce_graph
graph_schema:
  nodes:
    - label: Customer
      database: brahmand
      table: customers
      id_column: customer_id
    - label: Product
      database: brahmand
      table: products
      id_column: product_id
    - label: Order
      database: brahmand
      table: orders
      id_column: order_id
```

### 2. Schema Selection via USE Clause

```cypher
-- Select schema explicitly
USE social_graph;
MATCH (u:User) RETURN u.name LIMIT 10;

-- Switch to different schema
USE commerce_graph;
MATCH (c:Customer) RETURN c.name LIMIT 10;
```

### 3. Per-Request Schema Selection

```bash
# Via HTTP API
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u.name LIMIT 10",
    "schema_name": "social_graph"
  }'
```

```python
# Via Python client
import requests

def query_with_schema(query: str, schema: str):
    response = requests.post('http://localhost:8080/query', json={
        'query': query,
        'schema_name': schema
    })
    return response.json()

# Query social graph
result = query_with_schema(
    "MATCH (u:User) RETURN count(u)",
    "social_graph"
)

# Query commerce graph
result = query_with_schema(
    "MATCH (c:Customer) RETURN count(c)",
    "commerce_graph"
)
```

### 4. Schema Registration

**At Server Startup**:
```bash
# Load multiple schemas
export GRAPH_CONFIG_PATH="schemas/social.yaml,schemas/commerce.yaml,schemas/knowledge.yaml"

# Or via CLI
clickgraph \
  --schema schemas/social.yaml \
  --schema schemas/commerce.yaml \
  --schema schemas/knowledge.yaml
```

**Dynamic Registration** (via HTTP API):
```bash
# Load schema from file
curl -X POST http://localhost:8080/schemas/load \
  -H "Content-Type: application/json" \
  -d "$(jq -Rs '{schema_name: "new_graph", config_content: ., validate_schema: true}' new_schema.yaml)"

# Or with inline YAML content
curl -X POST http://localhost:8080/schemas/load \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "new_graph",
    "config_content": "name: new_graph\ngraph_schema:\n  nodes: ...",
    "validate_schema": true
  }'
```

---

## Dynamic Schema Features

### 1. Parameterized Views (Multi-Tenancy)

ClickGraph supports multi-tenant architectures through **ClickHouse parameterized views**. See [Multi-Tenancy Patterns](Multi-Tenancy-Patterns.md) for full details.

**Step 1: Create Parameterized Views in ClickHouse**:
```sql
-- Base table with tenant data
CREATE TABLE users (
    user_id UInt64,
    tenant_id String,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY (tenant_id, user_id);

-- Parameterized view for tenant isolation
CREATE VIEW users_by_tenant AS
SELECT * FROM users
WHERE tenant_id = {tenant_id:String};
```

**Step 2: Configure Schema with view_parameters**:
```yaml
graph_schema:
  database: my_database
  
  nodes:
    - label: User
      table: users_by_tenant          # Reference the parameterized view
      view_parameters: [tenant_id]     # Declare required parameters
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: name
        email: email

  edges:
    - type: PLACED
      table: orders_by_tenant         # Also parameterized
      view_parameters: [tenant_id]
      from_id: user_id
      to_id: order_id
      from_node: User
      to_node: Order
```

**Step 3: Query with Tenant Context**:
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

**Generated SQL** (tenant filter applied automatically):
```sql
SELECT name AS `u.name`, email AS `u.email`
FROM users_by_tenant(tenant_id = 'acme-corp') AS u
```

**Benefits**:
- ✅ Row-level security at database level
- ✅ Single cache entry shared across all tenants
- ✅ Native ClickHouse performance

### 2. Time-Based Filtering

**Use Case**: Query only recent data using parameterized views.

**ClickHouse View**:
```sql
CREATE VIEW posts_recent AS
SELECT * FROM posts
WHERE post_date >= today() - INTERVAL {days_back:UInt32} DAY;
```

**Schema Configuration**:
```yaml
graph_schema:
  nodes:
    - label: Post
      table: posts_recent
      id_column: post_id
      view_parameters: [days_back]
      property_mappings:
        post_id: post_id
        content: post_content
        post_date: post_date
```

**Usage**:
```json
{
  "query": "MATCH (p:Post) RETURN count(p)",
  "view_parameters": {"days_back": "7"}
}
```

### 3. Region-Based Filtering

**Use Case**: Filter data by geographic region.

**Schema Configuration with Static Filter**:
```yaml
graph_schema:
  nodes:
    - label: USUser
      table: users
      id_column: user_id
      filter: "country IN ('US', 'CA', 'MX')"  # North America only
      property_mappings:
        user_id: user_id
        name: full_name
        country: country
        
    - label: EUUser
      table: users
      id_column: user_id
      filter: "country IN ('DE', 'FR', 'UK', 'ES', 'IT')"  # Europe only
      property_mappings:
        user_id: user_id
        name: full_name
        country: country
```

---

## Schema Validation

### 1. Validation at Load Time

ClickGraph validates schemas when loading:

```bash
# Load schema with validation
export GRAPH_CONFIG_PATH="schemas/my_schema.yaml"
cargo run --bin clickgraph

# Output if valid:
[INFO] Schema loaded successfully: my_schema
[INFO] Nodes: 5, Edges: 8
[INFO] Validation: PASSED

# Output if invalid:
[ERROR] Schema validation failed: my_schema
[ERROR] Node 'User' references undefined source_table 'users_table'
[ERROR] Edge 'FOLLOWS' references undefined node 'UserProfile'
```

### 2. Common Validation Errors

**Missing Table**:
```yaml
nodes:
  - label: User
    database: brahmand
    table: users_bench  # Table must exist in ClickHouse
    id_column: user_id
```

**Undefined Node Reference**:
```yaml
edges:
  - type: FOLLOWS
    from_node: User        # Must be defined in nodes
    to_node: UserProfile   # ❌ ERROR: Not defined!
```

**Missing ID Column**:
```yaml
nodes:
  - label: User
    table: users
    # ❌ ERROR: id_column required
    property_mappings:
      name: full_name
```

**Invalid Property Mapping**:
```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    property_mappings:
      user_id: user_id
      name: nonexistent_column  # ⚠️ Warning: Column doesn't exist
```

### 3. Schema Testing Script

```python
# test_schema.py
import yaml
import requests
import sys

def validate_schema(schema_path: str):
    """Validate schema by loading and testing queries."""
    
    # 1. Parse YAML
    with open(schema_path) as f:
        schema = yaml.safe_load(f)
    
    print(f"✓ YAML syntax valid")
    
    # 2. Check required fields
    assert 'graph_schema' in schema, "Missing 'graph_schema' key"
    assert 'nodes' in schema['graph_schema'], "Missing 'nodes'"
    print(f"✓ Required fields present")
    
    # 3. Test basic query
    response = requests.post('http://localhost:8080/query', json={
        'query': 'MATCH (n) RETURN count(n) LIMIT 1'
    })
    
    if response.status_code == 200:
        print(f"✓ Schema loaded successfully")
        print(f"  Result: {response.json()}")
    else:
        print(f"✗ Schema validation failed: {response.text}")
        sys.exit(1)

if __name__ == '__main__':
    validate_schema(sys.argv[1])
```

**Usage**:
```bash
python test_schema.py schemas/my_schema.yaml
```

---

## Performance Optimization

### 1. Indexed Property Selection

**Choose id_column wisely**:

```yaml
# ✅ GOOD: Primary key is user_id
nodes:
  - label: User
    table: users
    id_column: user_id  # Indexed column
    property_mappings:
      user_id: user_id

# ❌ BAD: Email is not indexed
nodes:
  - label: User
    table: users
    id_column: email  # Not indexed!
```

**Impact**:
```cypher
-- With user_id (indexed): ~1ms
MATCH (u:User {user_id: 1}) RETURN u.name

-- With email (not indexed): ~100ms
MATCH (u:User {email: 'alice@example.com'}) RETURN u.name
```

### 2. Bidirectional Edge Optimization

**Create Reverse Indexes**:

```yaml
# Forward edge
edges:
  - type: FOLLOWS
    table: user_follows
    from_id: follower_id  # Indexed
    to_id: followed_id
    from_node: User
    to_node: User

  # Reverse edge (uses materialized view)
  - type: FOLLOWED_BY
    table: user_follows_reverse  # Materialized view
    from_id: followed_id  # Indexed
    to_id: follower_id
    from_node: User
    to_node: User
```
**ClickHouse Setup**:
```sql
-- Materialized view for reverse lookups
CREATE MATERIALIZED VIEW user_follows_reverse
ENGINE = MergeTree()
ORDER BY (followed_id, follower_id)
AS SELECT followed_id, follower_id, follow_date
FROM user_follows;
```

**Performance**:
```cypher
-- Forward: Uses user_follows (fast)
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(friend)
RETURN friend.name

-- Backward: Uses user_follows_reverse (also fast!)
MATCH (u:User {user_id: 1})<-[:FOLLOWED_BY]-(follower)
RETURN follower.name
```

### 3. Property Projection Optimization

**Only map properties you need**:

```yaml
# ❌ BAD: Map all columns (bloats results)
nodes:
  - label: User
    table: users
    id_column: user_id
    property_mappings:
      user_id: user_id
      name: full_name
      email: email_address
      bio: biography
      avatar: avatar_url
      created_at: registration_date
      # ... 50 more columns

# ✅ GOOD: Map only commonly used properties
nodes:
  - label: User
    table: users
    id_column: user_id
    property_mappings:
      user_id: user_id
      name: full_name
      email: email_address
      # Query-specific properties can be added later
```

### 4. Filter Pushdown

**Use filters in schema**:

```yaml
graph_schema:
  nodes:
    - label: ActiveUser
      table: users
      id_column: user_id
      filter: "is_active = 1 AND deleted_at IS NULL"
      property_mappings:
        user_id: user_id
        name: full_name
```

**Benefit**: Filters applied at table scan level (fastest).

---

## Advanced Use Cases

### 1. Temporal Graphs

**Model time-varying connections**:

```yaml
edges:
  - type: EMPLOYED_BY
    table: employment_history
    from_id: person_id
    to_id: company_id
    from_node: Person
    to_node: Company
    property_mappings:
      start_date: start_date
      end_date: end_date
      title: job_title
```

**Queries**:
```cypher
-- Current employment (end_date is NULL)
MATCH (p:Person)-[r:EMPLOYED_BY]->(c:Company)
WHERE r.end_date IS NULL
RETURN p.name, c.name, r.title

-- Employment during specific period
MATCH (p:Person)-[r:EMPLOYED_BY]->(c:Company)
WHERE r.start_date <= '2023-01-01' 
  AND (r.end_date IS NULL OR r.end_date >= '2023-01-01')
RETURN p.name, c.name
```

### 2. Hierarchical Data

**Model tree structures**:

```yaml
edges:
  - type: REPORTS_TO
    table: employee_hierarchy
    from_id: employee_id
    to_id: manager_id
    from_node: Employee
    to_node: Employee
    property_mappings:
      since: reporting_start_date
```

**Queries**:
```cypher
-- Direct reports
MATCH (manager:Employee {employee_id: 1})<-[:REPORTS_TO]-(report:Employee)
RETURN report.name

-- Entire reporting chain
MATCH path = (employee:Employee {employee_id: 42})-[:REPORTS_TO*]->(ceo:Employee)
WHERE NOT (ceo)-[:REPORTS_TO]->()
RETURN [n IN nodes(path) | n.name] AS chain
```

### 3. Weighted Graphs

**Model edge weights**:

```yaml
edges:
  - type: SIMILAR_TO
    table: product_similarity
    from_id: product_a_id
    to_id: product_b_id
    from_node: Product
    to_node: Product
    property_mappings:
      similarity: similarity_score
      method: similarity_method
```

**Queries**:
```cypher
-- High similarity products
MATCH (p1:Product {product_id: 123})-[r:SIMILAR_TO]->(p2:Product)
WHERE r.similarity > 0.8
RETURN p2.name, r.similarity
ORDER BY r.similarity DESC
LIMIT 10
```

### 4. Multi-Modal Graphs

**Mix different entity types**:

```yaml
graph_schema:
  nodes:
    - label: Person
      table: persons
      id_column: person_id
      
    - label: Company
      table: companies
      id_column: company_id
      
    - label: Location
      table: locations
      id_column: location_id
      
    - label: Skill
      table: skills
      id_column: skill_id
  
  edges:
    - type: WORKS_AT
      table: employment
      from_id: person_id
      to_id: company_id
      from_node: Person
      to_node: Company
      
    - type: LOCATED_IN
      table: company_locations
      from_id: company_id
      to_id: location_id
      from_node: Company
      to_node: Location
      
    - type: HAS_SKILL
      table: person_skills
      from_id: person_id
      to_id: skill_id
      from_node: Person
      to_node: Skill
```

**Queries**:
```cypher
-- Find Python developers in San Francisco companies
MATCH (p:Person)-[:HAS_SKILL]->(s:Skill {name: 'Python'})
MATCH (p)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(l:Location {city: 'San Francisco'})
RETURN p.name, c.name
```

---

## Migration Strategies

### 1. Schema Versioning

**Version Your Schemas**:
```yaml
name: social_graph
version: "2.0"  # Increment on breaking changes

graph_schema:
  nodes:
    # ... your nodes
```

### 2. Backward Compatible Changes

**✅ Safe Changes** (non-breaking):
- Add new nodes
- Add new edges
- Add new properties to existing nodes

```yaml
# v1.0 → v1.1 (backward compatible)
graph_schema:
  nodes:
    - label: User
      table: users
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        email: email_address
        # NEW in v1.1
        phone: phone_number  # ✅ Safe: Existing queries still work
```

**❌ Breaking Changes**:
- Rename nodes or edge types
- Remove properties
- Change id_column
- Change edge direction

```yaml
# v1.0 → v2.0 (breaking!)
nodes:
  - label: User
    table: users
    id_column: email  # ❌ Breaking: Was "user_id"
```

### 3. Blue-Green Schema Deployment

```bash
# Step 1: Deploy new schema alongside old
export GRAPH_CONFIG_PATH="schemas/v1.yaml,schemas/v2.yaml"

# Step 2: Gradually migrate clients
# Old clients: USE schema_v1
# New clients: USE schema_v2

# Step 3: Remove old schema when all clients migrated
export GRAPH_CONFIG_PATH="schemas/v2.yaml"
```

### 4. Schema Migration Script

```python
# migrate_schema.py
import yaml

def migrate_v1_to_v2(v1_path: str, v2_path: str):
    """Migrate schema from v1 to v2."""
    
    with open(v1_path) as f:
        v1 = yaml.safe_load(f)
    
    # Add version info
    v1['version'] = '2.0'
    
    # Add new properties to User node
    for node in v1['graph_schema']['nodes']:
        if node['label'] == 'User':
            node['property_mappings']['phone'] = 'phone_number'
    
    # Write v2
    with open(v2_path, 'w') as f:
        yaml.dump(v1, f, default_flow_style=False)
    
    print(f"✓ Migrated {v1_path} → {v2_path}")

if __name__ == '__main__':
    migrate_v1_to_v2('schemas/social_v1.yaml', 'schemas/social_v2.yaml')
```

---

## Schema Best Practices Checklist

**Planning**:
- [ ] Identify core entities (become nodes)
- [ ] Identify edges (foreign keys → edges)
- [ ] Choose meaningful identifier properties (primary keys)
- [ ] Document schema purpose and version

**Performance**:
- [ ] Use indexed columns as identifier_property
- [ ] Create reverse indexes for bidirectional edges
- [ ] Map only commonly used properties
- [ ] Add filters for common query patterns
- [ ] Use parameterized views for multi-tenancy

**Maintenance**:
- [ ] Version your schemas (semantic versioning)
- [ ] Document breaking vs. non-breaking changes
- [ ] Test schemas with validation script
- [ ] Use git for schema version control
- [ ] Plan migration strategy for breaking changes

**Security**:
- [ ] Use view_parameters for tenant isolation
- [ ] Add row-level filters for sensitive data
- [ ] Document access control requirements
- [ ] Review property exposure (PII, sensitive data)

---

## Next Steps

Now that you understand advanced schema configuration:

- **[Multi-Tenancy & RBAC](Multi-Tenancy-RBAC.md)** - Implement tenant isolation
- **[Schema Configuration Basics](Schema-Basics.md)** - Review fundamentals
- **[Performance Optimization](Performance-Query-Optimization.md)** - Optimize queries
- **[Migration Guide](Migration-from-Neo4j.md)** - Migrate from other systems

---

[← Back: Schema Basics](Schema-Basics.md) | [Home](Home.md) | [Next: Multi-Tenancy →](Multi-Tenancy-RBAC.md)
