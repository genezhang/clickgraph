# ClickGraph Schema Reference

Complete reference for YAML schema configuration, covering all attributes for nodes and edges.

## Key Differentiators

**🔗 Edge Constraints** - ClickGraph supports cross-node validation rules in relationship traversals. Define temporal ordering, referential integrity, or custom business logic directly in your schema:

```yaml
edges:
  - type_name: FILE_COPY
    from_node: File
    to_node: File
    constraints: "from.timestamp <= to.timestamp"  # Temporal ordering enforced
```

Constraints work across all query types (single-hop, variable-length paths, shortest path) and all schema patterns (standard, polymorphic, denormalized, FK-edge).

---

## Overview

ClickGraph schemas map existing ClickHouse tables to graph entities (nodes and edges). Schemas are defined in YAML files and support several patterns:

1. **Standard Pattern**: Separate tables for each node type and edge type
2. **Polymorphic Pattern**: Single table stores multiple edge/node types with discriminator columns  
3. **Denormalized Pattern**: Node properties embedded in edge tables (like OnTime flights)
4. **FK-Edge Pattern**: Edges represented by foreign key columns (no separate edge table)

## Schema File Structure

### Single Schema Format (Traditional)

```yaml
# Top-level structure
name: <schema_name>        # Optional: Schema identifier

graph_schema:
  nodes:                   # List of node definitions
    - label: <NodeLabel>
      # ... node attributes
      
  edges:                   # Preferred: New edge definition format
    - type_name: <EdgeType>
      # ... edge attributes (standard)
    - polymorphic: true
      # ... edge attributes (polymorphic)
```

### Multi-Schema Format (NEW in v0.6.1)

Load multiple independent graph schemas from a single YAML file:

```yaml
# Define default schema (used when no USE clause specified)
default_schema: social_network

schemas:
  # First schema
  - name: social_network
    graph_schema:
      nodes:
        - label: User
          database: social_db
          table: users
          node_id: user_id
          property_mappings:
            user_id: user_id
            name: name
      edges:
        - type: FOLLOWS
          database: social_db
          table: follows
          from_id: follower_id
          to_id: followed_id
          from_node: User
          to_node: User

  # Second schema (completely independent)
  - name: security_logs
    graph_schema:
      nodes:
        - label: IP
          database: security
          table: connections
          node_id: ip_address
          property_mappings:
            ip: ip_address
      edges:
        - type: CONNECTED_TO
          database: security
          table: connections
          from_id: source_ip
          to_id: dest_ip
          from_node: IP
          to_node: IP
```

**Key Features**:
- **default_schema**: Sets which schema is used when no `USE` clause is specified
- **Schema Isolation**: Each schema has independent node labels and edge types
- **Schema Selection**: Use `USE <schema_name>` clause to switch between schemas
- **Automatic Alias**: A `default` schema alias is created pointing to `default_schema`

**Usage Example**:
```cypher
-- Query social_network schema
USE social_network
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u.name, f.name

-- Switch to security_logs schema
USE security_logs
MATCH (ip1:IP)-[:CONNECTED_TO]->(ip2:IP)
RETURN ip1.ip, ip2.ip

-- Use default schema (no USE clause)
MATCH (u:User) RETURN count(u)
```

---

## Node Definition Attributes

### Required Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `label` | string | Node label (e.g., "User", "Airport") |
| `database` | string | ClickHouse database name |
| `table` | string | Source table name |
| `node_id` | string | Column used as node identifier |

### Optional Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `property_mappings` | object | `{}` | Maps Cypher property names to ClickHouse column names |
| `view_parameters` | list | `null` | Parameter names for parameterized views |
| `use_final` | bool | `null` | Override FINAL keyword usage (auto-detect if null) |
| `filter` | string | `null` | SQL predicate filter applied to all queries |
| `auto_discover_columns` | bool | `false` | Auto-map all table columns as properties |
| `exclude_columns` | list | `[]` | Columns to exclude from auto-discovery |
| `naming_convention` | string | `"snake_case"` | Property naming: "snake_case" or "camelCase" |

### Shared Table Attributes (for label_column pattern)

When multiple node types share a single table, use these to distinguish them:

| Attribute | Type | Description |
|-----------|------|-------------|
| `label_column` | string | Column containing node type discriminator |
| `label_value` | string | Value in label_column for this node type |

### Denormalized Node Attributes

For nodes whose properties exist in edge tables (OnTime pattern):

| Attribute | Type | Description |
|-----------|------|-------------|
| `from_node_properties` | object | Property mappings when node is source (from_node) |
| `to_node_properties` | object | Property mappings when node is target (to_node) |

---

## Edge Definition Attributes

### Standard Edge (type_name pattern)

For edges with their own dedicated table.

#### Required Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `type_name` | string | Edge type (e.g., "FOLLOWS", "LIKES") |
| `database` | string | ClickHouse database name |
| `table` | string | Edge table name |
| `from_node` | string | Source node label |
| `to_node` | string | Target node label |
| `from_id` | string | Column for source node ID |
| `to_id` | string | Column for target node ID |

#### Optional Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `property_mappings` | object | `{}` | Maps Cypher property names to columns |
| `edge_id` | object | `null` | Composite edge identifier for uniqueness |
| `view_parameters` | list | `null` | Parameter names for parameterized views |
| `use_final` | bool | `null` | Override FINAL keyword usage |
| `filter` | string | `null` | SQL predicate filter |
| `constraints` | string | `null` | Cross-node validation expression (e.g., `"from.timestamp <= to.timestamp"`) |

#### Edge Constraints

Edge constraints enable validation rules that span both source and target nodes. They are compiled into SQL and added to JOIN conditions or WHERE clauses.

**Syntax**: Boolean expression using `from.` and `to.` prefixes to reference node properties.

**Example**:
```yaml
edges:
  - type_name: COPIED_BY
    database: lineage
    table: file_lineage
    from_node: DataFile
    to_node: DataFile
    from_id: source_file_id
    to_id: target_file_id
    constraints: "from.timestamp <= to.timestamp"  # Temporal ordering
```

**Supported Operators**: `<`, `<=`, `>`, `>=`, `=`, `!=`, `AND`, `OR`

**Compilation**: 
- Property names (`from.timestamp`, `to.timestamp`) are resolved using node property_mappings
- Result: `f.created_timestamp <= t.created_timestamp` (in SQL)
- For single-hop: Added to target node JOIN ON clause
- For variable-length paths: Added to CTE WHERE clauses (base and recursive)

**Current Limitations**:
- Directional only (applies to specific from_node → to_node direction)
- Single relationship type per constraint
- No support for complex expressions (subqueries, aggregations)

---

### Polymorphic Edge (polymorphic: true pattern)

For edges stored in a shared table with discriminator columns.

#### Required Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `polymorphic` | bool | Must be `true` |
| `database` | string | ClickHouse database name |
| `table` | string | Edge table name |
| `from_id` | string | Column for source node ID |
| `to_id` | string | Column for target node ID |
| `type_column` | string | Column containing edge type discriminator |
| `type_values` | list | List of edge types in this table |

#### Endpoint Configuration

Polymorphic edges can have **fixed** or **polymorphic** endpoints. For each side (from/to), use one of:

**Fixed endpoint** (always same node type):
```yaml
from_node: User           # Fixed source: always User nodes
to_node: Group            # Fixed target: always Group nodes
```

**Polymorphic endpoint** (varies by row):
```yaml
from_label_column: member_type    # Column containing source node type
from_label_values: [User, Group]  # Valid source node types (closed-world)

to_label_column: target_type      # Column containing target node type  
to_label_values: [Folder, File]   # Valid target node types (closed-world)
```

#### Polymorphic Endpoint Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `from_node` | string | Fixed source node label (mutually exclusive with from_label_column) |
| `to_node` | string | Fixed target node label (mutually exclusive with to_label_column) |
| `from_label_column` | string | Column containing source node type |
| `to_label_column` | string | Column containing target node type |
| `from_label_values` | list | Valid labels for polymorphic source (for validation) |
| `to_label_values` | list | Valid labels for polymorphic target (for validation) |

#### Optional Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `property_mappings` | object | `{}` | Property mappings for all edge types |
| `edge_id` | object | `null` | Composite edge identifier |
| `view_parameters` | list | `null` | Parameter names for parameterized views |
| `use_final` | bool | `null` | Override FINAL keyword usage |
| `filter` | string | `null` | SQL predicate filter |

---

## Schema Pattern Examples

### 1. Standard Pattern

Separate tables for nodes and edges:

```yaml
name: social_network

graph_schema:
  nodes:
    - label: User
      database: social
      table: users
      node_id: user_id
      property_mappings:
        name: full_name       # u.name → users.full_name
        email: email_address  # u.email → users.email_address
        
    - label: Post
      database: social
      table: posts
      node_id: post_id
      property_mappings:
        title: title
        content: body
        
  edges:
    - type_name: FOLLOWS
      database: social
      table: user_follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings:
        since: follow_date
```

### 2. Polymorphic Edge Pattern

Multiple edge types in single table with discriminator:

```yaml
name: activity_graph

graph_schema:
  nodes:
    - label: User
      database: activity
      table: users
      node_id: user_id
      
    - label: Post
      database: activity
      table: posts
      node_id: post_id
      
  edges:
    - polymorphic: true
      database: activity
      table: interactions
      from_id: actor_id
      to_id: target_id
      type_column: interaction_type
      type_values: [LIKED, COMMENTED, SHARED]
      from_node: User         # Fixed: User is always source
      to_node: Post           # Fixed: Post is always target
```

### 3. Polymorphic Endpoints Pattern

When source/target node type varies:

```yaml
name: security_graph

graph_schema:
  nodes:
    - label: User
      database: security
      table: sec_users
      node_id: user_id
      
    - label: Group
      database: security  
      table: sec_groups
      node_id: group_id
      
    - label: Folder
      database: security
      table: sec_fs_objects
      node_id: object_id
      label_column: fs_type      # Discriminator column
      label_value: Folder        # This row represents a Folder
      
    - label: File
      database: security
      table: sec_fs_objects
      node_id: object_id
      label_column: fs_type
      label_value: File
      
  edges:
    # MEMBER_OF: (User or Group)-[:MEMBER_OF]->(Group)
    - polymorphic: true
      database: security
      table: sec_memberships
      from_id: member_id
      to_id: group_id
      type_column: membership_type
      type_values: [MEMBER_OF]
      from_label_column: member_type     # Varies: User or Group
      from_label_values: [User, Group]   # Valid source types
      to_node: Group                     # Fixed: always Group
      
    # CONTAINS: (Folder)-[:CONTAINS]->(Folder or File)
    - polymorphic: true
      database: security
      table: sec_containment
      from_id: parent_id
      to_id: child_id
      type_column: containment_type
      type_values: [CONTAINS]
      from_node: Folder                 # Fixed: always Folder
      to_label_column: child_type       # Varies: Folder or File
      to_label_values: [Folder, File]   # Valid target types
```

### 4. Denormalized Pattern (OnTime-style)

Node properties embedded in edge table:

```yaml
name: flight_network

graph_schema:
  nodes:
    - label: Airport
      database: flights
      table: ontime_flights      # No separate airport table!
      node_id: origin          # Will be resolved from edge
      from_node_properties:       # Properties when Airport is source
        code: Origin
        city: OriginCityName
        state: OriginState
      to_node_properties:         # Properties when Airport is target
        code: Dest
        city: DestCityName
        state: DestState
        
  edges:
    - type_name: FLIGHT
      database: flights
      table: ontime_flights
      from_node: Airport
      to_node: Airport
      from_id: Origin
      to_id: Dest
      property_mappings:
        date: FlightDate
        number: FlightNum
        carrier: Carrier
      edge_id:                    # Composite key for uniqueness
        composite: [FlightDate, FlightNum, Origin, Dest]
```

### 5. FK-Edge Pattern

Edge represented by foreign key (no separate edge table):

```yaml
name: ecommerce

graph_schema:
  nodes:
    - label: Order
      database: shop
      table: orders
      node_id: order_id
      
    - label: Customer
      database: shop
      table: customers
      node_id: customer_id
      
  edges:
    # Edge uses orders table, customer_id column points to customers
    - type_name: PLACED_BY
      database: shop
      table: orders              # Same as Order node table
      from_node: Order
      to_node: Customer
      from_id: order_id
      to_id: customer_id         # FK column in orders
```

---

## Advanced Features

### Composite Edge ID

For edges requiring multi-column uniqueness:

```yaml
edge_id:
  composite: [from_id, to_id, timestamp]
```

Or single column:
```yaml
edge_id:
  column: edge_uuid
```

### Parameterized Views

For multi-tenant or partitioned data:

```yaml
nodes:
  - label: User
    table: users_by_tenant
    view_parameters: [tenant_id]
```

Query: `MATCH (u:User {tenant_id: 'acme'}) RETURN u`

### Schema Filters

Apply permanent SQL filters:

```yaml
nodes:
  - label: ActiveUser
    table: users
    filter: "is_active = 1 AND deleted_at IS NULL"
```

### Auto-Discovery

Auto-map all columns as properties:

```yaml
nodes:
  - label: User
    table: users
    auto_discover_columns: true
    exclude_columns: [password_hash, internal_id]
```

---

## Attribute Applicability Matrix

| Attribute | Standard Node | Shared-Table Node | Standard Edge | Polymorphic Edge |
|-----------|:-------------:|:-----------------:|:-------------:|:----------------:|
| `label` | ✅ | ✅ | ❌ | ❌ |
| `type_name` | ❌ | ❌ | ✅ | ❌ |
| `polymorphic` | ❌ | ❌ | ❌ | ✅ |
| `database` | ✅ | ✅ | ✅ | ✅ |
| `table` | ✅ | ✅ | ✅ | ✅ |
| `node_id` | ✅ | ✅ | ❌ | ❌ |
| `from_id` / `to_id` | ❌ | ❌ | ✅ | ✅ |
| `from_node` / `to_node` | ❌ | ❌ | ✅ | ⚠️ Optional |
| `label_column` | ❌ | ✅ | ❌ | ❌ |
| `label_value` | ❌ | ✅ | ❌ | ❌ |
| `from_label_column` | ❌ | ❌ | ❌ | ⚠️ Optional |
| `to_label_column` | ❌ | ❌ | ❌ | ⚠️ Optional |
| `from_label_values` | ❌ | ❌ | ❌ | ⚠️ Optional |
| `to_label_values` | ❌ | ❌ | ❌ | ⚠️ Optional |
| `type_column` | ❌ | ❌ | ❌ | ✅ |
| `type_values` | ❌ | ❌ | ❌ | ✅ |
| `property_mappings` | ✅ | ✅ | ✅ | ✅ |
| `from_node_properties` | ⚠️ Denorm | ⚠️ Denorm | ❌ | ❌ |
| `to_node_properties` | ⚠️ Denorm | ⚠️ Denorm | ❌ | ❌ |
| `edge_id` | ❌ | ❌ | ✅ | ✅ |
| `view_parameters` | ✅ | ✅ | ✅ | ✅ |
| `use_final` | ✅ | ✅ | ✅ | ✅ |
| `filter` | ✅ | ✅ | ✅ | ✅ |
| `auto_discover_columns` | ✅ | ✅ | ❌ | ❌ |
| `exclude_columns` | ✅ | ✅ | ❌ | ❌ |
| `naming_convention` | ✅ | ✅ | ❌ | ❌ |

Legend: ✅ Required/Applicable, ⚠️ Conditional, ❌ Not Applicable

---

## Validation Rules

1. **Polymorphic edges** must have either `from_node` OR `from_label_column` (not both)
2. **Polymorphic edges** must have either `to_node` OR `to_label_column` (not both)
3. **label_value** requires `label_column` to be set
4. **from_label_values** should be set when `from_label_column` is used (closed-world validation)
5. **to_label_values** should be set when `to_label_column` is used (closed-world validation)
6. **type_values** is required for polymorphic edges (list of edge types)

---

## See Also

- [Configuration Guide](configuration.md) - Server configuration
- [Getting Started](getting-started.md) - Quick start guide
- [Denormalized Edge Tables](denormalized-edge-tables.md) - OnTime pattern details
