> **Note**: This documentation is for ClickGraph v0.6.0. [View latest docs →](../../wiki/Home.md)
# Schema Feature: Polymorphic Edges

**Feature Version**: v0.5.2+  
**Status**: Production-ready  
**Use Case**: Single table containing multiple edge types with dynamic node types

---

## Overview

**Polymorphic edges** allow you to store multiple edge types in a single ClickHouse table, with edge type and node types determined at runtime from column values. This pattern is common in event streams, activity logs, and interaction systems.

### Traditional vs Polymorphic Approach

**Traditional Approach** (separate tables):
```yaml
edges:
  - type: FOLLOWS
    table: user_follows
    from_node: User
    to_node: User
    
  - type: LIKES  
    table: user_likes
    from_node: User
    to_node: Post
    
  - type: AUTHORED
    table: post_authors
    from_node: User
    to_node: Post
```

**Polymorphic Approach** (single table):
```yaml
edges:
  - polymorphic: true
    table: interactions
    type_column: interaction_type
    from_label_column: from_type
    to_label_column: to_type
    type_values: [FOLLOWS, LIKES, AUTHORED]
```

**Benefits**:
- ✅ Single table = simpler schema
- ✅ Easy to add new edge types
- ✅ Natural fit for event streams
- ✅ Unified query interface

---

## Schema Configuration

### Required Fields

```yaml
edges:
  - polymorphic: true                    # Enable polymorphic mode
    database: brahmand
    table: interactions                  # Single source table
    from_id: from_id                     # Source node ID column
    to_id: to_id                         # Target node ID column
    
    # Discovery columns
    type_column: interaction_type        # Column containing edge type (FOLLOWS, LIKES, etc.)
    from_label_column: from_type         # Column containing source node label (User, Post, etc.)
    to_label_column: to_type             # Column containing target node label
    
    # Explicit edge types (REQUIRED - prevents table scan!)
    type_values:
      - FOLLOWS
      - LIKES
      - AUTHORED
      - COMMENTED
      - SHARED
```

### Optional Fields

```yaml
edges:
  - polymorphic: true
    # ... required fields ...
    
    # Composite edge ID (recommended for multiple interactions)
    edge_id: [from_id, to_id, timestamp, interaction_type]
    
    # Edge properties (shared across all types)
    property_mappings:
      timestamp: created_at
      weight: interaction_weight
      metadata: extra_data
    
    # View parameters (for multi-tenancy)
    view_parameters: [tenant_id]
    
    # Auto-discovery
    auto_discover_columns: true
    exclude_columns: [internal_id, updated_at]
```

---

## Table Schema Design

### Example Table

```sql
CREATE TABLE interactions (
    from_id UInt32,
    to_id UInt32,
    interaction_type String,    -- Edge type (FOLLOWS, LIKES, etc.)
    from_type String,            -- Source node label (User, Post, etc.)
    to_type String,              -- Target node label
    created_at DateTime,
    interaction_weight Float32,
    
    -- Composite edge ID (recommended)
    edge_id UInt64              -- Or use composite: (from_id, to_id, created_at, interaction_type)
) ENGINE = MergeTree()
ORDER BY (from_id, interaction_type, created_at);
```

**Data Example**:
```
from_id | to_id | interaction_type | from_type | to_type | created_at          | edge_id
--------|-------|------------------|-----------|---------|---------------------|--------
1001    | 1002  | FOLLOWS          | User      | User    | 2025-01-15 10:30:00 | 1
1001    | 5001  | LIKES            | User      | Post    | 2025-01-15 10:31:00 | 2
1003    | 5001  | AUTHORED         | User      | Post    | 2025-01-15 09:00:00 | 3
1002    | 5001  | COMMENTED        | User      | Post    | 2025-01-15 11:00:00 | 4
```

---

## Querying Polymorphic Edges

### Basic Queries

```cypher
-- Query specific edge type (uses WHERE interaction_type = 'FOLLOWS')
MATCH (u:User)-[:FOLLOWS]->(friend:User)
RETURN u.name, friend.name

-- Query multiple edge types (uses WHERE interaction_type IN ('FOLLOWS', 'LIKES'))
MATCH (u:User)-[:FOLLOWS|LIKES]->(target)
RETURN u.name, type(e) AS edge_type, target

-- Query all edge types from table
MATCH (u:User {name: 'Alice'})-[e]->(target)
RETURN type(e), target
```

### With Edge Properties

```cypher
-- Filter by edge property
MATCH (u:User)-[e:LIKES]->(p:Post)
WHERE e.timestamp > '2025-01-01'
RETURN u.name, p.title, e.timestamp

-- Aggregate by edge type
MATCH (:User)-[e]->(p:Post)
RETURN type(e) AS interaction_type, count(*) AS count
ORDER BY count DESC
```

### Variable-Length Paths

```cypher
-- Multi-hop traversal across polymorphic edges
MATCH path = (u:User {name: 'Alice'})-[:FOLLOWS|LIKES*1..3]->(target)
RETURN target.name, length(path) AS hops

-- Shortest path
MATCH path = shortestPath((a:User {name: 'Alice'})-[:FOLLOWS|AUTHORED*]-(b:Post))
RETURN [node IN nodes(path) | node.name] AS path_names
```

---

## How It Works

### Schema Generation

When ClickGraph loads a polymorphic edge definition with `type_values: [A, B, C]`, it generates **three separate EdgeSchema objects** internally:

```
polymorphic: true
type_values: [FOLLOWS, LIKES, AUTHORED]

    ↓ Generates ↓

EdgeSchema { type: "FOLLOWS", table: "interactions", filters: ["interaction_type = 'FOLLOWS'"] }
EdgeSchema { type: "LIKES", table: "interactions", filters: ["interaction_type = 'LIKES'"] }
EdgeSchema { type: "AUTHORED", table: "interactions", filters: ["interaction_type = 'AUTHORED'"] }
```

### Query Generation

**Cypher**:
```cypher
MATCH (u:User)-[:FOLLOWS|LIKES]->(target)
RETURN u.name, target.name
```

**Generated SQL** (simplified):
```sql
SELECT u.name, target.name
FROM (
    -- FOLLOWS edges
    SELECT from_id, to_id FROM interactions
    WHERE interaction_type = 'FOLLOWS' AND from_type = 'User'
    
    UNION ALL
    
    -- LIKES edges
    SELECT from_id, to_id FROM interactions
    WHERE interaction_type = 'LIKES' AND from_type = 'User'
) AS edges
JOIN users AS u ON edges.from_id = u.user_id
JOIN ... AS target ON edges.to_id = target.id
```

### Node Type Resolution

Node types are resolved at **query time** using the `from_label_column` and `to_label_column`:

```cypher
MATCH (u:User)-[:FOLLOWS]->(friend:User)
```

Generates:
```sql
WHERE interaction_type = 'FOLLOWS'
  AND from_type = 'User'     -- from_label_column check
  AND to_type = 'User'       -- to_label_column check
```

---

## Performance Considerations

### ✅ Best Practices

**1. Use explicit `type_values` list**
```yaml
type_values: [FOLLOWS, LIKES, AUTHORED]  # ✅ No table scan required
```

**2. Index discriminator columns**
```sql
ORDER BY (from_id, interaction_type, created_at)  -- ✅ interaction_type indexed
```

**3. Use composite edge IDs**
```yaml
edge_id: [from_id, to_id, timestamp, interaction_type]  # ✅ Unique edges
```

**4. Filter by specific edge types**
```cypher
-- ✅ Fast: Uses interaction_type = 'FOLLOWS'
MATCH (u:User)-[:FOLLOWS]->(friend:User)

-- ⚠️ Slower: Scans all interaction types
MATCH (u:User)-[e]->(target)
```

### 📊 Performance Characteristics

| Query Pattern | Performance | Reason |
|---------------|-------------|--------|
| Specific edge type `[:FOLLOWS]` | ⚡ Fast | Direct filter on `type_column` |
| Multiple edge types `[:FOLLOWS\|LIKES]` | ⚡ Fast | IN clause on `type_column` + UNION |
| All edge types `[e]` | ⚠️ Moderate | Scans all `type_values` with UNION |
| Variable-length paths `[:FOLLOWS*1..3]` | ⚡ Fast | Recursive CTE with type filter |

---

## Complete Example

### Schema YAML

```yaml
name: social_polymorphic
version: "1.0"

graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users
      node_id: user_id
      property_mappings:
        name: username
        email: email

    - label: Post
      database: brahmand
      table: posts
      node_id: post_id
      property_mappings:
        title: title
        content: body

  edges:
    - polymorphic: true
      database: brahmand
      table: interactions
      from_id: from_id
      to_id: to_id
      
      type_column: interaction_type
      from_label_column: from_type
      to_label_column: to_type
      
      type_values:
        - FOLLOWS
        - LIKES
        - AUTHORED
        - COMMENTED
        - SHARED
      
      edge_id: [from_id, to_id, timestamp, interaction_type]
      
      property_mappings:
        timestamp: created_at
        weight: interaction_weight
```

### Sample Queries

```cypher
-- 1. Who does Alice follow?
MATCH (u:User {name: 'Alice'})-[:FOLLOWS]->(friend:User)
RETURN friend.name

-- 2. What has Alice liked?
MATCH (u:User {name: 'Alice'})-[:LIKES]->(post:Post)
RETURN post.title, post.content

-- 3. Who authored posts that Alice liked?
MATCH (alice:User {name: 'Alice'})-[:LIKES]->(p:Post)<-[:AUTHORED]-(author:User)
RETURN author.name, p.title

-- 4. All interactions by Alice
MATCH (alice:User {name: 'Alice'})-[e]->(target)
RETURN type(e) AS interaction, target

-- 5. Interaction statistics
MATCH ()-[e:FOLLOWS|LIKES|AUTHORED]->()
RETURN type(e) AS interaction_type, count(*) AS count
ORDER BY count DESC
```

---

## Fixed-Endpoint Polymorphic Edges

**Feature Version**: v0.5.3+  
**Use Case**: One endpoint is fixed, the other varies dynamically

### Overview

Sometimes you need a **hybrid pattern** where one side of the edge is always the same node type, but the other side can vary. Common examples:

- **Group membership**: Groups can contain Users OR other Groups
- **File systems**: Folders can contain Files OR other Folders  
- **Organization charts**: Departments contain Employees OR sub-Departments
- **Email lists**: Mailing lists contain Users OR other lists

### Schema Configuration

Use `from_node` or `to_node` instead of the corresponding label column:

```yaml
edges:
  - polymorphic: true
    database: brahmand
    table: memberships
    from_id: parent_id
    to_id: member_id
    
    # Fixed source: always "Group"
    from_node: Group
    
    # Polymorphic target: "User" or "Group" from column
    to_label_column: member_type
    
    # Single edge type (type_column not needed)
    type_values:
      - PARENT_OF
    
    edge_id: [parent_id, member_id]
```

### Key Differences from Full Polymorphic

| Aspect | Full Polymorphic | Fixed-Endpoint |
|--------|------------------|----------------|
| Source node | `from_label_column` (dynamic) | `from_node: Group` (fixed) |
| Target node | `to_label_column` (dynamic) | `to_label_column` (dynamic) |
| `type_column` | Required if multiple types | Optional if single `type_values` |
| SQL filter | Filter both label columns | Filter only polymorphic side |

### Complete Example: Group Membership

**Table Schema**:
```sql
CREATE TABLE groups (
    group_id UInt32,
    name String,
    description String
) ENGINE = Memory;

CREATE TABLE users (
    user_id UInt32,
    name String,
    email String
) ENGINE = Memory;

-- Polymorphic memberships table
CREATE TABLE memberships (
    parent_id UInt32,      -- Always refers to groups.group_id
    member_id UInt32,      -- Refers to users.user_id OR groups.group_id
    member_type String     -- 'User' or 'Group'
) ENGINE = Memory;
```

**Sample Data**:
```sql
-- Groups
INSERT INTO groups VALUES (1, 'Engineering', 'Engineering department');
INSERT INTO groups VALUES (2, 'Backend', 'Backend team');
INSERT INTO groups VALUES (3, 'Frontend', 'Frontend team');

-- Users
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

-- Memberships (Group hierarchy + user assignments)
INSERT INTO memberships VALUES (1, 2, 'Group');  -- Engineering -> Backend
INSERT INTO memberships VALUES (1, 3, 'Group');  -- Engineering -> Frontend
INSERT INTO memberships VALUES (2, 1, 'User');   -- Backend -> Alice
INSERT INTO memberships VALUES (2, 2, 'User');   -- Backend -> Bob
```

**Schema YAML** (`schemas/examples/group_membership.yaml`):
```yaml
name: group_membership

graph_schema:
  nodes:
    - label: Group
      database: brahmand
      table: groups
      node_id: group_id
      property_mappings: {}

    - label: User
      database: brahmand
      table: users
      node_id: user_id
      property_mappings: {}

  edges:
    - polymorphic: true
      database: brahmand
      table: memberships
      from_id: parent_id
      to_id: member_id
      from_node: Group              # Fixed source
      to_label_column: member_type  # Polymorphic target
      type_values:
        - PARENT_OF
      edge_id: [parent_id, member_id]
```

**Queries**:
```cypher
-- Direct members of a group (users and subgroups)
MATCH (g:Group {name: 'Engineering'})-[:PARENT_OF]->(member)
RETURN member

-- Only user members
MATCH (g:Group {name: 'Backend'})-[:PARENT_OF]->(u:User)
RETURN u.name

-- Only subgroup members  
MATCH (g:Group {name: 'Engineering'})-[:PARENT_OF]->(sub:Group)
RETURN sub.name

-- Recursive: all users under Engineering (including subgroups)
MATCH (g:Group {name: 'Engineering'})-[:PARENT_OF*1..5]->(u:User)
RETURN DISTINCT u.name
```

### Validation Rules

The schema validation enforces:

1. **Exactly one source specification**: Either `from_label_column` OR `from_node` (not both, not neither)
2. **Exactly one target specification**: Either `to_label_column` OR `to_node` (not both, not neither)
3. **`type_column` optionality**: Only required when `type_values` has multiple entries

**Valid configurations**:
```yaml
# Full polymorphic (both sides dynamic)
from_label_column: from_type
to_label_column: to_type
type_column: edge_type
type_values: [FOLLOWS, LIKES]

# Fixed source (Group -> User|Group)
from_node: Group
to_label_column: member_type
type_values: [PARENT_OF]

# Fixed target (User|Group -> Document)
from_label_column: author_type
to_node: Document
type_values: [AUTHORED]
```

**Invalid configurations**:
```yaml
# ❌ Both from_node AND from_label_column
from_node: Group
from_label_column: from_type  # Error!

# ❌ Neither from_node NOR from_label_column
to_label_column: to_type  # Error: missing source specification

# ❌ Multiple type_values without type_column
type_values: [PARENT_OF, MEMBER_OF]  # Error: needs type_column
```

---

## Comparison with Standard Edges

### When to Use Polymorphic Edges

**✅ Use polymorphic edges when**:
- Single table contains multiple edge types (event streams, logs)
- Edge types need to be added dynamically
- Node types vary by edge instance (heterogeneous graphs)
- Unified storage simplifies operations

**❌ Use standard edges when**:
- Each edge type has different properties
- Tables are naturally separated
- Performance critical (separate tables = better indexing)
- Static, well-defined schema

### Migration Path

**From Standard to Polymorphic**:
```sql
-- Combine separate tables into one
INSERT INTO interactions
SELECT from_id, to_id, 'FOLLOWS' AS type, 'User' AS from_type, 'User' AS to_type, created_at
FROM user_follows
UNION ALL
SELECT from_id, to_id, 'LIKES' AS type, 'User' AS from_type, 'Post' AS to_type, created_at
FROM user_likes;
```

**From Polymorphic to Standard** (if needed for performance):
```sql
-- Extract specific edge type to dedicated table
CREATE TABLE user_follows AS
SELECT from_id, to_id, created_at
FROM interactions
WHERE interaction_type = 'FOLLOWS' AND from_type = 'User' AND to_type = 'User';
```

---

## Limitations

1. **All edge types share the same property schema**
   - Properties in `property_mappings` apply to all `type_values`
   - For type-specific properties, use standard edges

2. **Explicit `type_values` required**
   - No automatic discovery from table data (prevents expensive table scans)
   - Must update schema when adding new edge types

3. **Node type resolution requires data access**
   - `from_label_column` and `to_label_column` must be in table
   - Cannot determine node types from schema alone

---

## Implementation Details

### Code References

**Schema Parsing**:
- `src/graph_catalog/config.rs` - `PolymorphicEdgeDefinition` (lines 310-348)

**Edge Filtering**:
- `src/render_plan/plan_builder_helpers.rs` - `generate_polymorphic_edge_filters()`

**Testing**:
- `src/render_plan/tests/polymorphic_edge_tests.rs` - 5 unit tests
- `schemas/examples/social_polymorphic.yaml` - Example schema

---

## See Also

- [Schema Configuration Advanced](Schema-Configuration-Advanced.md) - General schema configuration
- [Schema Denormalized Properties](Schema-Denormalized-Properties.md) - Denormalized node properties
- [Edge ID Best Practices](../edge-id-best-practices.md) - Edge uniqueness and composite IDs
- [Cypher Basic Patterns](Cypher-Basic-Patterns.md) - Edge patterns and queries
