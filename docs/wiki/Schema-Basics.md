# Schema Basics

**Caution:** This entire document is AI-generated. It may contain mistakes. Double check and kindly raise issues and corrections if you find any.

Learn how to configure ClickGraph schemas to map your ClickHouse tables to graph structures using YAML configuration files.

## 📋 Table of Contents

- [What is a Graph Schema?](#what-is-a-graph-schema)
- [Schema File Structure](#schema-file-structure)
- [Defining Nodes](#defining-nodes)
- [Defining Edges](#defining-edges)
- [Property Mappings](#property-mappings)
- [Complete Example](#complete-example)
- [Using Your Schema](#using-your-schema)
- [Next Steps](#next-steps)

## What is a Graph Schema?

A **graph schema** is a YAML configuration file that tells ClickGraph how to interpret your ClickHouse tables as graph structures:

- **Nodes** represent entities (users, products, posts, etc.)
- **Edges** represent connections between nodes (follows, purchased, authored, etc.)
- **Properties** are the attributes of nodes and edges

**Key Point**: ClickGraph doesn't change your data. It provides a graph view over your existing ClickHouse tables.

## Schema File Structure

A basic schema file has three main sections:

```yaml
graph_schema:
  nodes:
    # Define your node types here
    
  edges: # accept relationships - deprecated starting v0.5.2
    # Define your edge types here
```

That's it! Let's look at each section in detail.

## Defining Nodes

A **node** definition maps a ClickHouse table to a graph node type.

### Basic Node Structure

```yaml
graph_schema:
  nodes:
    - label: User                    # Label used in Cypher queries
      database: brahmand             # ClickHouse database name
      table: users                   # ClickHouse table name
      id_column: user_id             # Column that uniquely identifies each node
      property_mappings:
        user_id: user_id             # Cypher property → ClickHouse column
        name: full_name              # Map 'name' in Cypher to 'full_name' in ClickHouse
        email: email_address
```

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `label` | The node type used in Cypher queries | `User`, `Post`, `Product` |
| `database` | ClickHouse database containing the table | `brahmand`, `analytics` |
| `table` | ClickHouse table name | `users`, `products` |
| `id_column` | Column that uniquely identifies nodes | `user_id`, `product_id` |
| `property_mappings` | Maps Cypher properties to ClickHouse columns | See below |

### Property Mappings Explained

Property mappings connect Cypher query syntax to your actual column names:

```yaml
property_mappings:
  # Format: cypher_property: clickhouse_column
  user_id: user_id              # Same name → direct mapping
  name: full_name               # Different names → rename mapping
  email: email_address          # Makes queries more intuitive
  registration_date: reg_date   # Shorter Cypher name
```

**In Cypher queries:**
```cypher
MATCH (u:User)
WHERE u.name = 'Alice'        -- Uses 'name' (Cypher property)
RETURN u.email                -- Uses 'email' (Cypher property)
```

**Generated ClickHouse SQL:**
```sql
SELECT email_address  -- Uses actual column names
FROM users
WHERE full_name = 'Alice'
```

### Multiple Node Types

You can define as many node types as you need:

```yaml
graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: full_name
    
    - label: Post
      database: brahmand
      table: posts
      id_column: post_id
      property_mappings:
        post_id: post_id
        title: post_title
        content: post_content
```

## Defining Edges

An **edge**  (a.k.a. relationship) definition maps a ClickHouse table to edges connecting nodes.

### Basic Edge Structure

```yaml
graph_schema:
  edges:
    - type: FOLLOWS                  # edge type in Cypher
      database: brahmand             # ClickHouse database
      table: user_follows            # ClickHouse table
      from_id: follower_id           # Column containing source node ID
      to_id: followed_id             # Column containing target node ID
      edge_id: follow_id             # Column that uniquely identifies each edge
      from_node: User                # Source node label
      to_node: User                  # Target node label
      property_mappings:
        follow_date: follow_date     # Relationship properties
```

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `type` | Edge type used in Cypher | `FOLLOWS`, `AUTHORED`, `PURCHASED` |
| `database` | ClickHouse database name | `brahmand` |
| `table` | ClickHouse table name | `user_follows`, `purchases` |
| `from_id` | Column containing source node ID | `follower_id`, `user_id` |
| `to_id` | Column containing target node ID | `followed_id`, `post_id` |
| `edge_id` | Column that uniquely identifies edges | `follow_id`, `purchase_id` |
| `from_node` | Label of source node type | `User`, `Customer` |
| `to_node` | Label of target node type | `User`, `Post`, `Product` |
| `property_mappings` | Relationship properties | `follow_date`, `amount` |

### Understanding Edge IDs

The `edge_id` field is **critical** for correct query results:

```yaml
edge_id: follow_id    # ✅ Unique column for each edge
```

**Why it matters:**
- Prevents duplicate edges in query results
- Essential for accurate aggregations (COUNT, SUM)
- Required for proper JOIN semantics

**Common patterns:**
- Dedicated edge ID column: `follow_id`, `purchase_id`
- Composite key: Use any unique column (often `to_id` or `from_id` if guaranteed unique)

👉 **For detailed edge ID guidance, see [Edge ID Best Practices](Edge-ID-Best-Practices.md)**

### Edge Directions

Edges are **directed** by default:

```cypher
-- Matches only: alice → bob
MATCH (alice:User)-[:FOLLOWS]->(bob:User)
WHERE alice.name = 'Alice'
RETURN bob.name

-- Matches both directions: alice ← → bob
MATCH (alice:User)-[:FOLLOWS]-(bob:User)
WHERE alice.name = 'Alice'
RETURN bob.name
```

### Self-Referencing Edges

Edges can connect nodes of the same type:

```yaml
- type: FOLLOWS
  from_node: User
  to_node: User      # Same as from_node - perfectly fine!
```

```cypher
-- Find who Alice follows
MATCH (alice:User)-[:FOLLOWS]->(followed:User)
WHERE alice.name = 'Alice'
RETURN followed.name
```

## Property Mappings

Property mappings make your Cypher queries intuitive while keeping your ClickHouse schema unchanged.

### Direct Mappings

When Cypher property names match column names:

```yaml
property_mappings:
  user_id: user_id
  email: email
  country: country
```

### Rename Mappings

When you want different names in Cypher:

```yaml
property_mappings:
  name: full_name              # Query with 'name', stored as 'full_name'
  email: email_address
  registered: registration_date
```

**Benefits:**
- Shorter, cleaner Cypher queries
- Consistent naming across different tables
- Domain-specific terminology

### All Properties Are Optional

You only need to map properties you'll query:

```yaml
# ClickHouse table has: user_id, full_name, email, phone, address, city, country
property_mappings:
  user_id: user_id
  name: full_name
  email: email
  # Don't map phone, address, city, country if you won't query them
```

**Note**: Unmapped properties won't be accessible in Cypher queries.

## Complete Example

Here's a complete schema for a simple social network:

```yaml
graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users
      id_column: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        email: email_address
        registration_date: registration_date
        is_active: is_active
        country: country

    - label: Post
      database: brahmand
      table: posts
      id_column: post_id
      property_mappings:
        post_id: post_id
        title: post_title
        content: post_content
        date: post_date

  edges:
    - type: FOLLOWS
      database: brahmand
      table: user_follows
      from_id: follower_id
      to_id: followed_id
      edge_id: follow_id
      from_node: User
      to_node: User
      property_mappings:
        follow_date: follow_date
    
    - type: AUTHORED
      database: brahmand
      table: posts
      from_id: author_id
      to_id: post_id
      edge_id: post_id
      from_node: User
      to_node: Post
      property_mappings:
        post_date: post_date
```

**Example Queries:**

```cypher
-- Find all users
MATCH (u:User)
RETURN u.name, u.email
LIMIT 10

-- Find who Alice follows
MATCH (alice:User)-[:FOLLOWS]->(followed:User)
WHERE alice.name = 'Alice'
RETURN followed.name, followed.country

-- Find Alice's posts
MATCH (alice:User)-[:AUTHORED]->(post:Post)
WHERE alice.name = 'Alice'
RETURN post.title, post.date
ORDER BY post.date DESC

-- Find posts by users Alice follows
MATCH (alice:User)-[:FOLLOWS]->(user:User)-[:AUTHORED]->(post:Post)
WHERE alice.name = 'Alice'
RETURN user.name, post.title, post.date
```

## Using Your Schema

### 1. Save Your Schema File

Save your YAML configuration to a file:

```bash
schemas/my_graph.yaml
```

### 2. Set Environment Variable

Tell ClickGraph where to find your schema:

```bash
export GRAPH_CONFIG_PATH="./schemas/my_graph.yaml"
```

### 3. Start ClickGraph

```bash
docker run -d --name clickgraph \
  --link clickhouse:clickhouse \
  -p 8080:8080 \
  -p 7687:7687 \
  -v $(pwd)/schemas:/schemas \
  -e GRAPH_CONFIG_PATH="/schemas/my_graph.yaml" \
  -e CLICKHOUSE_URL="http://clickhouse:8123" \
  -e CLICKHOUSE_USER="test_user" \
  -e CLICKHOUSE_PASSWORD="test_pass" \
  -e CLICKHOUSE_DATABASE="brahmand" \
  genezhang/clickgraph:latest
```

### 4. Query Your Graph

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (u:User) RETURN u.name LIMIT 5"
  }'
```

## Common Patterns

### E-Commerce Graph

```yaml
graph_schema:
  nodes:
    - label: Customer
      table: customers
      id_column: customer_id
      property_mappings:
        customer_id: customer_id
        name: full_name
        email: email
    
    - label: Product
      table: products
      id_column: product_id
      property_mappings:
        product_id: product_id
        name: product_name
        price: price
  
  edges:
    - type: PURCHASED
      table: orders
      from_id: customer_id
      to_id: product_id
      edge_id: order_id
      from_node: Customer
      to_node: Product
      property_mappings:
        amount: total_amount
        date: order_date
```

### Knowledge Graph

```yaml
graph_schema:
  nodes:
    - label: Person
      table: people
      id_column: person_id
      property_mappings:
        person_id: person_id
        name: full_name
    
    - label: Organization
      table: organizations
      id_column: org_id
      property_mappings:
        org_id: org_id
        name: org_name
  
  edges:
    - type: WORKS_AT
      table: employment
      from_id: person_id
      to_id: org_id
      edge_id: employment_id
      from_node: Person
      to_node: Organization
      property_mappings:
        title: job_title
        start_date: start_date
```

## Validation Tips

### Check Your Schema

Common issues to watch for:

1. **Missing ID columns**: Every node and relationship needs an ID
2. **Wrong node labels**: `from_node` and `to_node` must match defined node labels
3. **Typos in column names**: Double-check your ClickHouse column names
4. **Missing database names**: Always specify the database

### Test Your Schema

Start with simple queries:

```cypher
-- 1. Can I retrieve nodes?
MATCH (n:User) RETURN n LIMIT 5

-- 2. Can I access properties?
MATCH (n:User) RETURN n.name, n.email LIMIT 5

-- 3. Can I traverse edges?
MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name LIMIT 5

-- 4. Can I filter?
MATCH (n:User) WHERE n.country = 'USA' RETURN n.name
```

## Next Steps

Now that you understand schema basics, explore advanced features:

- **[Schema Configuration Advanced](Schema-Configuration-Advanced.md)** - Auto-discovery, multi-schema, FINAL modifier, parameterized views
- **[Schema Polymorphic Edges](Schema-Polymorphic-Edges.md)** - Multiple relationship types in a single table
- **[Schema Denormalized Properties](Schema-Denormalized-Properties.md)** - Dramatically faster queries by denormalizing properties
- **[Edge ID Best Practices](Edge-ID-Best-Practices.md)** - Optimize edge uniqueness tracking
- **[Multi-Tenancy Patterns](Multi-Tenancy-Patterns.md)** - Row-level security and tenant isolation

## Quick Reference

### Node Definition Template

```yaml
- label: <CypherLabel>
  database: <clickhouse_database>
  table: <clickhouse_table>
  id_column: <unique_id_column>
  property_mappings:
    <cypher_prop>: <clickhouse_column>
```

### Relationship Definition Template

```yaml
- type: <CYPHER_TYPE>
  database: <clickhouse_database>
  table: <clickhouse_table>
  from_id: <source_id_column>
  to_id: <target_id_column>
  edge_id: <unique_edge_id_column>
  from_node: <SourceNodeLabel>
  to_node: <TargetNodeLabel>
  property_mappings:
    <cypher_prop>: <clickhouse_column>
```

---

**Need Help?**
- 📖 [Quick Start Guide](Quick-Start-Guide.md) - Get started in 5 minutes
- 🎯 [Basic Cypher Patterns](Cypher-Basic-Patterns.md) - Learn Cypher query syntax
- 🚀 [Advanced Schema Configuration](Schema-Configuration-Advanced.md) - Advanced features
- 💡 [Example Use Cases](Use-Case-Social-Network.md) - Real-world examples
