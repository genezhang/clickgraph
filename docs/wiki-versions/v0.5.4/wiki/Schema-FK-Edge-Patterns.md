> **Note**: This documentation is for ClickGraph v0.5.4. [View latest docs →](../../wiki/Home.md)
# Schema: FK-Edge Patterns

Self-referencing foreign key patterns for hierarchical data like file systems, org charts, and category trees.

## Table of Contents
- [What is an FK-Edge Pattern?](#what-is-an-fk-edge-pattern)
- [Schema Configuration](#schema-configuration)
- [Query Examples](#query-examples)
- [Use Cases](#use-cases)
- [Best Practices](#best-practices)

---

## What is an FK-Edge Pattern?

An **FK-Edge pattern** (Foreign Key Edge) occurs when a table has a self-referencing foreign key, meaning the edge table is the same as the node table.

**Classic Examples**:
- **File systems**: `parent_id` → `object_id` (folders contain folders/files)
- **Org charts**: `manager_id` → `employee_id` (managers supervise employees)
- **Category trees**: `parent_category_id` → `category_id` (nested categories)
- **Comment threads**: `reply_to_id` → `comment_id` (threaded discussions)

```
Traditional Graph Schema:
┌──────────┐    ┌──────────┐    ┌──────────┐
│  Node A  │───▶│   Edge   │───▶│  Node B  │
│  (users) │    │(follows) │    │  (users) │
└──────────┘    └──────────┘    └──────────┘
   Table 1         Table 2         Table 1

FK-Edge Schema:
┌───────────────────────────────────────────┐
│           Single Table                     │
│  (id, parent_id, name, type, ...)         │
│                                            │
│  parent_id ──────FK──────▶ id             │
└───────────────────────────────────────────┘
   Node AND Edge in same table
```

---

## Schema Configuration

### Basic FK-Edge Schema

```yaml
name: filesystem
graph_schema:
  nodes:
    - label: Object
      table: fs_objects
      node_id: object_id
      properties:
        - name: name
          column: name
        - name: type
          column: object_type
        - name: size
          column: size_bytes

  edges:
    - type: PARENT
      table: fs_objects          # Same table as node!
      from_id: parent_id         # FK column (child's parent reference)
      to_id: object_id           # Target PK (parent's ID)
```

**Key Points**:
- Edge `table` equals the node `table`
- `from_id` is the FK column (e.g., `parent_id`)
- `to_id` is the PK column (e.g., `object_id`)
- The edge represents "child points to parent"

### Complete Example Schema

```yaml
# schemas/examples/filesystem_single.yaml
name: filesystem
graph_schema:
  nodes:
    - label: Object
      table: fs_objects_single
      node_id: object_id
      properties:
        - name: name
          column: name
        - name: type
          column: object_type
        - name: size
          column: size_bytes
        - name: created_at
          column: created_at

  edges:
    - type: PARENT
      table: fs_objects_single
      from_id: parent_id
      to_id: object_id
      # No edge properties needed - it's implicit in the FK relationship
```

### ClickHouse Table DDL

```sql
CREATE TABLE fs_objects_single (
    object_id UInt64,
    parent_id Nullable(UInt64),  -- NULL for root objects
    name String,
    object_type Enum('folder' = 1, 'file' = 2),
    size_bytes UInt64 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY object_id;

-- Sample hierarchical data
INSERT INTO fs_objects_single VALUES
    (1, NULL, 'root', 'folder', 0, now()),
    (2, 1, 'Documents', 'folder', 0, now()),
    (3, 1, 'Photos', 'folder', 0, now()),
    (4, 2, 'Work', 'folder', 0, now()),
    (5, 2, 'Personal', 'folder', 0, now()),
    (6, 4, 'report.pdf', 'file', 1024, now()),
    (7, 4, 'notes.txt', 'file', 256, now()),
    (8, 3, 'vacation.jpg', 'file', 2048, now());
```

---

## Query Examples

### Direct Parent/Child Queries

```cypher
-- Get direct children of a folder
MATCH (child:Object)-[:PARENT]->(parent:Object)
WHERE parent.name = 'Documents'
RETURN child.name, child.type

-- Get parent of a file
MATCH (file:Object)-[:PARENT]->(parent:Object)
WHERE file.name = 'report.pdf'
RETURN parent.name
```

### Variable-Length Ancestor Queries

Find all ancestors (path to root):

```cypher
-- All ancestors of a file (up to 10 levels)
MATCH (file:Object)-[:PARENT*1..10]->(ancestor:Object)
WHERE file.name = 'notes.txt'
RETURN ancestor.name

-- Result: ['Work', 'Documents', 'root']
```

**Note**: The `*1..10` limits recursion depth. Adjust based on your hierarchy depth.

### Variable-Length Descendant Queries

Find all descendants (everything under a folder):

```cypher
-- All objects under root
MATCH (child:Object)-[:PARENT*1..10]->(parent:Object)
WHERE parent.name = 'root'
RETURN child.name, child.type

-- All files (not folders) under Documents
MATCH (file:Object)-[:PARENT*1..10]->(folder:Object)
WHERE folder.name = 'Documents' AND file.type = 'file'
RETURN file.name, file.size
```

### Exact Hop Count Queries

```cypher
-- Direct children only (1 hop)
MATCH (child:Object)-[:PARENT*1]->(parent:Object)
WHERE parent.name = 'root'
RETURN child.name

-- Grandchildren (exactly 2 hops from root)
MATCH (grandchild:Object)-[:PARENT*2]->(root:Object)
WHERE root.name = 'root'
RETURN grandchild.name

-- Great-grandchildren (exactly 3 hops)
MATCH (obj:Object)-[:PARENT*3]->(root:Object)
WHERE root.name = 'root'
RETURN obj.name
```

### Finding Root Objects

```cypher
-- Objects with no parent (roots)
-- Note: This requires checking for NULL parent_id
-- Currently use a WHERE clause on the node
MATCH (root:Object)
WHERE root.parent_id IS NULL
RETURN root.name
```

### Path Depth Queries

```cypher
-- Find depth of each object from root
MATCH (obj:Object)-[path:PARENT*1..20]->(root:Object)
WHERE root.name = 'root'
RETURN obj.name, length(path) AS depth
ORDER BY depth, obj.name
```

---

## Use Cases

### 1. File System Navigation

```cypher
-- Breadcrumb path for a file
MATCH (file:Object)-[:PARENT*1..10]->(ancestor:Object)
WHERE file.name = 'report.pdf'
RETURN ancestor.name
ORDER BY length(path) DESC

-- List folder contents with sizes
MATCH (item:Object)-[:PARENT]->(folder:Object)
WHERE folder.name = 'Work'
RETURN item.name, item.type, item.size
```

### 2. Organizational Hierarchy

```yaml
# org_chart.yaml
graph_schema:
  nodes:
    - label: Employee
      table: employees
      node_id: employee_id
      properties:
        - name: name
          column: full_name
        - name: title
          column: job_title
        - name: department
          column: dept_name

  edges:
    - type: REPORTS_TO
      table: employees
      from_id: manager_id
      to_id: employee_id
```

```cypher
-- All reports (direct and indirect) under a manager
MATCH (report:Employee)-[:REPORTS_TO*1..5]->(manager:Employee)
WHERE manager.name = 'Jane CEO'
RETURN report.name, report.title

-- Chain of command for an employee
MATCH (emp:Employee)-[:REPORTS_TO*1..10]->(boss:Employee)
WHERE emp.name = 'Bob Developer'
RETURN boss.name, boss.title
```

### 3. Category Trees (E-commerce)

```yaml
# categories.yaml
graph_schema:
  nodes:
    - label: Category
      table: product_categories
      node_id: category_id
      properties:
        - name: name
          column: category_name
        - name: slug
          column: url_slug

  edges:
    - type: SUBCATEGORY_OF
      table: product_categories
      from_id: parent_category_id
      to_id: category_id
```

```cypher
-- All subcategories under "Electronics"
MATCH (sub:Category)-[:SUBCATEGORY_OF*1..5]->(parent:Category)
WHERE parent.name = 'Electronics'
RETURN sub.name

-- Breadcrumb for a category
MATCH (cat:Category)-[:SUBCATEGORY_OF*1..10]->(ancestor:Category)
WHERE cat.slug = 'smartphones'
RETURN ancestor.name
```

### 4. Threaded Comments

```yaml
# comments.yaml
graph_schema:
  nodes:
    - label: Comment
      table: comments
      node_id: comment_id
      properties:
        - name: text
          column: content
        - name: author
          column: author_name
        - name: posted_at
          column: created_at

  edges:
    - type: REPLY_TO
      table: comments
      from_id: parent_comment_id
      to_id: comment_id
```

```cypher
-- All replies in a thread
MATCH (reply:Comment)-[:REPLY_TO*1..20]->(root:Comment)
WHERE root.comment_id = 1
RETURN reply.author, reply.text, reply.posted_at
ORDER BY reply.posted_at

-- Thread depth
MATCH (reply:Comment)-[path:REPLY_TO*1..20]->(root:Comment)
WHERE root.comment_id = 1
RETURN reply.text, length(path) AS nesting_level
```

---

## Best Practices

### 1. Set Appropriate Recursion Limits

Always specify `max_hops` to prevent runaway queries:

```cypher
-- ✅ Good: Bounded recursion
MATCH (child)-[:PARENT*1..20]->(root)

-- ⚠️ Risky: Unbounded (uses server default, typically 100)
MATCH (child)-[:PARENT*]->(root)
```

**Server Configuration**:
```bash
# Set max recursion depth via environment
CLICKGRAPH_MAX_RECURSION_DEPTH=50 clickgraph

# Or CLI argument
clickgraph --max-recursion-depth 50
```

### 2. Index the FK Column

```sql
-- Ensure the FK column is indexed for fast traversals
CREATE TABLE fs_objects (
    object_id UInt64,
    parent_id Nullable(UInt64),
    ...
) ENGINE = MergeTree()
ORDER BY (parent_id, object_id);  -- FK first for hierarchy queries
```

### 3. Handle NULL Parents (Roots)

Root objects have `NULL` parent_id. Design queries accordingly:

```cypher
-- This works for finding roots
MATCH (root:Object)
WHERE root.parent_id IS NULL
RETURN root.name
```

### 4. Use Direction Consistently

In FK-edge patterns, the edge direction matters:
- `(child)-[:PARENT]->(parent)` = child points TO parent (follow FK)
- Ancestors: Filter on child, traverse toward root
- Descendants: Filter on parent, find all pointing to it

### 5. Consider Materialized Paths for Deep Hierarchies

For very deep hierarchies (100+ levels), consider storing materialized paths in ClickHouse:

```sql
-- Materialized path column
ALTER TABLE fs_objects ADD COLUMN path String;
-- Store: '/1/2/4/7' for object 7

-- Then query with LIKE or array functions
SELECT * FROM fs_objects WHERE path LIKE '/1/2/%';
```

This trades storage for query performance on deep hierarchies.

---

## Related Documentation

- [Schema Basics](Schema-Basics.md) - Introduction to schema configuration
- [Schema Configuration: Advanced](Schema-Configuration-Advanced.md) - Complex schema patterns
- [Cypher Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md) - Variable-length path syntax
- [Performance: Query Optimization](Performance-Query-Optimization.md) - Optimizing graph queries
