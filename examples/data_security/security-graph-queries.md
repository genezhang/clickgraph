# Security Graph Query Examples

This document contains example Cypher queries for security analysis using ClickGraph's chained variable-length path (VLP) support.

## Schema Overview

The data security schema (`examples/data_security/data_security.yaml`) models:
- **Nodes**: User, Group, Folder, File
- **Edges**: HAS_ACCESS (polymorphic), CONTAINS (denormalized via parent_id), MEMBER_OF (user-group)

**Key Feature**: Uses denormalized `parent_id` column for CONTAINS relationship:
- `parent_id = 0` indicates a **root folder**
- No separate edge table needed for folder hierarchy

## Setup

```bash
# Start server with data security schema
export GRAPH_CONFIG_PATH="./examples/data_security/data_security.yaml"
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="data_security"
./target/release/clickgraph
```

---

## Basic Statistics Queries

### 1. Total Groups Count
```cypher
MATCH (g:Group) 
RETURN COUNT(g) AS total_groups
```

### 2. User Statistics (Total and External)
```cypher
MATCH (u:User) 
RETURN 
    COUNT(u) AS total_users, 
    SUM(CASE WHEN u.exposure = 'external' THEN 1 ELSE 0 END) AS external_users
```

### 3. Root Folder Statistics (Files per Folder, Sensitive %)

**Finding True Root Folders**: This schema uses denormalized `parent_id` column:
- `parent_id = 0` indicates a root folder (top-level, no parent)
- Query root folders directly with `WHERE folder.parent_id = 0`

```cypher
-- List all root folders (true filesystem roots)
MATCH (folder:Folder)
WHERE folder.parent_id = 0
RETURN folder.fs_id, folder.name, folder.path
ORDER BY folder.name
```

Count files under each root folder:

```cypher
MATCH (root:Folder)-[:CONTAINS*1..5]->(f:File)
WHERE root.parent_id = 0
RETURN 
    root.name AS root_name, 
    root.fs_id AS root_id, 
    COUNT(DISTINCT f) AS file_count, 
    SUM(CASE WHEN f.sensitive_data = 1 THEN 1 ELSE 0 END) AS sensitive_count 
ORDER BY file_count DESC 
LIMIT 15
```

Alternatively, find which root folders users have access to:

```cypher
-- Root folders with user permissions
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)
WHERE root.parent_id = 0
RETURN DISTINCT root.fs_id, root.name, COUNT(DISTINCT u) AS user_count
ORDER BY user_count DESC
LIMIT 20
```

---

## Basic Access Queries

### 4. Direct User Access to Files via Folders
Find all files a specific user can access through folder permissions:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*1..3]->(f:File)
WHERE u.user_id = 1
RETURN u.name, folder.name, f.name, f.path
LIMIT 20
```

### 5. Group-Based Access
Find files accessible through group membership:

```cypher
MATCH (u:User)-[:MEMBER_OF]->(grp:Group)-[:HAS_ACCESS]->(f:File)
WHERE u.user_id = 1
RETURN u.name, grp.name, f.name
LIMIT 20
```

### 6. Full Access Chain (User → Group → Folder → Files)
Most complex pattern - user access through groups to folders containing files:

```cypher
MATCH (u:User)-[:MEMBER_OF]->(grp:Group)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*1..3]->(f:File)
WHERE u.user_id = 1
RETURN u.name, grp.name, folder.name, f.name
LIMIT 20
```

---

## Risk Assessment Queries

### 7. Sensitive Files with User Access Count
For each sensitive file, count how many users can access it and how many are external:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*1..3]->(f:File)
WHERE f.sensitive_data = 1
RETURN 
    f.name AS file_name, 
    f.path AS file_path,
    COUNT(DISTINCT u) AS total_users,
    SUM(CASE WHEN u.exposure = 'external' THEN 1 ELSE 0 END) AS external_users
ORDER BY external_users DESC
LIMIT 10
```

### 8. External User File Access Summary (Paths vs Distinct Files)
For each external user, show both path counts (risk vectors) and distinct file counts:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)-[:CONTAINS*1..5]->(f:File)
WHERE u.exposure = 'external'
RETURN 
    u.name AS user_name,
    u.user_id AS user_id,
    root.name AS root_name,
    COUNT(*) AS total_paths,
    COUNT(DISTINCT f) AS distinct_files,
    SUM(CASE WHEN f.sensitive_data = 1 THEN 1 ELSE 0 END) AS sensitive_paths
ORDER BY sensitive_paths DESC
LIMIT 15
```

> **Note on Path vs File Counts**: `total_paths` may exceed `distinct_files` when:
> - Multiple permission records exist for the same user-folder (e.g., read, write, execute as separate grants)
> - Multiple access paths exist through the folder hierarchy (symlinks, hard links)
> - Path count represents "risk vectors" - more paths = more ways to access the data

### 9. External User Sensitive File Access (Distinct Count)
Filter to sensitive files only and count distinct sensitive files:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)-[:CONTAINS*1..5]->(f:File)
WHERE u.exposure = 'external' AND f.sensitive_data = 1
RETURN 
    u.name AS user_name,
    u.user_id AS user_id,
    root.name AS root_name,
    COUNT(*) AS sensitive_paths,
    COUNT(DISTINCT f) AS distinct_sensitive_files
ORDER BY distinct_sensitive_files DESC
LIMIT 15
```

### 10. High-Risk External Users (Sensitive Data Access)
Find external users with access to the most sensitive files:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*1..3]->(f:File)
WHERE u.exposure = 'external' AND f.sensitive_data = 1
RETURN 
    u.name AS user_name,
    u.department AS department,
    COUNT(DISTINCT f) AS sensitive_file_count
ORDER BY sensitive_file_count DESC
LIMIT 10
```

### 11. Per Root Folder - Users with Sensitive File Access
For each root folder, count users who can access sensitive files within:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)-[:CONTAINS*1..5]->(f:File) 
WHERE f.sensitive_data = 1 
RETURN 
    root.name AS root_name, 
    root.fs_id AS root_id, 
    COUNT(DISTINCT u) AS users_with_access, 
    SUM(CASE WHEN u.exposure = 'external' THEN 1 ELSE 0 END) AS external_user_access_count 
ORDER BY external_user_access_count DESC 
LIMIT 15
```

### 12. Per User Per Root Folder - File Access Matrix
For each user and each root folder they can access, count files and sensitive files:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)-[:CONTAINS*1..5]->(f:File) 
RETURN 
    u.name AS user_name, 
    u.user_id AS user_id, 
    root.name AS root_name, 
    root.fs_id AS root_id, 
    COUNT(DISTINCT f) AS total_files, 
    SUM(CASE WHEN f.sensitive_data = 1 THEN 1 ELSE 0 END) AS sensitive_files 
ORDER BY sensitive_files DESC 
LIMIT 20
```

### 13. External Users Per Root Folder - Sensitive Access
Same as above but filtered to external users only:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(root:Folder)-[:CONTAINS*1..5]->(f:File) 
WHERE u.exposure = 'external' 
RETURN 
    u.name AS user_name, 
    u.user_id AS user_id, 
    root.name AS root_name, 
    root.fs_id AS root_id, 
    COUNT(DISTINCT f) AS total_files, 
    SUM(CASE WHEN f.sensitive_data = 1 THEN 1 ELSE 0 END) AS sensitive_files 
ORDER BY sensitive_files DESC 
LIMIT 20
```

---

## Folder Structure Queries

### 14. Deep Folder Traversal from Root Folders
Find all files nested up to 5 levels deep starting from root folders:

```cypher
MATCH (root:Folder)-[:CONTAINS*1..5]->(f:File)
WHERE root.parent_id = 0
RETURN root.name AS root_folder, f.name AS file_name, f.path
ORDER BY root.name, f.path
LIMIT 50
```

### 15. Root Folder Hierarchy Overview
For each root folder, show folder and file counts at each level:

```cypher
MATCH (root:Folder)-[:CONTAINS*1..3]->(item)
WHERE root.parent_id = 0
RETURN 
    root.name AS root_folder,
    COUNT(DISTINCT item) AS total_items
ORDER BY total_items DESC
LIMIT 20
```

### 16. Folder Permission Audit
List all users with access to a specific folder and its contents:

```cypher
MATCH (u:User)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*0..3]->(item)
WHERE folder.fs_id = 100
RETURN DISTINCT u.name, u.exposure, folder.name
```

### 17. Root Folders Without User Access (Orphaned)
Find root folders that have no direct user permissions:

```cypher
MATCH (root:Folder)
WHERE root.parent_id = 0 
  AND NOT EXISTS { MATCH (u:User)-[:HAS_ACCESS]->(root) }
RETURN root.fs_id, root.name, root.path
ORDER BY root.name
```

---

## Performance Notes

1. **Variable-length paths** (`*1..3`) use recursive CTEs in ClickHouse
2. **Chained patterns** (multiple edge types) generate complex JOINs automatically
3. **Aggregation queries** with `COUNT`, `SUM` work with chained VLP patterns
4. **CASE WHEN** expressions enable conditional counting in aggregations

## Generated SQL Example

The query in #4 generates SQL similar to:

```sql
WITH RECURSIVE variable_path AS (
    -- Base case: direct folder→file connection
    SELECT folder.fs_id AS start_id, file.fs_id AS end_id, 1 AS hop_count, ...
    FROM sec_fs_objects AS folder
    JOIN sec_fs_contents AS rel ON folder.fs_id = rel.parent_id
    JOIN sec_fs_objects AS file ON rel.child_id = file.fs_id
    WHERE rel.child_type = 'File' AND file.sensitive_data = 1
    
    UNION ALL
    
    -- Recursive case: traverse through folders
    SELECT vp.start_id, file.fs_id, vp.hop_count + 1, ...
    FROM variable_path AS vp
    JOIN sec_fs_contents AS rel ON vp.end_id = rel.parent_id
    JOIN sec_fs_objects AS file ON rel.child_id = file.fs_id
    WHERE vp.hop_count < 3 AND rel.child_type = 'File' AND file.sensitive_data = 1
)
SELECT f.name, COUNT(DISTINCT u.user_id), SUM(CASE WHEN u.exposure = 'external' THEN 1 ELSE 0 END)
FROM variable_path AS t
JOIN sec_fs_objects AS folder ON t.start_id = folder.fs_id
JOIN sec_fs_objects AS f ON t.end_id = f.fs_id
JOIN sec_permissions AS edge ON folder.fs_id = edge.object_id
JOIN sec_users AS u ON edge.subject_id = u.user_id
WHERE f.sensitive_data = 1
GROUP BY f.name
ORDER BY external_users DESC
LIMIT 10
```

---

## Testing These Queries

Use curl to test:

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User)-[:HAS_ACCESS]->(folder:Folder)-[:CONTAINS*1..3]->(f:File) WHERE f.sensitive_data = 1 RETURN f.name, COUNT(u) LIMIT 5"}'
```

Or use the Python test client in `tests/`.
