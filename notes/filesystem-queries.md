# Filesystem Hierarchical Queries - Test Results

## Schema: `filesystem_single.yaml` (FK-Edge Pattern)

**Table**: `test_integration.fs_objects_single`

| object_id | name         | object_type | parent_id |
|-----------|--------------|-------------|-----------|
| 1         | root         | folder      | NULL      |
| 2         | Documents    | folder      | 1         |
| 3         | Projects     | folder      | 1         |
| 4         | Pictures     | folder      | 1         |
| 5         | Work         | folder      | 2         |
| 6         | Personal     | folder      | 2         |
| 7         | report.pdf   | file        | 5         |
| 8         | notes.txt    | file        | 5         |
| 9         | vacation.jpg | file        | 4         |
| 10        | family.png   | file        | 4         |

**Tree Structure:**
```
root (1)
├── Documents (2)
│   ├── Work (5)
│   │   ├── report.pdf (7)
│   │   └── notes.txt (8)
│   └── Personal (6)
├── Pictures (4)
│   ├── vacation.jpg (9)
│   └── family.png (10)
└── Projects (3)
```

---

## Working Queries

### 1. Find All Root Objects ✅

```cypher
MATCH (r:Object) WHERE r.parent_id IS NULL RETURN r.name, r.type
```

**Result**: `{"r.name": "root", "r.type": "folder"}`

---

### 2. Count Root Objects ✅

```cypher
MATCH (r:Object) WHERE r.parent_id IS NULL RETURN count(DISTINCT r) AS root_count
```

**Result**: `{"root_count": 1}`

---

### 3. Find All Descendants of Root ✅

Using **Outgoing** direction: `(parent)-[:PARENT*]->(child)`

```cypher
MATCH (root:Object)-[:PARENT*1..10]->(descendant:Object)
WHERE root.parent_id IS NULL
RETURN descendant.name, descendant.type
```

**Result**: Returns all 9 descendants (3 folders + 4 files + 2 subfolders)

Note: Direction is **Outgoing** because the PARENT relationship is defined as `from_id: parent_id, to_id: object_id`, meaning `child.parent_id -> parent.object_id`. So following outward from `root` means finding all rows that point to root as their parent.

---

### 4. Count Descendants per Root ✅

```cypher
MATCH (root:Object)-[:PARENT*1..10]->(descendant:Object)
WHERE root.parent_id IS NULL
RETURN root.name, count(*) AS total_descendants
```

**Result**: `{"root_name": "root", "total_descendants": 9}`

---

### 5. Find Ancestors of a File ⚠️ BUG

```cypher
MATCH (file:Object)-[:PARENT*1..10]->(ancestor:Object)
WHERE file.name = 'notes.txt'
RETURN file.name, ancestor.name
```

**Expected**: Should return ancestors: Work → Documents → root

**Actual**: Empty result

**Bug**: The variable-length path CTE generation does a 3-way self-join (`start_node JOIN rel JOIN end_node`) which doesn't work for FK-edge patterns where the edge IS the same table as the nodes. For FK-edge, we need a 2-way join: `start_node.parent_id = end_node.object_id`.

---

### 6. Collect Ancestors ⚠️ BLOCKED

Blocked by Query 5 bug.

---

## Known Issues

### Issue 1: FK-Edge CTE Generation Bug

**Problem**: The recursive CTE for variable-length paths assumes a separate edge table, doing:
```sql
FROM start_node
JOIN rel ON start_node.object_id = rel.parent_id  -- WRONG for FK-edge
JOIN end_node ON rel.object_id = end_node.object_id
```

For FK-edge patterns, the correct join is:
```sql
FROM start_node
JOIN end_node ON start_node.parent_id = end_node.object_id  -- CORRECT
```

**Root Cause**: `build_recursive_cte_for_variable_path` in `render_plan/mod.rs` doesn't handle the FK-edge case where edge table = node table.

**Workaround**: Fixed-hop traversals work. Only variable-length (`*1..10`) paths are affected.

### Issue 2: Property Mapping in CASE Expressions

When using CASE expressions inside aggregates:
```cypher
RETURN sum(CASE WHEN d.type = 'folder' THEN 1 ELSE 0 END) AS folder_count
```

The property `d.type` doesn't get mapped to `d.object_type` because property mapping isn't applied recursively inside CASE expressions.

---

## Summary

| Query | Status | Notes |
|-------|--------|-------|
| 1. Find roots | ✅ Working | Simple filter |
| 2. Count roots | ✅ Working | count(DISTINCT n) |
| 3. Find descendants | ✅ Working | Outgoing direction |
| 4. Count descendants | ✅ Working | count(*) with GROUP BY |
| 5. Find ancestors | ❌ Bug | FK-edge CTE generation issue |
| 6. Collect ancestors | ❌ Blocked | Depends on #5 |
