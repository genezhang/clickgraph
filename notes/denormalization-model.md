# Denormalization Conceptual Model

## Summary of Fix (Dec 21, 2025)

**Root Cause**: The `rel_type_index` was being built with BOTH simple keys (`"REQUESTED"`) 
AND composite keys (`"REQUESTED::IP::Domain"`) for the same relationship. When 
`expand_generic_relationship_type("REQUESTED")` ran, it returned both entries, causing
the system to think there were 2 relationship types and create a CTE placeholder instead
of using the actual table.

**Fix**: Modified `build_rel_type_index()` in `graph_schema.rs` to skip simple keys when
composite keys exist for the same type. This ensures each relationship type resolves to
exactly ONE entry (the composite key with full type info).

**Result**: 23/24 tests passing (up from 21/24). The remaining failure is unrelated to
denormalization.

---

## The Problem
We were confusing "edge denormalization" with "node denormalization". The edge itself is never "denormalized" - it's always a table. What gets denormalized is the NODES into the edge table.

## Correct Mental Model

### Graph Pattern: `(from_node)-[edge]->(to_node)`

**Three tables in relational model:**
1. `from_node_table` - stores from node properties  
2. `edge_table` - stores edge properties + FKs to nodes
3. `to_node_table` - stores to node properties

**Denormalization options:**
- **Standard (no denormalization)**: Edge table only has FKs, must JOIN to both node tables to get node properties
- **From node denormalized**: Node properties stored IN edge table via `from_node_properties` columns - no JOIN needed to from_node_table
- **To node denormalized**: Node properties stored IN edge table via `to_node_properties` columns - no JOIN needed to to_node_table  
- **Both nodes denormalized**: All node properties in edge table - NO JOINs needed at all

**FK-edge pattern (one-to-many):**
- The "edge" is actually part of the node table
- Example: User table has `follows_user_id` FK column
- Think of it as: edge table IS the node table, and one node is denormalized

## The Simple Rule

**When generating SQL:**
1. **Edge table** - ALWAYS use it as base FROM clause (it's the real table)
2. **From node** - Only JOIN if we need properties AND `from_node_properties` is NOT present
3. **To node** - Only JOIN if we need properties AND `to_node_properties` is NOT present

## Query Examples

### Query 1: `MATCH ()-[r:REQUESTED]->() RETURN count(*) as total`
- No node labels, no node properties accessed
- **SQL**: `SELECT count(*) FROM test_zeek.dns_log AS r`
- **No JOINs needed** - we only access the edge table

### Query 2: `MATCH (a)-[r:REQUESTED]->() RETURN a.ip`
- Need from node property `a.ip`
- Schema has `from_node_properties: ["ip"]`  
- **SQL**: `SELECT r.ip FROM test_zeek.dns_log AS r`  
- **No JOIN needed** - ip is denormalized into dns_log

### Query 3: `MATCH (a:IP)-[r:REQUESTED]->(b:Domain) RETURN a.ip, b.name`
- Need both node properties
- Both denormalized: `from_node_properties: ["ip"]`, `to_node_properties: ["name"]`
- **SQL**: `SELECT r.ip, r.name FROM test_zeek.dns_log AS r`
- **No JOINs needed** - both properties in edge table

### Query 4: Standard edge (not denormalized)
```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.name, b.name
```
- Schema has NO `from_node_properties` or `to_node_properties`
- **SQL**: 
  ```sql
  SELECT a.name, b.name  
  FROM follows_table AS r
  JOIN users_table AS a ON r.from_user_id = a.user_id
  JOIN users_table AS b ON r.to_user_id = b.user_id
  ```
- **JOINs needed** - properties are in separate tables

## Current Code Issues

### Problem 1: Checking "center_is_denormalized"
```rust
let center_is_denormalized = scan.from_node_properties.is_some() 
    && scan.to_node_properties.is_some();
```
This checks if BOTH nodes are denormalized, but:
- We should ALWAYS use the edge table as FROM
- JOINs are decided per-node, not for the entire pattern

### Problem 2: GraphNode.is_denormalized flag
Currently used to mean "this node's properties are in the edge table", but:
- It's set during TypeInference for anonymous nodes
- It should really mean: "skip creating a separate ViewScan/CTE for this node"
- Better name: `is_embedded` or `in_edge_table`

## The Fix

**TypeInference should:**
- Always create ViewScan for the edge/relationship
- For nodes: check if properties are in edge table (via from_node_properties/to_node_properties)
- If yes: mark node with `is_denormalized: true` (meaning: don't create separate ViewScan)
- If no: create separate ViewScan for node table

**RenderPlan should:**
1. **extract_from()**: ALWAYS use the edge ViewScan as the base table
   - Never check "center_is_denormalized" - edge is always the base!
   
2. **extract_joins()**: For each node:
   - If `GraphNode.is_denormalized == true`: Skip JOIN (properties in edge table)
   - If `GraphNode.is_denormalized == false`: Add JOIN to node table

## Implementation Plan

1. **Remove "center_is_denormalized" checks in extract_from()**
   - Edge ViewScan is ALWAYS the FROM table
   - Decision: use actual table name vs CTE reference based on whether it's a rel ViewScan

2. **Keep GraphNode.is_denormalized for JOIN decisions**
   - Rename to `is_embedded_in_edge` for clarity? (optional)
   - Use in extract_joins() to decide whether JOIN is needed

3. **Simplify TypeInference logic**
   - Check if node's required properties are in from_node_properties/to_node_properties
   - If yes: mark node as denormalized (embedded in edge)
   - If no: node needs its own table

## Testing

After fix, all these should work:
- ✅ `MATCH ()-[r:REQUESTED]->() RETURN count(*)` → `FROM dns_log`
- ✅ `MATCH (a)-[r:REQUESTED]->() RETURN a.ip` → `FROM dns_log`, no JOIN
- ✅ `MATCH (a)-[r:REQUESTED]->(b) RETURN a.ip, b.name` → `FROM dns_log`, no JOINs
- ✅ Standard edges with separate node tables → `FROM edge_table JOIN node_tables`
