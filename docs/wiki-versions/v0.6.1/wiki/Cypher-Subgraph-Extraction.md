> **Note**: This documentation is for ClickGraph v0.6.1. [View latest docs →](../../wiki/Home.md)
# Subgraph Extraction (Nebula GET SUBGRAPH Equivalent)

ClickGraph supports subgraph extraction using standard Cypher `MATCH` patterns. This is equivalent to Nebula Graph's `GET SUBGRAPH` command.

## Use Case

Subgraph extraction is valuable for:
- **GraphRAG**: Extracting local context around entities for LLM prompts
- **Neighborhood analysis**: Finding all connections within N hops
- **Knowledge graph exploration**: Discovering related entities

## Nebula Graph Comparison

### Syntax Mapping

Nebula GET SUBGRAPH syntax:
```
GET SUBGRAPH [WITH PROP] [<step_count> STEPS] FROM {<vid>, <vid>...}
[{IN | OUT | BOTH} <edge_type>, <edge_type>...]
[YIELD [VERTICES AS <vertex_alias>] [,EDGES AS <edge_alias>]];
```

| Nebula Option | Description | ClickGraph Cypher | Status |
|---------------|-------------|-------------------|--------|
| `WITH PROP` | Include properties | Default in RETURN | ✅ |
| `<N> STEPS` | Hop count | `*1..N` or `*N` | ✅ |
| `FROM {vid1, vid2}` | Starting vertices | `WHERE start.id IN [...]` | ✅ |
| `IN` | Incoming edges | `<-[r]-` | ✅ |
| `OUT` | Outgoing edges | `-[r]->` | ✅ |
| `BOTH` | Bidirectional | `-[r]-` (UNION ALL) | ✅ |
| `<edge_type>` | Single edge type | `[:TYPE]` | ✅ |
| `type1, type2` | Multiple types | `[:TYPE1|TYPE2]` | ✅ |
| `YIELD VERTICES` | Return vertices | `RETURN neighbor` | ✅ |
| `YIELD EDGES` | Return edges | `RETURN type(r), r.*` | ✅ |

### Example Mappings

```cypher
-- Nebula: GET SUBGRAPH 2 STEPS FROM "player101" BOTH follow, serve
MATCH (start)-[:follow|serve*1..2]-(neighbor)
WHERE start.id = 'player101'
RETURN DISTINCT start, neighbor

-- Nebula: GET SUBGRAPH WITH PROP 1 STEPS FROM "player101" OUT follow
MATCH (start)-[:follow]->(neighbor)
WHERE start.id = 'player101'
RETURN start.user_id AS head_id, 'follow' AS relation, neighbor.user_id AS tail_id

-- Nebula: GET SUBGRAPH 1 STEPS FROM "player101", "player102" IN follow
MATCH (start)<-[:follow]-(neighbor)
WHERE start.id IN ['player101', 'player102']
RETURN neighbor.id AS head_id, 'follow' AS relation, start.id AS tail_id
```

## Triple Format Output (head, relation, tail)

The most common use case for GraphRAG is extracting triples. Each row represents one edge with its endpoints.

### 1-Hop Subgraph (Direct Connections)

```cypher
-- All edges connected to user 1 (bidirectional)
MATCH (start:User)-[r:FOLLOWS]-(neighbor:User)
WHERE start.user_id = 1
RETURN 
    start.user_id AS head_id, 
    start.name AS head_name,
    'FOLLOWS' AS relation_type,
    neighbor.user_id AS tail_id, 
    neighbor.name AS tail_name
```

**Generated SQL**:
```sql
SELECT * FROM (
  -- Outgoing: start -> neighbor
  SELECT start.user_id AS head_id, start.full_name AS head_name, ...
  FROM users_bench AS start
  INNER JOIN user_follows_bench AS r ON r.follower_id = start.user_id
  INNER JOIN users_bench AS neighbor ON neighbor.user_id = r.followed_id
  WHERE start.user_id = 1
  
  UNION ALL
  
  -- Incoming: neighbor -> start  
  SELECT start.user_id AS head_id, start.full_name AS head_name, ...
  FROM users_bench AS start
  INNER JOIN user_follows_bench AS r ON r.followed_id = start.user_id
  INNER JOIN users_bench AS neighbor ON neighbor.user_id = r.follower_id
  WHERE start.user_id = 1
) AS __union
```

### 2-Hop Subgraph (Extended Neighborhood)

For multi-hop subgraphs, use variable-length paths:

```cypher
-- All nodes within 2 hops of user 1
MATCH (start:User)-[*1..2]-(neighbor:User)
WHERE start.user_id = 1
RETURN DISTINCT 
    start.user_id AS center_id,
    neighbor.user_id AS neighbor_id,
    neighbor.name AS neighbor_name
```

### Directed Subgraph (Outgoing Only)

If you only want outgoing edges:

```cypher
MATCH (start:User)-[r:FOLLOWS]->(neighbor:User)
WHERE start.user_id = 1
RETURN start.user_id AS head_id, 'FOLLOWS' AS relation, neighbor.user_id AS tail_id
```

### Directed Subgraph (Incoming Only)

If you only want incoming edges:

```cypher
MATCH (start:User)<-[r:FOLLOWS]-(neighbor:User)
WHERE start.user_id = 1
RETURN neighbor.user_id AS head_id, 'FOLLOWS' AS relation, start.user_id AS tail_id
```

## Polymorphic Edge Tables

For polymorphic edges where edge type is stored in a column:

```cypher
-- Schema has: type_column: interaction_type
MATCH (start:User)-[r]-(neighbor)
WHERE start.user_id = 1
RETURN 
    start.user_id AS head_id,
    r.interaction_type AS relation_type,  -- Edge type from column
    neighbor.user_id AS tail_id
```

## Multi-Relationship Type Subgraph

Extract multiple relationship types at once:

```cypher
MATCH (start:User)-[r:FOLLOWS|LIKES|PURCHASED]-(neighbor)
WHERE start.user_id = 1
RETURN 
    start.user_id AS head_id,
    type(r) AS relation_type,
    neighbor.user_id AS tail_id
```

## GraphRAG Context Extraction

For GraphRAG applications, extract rich context:

```cypher
-- Get all relationships around a topic entity
MATCH (topic:Topic {name: 'Machine Learning'})-[r]-(related)
RETURN 
    topic.name AS subject,
    type(r) AS predicate,
    COALESCE(related.name, related.title, toString(related.id)) AS object
```

## Best Practices

### 1. Use DISTINCT for Multi-Hop

Multi-hop patterns can return duplicate paths:

```cypher
-- Good: Deduplicate results
MATCH (start)-[*1..3]-(neighbor)
WHERE start.id = 1
RETURN DISTINCT neighbor.id, neighbor.name

-- Without DISTINCT, same neighbor may appear multiple times
```

### 2. Limit Results for Large Graphs

Always limit results for large graphs:

```cypher
MATCH (start:User)-[r]-(neighbor)
WHERE start.user_id = 1
RETURN start.name, type(r), neighbor.name
LIMIT 100
```

### 3. Filter by Relationship Type

Be specific about relationship types when possible:

```cypher
-- More efficient: specific type
MATCH (start:User)-[:FOLLOWS]-(neighbor:User)

-- Less efficient: all types
MATCH (start:User)-[r]-(neighbor)
```

### 4. Use Node Labels

Always specify node labels for better performance:

```cypher
-- Good: labels specified
MATCH (start:User {id: 1})-[:FOLLOWS]->(neighbor:User)

-- Less efficient: no labels
MATCH (start {id: 1})-[:FOLLOWS]->(neighbor)
```

## Performance Considerations

1. **1-hop subgraphs** are efficient (simple JOINs)
2. **Multi-hop subgraphs** use recursive CTEs (more expensive)
3. **Bidirectional patterns** generate UNION ALL (2x the queries)
4. **DISTINCT** on large results requires sorting

---

## Verified Pattern Support Matrix

The following patterns have been verified working (as of January 2026):

### Edge Type Specification

| Pattern | Description | Status | SQL Strategy |
|---------|-------------|--------|--------------|
| `[:TYPE]` | Single specific type | ✅ | Direct JOIN |
| `[:TYPE*1..N]` | Single type, variable length | ✅ | Recursive CTE |
| `[:TYPE1\|TYPE2]` | Multiple explicit types | ✅ | UNION ALL branches |
| `[:TYPE1\|TYPE2*1..N]` | Multiple types, variable length | ✅ | UNION ALL + recursive CTE |
| `[*1..N]` | Generic (infer from schema) | ✅* | UNION ALL over inferred types |

\* Generic patterns work when **≤4 matching edge types** can be inferred from the schema. Exceeding this limit returns an error asking for explicit types.

### Direction Support

| Pattern | Description | Status | Notes |
|---------|-------------|--------|-------|
| `-[r]->` | Outgoing | ✅ | Standard join direction |
| `<-[r]-` | Incoming | ✅ | Swaps from/to columns |
| `-[r]-` | Undirected | ✅ | UNION ALL of both directions |

### Hop Count Support

| Pattern | Description | Status | SQL Strategy |
|---------|-------------|--------|--------------|
| `*N` | Exactly N hops | ✅ | Chained JOINs (optimized) |
| `*M..N` | Range M to N hops | ✅ | Recursive CTE with hop counter |
| `*..N` | Up to N hops | ✅ | Recursive CTE (starts at 1) |
| `*N..` | At least N hops | ✅ | Recursive CTE with max depth config |

### Schema Pattern Support

| Schema Pattern | Description | VLP Status | Notes |
|---------------|-------------|------------|-------|
| Standard node/edge tables | Separate tables for nodes and edges | ✅ | Full VLP support |
| FK-edge pattern | Relationship via FK on node table | ✅ | 2-table JOINs in CTE |
| Denormalized edges | Node properties in edge table | ✅ | Virtual node resolution |
| Polymorphic edges | Type discriminator column | ✅ | `type_column` in schema |
| Coupled edges | Multiple types in one table | ✅ | Schema filter constraints |

### Edge Constraints (ClickGraph Differentiator)

Edge constraints define validation rules in the schema that filter invalid paths:

```yaml
edges:
  - type: COPIED_BY
    constraints: "from.timestamp <= to.timestamp"
```

| Feature | Status | Notes |
|---------|--------|-------|
| Constraint in base case | ✅ | `start_node.timestamp <= end_node.timestamp` |
| Constraint in recursive case | ✅ | `current_node.timestamp <= end_node.timestamp` |
| Constraint with relationship filters | ✅ | Both applied correctly |

**Example**: Data lineage with temporal ordering
```cypher
-- Finds valid forward-in-time paths only
MATCH (f:DataFile {file_id: 1})-[:COPIED_BY*1..3]->(d:DataFile)
RETURN f.path, d.path
```

---

## Limitations

1. **Path deduplication**: For complex multi-hop patterns, consider using DISTINCT at the row level.

2. **Mixed node types**: When extracting subgraphs with multiple node types, results are unioned from separate node tables.

3. **YIELD EDGES format**: Nebula returns edge objects directly; in Cypher, use `type(r)` and edge properties to construct equivalent output.

4. **Generic pattern type limit**: The `[*1..N]` pattern without explicit types is limited to schemas where ≤4 edge types match the inferred pattern. For larger schemas, explicitly list the types: `[:TYPE1|TYPE2|TYPE3*1..N]`.

5. **Relationship property filters in VLP**: While relationship property filters work in variable-length paths, they are applied at each hop in the recursive CTE, not as post-filtering.

---

## Example: Complete Subgraph for GraphRAG

```cypher
-- Extract 1-hop context for an entity (GraphRAG use case)
MATCH (entity:Entity {id: $entity_id})-[r]-(related)
RETURN 
    entity.name AS subject,
    type(r) AS predicate,
    related.name AS object,
    related.description AS object_context
LIMIT 50
```

This returns triples suitable for building LLM context windows.
