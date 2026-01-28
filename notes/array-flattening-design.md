# Array Flattening Design for Denormalized Edge Tables

## User Request Context

A user has Zeek network logs (DNS, connection logs) stored in ClickHouse and wants to model them as graphs. Key use case:

```json
{
  "query": "testmyids.com",
  "answers": ["31.3.245.133", "192.168.1.1"],  // Array of resolved IPs!
  ...
}
```

Graph model needed:
- `(Domain)-[:RESOLVED_TO]->(IP)` 
- One edge per element in `answers` array

## ClickHouse ARRAY JOIN

ClickHouse provides `ARRAY JOIN` to flatten arrays:

```sql
-- Original: 1 row with answers = ['31.3.245.133', '192.168.1.1']
-- After ARRAY JOIN: 2 rows, one per answer

SELECT query, answer 
FROM dns_log 
ARRAY JOIN answers AS answer

-- Result:
-- | query          | answer         |
-- |----------------|----------------|
-- | testmyids.com  | 31.3.245.133   |
-- | testmyids.com  | 192.168.1.1    |
```

For parallel arrays (e.g., `answers` and `TTLs`):
```sql
SELECT query, answer, ttl 
FROM dns_log 
ARRAY JOIN answers AS answer, TTLs AS ttl
```

## Proposed Schema Extension

Add `array_flatten` to relationship definition:

```yaml
relationships:
  - type: RESOLVED_TO
    database: zeek
    table: dns_log
    from_id: query
    to_id: answer  # References the flattened element
    from_node: Domain
    to_node: ResolvedIP
    
    # NEW: Array flattening configuration
    array_flatten:
      # Required: The array column(s) to flatten
      columns:
        - source: answers    # Array column name in table
          alias: answer      # Alias for individual element
        - source: TTLs       # Optional: parallel array
          alias: ttl
      
    edge_id: [uid, answer]  # Include flattened element for uniqueness
```

## Implementation Approach

### Phase 1: Schema Support (This PR)

1. **Add to `StdEdge` config struct:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayFlattenConfig {
    pub columns: Vec<ArrayFlattenColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayFlattenColumn {
    pub source: String,  // Original array column
    pub alias: String,   // Alias for flattened element
}
```

2. **Add to `RelationshipSchema`:**
```rust
pub array_flatten: Option<ArrayFlattenConfig>,
```

3. **Parse in config loading** - validate that:
   - Array columns exist in the table
   - Aliases match columns referenced in `to_id`, `edge_id`, etc.

### Phase 2: SQL Generation

Modify SQL generation to include `ARRAY JOIN` when `array_flatten` is present:

```sql
-- Generated SQL for MATCH (d:Domain)-[:RESOLVED_TO]->(ip:ResolvedIP) RETURN d.name, ip.ip

SELECT query AS "d.name", answer AS "ip.ip"
FROM dns_log
ARRAY JOIN answers AS answer
```

### Phase 3: Property Access

Ensure flattened columns can be accessed as node/edge properties:

- `ip.ip` → `answer` (via to_node_properties mapping)
- `r.ttl` → `ttl` (via edge property_mappings)

## Edge Cases

1. **Empty arrays**: `ARRAY JOIN` excludes rows with empty arrays by default
   - Use `LEFT ARRAY JOIN` to keep rows with empty arrays (NULL values)
   - Schema option: `include_empty: true/false`

2. **Nested arrays**: Not supported in initial implementation
   - `answers: [["a", "b"], ["c"]]` → requires nested ARRAY JOIN

3. **Multiple array columns with different lengths**: 
   - ClickHouse pairs by index; shorter arrays get NULL padding

## Alternative: Flatten at Query Time

Instead of schema configuration, users could use Cypher functions:

```cypher
MATCH (d:Domain) 
UNWIND d.answers AS ip
RETURN d.query, ip
```

However, this requires:
1. Parser support for `UNWIND`
2. Knowledge of array column types
3. More complex queries for users

**Recommendation**: Schema-based approach is cleaner for denormalized edge patterns.

## Implementation Priority

1. ✅ Document use case (this file)
2. ⬜ Add schema support (struct changes)
3. ⬜ SQL generation with ARRAY JOIN  
4. ⬜ E2E tests with Zeek-like data
5. ⬜ User documentation

## Status

- **Current**: Design phase
- **Blocking**: None (existing denormalized support works for non-array cases)
- **User workaround**: Create a ClickHouse VIEW that pre-flattens the array
