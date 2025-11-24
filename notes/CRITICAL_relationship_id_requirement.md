# CRITICAL: Relationship ID Requirement for Uniqueness

**Date**: November 22, 2025  
**Status**: 🚨 **FUNDAMENTAL SCHEMA ISSUE**

## The Core Problem

**We cannot enforce relationship uniqueness without a relationship ID!**

### Why `(from_id, to_id)` Is NOT Always a Unique Key

Consider realistic scenarios:

**Example 1: Temporal Relationships**
```
Alice follows Bob on 2024-01-01
Alice unfollows Bob
Alice follows Bob again on 2024-06-01

follows table:
(follower_id=1, followed_id=2, created_at='2024-01-01', id=100)
(follower_id=1, followed_id=2, created_at='2024-06-01', id=200)
```

Same `(from_id, to_id)` = `(1, 2)`, but **two different relationship instances!**

**Example 2: Multiple Relationship Types**
```
message_events table:
(from_user=1, to_user=2, timestamp='2024-01-01 10:00', message_id=500)
(from_user=1, to_user=2, timestamp='2024-01-01 11:00', message_id=501)
(from_user=1, to_user=2, timestamp='2024-01-01 12:00', message_id=502)
```

Same `(from_id, to_id)`, but **multiple messages sent!**

**Example 3: Transaction Graphs**
```
transfers table:
(from_account=100, to_account=200, amount=50, txn_id='tx1')
(from_account=100, to_account=200, amount=75, txn_id='tx2')
(from_account=100, to_account=200, amount=30, txn_id='tx3')
```

Same accounts, **multiple transfers!**

### Current Assumption (WRONG!)

Our current logic assumes:
```sql
-- This assumes (from_id, to_id) uniquely identifies a relationship
WHERE NOT (
    (r1.follower_id = r2.follower_id AND r1.followed_id = r2.followed_id)
)
```

**Problem**: If there are multiple rows with same `(from_id, to_id)`, this filter doesn't prevent reusing **different instances**!

## What Neo4j Does

Neo4j has **built-in relationship IDs** (`id(r)` or `elementId(r)`):

```cypher
MATCH (a)-[r1]-(b)-[r2]-(c)
WHERE id(r1) = id(r2)
RETURN count(*)
-- Result: 0 (enforced by Neo4j internally)
```

Every relationship instance has a unique ID, regardless of endpoints.

## What ClickGraph Must Do

### Option 1: Require Relationship ID in Schema ⭐ **RECOMMENDED**

**Add to schema YAML**:
```yaml
relationships:
  - name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    relationship_id: id  # ← NEW REQUIRED FIELD (or composite key)
    properties:
      - name: follow_date
        column: created_at
```

**Alternatives for `relationship_id`**:

1. **Single column** (most common):
   ```yaml
   relationship_id: id
   ```

2. **Composite key** (for temporal/versioned relationships):
   ```yaml
   relationship_id: [from_id, to_id, created_at]
   ```

3. **Default to (from_id, to_id)** if truly unique:
   ```yaml
   relationship_id: [from_id, to_id]  # Only if guaranteed unique
   ```

### Option 2: Warn User If No Relationship ID

If schema doesn't specify `relationship_id`:

**Warning**:
```
⚠️  WARNING: Relationship 'FOLLOWS' has no relationship_id defined.
Relationship uniqueness cannot be guaranteed for undirected patterns.

If multiple rows can exist with same (from_id, to_id), specify a relationship_id:
  relationship_id: id
  -- OR --
  relationship_id: [from_id, to_id, timestamp]

Continuing with best-effort using (from_id, to_id) as key...
```

### Option 3: Auto-Generate Row Numbers (FRAGILE)

Use ClickHouse row numbering:
```sql
WITH r1_numbered AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY from_id, to_id) as rn
  FROM follows
)
```

**Problems**:
- Non-deterministic order
- Changes with data mutations
- Performance overhead
- **NOT RECOMMENDED**

## Implementation Plan

### Phase 1: Schema Enhancement

**File**: `brahmand/src/graph_catalog/graph_schema.rs`

Add to `RelationshipConfig`:
```rust
pub struct RelationshipConfig {
    pub name: String,
    pub from_id: String,
    pub to_id: String,
    pub relationship_id: Option<RelationshipId>,  // ← NEW
    // ... existing fields
}

pub enum RelationshipId {
    SingleColumn(String),           // "id"
    CompositeKey(Vec<String>),      // ["from_id", "to_id", "timestamp"]
}
```

### Phase 2: SQL Generation

**File**: `src/render_plan/plan_builder.rs`

Track relationship IDs when building joins:
```rust
struct RelationshipInfo {
    alias: String,
    is_undirected: bool,
    id_columns: Vec<String>,  // e.g., ["r1.id"] or ["r1.from_id", "r1.to_id"]
}
```

Generate uniqueness filters:
```rust
fn generate_rel_uniqueness_filter(r1: &RelationshipInfo, r2: &RelationshipInfo) -> String {
    if r1.id_columns.is_empty() || r2.id_columns.is_empty() {
        // Fallback to (from_id, to_id) with warning
        return generate_composite_key_filter(r1, r2);
    }
    
    // Proper filter using relationship IDs
    let conditions: Vec<String> = r1.id_columns.iter()
        .zip(r2.id_columns.iter())
        .map(|(col1, col2)| format!("{} = {}", col1, col2))
        .collect();
    
    format!("NOT ({})", conditions.join(" AND "))
}
```

### Phase 3: Validation

Add schema validation:
```rust
fn validate_relationship_schema(rel: &RelationshipConfig) -> Result<(), SchemaError> {
    if rel.relationship_id.is_none() {
        warn!(
            "Relationship '{}' has no relationship_id. \
             Uniqueness may not be enforced correctly for undirected patterns.",
            rel.name
        );
    }
    Ok(())
}
```

### Phase 4: Documentation

Update schema documentation:
```markdown
## Relationship ID (IMPORTANT!)

For correct uniqueness semantics, specify how to uniquely identify relationship instances:

**Simple case** (auto-increment ID):
```yaml
relationship_id: id
```

**Temporal relationships**:
```yaml
relationship_id: [from_id, to_id, created_at]
```

**If (from_id, to_id) is truly unique**:
```yaml
relationship_id: [from_id, to_id]
```

**If omitted**: ClickGraph will use (from_id, to_id) as default, but this may give 
incorrect results if multiple relationship instances can exist between same nodes!
```

## Examples

### Example 1: Simple Social Graph

```yaml
relationships:
  - name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    relationship_id: id  # ← Each follow has unique ID
```

**SQL Generated**:
```sql
WHERE NOT (r1.id = r2.id)  -- Simple, correct! ✅
```

### Example 2: Temporal Messages

```yaml
relationships:
  - name: SENT_MESSAGE
    table: messages
    from_id: sender_id
    to_id: recipient_id
    relationship_id: message_id  # ← Each message has unique ID
```

**SQL Generated**:
```sql
WHERE NOT (r1.message_id = r2.message_id)  -- Correct! ✅
```

### Example 3: No Explicit ID (Risky)

```yaml
relationships:
  - name: KNOWS
    table: friendships
    from_id: person1_id
    to_id: person2_id
    # relationship_id omitted - assumes (from_id, to_id) is unique
```

**SQL Generated**:
```sql
-- Fallback to composite key
WHERE NOT (
    (r1.person1_id = r2.person1_id AND r1.person2_id = r2.person2_id) OR
    (r1.person1_id = r2.person2_id AND r1.person2_id = r2.person1_id)
)
-- Only works if truly unique! Otherwise incorrect! ⚠️
```

## Migration Path

### For Existing Schemas

1. **Analyze relationships**: Does the table have an ID column?
2. **Add `relationship_id`** to schema YAML
3. **Test**: Run queries with undirected patterns
4. **Verify**: Check that duplicate relationships aren't matched

### For New Schemas

1. **Require `relationship_id`** in schema validation (with warning if omitted)
2. **Document** the requirement prominently
3. **Provide examples** in schema templates

## Performance Impact

### With Relationship ID Column (Recommended)

```sql
WHERE NOT (r1.id = r2.id)
```
- ✅ Single column comparison (fast!)
- ✅ Can use indexes on `id` column
- ✅ O(1) check per row pair

### Without Relationship ID (Fallback)

```sql
WHERE NOT (
    (r1.from_id = r2.from_id AND r1.to_id = r2.to_id) OR
    (r1.from_id = r2.to_id AND r1.to_id = r2.from_id)
)
```
- ⚠️ Four column comparisons
- ⚠️ Composite conditions harder to optimize
- ⚠️ May not be correct if duplicates exist!

**Performance difference**: ~2-3x slower, and potentially WRONG results!

## Key Decisions

### Decision 1: Make `relationship_id` Recommended, Not Required

**Rationale**:
- Some simple graphs truly have unique `(from_id, to_id)` pairs
- Backward compatibility with existing schemas
- Users should understand the implications

**Implementation**:
- Emit **WARNING** if omitted for undirected relationships
- Use `(from_id, to_id)` as fallback
- Document prominently

### Decision 2: Support Both Single Column and Composite Keys

**Rationale**:
- Flexibility for different data models
- Some relationships need temporal/versioned keys
- Schema can express actual uniqueness constraints

**Implementation**:
```yaml
relationship_id: id           # Single column
# OR
relationship_id: [col1, col2] # Composite key
```

### Decision 3: Only Apply to Undirected Patterns

**Rationale**:
- Directed patterns don't have the reuse problem (verified!)
- Avoid unnecessary overhead for directed queries
- Matches actual Neo4j semantics

**Implementation**:
- Check pattern direction before adding filters
- Only generate uniqueness filters for undirected relationships

## Testing Strategy

### Unit Tests

1. Schema parsing with `relationship_id` field
2. Filter generation with single column ID
3. Filter generation with composite key
4. Warning emission when ID omitted

### Integration Tests

1. **Undirected pattern with ID column**: Should prevent reuse
2. **Undirected pattern without ID**: Should emit warning
3. **Directed pattern**: Should NOT add filters
4. **Multiple messages between users**: Should distinguish instances

### Neo4j Verification

1. Create graph with duplicate `(from, to)` pairs
2. Test undirected patterns in Neo4j
3. Verify ClickGraph matches Neo4j results

## Conclusion

**Your gut feeling was 100% correct!** 🎯

- ✅ Directed patterns: Safe (join conditions prevent reuse)
- ⚠️ Undirected patterns: Need relationship IDs
- 🚨 `(from_id, to_id)` alone is NOT sufficient in general case

**Next Steps**:
1. Enhance schema to support `relationship_id` field
2. Update SQL generation to use relationship IDs
3. Add validation warnings for missing IDs
4. Document the requirement clearly
5. Test with realistic multi-instance scenarios

**The fix is well-defined but requires schema changes!**
