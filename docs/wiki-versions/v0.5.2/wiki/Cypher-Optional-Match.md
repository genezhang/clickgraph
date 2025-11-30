> **Note**: This documentation is for ClickGraph v0.5.2. [View latest docs →](../../wiki/Home.md)
# OPTIONAL MATCH Feature Guide

## Overview

ClickGraph fully supports OpenCypher `OPTIONAL MATCH` patterns, providing LEFT JOIN semantics for optional graph relationships. When a pattern in an `OPTIONAL MATCH` clause doesn't match, NULL values are returned instead of filtering out the entire row.

**Status**: ✅ **Production Ready** (October 17, 2025)
- 11/11 tests passing (100%)
- Complete LEFT JOIN SQL generation
- Full integration with query planner

## Basic Syntax

```cypher
MATCH <required-pattern>
OPTIONAL MATCH <optional-pattern>
[WHERE <condition>]
RETURN <columns>
```

## Use Cases

### 1. Find Entities with Optional Relationships

**Problem**: Get all users, showing friends if they exist

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
RETURN u.name, friend.name
```

**Result**:
| u.name  | friend.name |
|---------|-------------|
| Alice   | Bob         |
| Alice   | Charlie     |
| Bob     | Alice       |
| Charlie | NULL        |
| Diana   | NULL        |

→ Charlie and Diana have no friends, so `friend.name` is NULL

### 2. Aggregate with Optional Patterns

**Problem**: Count relationships that may not exist

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
RETURN u.name, COUNT(friend) as friend_count
ORDER BY friend_count DESC
```

**Result**:
| u.name  | friend_count |
|---------|--------------|
| Alice   | 2            |
| Bob     | 1            |
| Charlie | 0            |
| Diana   | 0            |

→ `COUNT(friend)` correctly counts NULL as 0

### 3. Multiple Optional Patterns

**Problem**: Check multiple optional relationships independently

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:LIKES]->(p:Post)
OPTIONAL MATCH (u)-[:FOLLOWS]->(other:User)
RETURN u.name, p.title, other.name
```

**SQL Generated**:
```sql
SELECT u.name, p.title, other.name
FROM users AS u
LEFT JOIN user_likes AS ul ON u.user_id = ul.user_id
LEFT JOIN posts AS p ON ul.post_id = p.post_id
LEFT JOIN user_follows AS uf ON u.user_id = uf.follower_id
LEFT JOIN users AS other ON uf.followed_id = other.user_id
```

→ Each `OPTIONAL MATCH` generates an independent `LEFT JOIN`

### 4. Mixed Required and Optional Patterns

**Problem**: Filter on required relationships, show optional ones

```cypher
MATCH (u:User)-[:AUTHORED]->(p:Post)
OPTIONAL MATCH (p)-[:LIKED_BY]->(liker:User)
WHERE p.created_date > '2024-01-01'
RETURN u.name AS author, 
       p.title, 
       COUNT(DISTINCT liker) as likes
GROUP BY u.name, p.title
```

**SQL Generated**:
```sql
SELECT u.name AS author, 
       p.title,
       COUNT(DISTINCT liker.user_id) as likes
FROM users AS u
INNER JOIN posts AS p ON u.user_id = p.author_id  -- Required
LEFT JOIN post_likes AS pl ON p.post_id = pl.post_id  -- Optional
LEFT JOIN users AS liker ON pl.user_id = liker.user_id
WHERE p.created_date > '2024-01-01'
GROUP BY u.name, p.title
```

→ `AUTHORED` uses `INNER JOIN` (required), `LIKED_BY` uses `LEFT JOIN` (optional)

### 5. Optional Patterns with WHERE Filters

**Problem**: Filter optional matches but keep unmatched rows

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
WHERE friend.age > 25
RETURN u.name, friend.name, friend.age
```

**Behavior**:
- Users with no friends → 1 row with friend columns = NULL
- Users with friends under 25 → 1 row with friend columns = NULL
- Users with friends over 25 → Multiple rows (one per friend)

**Note**: The WHERE clause filters *which friends* are returned, not *which users* are returned.

## NULL Handling

### Checking for Existence

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
RETURN u.name, 
       CASE WHEN friend IS NULL 
            THEN 'No friends' 
            ELSE friend.name 
       END as friend_status
```

### Filtering NULL Values

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
WHERE friend IS NOT NULL
RETURN u.name, friend.name
```

→ Equivalent to a regular `MATCH` (filters out users with no friends)

### Aggregation with NULLs

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
RETURN u.name,
       COUNT(*)            as total_rows,     -- Includes NULL rows
       COUNT(friend)       as friend_count,   -- Excludes NULL
       COUNT(friend.name)  as named_friends   -- Excludes NULL
```

## Performance Considerations

### Efficient Queries

✅ **Good**: Optional match after filtering
```cypher
MATCH (u:User)
WHERE u.country = 'USA'
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
RETURN u.name, friend.name
```
→ Filters users first, then LEFT JOIN on smaller dataset

❌ **Less Efficient**: Filter in optional match
```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
WHERE u.country = 'USA'  -- Filters AFTER join
RETURN u.name, friend.name
```
→ Performs full LEFT JOIN, then filters (larger intermediate result)

### Index Usage

OPTIONAL MATCH patterns benefit from indexes on:
- Foreign key columns (`user_id`, `post_id`, etc.)
- Filtered columns in WHERE clauses
- Columns used in aggregations

## Advanced Patterns

### Nested Optional Matches

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
OPTIONAL MATCH (friend)-[:LIKES]->(p:Post)
RETURN u.name, friend.name, p.title
```

→ Two-level optional traversal: users → friends → posts they like

### Combining with Variable-Length Paths

```cypher
MATCH (u:User {name: 'Alice'})
OPTIONAL MATCH (u)-[:FRIENDS_WITH*1..3]->(distant:User)
RETURN u.name, distant.name, LENGTH(path) as distance
```

→ Find friends within 3 hops, return NULL if no path exists

## Implementation Details

### How It Works

1. **Parser**: Recognizes `OPTIONAL MATCH` as two-word keyword
2. **Logical Plan**: Marks aliases from optional patterns in `PlanCtx.optional_aliases`
3. **Join Inference**: Checks if alias is optional before generating JOIN
4. **SQL Generation**: Emits `LEFT JOIN` instead of `INNER JOIN`

### SQL Translation Examples

**Cypher**:
```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
RETURN u.name, friend.name
```

**Generated ClickHouse SQL**:
```sql
SELECT u.name, friend.name
FROM users AS u
LEFT JOIN friendships AS f 
  ON u.user_id = f.user1_id
LEFT JOIN users AS friend 
  ON f.user2_id = friend.user_id
```

### Performance Characteristics

- **LEFT JOIN Generation**: O(1) overhead per join (HashSet lookup)
- **No Regular MATCH Impact**: Zero performance cost for queries without OPTIONAL MATCH
- **ClickHouse Optimization**: LEFT JOINs optimized by ClickHouse query planner

## Testing

### Test Coverage

✅ **Parser Tests** (9/9 passing):
- Two-word keyword recognition
- Single optional match
- Multiple optional matches
- Optional match with WHERE
- Path patterns and property access

✅ **Logical Plan Tests** (2/2 passing):
- Alias marking in PlanCtx
- Integration with query builder

✅ **SQL Generation Tests**:
- LEFT JOIN at all join sites (14+ locations)
- Mixed INNER + LEFT JOINs
- Property selection in optional patterns

### Running Tests

```bash
# Run all tests
cargo test

# Run OPTIONAL MATCH specific tests
cargo test optional_match

# Run with output
cargo test optional_match -- --nocapture
```

## Troubleshooting

### Common Issues

**Issue**: OPTIONAL MATCH returns fewer rows than expected

**Solution**: Check if WHERE clause is in the right place
```cypher
-- ❌ Wrong: Filters users
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
WHERE friend.age > 25  -- Excludes users with no friends over 25

-- ✅ Correct: Filters friends only
MATCH (u:User)
WHERE u.active = true
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
  AND friend.age > 25
```

**Issue**: Aggregation produces unexpected results

**Solution**: Use `COUNT(<column>)` not `COUNT(*)`
```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FRIENDS_WITH]->(friend:User)
RETURN u.name,
       COUNT(*)       as total_rows,    -- Counts NULL rows
       COUNT(friend)  as friend_count   -- Excludes NULL (correct)
```

## Comparison with Neo4j

ClickGraph's OPTIONAL MATCH implementation follows Neo4j semantics:

| Feature | Neo4j | ClickGraph | Notes |
|---------|-------|------------|-------|
| Basic OPTIONAL MATCH | ✅ | ✅ | Identical behavior |
| Multiple OPTIONAL clauses | ✅ | ✅ | Independent LEFT JOINs |
| Optional + WHERE | ✅ | ✅ | Same filtering rules |
| NULL handling | ✅ | ✅ | Compatible semantics |
| Aggregation | ✅ | ✅ | COUNT behavior matches |

## References

- **OpenCypher Specification**: [OPTIONAL MATCH clause](http://www.opencypher.org/)
- **Neo4j Documentation**: [OPTIONAL MATCH](https://neo4j.com/docs/cypher-manual/current/clauses/optional-match/)
- **ClickGraph Implementation**: See `OPTIONAL_MATCH_COMPLETE.md` for technical details

## Related Features

- [Variable-Length Paths](docs/variable-length-paths-guide.md) - Recursive traversals
- [Relationship Patterns](docs/features.md#relationships) - Basic relationship matching
- [Aggregations](docs/features.md#aggregations) - COUNT, SUM, AVG with optional matches

---

**Last Updated**: October 17, 2025  
**Status**: Production Ready ✅  
**Test Coverage**: 11/11 (100%)



