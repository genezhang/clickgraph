# Duplicate Results Investigation

## Problem
Query returns duplicate results:
- Actual: `[{"mutual.name":"Charlie"},{"mutual.name":"Charlie"},{"mutual.name":"Diana"},{"mutual.name":"Diana"}]`
- Expected: `[{"mutual.name":"Charlie"},{"mutual.name":"Diana"}]`

## Query Pattern
```cypher
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN mutual.name
```

## Analysis

### Cypher Semantics
In standard Cypher (Neo4j):
- `MATCH` returns ALL matching patterns
- Duplicates ARE expected if multiple paths exist
- To remove duplicates, use `RETURN DISTINCT mutual.name`

### Why Duplicates Occur
The pattern creates a bidirectional traversal:
1. `(a)-[:FOLLOWS]->(mutual)` - Find all mutuals that Alice follows
2. `(mutual)<-[:FOLLOWS]-(b)` - For each mutual, find all users who follow it

If the data has:
- Alice → Charlie
- Bob → Charlie  
- Alice → Diana
- Bob → Diana

The SQL JOIN creates:
- Row 1: a=Alice, rel1=(Alice→Charlie), mutual=Charlie, rel2=(Bob→Charlie), b=Bob ✅
- Row 2: a=Alice, rel1=(Alice→Diana), mutual=Diana, rel2=(Bob→Diana), b=Bob ✅

This should give 2 rows, not 4!

### Hypothesis: Why Are We Getting 4 Rows?

**Possible causes:**
1. **Duplicate relationship entries** in `user_follows_bench` table
2. **Cartesian product** from incorrect JOIN logic
3. **Multiple relationship records** for the same follow (shouldn't happen but possible)

## SQL Generation Check

Expected SQL:
```sql
SELECT mutual.full_name AS "mutual.name"
FROM brahmand.users_bench AS b
INNER JOIN brahmand.user_follows_bench AS rel1 ON rel1.follower_id = b.user_id
INNER JOIN brahmand.users_bench AS mutual ON mutual.user_id = rel1.followed_id
INNER JOIN brahmand.user_follows_bench AS rel2 ON rel2.followed_id = mutual.user_id
INNER JOIN brahmand.users_bench AS a ON a.user_id = rel2.follower_id
WHERE b.full_name = 'Bob' AND a.full_name = 'Alice'
```

This SQL should only return 2 rows IF:
- Each follow relationship appears once in the table
- JOIN conditions are correct

## Next Steps

1. ✅ Need to examine actual generated SQL from server logs
2. ✅ Check if there are duplicate FOLLOWS records in test data
3. ✅ Verify JOIN conditions are correct
4. ⏳ Determine if we need DISTINCT by default or if SQL is wrong
