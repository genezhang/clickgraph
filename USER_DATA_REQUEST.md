# User Duplicate Results Investigation - Data Request

**Status**: Waiting for user's actual data

## What We Need from User

To investigate why they're seeing duplicate results for the mutual friends query, we need:

### 1. Table Structure
```sql
-- What tables do they have?
SHOW TABLES FROM <their_database>;

-- What columns?
DESCRIBE <their_users_table>;
DESCRIBE <their_relationships_table>;
```

### 2. Actual Data
```sql
-- All users (especially Alice and Bob)
SELECT * FROM <users_table>;

-- All relationships
SELECT * FROM <relationships_table>;

-- Or specific:
SELECT * FROM <users_table> WHERE name IN ('Alice', 'Bob');
SELECT * FROM <relationships_table> 
WHERE follower_id IN (SELECT user_id FROM users WHERE name IN ('Alice', 'Bob'))
   OR followed_id IN (SELECT user_id FROM users WHERE name IN ('Alice', 'Bob'));
```

### 3. Exact Query They're Running
```cypher
-- The exact Cypher query that shows duplicates
MATCH (a:User)-[:FOLLOWS]->(mutual:User)<-[:FOLLOWS]-(b:User) 
WHERE a.name = "Alice" AND b.name = "Bob" 
RETURN mutual.name

-- How many results? What are they?
```

### 4. Schema Configuration
```yaml
# Their graph schema YAML file
# What property mappings are they using?
# What are the table names in the schema config?
```

## What We Know So Far

✅ **Verified with test data**: Both Neo4j and ClickGraph return 1 result (Charlie) with NO duplicates

Possible causes if user IS seeing duplicates:

1. **Duplicate relationships in database**
   - Check: `SELECT follower_id, followed_id, count(*) FROM follows GROUP BY follower_id, followed_id HAVING count(*) > 1`
   - If they have Alice→Charlie twice and Bob→Charlie twice, they'll get 4 results

2. **Different query than test case**
   - Bidirectional patterns: `-[:FOLLOWS]-` instead of `-[:FOLLOWS]->`
   - Missing WHERE clause
   - Different property names

3. **Schema mapping issue**
   - Wrong table being queried
   - Property names don't match columns
   - Multiple relationship types with same name

4. **Additional relationships**
   - More complex graph structure than test case
   - Multiple paths between Alice/Bob and mutual friends

## Investigation Script Ready

Run `scripts/test/investigate_user_duplicates.py` once we have their data loaded to:
- Check for duplicate relationships
- Verify Alice and Bob exist
- Map the actual paths in their graph
- Test their exact query

## Next Steps

1. **Wait for user data** ✓ (in progress)
2. Load their data into test_integration database
3. Run their exact query
4. Identify root cause
5. Provide solution or confirm bug
