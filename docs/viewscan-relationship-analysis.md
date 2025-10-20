# ViewScan Relationship Query Analysis

**Date**: October 18, 2025  
**Context**: After fixing alias bugs, discovered issues with CTE-based relationship queries

## Question 1: Do we need CTEs for simple relationship queries?

### Answer: **NO** for simple 1-hop traversals

**Current Implementation** (unnecessary complexity):
```sql
-- For: MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) RETURN u.name, f.name
WITH User_u AS (...),
     FRIENDS_WITH_r AS (
       SELECT r.user1_id AS from_id, r.user2_id AS to_id 
       FROM friendships AS r 
       WHERE r.to_id IN (SELECT u.user_id FROM User_u AS t)
     )
SELECT u.name, f.name
FROM ...
```

**Optimal Implementation** (what we should generate):
```sql
-- Simple JOIN - faster, clearer, no CTEs needed
SELECT u.name, f.name
FROM users AS u
INNER JOIN friendships AS r ON u.user_id = r.user1_id
INNER JOIN users AS f ON r.user2_id = f.user_id
LIMIT 5
```

### When CTEs ARE Needed

1. **Variable-length paths**: `-[*]->`, `-[*1..3]->`, `-[*2..]->`
   - Requires recursive CTEs
   - WITH RECURSIVE for unbounded or multi-hop patterns

2. **Complex subquery optimization**
   - Multiple aggregations
   - Reused intermediate results

3. **Recursive queries**
   - Transitive closure
   - Path finding algorithms

### Performance Impact

- **JOINs**: ~10-100x faster for simple queries
- **CTEs**: Necessary evil for recursion, overhead for simple cases

### Recommendation

```rust
// TODO(performance): Refactor relationship planning
// Current: Always uses CTE-based approach (edge list method)
// Needed: 
//   - Simple 1-hop patterns → Direct JOIN
//   - Variable-length (*) → Recursive CTE
//   - Multi-hop (fixed) → Chained JOIN or CTE depending on complexity
// Location: brahmand/src/query_planner/analyzer/graph_traversal_planning.rs
```

---

## Question 2: Truncated Error Messages - Why?

### Answer: **ClickHouse and network libraries truncate long errors**

**The Problem**:
```
Clickhouse Error: Code: 62. DB::Exception: Syntax error: ...
WHERE r.to_id IN (SELECT u.user_id FROM User_u AS t)
)
SEL...   <-- TRUNCATED! Missing the main SELECT
```

**Root Causes**:
1. ClickHouse truncates error messages over ~500-1000 chars
2. HTTP response size limits in `clickhouse` Rust crate
3. JSON serialization may truncate strings

### Solution Applied

Added comprehensive logging in `handlers.rs`:

```rust
async fn execute_cte_queries(...) {
    let ch_query_string = ch_sql_queries.join(" ");
    
    // ✅ BEFORE execution - always log
    log::debug!("Executing SQL:\n{}", ch_query_string);
    
    .query(&ch_query_string)
    .fetch_bytes(output_format)
    .map_err(|e| {
        // ✅ ON ERROR - log full SQL for debugging
        log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", 
                    ch_query_string, e);
        ...
    })?
```

### How to See Full SQL

**Option 1: Debug Script (Visible Logs)**
```bash
python test_relationship_debug.py

# In another terminal:
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) RETURN u.name"}'
```

**Option 2: SQL-Only Mode**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH ...", "sql_only": true}'
```

Returns full generated SQL without executing.

**Option 3: Log Files** (if configured)
```bash
# Set RUST_LOG=debug in environment
export RUST_LOG=debug
cargo run --bin brahmand

# Logs will show:
# [DEBUG] Executing SQL:
# WITH User_u AS (...),
#      FRIENDS_WITH_r AS (...)
# SELECT ...
# [complete query here]
```

### Best Practice Going Forward

1. **Always use `sql_only` mode** during development to see generated SQL
2. **Check debug logs** when queries fail (full SQL + error context)
3. **Add test cases** that verify generated SQL structure
4. **Document** that ClickHouse truncates - this is expected behavior

---

## Current Status

### ✅ Fixed
- Alias preservation through sanitization pass
- Column qualification in WHERE clauses
- CTE nesting prevention

### ⚠️ Remaining Issues
- Empty CTE content for node scans
- CTE-based approach for simple queries (performance)

### 📋 Next Steps

1. **Fix empty CTE content** - Node scans need proper SELECT/FROM in CTEs
2. **Refactor to JOINs** - Simple patterns should generate direct JOINs
3. **Add sql_only tests** - Verify generated SQL structure
4. **Performance benchmarks** - Compare CTE vs JOIN performance

---

## Technical Debt Markers Added

```rust
// TODO(ViewScan): Empty CTE content - Scans in CTEs missing SELECT/FROM
// Location: render_plan/plan_builder.rs, graph_traversal_planning.rs

// TODO(performance): Simple relationships use CTEs, should be JOINs
// Impact: 10-100x slower than necessary for MATCH (a)-[r]->(b) patterns
// Location: query_planner/analyzer/graph_traversal_planning.rs

// FIXME(debugging): ClickHouse truncates errors, use log::debug for full SQL
// Workaround: sql_only mode or check server logs
// Location: server/handlers.rs
```
