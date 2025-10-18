# Known Issues

## ❌ Windows Native Server Crash (Critical)

**Status**: Open  
**Severity**: High (Windows only)  
**Discovered**: October 15, 2025  
**Workaround**: Use Docker or WSL

### Description
The HTTP server crashes immediately upon receiving **any** HTTP request when running natively on Windows. The server exits cleanly without error messages, panic hooks don't fire, and even minimal test handlers cause the crash.

### Symptoms
- Server starts successfully and binds to port 8080
- Logs show "Brahmand server is running"
- Upon receiving ANY HTTP request (even simple test endpoints), server exits immediately
- No error message, no panic, no stack trace
- Process terminates with exit code 0 (clean exit)

### Testing Performed
1. ✅ **Tested in Docker/Linux**: Server works perfectly! All queries execute correctly.
2. ❌ **Windows native build**: Crashes on any HTTP request
3. Tried minimal test handlers: Still crashes
4. Added panic hooks and extensive logging: Server just exits cleanly
5. Updated axum to 0.8.6 (latest): No change
6. Tested with tokio console: No async runtime issues detected before crash

### Root Cause
Suspected **tokio/axum runtime issue specific to Windows**. Not application code - even the simplest possible handler crashes:
```rust
async fn test_handler() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("test response"))
        .unwrap()
}
```

### Affected Versions
- Windows 10/11 (tested on Windows with PowerShell 5.1)
- axum 0.8.3 → 0.8.6 (all versions affected)
- tokio 1.x (runtime component)

### Workaround ✅
Use Docker or WSL for development and deployment:

```bash
# Docker approach (VERIFIED WORKING)
docker-compose up -d

# Test queries
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:User) RETURN u.full_name LIMIT 5"}'
```

### Server Status by Platform
| Platform | HTTP API | Bolt Protocol | Status |
|----------|----------|---------------|--------|
| Linux (Docker/Native) | ✅ Working | ✅ Working | Fully functional |
| macOS | ❓ Untested | ❓ Untested | Likely works |
| Windows (Native) | ❌ Crashes | ❓ Untested | Use Docker |
| WSL 2 | ✅ Working | ✅ Working | Recommended for Windows |

### Files Involved
- `brahmand/src/server/mod.rs` - Server initialization
- `brahmand/src/server/handlers.rs` - Request handlers
- `brahmand/Cargo.toml` - Dependencies (axum, tokio)

### Next Steps
1. File issue with tokio-rs/axum project
2. Test on different Windows versions
3. Consider alternative async runtime (async-std, smol)
4. Document Docker/WSL as primary deployment method for Windows

---

## ✅ FIXED: GROUP BY Aggregation with Variable-Length Paths

**Status**: Fixed (October 17, 2025)  
**Severity**: Low  
**Fixed in**: commit [pending]

### Description
When using aggregation functions (COUNT, SUM, etc.) with GROUP BY in variable-length path queries, the SQL generator was referencing the original node aliases (e.g., `u1.full_name`) instead of the CTE column aliases (e.g., `t.start_full_name`).

### Example
```cypher
MATCH (u1:User)-[r:FRIEND*1..3]->(u2:User) 
RETURN u1.full_name, u2.full_name, COUNT(*) as path_count
```

**Previous Error**: `Unknown expression identifier 'u1.full_name' in scope`  
**Now**: Works correctly! Expressions are rewritten to use CTE column names.

### Fix Details
Extended the expression rewriting logic to handle GROUP BY and ORDER BY clauses in addition to SELECT items. When a variable-length CTE is present, all property references are automatically rewritten:
- `u1.property` → `t.start_property`
- `u2.property` → `t.end_property`

### Files Modified
- `brahmand/src/render_plan/plan_builder.rs`: Added rewriting for GROUP BY and ORDER BY expressions

---

## 📝 Multi-hop Base Cases (*2, *3..5)

**Status**: Planned  
**Severity**: Low  
**Target**: Future enhancement

### Description
Variable-length paths starting at hop count > 1 (e.g., `*2`, `*3..5`) currently use a placeholder `WHERE false` clause instead of generating proper base cases with chained JOINs.

### Example
```cypher
MATCH (u1:User)-[r:FRIEND*2]->(u2:User) RETURN u1.name, u2.name
```

**Current**: Uses recursive CTE starting from 1, filters to hop_count = 2  
**Desired**: Generate base case with 2 chained JOINs for better performance

### Impact
Functional but suboptimal performance for exact hop count queries.

---

## 📋 Test Coverage Gaps

**Status**: Tracked  
**Severity**: Low  
**Target**: Future enhancement

### Missing Test Scenarios
- Edge cases: 0 hops, negative ranges, circular paths
- Relationship properties in variable-length patterns
- WHERE clauses on path properties
- Multiple variable-length patterns in single query
- Performance benchmarks for deep traversals (>5 hops)

### Impact
Core functionality works, but edge cases may have unexpected behavior.
