# ClickGraph v0.6.0 Release Notes

**Released**: December 22, 2025  
**Test Status**: 2446/3359 passing (72.8%)

## 🎉 Highlights

This release brings **elegant semantic validation** for variable-length paths, preventing invalid recursive patterns and improving performance.

## 🚀 New Features

### VLP Transitivity Check

**The Problem:**
```cypher
MATCH (a:IP)-[r:DNS_REQUESTED*]->(b) RETURN a, b
```

This query looks valid, but it's semantically wrong! Domain nodes never start DNS_REQUESTED edges, so recursion is impossible. Previously, ClickGraph would generate complex recursive CTEs that could never actually recurse.

**The Solution:**
New `VlpTransitivityCheck` analyzer pass validates relationship transitivity:
- Checks if TO nodes can be FROM nodes for the relationship type
- Non-transitive patterns → removes `variable_length` → simple single-hop query
- Errors if `min_hops > 1` on non-transitive (impossible path length)

**Benefits:**
- ✅ No CTE generation for non-transitive patterns (performance++)
- ✅ Sidesteps downstream property expansion issues
- ✅ Clear semantic validation at analyzer level
- ✅ Simpler, more correct SQL

**Example Output:**
```sql
-- Instead of complex recursive CTE:
SELECT r."id.orig_h" AS "a_ip", 
       r.query AS "b_name"
FROM zeek.dns_log AS r
LIMIT 10
```

**Architecture:**
- 283-line analyzer pass at Step 2.5 in pipeline
- Runs after TypeInference, before CTE resolution
- Uses `get_all_rel_schemas_by_type()` to check transitivity

## 🐛 Bug Fixes

### Multi-Table Label Schema Support

Fixed several issues with multi-table label schemas (like zeek_merged):

1. **Denormalization Metadata**: Copy `is_denormalized`, `from_node_properties`, `to_node_properties` from node_schema to ViewScan
2. **Type Inference**: Process children bottom-up for multi-hop pattern label resolution
3. **VLP ID Columns**: Use relationship schema columns (`from_id`/`to_id`) not node schema columns
4. **Relationship Lookup**: Register both composite and simple keys for backward compatibility
5. **Error Handling**: Remove `.unwrap()` calls, use proper Result/Option propagation

### Other Fixes

- **Cycle Prevention**: Skip for `*1` patterns (single hop can't have cycles)
- **Test Compilation**: Fixed missing imports for `Projection` and `ProjectionItem` in tests

## 📊 Test Statistics

- **Overall**: 2446/3359 passing (72.8%)
- **Unit Tests**: 646/655 (98.6%)
- **Integration Tests**: 2446/3359 (72.8%)
- **Matrix Tests**: 283/397 (71.3%)

## 🔄 Upgrade Notes

No breaking changes! This is a pure improvement release.

Simply update your Cargo.toml:
```toml
clickgraph = "0.6.0"
```

## 📝 What's Next

Focus areas for v0.7.0:
- Additional graph algorithms (centrality measures, community detection)
- Pattern extensions (path comprehensions)
- Continued test coverage improvements

## 🙏 Contributors

Thank you to everyone who reported issues and provided feedback!

---

**Full Changelog**: https://github.com/genezhang/clickgraph/blob/main/CHANGELOG.md
