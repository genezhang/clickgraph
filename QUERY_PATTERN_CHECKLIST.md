# Query Pattern Test Checklist

**Purpose**: Comprehensive validation of all Cypher query patterns  
**Status**: In Progress - Building comprehensive coverage  
**Last Updated**: November 25, 2025

This checklist tracks which query patterns work correctly across standard and denormalized schemas.

---

## ✅ Legend

- ✅ **Working** - Tested and passing
- 🔄 **Needs Testing** - Not yet validated
- ⚠️ **Partial** - Works in some cases, needs more testing
- ❌ **Broken** - Known issue
- 🚫 **Not Supported** - Out of scope (write operations)

---

## 1. Basic Node Patterns

### 1.1 Node Matching
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `MATCH (n)` | ✅ | ✅ | All nodes |
| `MATCH (n:Label)` | ✅ | ✅ | With label |
| `MATCH (n:Label1\|Label2)` | 🔄 | 🔄 | Multi-label OR |
| `MATCH (n {prop: value})` | 🔄 | 🔄 | Inline properties |

### 1.2 Node Properties
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `RETURN n.property` | ✅ | ✅ | Simple property access |
| `WHERE n.prop = value` | ✅ | ✅ | Property filter |
| `WHERE n.prop1 + n.prop2 < 10` | ✅ | ✅ | **FIXED TODAY** - Mixed expressions |
| `WHERE n.prop IN [1,2,3]` | 🔄 | 🔄 | IN operator |
| `WHERE n.prop IS NULL` | 🔄 | 🔄 | NULL check |
| `WHERE n.prop =~ 'regex'` | 🔄 | 🔄 | Regex match |

---

## 2. Relationship Patterns

### 2.1 Basic Relationships
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `(a)-[r]->(b)` | ✅ | ✅ | Directed |
| `(a)-[r:TYPE]->(b)` | ✅ | ✅ | Typed |
| `(a)-[r]-(b)` | ✅ | ✅ | Undirected |
| `(a)<-[r]-(b)` | ✅ | ✅ | Reverse direction |
| `(a)-[r:TYPE1\|TYPE2]->(b)` | ✅ | ✅ | Multiple types |
| `(a)-[r WHERE r.prop > 10]->(b)` | 🔄 | 🔄 | Inline WHERE |

### 2.2 Multi-Hop Patterns
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `(a)-[]->(b)-[]->(c)` | ✅ | ⚠️ | 2-hop, **denorm needs testing** |
| `(a)-[]->(b)-[]->(c)-[]->(d)` | ✅ | 🔄 | 3-hop |
| `(a)-[:T1]->(b)-[:T2]->(c)` | ✅ | 🔄 | Mixed relationship types |
| `(a)-[r1]->(b)<-[r2]-(c)` | 🔄 | 🔄 | Converging paths |
| `(a)-[]->(b)-[]->(a)` | 🔄 | 🔄 | Cyclic pattern |

### 2.3 Variable-Length Paths
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `(a)-[*]->(b)` | ✅ | 🔄 | Unbounded |
| `(a)-[*2]->(b)` | ✅ | 🔄 | Exact hops |
| `(a)-[*1..3]->(b)` | ✅ | 🔄 | Bounded range |
| `(a)-[*..5]->(b)` | ✅ | 🔄 | Max depth |
| `(a)-[*2..]->(b)` | ✅ | 🔄 | Min depth |
| `(a)-[:TYPE*1..3]->(b)` | ✅ | 🔄 | Typed variable-length |
| `(a)-[:T1\|T2*2..4]->(b)` | ✅ | 🔄 | Multi-type variable-length |

---

## 3. Mixed Property Expressions (NEW ✅)

### 3.1 WHERE Clause
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `WHERE u1.id + u2.id < 10` | ✅ | ✅ | Arithmetic across nodes |
| `WHERE length(s.code) + length(t.code) > 5` | ✅ | ✅ | Functions on multiple nodes |
| `WHERE u1.age > u2.age` | 🔄 | 🔄 | Comparison across nodes |
| `WHERE concat(a.x, b.y) = 'value'` | ✅ | 🔄 | String concat |
| `WHERE a.x * b.y + c.z > 100` | 🔄 | 🔄 | Three-node expression |
| `WHERE r.weight * (u1.score + u2.score) > 50` | 🔄 | 🔄 | Edge + node properties |

### 3.2 RETURN Clause
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `RETURN u1.x + u2.x` | ✅ | ✅ | Simple arithmetic |
| `RETURN concat(s.code, '-', t.code)` | ✅ | ✅ | String functions |
| `RETURN u1.score / u2.score AS ratio` | 🔄 | 🔄 | Division |
| `RETURN CASE WHEN a.x > b.y THEN...` | 🔄 | 🔄 | CASE with mixed props |

### 3.3 ORDER BY Clause  
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `ORDER BY u1.x + u2.x` | ✅ | 🔄 | Mixed expression ordering |
| `ORDER BY u1.name, u2.name` | 🔄 | 🔄 | Multiple node properties |

---

## 4. Aggregations

### 4.1 Basic Aggregations
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `RETURN COUNT(*)` | ✅ | 🔄 | Count all |
| `RETURN COUNT(n)` | ✅ | 🔄 | Count nodes |
| `RETURN COUNT(DISTINCT n.prop)` | ✅ | 🔄 | Distinct count |
| `RETURN SUM(n.value)` | ✅ | 🔄 | Sum |
| `RETURN AVG(n.value)` | ✅ | 🔄 | Average |
| `RETURN MIN(n.value), MAX(n.value)` | ✅ | 🔄 | Min/Max |
| `RETURN collect(n.name)` | 🔄 | 🔄 | Collect into list |

### 4.2 GROUP BY Patterns
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `RETURN n.type, COUNT(*)` | ✅ | 🔄 | Group by property |
| `RETURN n.category, SUM(n.value)` | ✅ | 🔄 | Group with aggregation |
| `WITH n, COUNT(*) AS cnt WHERE cnt > 5` | 🔄 | 🔄 | HAVING equivalent |

---

## 5. Path Functions

### 5.1 Path Variables
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `p = (a)-[*]->(b) RETURN p` | ✅ | 🔄 | Path assignment |
| `RETURN length(p)` | ✅ | 🔄 | Path length |
| `RETURN nodes(p)` | ✅ | 🔄 | Nodes in path |
| `RETURN relationships(p)` | ✅ | 🔄 | Relationships in path |

### 5.2 Shortest Path
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `shortestPath((a)-[*]-(b))` | ✅ | 🔄 | Single shortest |
| `allShortestPaths((a)-[*]-(b))` | ✅ | 🔄 | All shortest |
| `shortestPath((a)-[:TYPE*]-(b))` | ✅ | 🔄 | Typed shortest path |

---

## 6. Optional Patterns

### 6.1 OPTIONAL MATCH
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `OPTIONAL MATCH (a)-[]->(b)` | ✅ | 🔄 | Basic optional |
| `OPTIONAL MATCH (a)-[:TYPE]->(b)` | ✅ | 🔄 | Typed optional |
| `MATCH (a) OPTIONAL MATCH (a)-[]->(b)` | ✅ | 🔄 | Mixed required/optional |
| Multiple OPTIONAL MATCH | 🔄 | 🔄 | Multiple optional patterns |

---

## 7. Subqueries and Composition

### 7.1 WITH Clause
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `WITH n.prop AS x RETURN x` | 🔄 | 🔄 | Simple projection |
| `WITH n, COUNT(*) AS cnt RETURN n, cnt` | 🔄 | 🔄 | WITH aggregation |
| `WITH n WHERE n.prop > 10 RETURN n` | 🔄 | 🔄 | WITH filtering |
| Multiple WITH clauses | 🔄 | 🔄 | Chained WITH |

### 7.2 UNION
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `MATCH (n:A) RETURN n UNION MATCH (n:B) RETURN n` | ✅ | 🔄 | UNION |
| `... UNION ALL ...` | ✅ | 🔄 | UNION ALL |

---

## 8. Functions

### 8.1 String Functions
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `toUpper(n.name)` | ✅ | 🔄 | Upper case |
| `toLower(n.name)` | ✅ | 🔄 | Lower case |
| `trim(n.name)` | ✅ | 🔄 | Trim whitespace |
| `substring(n.name, 0, 5)` | ✅ | 🔄 | Substring |
| `replace(n.text, 'old', 'new')` | ✅ | 🔄 | Replace |
| `concat(a.x, '-', b.y)` | ✅ | ✅ | Concatenation |

### 8.2 Numeric Functions
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `abs(n.value)` | 🔄 | 🔄 | Absolute value |
| `round(n.value)` | 🔄 | 🔄 | Round |
| `floor(n.value)` | 🔄 | 🔄 | Floor |
| `ceil(n.value)` | 🔄 | 🔄 | Ceiling |

### 8.3 Temporal Functions
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `date(n.timestamp)` | 🔄 | 🔄 | Date conversion |
| `datetime(n.iso_string)` | 🔄 | 🔄 | DateTime |
| Date arithmetic | 🔄 | 🔄 | Date + interval |

---

## 9. CASE Expressions

### 9.1 Simple CASE
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `CASE n.type WHEN 'A' THEN 1 ELSE 0 END` | ✅ | 🔄 | Simple case |
| Multiple WHEN branches | ✅ | 🔄 | Multi-branch |
| CASE with NULL | ✅ | 🔄 | NULL handling |

### 9.2 Searched CASE
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `CASE WHEN n.x > 10 THEN 'high' ELSE 'low' END` | ✅ | 🔄 | Searched case |
| Nested CASE | ✅ | 🔄 | CASE in CASE |
| CASE in WHERE | ✅ | 🔄 | Filter by CASE result |
| CASE in aggregation | ✅ | 🔄 | Conditional aggregation |

---

## 10. Graph Algorithms

### 10.1 Centrality
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `CALL pagerank(...)` | ✅ | 🔄 | PageRank |
| Degree centrality | 🔄 | 🔄 | Count relationships |
| Betweenness centrality | 🚫 | 🚫 | Not implemented |

### 10.2 Community Detection
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| Connected components | 🔄 | 🔄 | Need to implement |
| Label propagation | 🚫 | 🚫 | Not implemented |

---

## 11. Schema Features

### 11.1 Multi-Schema
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `USE schema_name` | ✅ | ✅ | Schema selection |
| `schema_name` parameter | ✅ | ✅ | API parameter |
| Cross-schema queries | 🚫 | 🚫 | Not supported |

### 11.2 Parameters
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `WHERE n.id = $userId` | ✅ | 🔄 | Parameter substitution |
| `view_parameters` | ✅ | 🔄 | Multi-tenancy |

---

## 12. Edge Cases and Error Handling

### 12.1 NULL Handling
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| `WHERE n.prop IS NULL` | 🔄 | 🔄 | NULL check |
| `WHERE n.prop IS NOT NULL` | 🔄 | 🔄 | NOT NULL |
| NULL in expressions | 🔄 | 🔄 | NULL propagation |

### 12.2 Empty Results
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| No nodes found | ✅ | ✅ | Returns empty array |
| No relationships found | ✅ | ✅ | Returns empty array |
| OPTIONAL MATCH with no match | ✅ | 🔄 | Returns NULL |

### 12.3 Performance Limits
| Pattern | Standard | Denormalized | Notes |
|---------|----------|--------------|-------|
| Large LIMIT values | 🔄 | 🔄 | Memory limits |
| Deep recursion (max depth) | ✅ | 🔄 | Configurable limit |
| Cartesian products | 🔄 | 🔄 | Large result sets |

---

## 13. Write Operations (🚫 NOT SUPPORTED)

ClickGraph is **read-only**. The following are out of scope:

- ❌ `CREATE` - Node/relationship creation
- ❌ `SET` - Property updates
- ❌ `DELETE` - Node/relationship deletion
- ❌ `MERGE` - Upsert operations
- ❌ `REMOVE` - Property removal
- ❌ Transactions

---

## Testing Strategy

### Phase 1: Core Patterns (In Progress)
1. ✅ Basic node matching with properties
2. ✅ Simple relationships (1-hop)
3. ✅ Mixed property expressions (WHERE, RETURN, ORDER BY)
4. 🔄 Multi-hop patterns (2-3 hops)
5. 🔄 Variable-length paths

### Phase 2: Advanced Features
1. 🔄 Aggregations with GROUP BY
2. 🔄 OPTIONAL MATCH edge cases
3. 🔄 Path functions and shortest path
4. 🔄 CASE expressions in complex contexts
5. 🔄 Function composition

### Phase 3: Denormalized Schema Coverage
1. ✅ Simple property access
2. ✅ WHERE filters with mixed expressions
3. 🔄 Multi-hop denormalized patterns
4. 🔄 Edge property access
5. 🔄 Aggregations on denormalized data

### Phase 4: Edge Cases
1. 🔄 NULL handling throughout
2. 🔄 Empty result handling
3. 🔄 Performance limits and error messages
4. 🔄 Schema validation errors

---

## Next Steps

### Immediate (This Week)
1. ✅ Document JOIN order fix for mixed expressions
2. 🔄 **Test multi-hop patterns with denormalized schema**
3. 🔄 **Validate aggregations work correctly**
4. 🔄 **Test edge property access in denormalized patterns**

### Short-Term (Next 2 Weeks)
1. Fill in 🔄 items for standard schema
2. Achieve 80%+ coverage for denormalized schema
3. Add automated regression tests
4. Document known limitations

### Long-Term
1. Add missing functions (date/time, advanced math)
2. Implement community detection algorithms
3. Optimize performance for large graphs
4. Add more comprehensive error messages

---

## Coverage Statistics

**Standard Schema**: ~40/100 patterns tested (40%)  
**Denormalized Schema**: ~10/100 patterns tested (10%)  
**Overall**: ~50/200 pattern combinations tested (25%)

**Target**: 80% coverage for production release
