# Array Handling in ClickGraph - Design Considerations

**Date**: November 23, 2025  
**Context**: Property expression design - array transformations and ClickHouse-specific syntax

---

## The Problem

ClickHouse has rich array functionality that's common in real schemas:

```yaml
# Real-world examples
property_mappings:
  tag_names: "arrayMap(x -> upper(x), tags)"           # Lambda!
  first_tag: "tags[1]"                                  # Array indexing
  tag_count: "length(tags)"                             # Simple function
  top_tags: "arraySlice(tags, 1, 5)"                    # Array functions
  has_premium: "has(features, 'premium')"               # Array search
  joined_tags: "arrayStringConcat(tags, ', ')"          # Array string ops
  filtered: "arrayFilter(x -> x > 10, scores)"          # Lambda filtering
```

**Challenge**: ClickHouse array syntax includes:
1. **Lambda expressions**: `x -> expression` 
2. **Array indexing**: `array[index]` (1-based!)
3. **Array functions**: 100+ array-specific functions
4. **Higher-order functions**: `arrayMap`, `arrayFilter`, `arrayReduce`

---

## The Dilemma

### Option 1: Parse ClickHouse Lambda Syntax ❌ HIGH COMPLEXITY

**Would require**:
```rust
// Need to parse ClickHouse lambda syntax
fn parse_lambda_expr(input: &str) -> IResult<&str, LambdaExpr> {
    // x -> upper(x)
    // (x, y) -> x + y
    // x -> arrayMap(y -> y * 2, x)  // NESTED!
}
```

**Problems**:
- ClickHouse lambdas ≠ Cypher syntax
- Nested lambdas are complex
- Would need full ClickHouse expression parser
- Parser becomes CH-specific, not Cypher

**Estimated complexity**: +40 hours (2 weeks)

### Option 2: Treat as Opaque Strings ⚠️ LEAKY ABSTRACTION

```rust
// Just pass through to ClickHouse
PropertyValue::RawSQL(String)
```

**Problems**:
- Can't apply table alias prefixing
- Column references not detected
- No validation until runtime
- Security risk (SQL injection potential)

**Example failure**:
```yaml
# This won't work!
tag_names: "arrayMap(x -> upper(name), tags)"
#                              ^^^^ needs to be u.name
```

### Option 3: Defer Array Transformations ✅ **RECOMMENDED**

**Simple arrays work today**:
```yaml
# ✅ These work without special handling
property_mappings:
  tag_count: "length(tags)"              # Simple function
  first_tag: "tags[1]"                   # Array indexing (passthrough)
  has_feature: "has(features, 'premium')" # Array search
  joined: "arrayStringConcat(tags, ',')" # No lambdas
```

**Complex arrays deferred**:
```yaml
# ❌ Not supported yet - use query time
# tag_names: "arrayMap(x -> upper(x), tags)"
# filtered: "arrayFilter(x -> x > 10, scores)"
```

**Query-time alternative**:
```cypher
// User can do this in RETURN clause
MATCH (u:User)
RETURN u.tags AS raw_tags,
       [x IN u.tags | upper(x)] AS tag_names  // Cypher list comprehension
```

---

## Recommendation: Phased Approach

### Phase 1 (Current): Simple Arrays ✅

**Support now** (no lambda parsing needed):
```yaml
property_mappings:
  # Array access (passthrough to CH)
  first_tag: "tags[1]"
  last_tag: "tags[-1]"
  
  # Simple array functions
  tag_count: "length(tags)"
  empty_check: "empty(tags)"
  
  # Array search (no lambdas)
  has_premium: "has(features, 'premium')"
  index_of: "indexOf(tags, 'rust')"
  
  # Array to string
  tag_string: "arrayStringConcat(tags, ', ')"
  
  # Array slicing
  top_five: "arraySlice(tags, 1, 5)"
```

**Implementation**: Already handled by current design
- Parser treats `[]` as operator
- Function calls work normally
- No lambda syntax needed

### Phase 2 (Future): Lambda Expressions ⏳

**Defer until v0.6.x or later**:
```yaml
property_mappings:
  # Higher-order functions with lambdas
  tag_names: "arrayMap(x -> upper(x), tags)"
  filtered: "arrayFilter(x -> x > 10, scores)"
  reduced: "arrayReduce('sum', scores)"
```

**Prerequisites for Phase 2**:
1. Decide: Parse lambdas or use Cypher list comprehensions?
2. If parsing: Implement ClickHouse lambda parser
3. If Cypher: Convert list comprehensions to CH SQL
4. Handle nested lambdas
5. Extensive testing

**Estimated effort**: 2-3 weeks

### Phase 3 (Maybe): Full ClickHouse Passthrough 🤔

**Alternative approach**: Let advanced users bypass parser

```yaml
property_mappings:
  # Explicit SQL mode
  complex_calc:
    type: raw_sql
    expression: "arrayMap(x -> upper(x), tags)"
    columns: [tags]  # Manual column list for aliasing
```

**Trade-offs**:
- ✅ Supports any ClickHouse syntax
- ❌ User responsible for correctness
- ❌ No validation
- ⚠️ Security implications

---

## Comparison with Neo4j/Cypher

### Neo4j Arrays (Lists)

```cypher
// Cypher list comprehensions
MATCH (u:User)
RETURN [x IN u.tags | upper(x)] AS tag_names,
       [x IN u.scores WHERE x > 10] AS filtered,
       reduce(sum = 0, x IN u.scores | sum + x) AS total
```

**Cypher syntax**:
- `[x IN list | expression]` - map
- `[x IN list WHERE condition]` - filter
- `reduce(init, x IN list | expression)` - reduce

### ClickHouse Arrays

```sql
-- ClickHouse lambda syntax
SELECT 
    arrayMap(x -> upper(x), tags) AS tag_names,
    arrayFilter(x -> x > 10, scores) AS filtered,
    arrayReduce('sum', scores) AS total
```

**Different syntax**:
- `arrayMap(x -> expr, arr)` vs `[x IN arr | expr]`
- `arrayFilter(x -> cond, arr)` vs `[x IN arr WHERE cond]`
- `arrayReduce('func', arr)` vs `reduce(init, x IN arr | expr)`

---

## Decision Framework

### Ask: Where does the transformation happen?

**Schema-time transformations** (property mappings):
- **Purpose**: Convenience, denormalization
- **When**: Simple, reusable across queries
- **Example**: `full_name: "concat(first, last)"` ✅

**Query-time transformations** (Cypher expressions):
- **Purpose**: Business logic, conditionals, complex operations
- **When**: Query-specific, conditional, complex
- **Example**: `[x IN tags | upper(x)]` in RETURN clause ✅

### Decision Criteria

**Use property mappings when**:
- ✅ Transformation is simple (single function or operator)
- ✅ Used frequently across multiple queries
- ✅ No conditionals or lambdas
- ✅ Represents denormalized data model

**Use query-time expressions when**:
- ✅ Transformation is complex (lambdas, nested logic)
- ✅ Query-specific or rarely reused
- ✅ Requires conditionals or filtering
- ✅ Dynamic or context-dependent

---

## Recommended Scope for v0.5.2

### ✅ Support (Simple Arrays)

```yaml
property_mappings:
  # Array indexing (passthrough)
  first_item: "items[1]"
  
  # Simple array functions (no lambdas)
  count: "length(items)"
  is_empty: "empty(items)"
  contains_x: "has(items, 'value')"
  to_string: "arrayStringConcat(items, ',')"
  
  # Array slicing
  top_five: "arraySlice(items, 1, 5)"
```

**Rationale**: These work with current parser design (no lambda syntax)

### ❌ Defer (Complex Arrays)

```yaml
# Use in query RETURN clause instead
property_mappings:
  # mapped: "arrayMap(x -> upper(x), items)"
  # filtered: "arrayFilter(x -> x > 10, scores)"
  # nested: "arrayMap(x -> arrayMap(y -> y * 2, x), matrix)"
```

**Rationale**: 
- Lambda parsing adds 2-3 weeks complexity
- Can be done at query time with Cypher list comprehensions
- Not essential for convenience mappings

### 📝 Document Clearly

**In schema docs**:
```markdown
## Array Handling

### Supported (v0.5.2)
Simple array operations without lambda expressions:
- Array indexing: `items[1]`, `items[-1]`
- Array functions: `length()`, `empty()`, `has()`, `indexOf()`
- Array to string: `arrayStringConcat(items, delimiter)`
- Array slicing: `arraySlice(items, start, length)`

### Not Yet Supported
Complex array transformations (use query-time expressions):
- Lambda expressions: `arrayMap(x -> expr, arr)` ❌
- Array filtering: `arrayFilter(x -> condition, arr)` ❌
- Array reduction: `arrayReduce('func', arr)` ❌

**Alternative**: Use Cypher list comprehensions in query:
```cypher
MATCH (u:User)
RETURN [x IN u.tags | upper(x)] AS uppercase_tags,
       [x IN u.scores WHERE x > 10] AS high_scores
```
```

---

## Implementation Impact

### Current Parser (Simplified)

```rust
// ✅ Already handles simple array operations
fn parse_term(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        parse_function_call_expr,      // length(arr), has(arr, val)
        parse_array_index_expr,         // arr[1] - NEW
        parse_quoted_identifier,
        parse_identifier_expr,
        parse_literal_expr,
        delimited(char('('), parse_clickhouse_scalar_expr, char(')')),
    ))(input)
}

// Simple array indexing (passthrough)
fn parse_array_index_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, arr) = parse_identifier_expr(input)?;
    let (input, _) = char('[')(input)?;
    let (input, idx) = parse_clickhouse_scalar_expr(input)?;
    let (input, _) = char(']')(input)?;
    
    Ok((input, ClickHouseExpr::ArrayIndex {
        array: Box::new(arr),
        index: Box::new(idx),
    }))
}
```

### Lambda Parser (Deferred)

```rust
// ❌ NOT implementing for v0.5.2
fn parse_lambda_expr(input: &str) -> IResult<&str, LambdaExpr> {
    // x -> expression
    // (x, y) -> expression
    // Recursive descent for nested lambdas
    // ... 200+ lines of complex parsing
}
```

---

## User Communication Strategy

### Documentation
```markdown
## Schema Property Mappings: What Can I Transform?

### ✅ Supported Transformations
- **String operations**: concat, upper, substring
- **Date calculations**: dateDiff, toDate, today()
- **Math operations**: +, -, *, /, %
- **Type conversions**: toUInt8, toString
- **Simple arrays**: length, has, arrayStringConcat, indexing

### ⏳ Coming Soon (v0.6.x)
- **Array transformations**: arrayMap, arrayFilter
- **Lambda expressions**: x -> expression syntax

### ❌ Not Planned for Schema Mappings
- **Conditionals**: CASE WHEN, multiIf (use WHERE/RETURN)
- **Aggregations**: sum, count (use aggregation queries)
- **Subqueries**: Complex nested queries (use JOINs)

### 💡 When to Use Query-Time Expressions

For complex logic, use Cypher expressions in your queries:

```cypher
// Schema: Keep it simple
property_mappings:
  tag_count: "length(tags)"  ✅

// Query: Do complex transformations here
MATCH (u:User)
WHERE u.tag_count > 5
RETURN u.name,
       [x IN u.tags | upper(x)] AS uppercase_tags,  // ✅ List comprehension
       [x IN u.tags WHERE x STARTS WITH 'rust'] AS rust_tags
```
```

---

## Summary & Recommendation

### For v0.5.2: **Defer Lambda Expressions** ✅

**What we support**:
- ✅ Simple array functions (length, has, indexOf, arrayStringConcat)
- ✅ Array indexing (`arr[1]`)
- ✅ Array slicing (arraySlice)

**What we defer**:
- ⏳ Lambda expressions (arrayMap, arrayFilter, arrayReduce)
- ⏳ Higher-order array functions

**Why**:
1. **Complexity**: Lambda parsing adds 2-3 weeks
2. **Scope creep**: Would need full CH expression parser
3. **Alternative exists**: Cypher list comprehensions work at query time
4. **Diminishing returns**: Schema mappings are for simple convenience, not complex logic

**User guidance**:
```
Simple arrays in schema ✅ → Complex arrays in query ✅
```

### Future (v0.6.x): Evaluate Need

**Before implementing**:
1. Collect user feedback: Do people need lambda mappings?
2. Count requests: How often is this a blocker?
3. Consider alternatives: Raw SQL mode? Cypher comprehensions?

**Implementation options** (if needed):
- **Option A**: Parse CH lambdas (2-3 weeks, CH-specific)
- **Option B**: Convert Cypher comprehensions to CH SQL (cleaner)
- **Option C**: Raw SQL escape hatch for advanced users

---

## Action Items

**For current PR**:
1. ✅ Document simple array support
2. ✅ Add array indexing to parser (20 lines)
3. ✅ Test with simple array functions
4. ✅ Document lambda deferral clearly

**For future**:
- [ ] Track user requests for lambda support
- [ ] Design lambda parsing if demand is high
- [ ] Consider Cypher list comprehension → CH SQL translation

**Timeline**: Current simplified parser → 1.5 days (no change)

---

**Decision**: Defer lambda expressions to keep v0.5.2 focused and achievable. Simple arrays work fine, complex transformations belong in queries.
