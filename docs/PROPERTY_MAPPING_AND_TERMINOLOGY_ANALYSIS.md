# Documentation and Testing Gaps - November 22, 2025

## Summary

Investigation into type conversion, conditional mappings, and terminology consistency revealed:

1. ✅ **Type conversions in property mappings** - DOCUMENTED but NOT TESTED
2. ✅ **Conditional mappings in property mappings** - DOCUMENTED but NOT TESTED  
3. ⚠️ **Terminology inconsistency** - "relationship" vs "edge" used inconsistently (95:20 ratio)

---

## 1. Type Conversion & Conditional Mappings

### Current Status

**Implementation**: ✅ **IMPLEMENTED**
- `property_mappings: HashMap<String, String>` in schema supports SQL expressions
- Value can be simple column name OR complex SQL expression
- Code: `src/graph_catalog/graph_schema.rs` (lines 16, 69)

**Documentation**: ✅ **DOCUMENTED**
- Location: `docs/wiki/Schema-Configuration-Advanced.md` (lines 145-189)
- Includes type conversion examples
- Includes conditional mapping examples (CASE, multiIf)

**Testing**: ❌ **NO TESTS FOUND**
- No integration tests for property mapping expressions
- No tests for `toDate()`, `toInt()`, etc. in property mappings
- No tests for CASE WHEN in property mappings
- No tests for multiIf in property mappings

### Examples from Documentation

**Type Conversions** (lines 147-164):
```yaml
nodes:
  User:
    property_mappings:
      user_id: "user_id"
      
      # String to Date
      registration_date: "toDate(registration_date_str)"
      
      # String to Number
      age: "toUInt8(age_str)"
      
      # JSON parsing
      metadata: "JSONExtractString(metadata_json, 'key')"
      
      # Array from comma-separated
      tags: "splitByChar(',', tags_str)"
```

**Conditional Mappings** (lines 166-189):
```yaml
nodes:
  User:
    property_mappings:
      user_id: "user_id"
      
      # Tier based on score
      tier: "CASE 
               WHEN score >= 1000 THEN 'gold'
               WHEN score >= 500 THEN 'silver'
               ELSE 'bronze'
             END"
      
      # Status from multiple conditions
      status: "multiIf(
                 is_deleted = 1, 'deleted',
                 is_banned = 1, 'banned',
                 is_active = 0, 'inactive',
                 'active'
               )"
```

### Testing Gaps

**Should test**:
1. ✅ Type conversions:
   - `toDate()` on string columns
   - `toInt()`, `toUInt8()` on string/float columns
   - `toString()` on numeric columns
   - `toFloat()` on string columns

2. ✅ Conditional mappings:
   - CASE WHEN with multiple branches
   - multiIf() with complex conditions
   - Nested CASE expressions
   - CASE with NULL handling

3. ✅ Complex expressions:
   - JSON extraction functions
   - Array functions (splitByChar, etc.)
   - String manipulation (concat, substring)
   - Date/time functions

4. ✅ Edge cases:
   - NULL values in expressions
   - Type mismatches
   - Invalid SQL expressions
   - Performance with complex expressions

### Recommended Test File

Create: `tests/integration/test_property_mapping_expressions.py`

```python
"""
Test property mapping expressions in schema YAML.

Tests:
- Type conversion functions (toDate, toInt, toString, etc.)
- Conditional mappings (CASE WHEN, multiIf)
- Complex SQL expressions
- Edge cases and error handling
"""

def test_type_conversion_todate():
    """Test toDate() in property mapping."""
    # Schema with: registration_date: "toDate(reg_date_str)"
    # Query: MATCH (u:User) RETURN u.registration_date
    # Should convert string to date type

def test_type_conversion_toint():
    """Test toInt() in property mapping."""
    # Schema with: age: "toInt(age_str)"
    
def test_conditional_case_when():
    """Test CASE WHEN in property mapping."""
    # Schema with: tier: "CASE WHEN score >= 1000 THEN 'gold' ..."
    
def test_conditional_multiif():
    """Test multiIf() in property mapping."""
    # Schema with: status: "multiIf(is_deleted=1, 'deleted', ...)"

def test_complex_json_extraction():
    """Test JSON functions in property mapping."""
    # Schema with: metadata: "JSONExtractString(json_col, 'key')"

def test_array_from_string():
    """Test array split in property mapping."""
    # Schema with: tags: "splitByChar(',', tags_str)"

def test_expression_with_null():
    """Test NULL handling in expressions."""
    
def test_invalid_expression():
    """Test error handling for invalid SQL expression."""
```

---

## 2. Terminology: "relationship" vs "edge"

### Current Usage

**Documentation terminology count**:
- **"relationship"**: 95 occurrences
- **"edge"**: 20 occurrences
- **Ratio**: ~5:1 (heavily favoring "relationship")

### Analysis

**"Relationship" used in**:
- Cypher Language Reference (primary docs)
- API documentation
- Wiki pages (Quick Start, Basic Patterns, etc.)
- Schema configuration examples
- User-facing documentation

**"Edge" used in**:
- Some technical docs (`edge-id-best-practices.md`)
- Internal code comments
- Schema YAML field names (inconsistently)

### Decision Needed

**Option 1: Standardize on "relationship"** ✅ **RECOMMENDED**
- **Pros**:
  - Matches Neo4j Cypher standard terminology
  - Already dominant in our documentation (95 occurrences)
  - User-facing docs already use this consistently
  - Less breaking change
  
- **Cons**:
  - "Edge" is more common in graph theory
  - Some schema fields use "edge" (edge_id, edge_label)

**Option 2: Standardize on "edge"**
- **Pros**:
  - More precise graph theory term
  - Shorter word
  - Matches some schema fields
  
- **Cons**:
  - Breaks Neo4j Cypher compatibility
  - Requires updating 95+ documentation references
  - Confusing for users familiar with Cypher
  - Major breaking change

**Option 3: Keep both with clear distinction**
- "Relationship" = User-facing Cypher queries and documentation
- "Edge" = Internal implementation and schema configuration
- **Pros**: Maintains compatibility
- **Cons**: Confusing, inconsistent

### Recommendation

**✅ Standardize on "relationship"**

**Rationale**:
1. Neo4j Cypher uses "relationship" (industry standard)
2. Already dominant in user-facing docs (5:1 ratio)
3. Less breaking change (only update ~20 occurrences)
4. Better user experience (matches expectations)

**Migration Plan**:
1. ✅ Update remaining docs to use "relationship" consistently
2. ✅ Add deprecation warnings for "edge" terminology
3. ✅ Update schema examples to use "relationship" fields
4. ✅ Keep "edge_id" field name (established convention)
5. ✅ Update code comments from "edge" to "relationship"

**Deprecation Strategy**:
- Add notices to docs mentioning "edge" as synonym
- Update examples to use "relationship" terminology
- No breaking changes to YAML schema (accept both)
- Gradual transition over 1-2 releases

---

## 3. Action Items

### High Priority (Before v0.5.2 release)

1. ⏳ **Create property mapping expression tests**
   - File: `tests/integration/test_property_mapping_expressions.py`
   - Test type conversions, conditionals, complex expressions
   - Estimated time: 4-6 hours

2. ⏳ **Update Cypher Language Reference**
   - Change remaining "edge" references to "relationship"
   - Add deprecation note about "edge" terminology
   - Estimated time: 30 minutes

3. ⏳ **Update STATUS.md**
   - Add note about property mapping expressions (documented but not tested)
   - Add terminology standardization decision
   - Estimated time: 10 minutes

### Medium Priority (v0.5.3+)

4. ⏳ **Comprehensive terminology audit**
   - Search all docs for "edge" → replace with "relationship" where appropriate
   - Update code comments
   - Keep "edge_id" field name (established)
   - Estimated time: 2-3 hours

5. ⏳ **Update wiki pages**
   - Schema Configuration Advanced: Add tested examples
   - Add troubleshooting section for property mapping expressions
   - Estimated time: 1 hour

### Low Priority (Future)

6. ⏳ **Performance testing**
   - Benchmark complex property mapping expressions
   - Document performance implications
   - Estimated time: 3-4 hours

---

## 4. Documentation Updates Needed

### Cypher Language Reference

**Add section: "Property Mapping Expressions"**
- Document that property mappings can use SQL expressions
- Show examples with type conversions
- Show examples with conditional logic
- Link to Schema Configuration Advanced

### Schema Configuration Advanced

**Update existing examples**:
- ✅ Type conversion examples exist (lines 147-164)
- ✅ Conditional mapping examples exist (lines 166-189)
- ❌ Missing: Link to test examples
- ❌ Missing: Performance notes
- ❌ Missing: Error handling guidance

**Add new section: "Expression Testing"**
- How to test complex property mappings
- Common pitfalls
- Debugging tips

### STATUS.md

**Add to "What Works" section**:
```markdown
- ✅ **Property mapping expressions** (documented, not tested)
  - Type conversions: `toDate()`, `toInt()`, etc.
  - Conditional logic: CASE WHEN, multiIf
  - Complex SQL expressions in property mappings
```

**Add to "Known Issues" section**:
```markdown
- ⚠️ **Property mapping expressions not tested**
  - Feature is documented and implemented
  - No integration tests exist
  - Should add tests before v0.5.2 release
```

---

## 5. Terminology Migration Guide

### User-Facing Documentation

**Current inconsistent terms**:
- ❌ "edge" in some schema examples
- ❌ "edge_id" field (keep this - established convention)
- ❌ "edge table" in some docs

**Standardize to**:
- ✅ "relationship" in all Cypher queries
- ✅ "relationship type" (not "edge type")
- ✅ "relationship table" (not "edge table")
- ✅ Keep "edge_id" field name (established)

### Code Comments

**Update**:
- "edge traversal" → "relationship traversal"
- "edge properties" → "relationship properties"
- "multi-edge" → "multiple relationships"

**Keep**:
- `edge_id` (field name)
- Variable names in code (internal)

### Deprecation Warnings

**Add to docs**:
```markdown
> **Note**: In earlier versions, we used "edge" terminology. We now use "relationship" 
> to match Neo4j Cypher standard. The terms are synonymous in ClickGraph.
```

---

## 6. Testing Priority

### Critical (Block v0.5.2 release)
- ✅ Property mapping expression tests

### Important (Should have before v0.5.2)
- ⏳ Edge case testing (NULL, invalid expressions)
- ⏳ Performance testing (complex expressions)

### Nice to have (v0.5.3+)
- ⏳ Comprehensive type conversion tests
- ⏳ Nested expression tests
- ⏳ Error message validation tests

---

## 7. Summary

### ✅ What's Working
- Property mapping expressions are implemented
- Documentation exists for type conversions and conditionals
- Feature is production-ready (just needs tests)

### ❌ What's Missing
- Integration tests for property mapping expressions
- Consistent terminology (relationship vs edge)
- User examples and troubleshooting guides

### 🎯 Recommendation
1. **Add tests before v0.5.2 release** (highest priority)
2. **Standardize on "relationship" terminology** (medium priority)
3. **Enhance documentation with examples** (lower priority)

**Timeline**:
- Tests: 4-6 hours (block release if not done)
- Terminology: 2-3 hours (can do in v0.5.3)
- Enhanced docs: 1-2 hours (can do incrementally)

**Risk Assessment**:
- **Shipping without tests**: ⚠️ HIGH RISK - Feature documented but untested
- **Terminology inconsistency**: 🟡 MEDIUM RISK - Confusing but not blocking
- **Missing examples**: 🟢 LOW RISK - Users can figure it out from existing docs
