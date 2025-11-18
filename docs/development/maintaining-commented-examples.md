# Managing Commented-Out Documentation Examples

This guide explains how to handle examples that are commented out in the wiki documentation.

## Why Examples Are Commented Out

Examples are hidden using HTML comments (`<!-- -->`) for these reasons:

1. **Feature Not Yet Implemented** - Functionality planned but not yet coded
2. **Schema Mismatch** - Example uses properties not in benchmark schema
3. **Known Bugs** - Pattern has reproducible failures
4. **Architectural Limitations** - Would require significant refactoring

## Comment Format

All commented-out examples follow this structure:

```markdown
<!-- 
⚠️ CATEGORY - Short description

Detailed explanation of what needs to happen before this can be uncommented.
Implementation notes, file references, or workarounds.

```cypher
-- Example query that doesn't work yet
MATCH (n)
RETURN n
```
-->
```

## Categories

### 🔮 FUTURE FEATURE
**Meaning**: Planned functionality not yet implemented

**Example**:
```markdown
<!-- 
⚠️ FUTURE FEATURE - Commented out until labelless node support is implemented

Labelless node matching is not yet supported due to architectural limitations.
To implement: Need to add UNION ALL across all node types or implement type inference.

```cypher
MATCH (n) RETURN n
```
-->
```

**What to do**: 
- Don't uncomment until feature is implemented
- Add tests in `tests/integration/future_features/` when ready
- Update STATUS.md when completed

### 📦 PROPERTY DOESN'T EXIST
**Meaning**: Example uses properties not in benchmark schema

**Example**:
```markdown
<!-- 
⚠️ EXAMPLE USES NON-EXISTENT PROPERTY - Commented out

The 'age' property doesn't exist in benchmark schema.
Use registration_date or other actual properties instead.

```cypher
MATCH (u:User) WHERE u.age > 30 RETURN u
```
-->
```

**What to do**:
- Replace with working alternative using actual schema properties
- Only uncomment if you add `age` to benchmark schema
- Keep as reference for users with different schemas

### 🐛 NOT YET SUPPORTED
**Meaning**: Syntax parses but generates incorrect SQL or errors

**Example**:
```markdown
<!-- 
⚠️ NOT YET SUPPORTED - Inline property filters

To implement: Update match_clause.rs to expand inline properties to WHERE conditions.

```cypher
MATCH (u:User {name: 'Alice'}) RETURN u
```
-->
```

**What to do**:
- Create GitHub issue with query that fails
- Add to KNOWN_ISSUES.md
- Uncomment only after fix is merged and tested

## How to Uncomment Examples

### Step 1: Verify Feature Works

**For code features**:
```bash
# Test the specific query pattern
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN n LIMIT 1"}'
```

**For schema features**:
```bash
# Verify property exists
python scripts/validate_schema_properties.py
```

### Step 2: Add Integration Test

Create test in `tests/integration/wiki/test_cypher_basic_patterns.py`:

```python
def test_previously_unsupported_feature(self):
    """Test for feature XYZ that was previously commented out."""
    query = "MATCH (n) RETURN n LIMIT 10"
    result = execute_query(query)
    assert result["success"]
```

### Step 3: Run Validation

```powershell
# Run full validation suite
python scripts\validate_wiki_docs.py --docs-dir docs\wiki

# Or run specific test
pytest tests\integration\wiki\test_cypher_basic_patterns.py::TestClass::test_method -v
```

### Step 4: Uncomment and Document

1. Remove HTML comment wrapper
2. Update STATUS.md to mark feature as "✅ Supported"
3. Add example to CHANGELOG.md under "Added"
4. Remove from KNOWN_ISSUES.md if listed

## Maintaining Commented Examples

### Monthly Review

Check commented examples monthly:

```bash
# Find all commented examples
grep -r "⚠️ FUTURE FEATURE" docs/wiki/
grep -r "⚠️ NOT YET SUPPORTED" docs/wiki/
grep -r "⚠️ EXAMPLE USES NON-EXISTENT" docs/wiki/
```

### When Feature Gets Implemented

1. Search for related comments: `grep -r "labelless node" docs/wiki/`
2. Follow "How to Uncomment" steps above
3. Update this maintenance guide if needed

### When Schema Changes

If benchmark schema adds new properties:

1. Search for property-related comments: `grep -r "age.*PROPERTY DOESN'T EXIST" docs/wiki/`
2. Update examples to use new properties
3. Uncomment if appropriate
4. Add tests for new property patterns

## Examples by File

### Cypher-Basic-Patterns.md

**Commented out**:
- Labelless node matching `MATCH (n)`
- Inline property filters `{name: 'Alice'}`
- Inline relationship properties `[:REL {prop: val}]`
- Age-based CASE expressions (schema issue)

**To uncomment when**:
- Labelless: After implementing UNION-based node scanning
- Inline properties: After parser update in `match_clause.rs`
- Age examples: After adding `age` to benchmark schema or replacing with date logic

### Other Wiki Files

(Add as you comment out examples in other files)

## Best Practices

### DO ✅
- Include implementation hints in comments
- Reference specific files that need changes
- Provide working alternatives in visible text
- Keep comments up-to-date with codebase

### DON'T ❌
- Comment out working examples just because they're complex
- Remove commented examples entirely (they're useful references)
- Forget to test before uncommenting
- Uncomment without adding integration tests

## Tracking Commented Examples

Maintain list in KNOWN_ISSUES.md:

```markdown
### Commented Out Documentation Examples

- [ ] Labelless node matching - `docs/wiki/Cypher-Basic-Patterns.md:23`
- [ ] Inline property filters - `docs/wiki/Cypher-Basic-Patterns.md:45`
- [ ] Age-based examples - Multiple files (schema issue)
```

## Tools

### Find All Commented Examples

```powershell
# PowerShell
Select-String -Path "docs\wiki\*.md" -Pattern "⚠️" | 
  Where-Object { $_.Line -match "<!--" }
```

```bash
# Bash
grep -r "⚠️.*<!--" docs/wiki/
```

### Count by Category

```powershell
(Select-String -Path "docs\wiki\*.md" -Pattern "FUTURE FEATURE").Count
(Select-String -Path "docs\wiki\*.md" -Pattern "NOT YET SUPPORTED").Count
(Select-String -Path "docs\wiki\*.md" -Pattern "PROPERTY DOESN'T EXIST").Count
```

## Example Workflow: Implementing Inline Property Filters

1. **Before**: Example is commented out
2. **Development**: 
   - Create branch: `feature/inline-property-filters`
   - Update `open_cypher_parser/match_clause.rs`
   - Add test: `test_inline_property_filter()`
3. **Testing**:
   - Manual test with query
   - Run integration tests
   - Run validation suite
4. **Documentation**:
   - Uncomment example in Cypher-Basic-Patterns.md
   - Add to CHANGELOG.md
   - Update STATUS.md
5. **Merge**: PR with tests + documentation

---

**Remember**: Commented examples are promises to users. Keep them accurate and work towards uncommenting them!
