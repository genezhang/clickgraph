# Schema Name vs Database Name - Quick Reference

## 🎯 Critical Distinction

**Problem**: Tests were failing because they used **database name** instead of **schema name** in USE clause.

**Solution**: Always use **schema name** (logical graph identifier), NOT database name (physical storage).

---

## 📋 Quick Reference

| Concept | What It Is | Where It's Defined | Usage |
|---------|-----------|-------------------|--------|
| **Schema Name** | Logical graph identifier | YAML `name:` field | USE clause, schema_name API parameter |
| **Database Name** | Physical ClickHouse database | YAML metadata, ClickHouse | Table references in SQL |
| **View Name** | ClickHouse view/table | YAML `view:` field | Internal SQL generation |
| **Label/Type** | Graph entity type | YAML `label:`/`type:` | Cypher MATCH patterns |

---

## ✅ Correct Usage

### Schema Definition (YAML)
```yaml
name: test_graph_schema        # ← Schema name (USE THIS in USE clause)
database: test_integration      # ← Database name (DON'T use in USE clause)
nodes:
  - label: User                 # ← Label (use in MATCH)
    view: users_bench          # ← View name (internal)
```

### Cypher Query
```cypher
USE test_graph_schema;         # ✅ CORRECT: Use schema name
MATCH (u:User) RETURN u;       # ✅ CORRECT: Use label
```

### HTTP API
```bash
curl -X POST http://localhost:8080/query \
  -d '{
    "query": "MATCH (u:User) RETURN u",
    "schema_name": "test_graph_schema"    # ✅ CORRECT: Schema name
  }'
```

---

## ❌ Common Mistakes

### WRONG: Using database name in USE clause
```cypher
USE test_integration;          # ❌ WRONG: This is database name
MATCH (u:User) RETURN u;
# Error: Schema 'test_integration' not found
```

### WRONG: Using view name in MATCH
```cypher
USE test_graph_schema;
MATCH (u:users_bench) RETURN u;  # ❌ WRONG: This is view name
# Use label instead: (u:User)
```

---

## 🔍 How to Identify Each

### Finding Schema Name
```python
# In test fixtures
simple_graph = {
    "schema_name": "test_graph_schema",  # ← THIS is schema name
    "database": "test_integration"        # ← NOT this
}

# In YAML
name: social_graph              # ← Schema name (first line)
```

### Finding Database Name
```yaml
name: social_graph              # Schema name
database: brahmand              # ← Database name (for internal SQL)
```

### Finding Label/Type
```yaml
nodes:
  - label: User                 # ← Label for MATCH (u:User)
    view: users_bench
relationships:
  - type: FOLLOWS               # ← Type for MATCH -[:FOLLOWS]->
    view: user_follows_bench
```

---

## 🧪 Test Pattern (Correct)

### Test Setup
```python
@pytest.fixture
def simple_graph():
    return {
        "schema_name": "test_graph_schema",   # Schema name
        "database": "test_integration",       # Database name (don't use in USE)
        "yaml_path": "path/to/schema.yaml"
    }
```

### Test Usage
```python
def test_use_clause(simple_graph):
    # ✅ CORRECT: Use schema_name
    query = f"USE {simple_graph['schema_name']}; MATCH (n) RETURN count(n)"
    
    # ❌ WRONG: Don't use database
    # query = f"USE {simple_graph['database']}; ..."  # This fails!
```

---

## 📊 Mapping Flow

```
YAML Config
├─ name: social_graph                    → Schema Name (USE social_graph)
├─ database: social_db                   → Database Name (internal SQL)
└─ nodes:
   └─ label: User                        → Label (MATCH (u:User))
      └─ view: users_table               → View Name (internal SQL)
         ├─ properties:
         │  └─ user_id → column: id      → Property Mapping
         └─ database_table: social_db.users_table  → Full table reference
```

**Query Path**:
1. `USE social_graph` → Selects schema by name
2. `MATCH (u:User)` → Finds node with label "User"
3. Schema maps `User` → `users_table` view → `social_db.users_table`
4. Generated SQL: `SELECT ... FROM social_db.users_table`

---

## 🎯 Test Fix Examples

### Before (Wrong)
```python
# ❌ Using database name
query = f"USE {simple_graph['database']};"  # test_integration
# Error: Schema 'test_integration' not found
```

### After (Correct)
```python
# ✅ Using schema name
query = f"USE {simple_graph['schema_name']};"  # test_graph_schema
# Success: Schema loaded
```

### Fix Pattern
```bash
# Find all incorrect usages
grep -r "simple_graph\[\"database\"\]" tests/

# Replace with correct key
# Change: simple_graph["database"]
# To:     simple_graph["schema_name"]
```

---

## 💡 Remember

1. **Schema name** = Logical graph identifier (what users see)
2. **Database name** = Physical storage (internal implementation)
3. **USE clause** = Always use schema name
4. **Test fixtures** = Return both, use `schema_name` for USE clause

**When in doubt**: Check the YAML `name:` field - that's your schema name!
