# LDBC Query Audit Workflow

## Current Status (Dec 13, 2025)

**Problem**: Created benchmarking infrastructure before validating queries actually work.

**Reality Check**:
- ✅ All queries generate SQL successfully
- ❌ Database is empty (can't validate query correctness)
- ❓ Unknown if generated SQL produces correct results

## Focused Audit Process

### Step 1: Load Small Dataset (sf0.003)

```bash
cd /home/gz/clickgraph/benchmarks/ldbc_snb

# Check what data we have
ls -lh data/sf0.003/graphs/csv/interactive/composite-projected-fk/

# Load using existing script
./scripts/load_data_docker_v2.sh sf0.003
```

**Expected**: ~3K persons, ~6K comments/posts, ~50K relationships loaded in ~30 seconds

### Step 2: Verify Data Loaded

```bash
# Check row counts
curl -s 'http://localhost:8123/' --data 'SELECT count(*) FROM ldbc.Person' --user default:default
curl -s 'http://localhost:8123/' --data 'SELECT count(*) FROM ldbc.Post' --user default:default
curl -s 'http://localhost:8123/' --data 'SELECT count(*) FROM ldbc.Person_knows_Person' --user default:default
```

**Expected**: Non-zero counts for all tables

### Step 3: Run Query Audit

```bash
cd scripts

# Test all queries with execution
python3 query_audit.py --all

# Or test category by category
python3 query_audit.py --is   # Interactive Short
python3 query_audit.py --ic   # Interactive Complex  
python3 query_audit.py --bi   # Business Intelligence
```

**Expected**: Queries return actual results, not 0 rows

### Step 4: Investigate Failures

For any failed queries:

```bash
# Show generated SQL
python3 query_audit.py --sql --ic  # Shows SQL only, no execution

# Test specific query manually
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"graph_name": "ldbc_snb", "query": "MATCH (p:Person {id: 933}) RETURN p.firstName, p.lastName"}'
```

**Debug checklist**:
- [ ] Does generated SQL look correct?
- [ ] Run SQL directly in ClickHouse - does it work?
- [ ] Is it a schema mapping issue?
- [ ] Is it a parser bug?
- [ ] Is it a query planner bug?
- [ ] Is it a SQL generator bug?

### Step 5: Fix and Re-audit

When fixing bugs:

1. **Identify root cause** (parser/planner/generator)
2. **Create minimal reproduction** in unit tests
3. **Fix the bug**
4. **Re-run audit** to verify fix
5. **Check for regressions** (other queries still work)

**DO NOT** move to next bug until current one is fixed and verified.

## Audit Results Template

```
=== Audit Results - sf0.003 - 2025-12-13 ===

Interactive Short (IS): 7/7 ✅
- IS1-IS7: All pass, return expected results

Interactive Complex (IC): X/11 ⚠️
- IC1, IC2, IC3: Pass ✅
- IC4: FAIL - Missing HAVING clause ❌
- IC5: FAIL - Wrong date comparison ❌
- ...

Business Intelligence (BI): X/6 ⚠️
- BI1, BI2: Pass ✅
- BI3: FAIL - Incorrect JOIN order ❌
- ...

=== Next Actions ===
1. Fix IC4 HAVING clause generation
2. Fix IC5 date comparison
3. Re-audit IC queries
```

## What NOT to Do ❌

- ❌ Create more benchmark scripts without testing
- ❌ Write extensive documentation before validation
- ❌ Assume queries work because SQL generates
- ❌ Test multiple scale factors before fixing bugs
- ❌ Skip data loading step
- ❌ Test on empty database

## What TO Do ✅

- ✅ Load data first (sf0.003 sufficient for validation)
- ✅ Run audit with execution (not just SQL generation)
- ✅ Fix bugs one at a time
- ✅ Verify fix before moving to next bug
- ✅ Keep it simple - use existing audit script
- ✅ Document actual bugs found, not assumptions

## Key Commands Reference

```bash
# Start ClickHouse
cd benchmarks/ldbc_snb
docker-compose up -d

# Load data
./scripts/load_data_docker_v2.sh sf0.003

# Verify data
curl -s 'http://localhost:8123/' --data 'SELECT count(*) FROM ldbc.Person' --user default:default

# Start ClickGraph
cd ../..
export CLICKHOUSE_URL='http://localhost:8123'
export CLICKHOUSE_USER='default'
export CLICKHOUSE_PASSWORD='default' 
export CLICKHOUSE_DATABASE='ldbc'
export GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'
cargo run --release &

# Wait for server
sleep 5
curl http://localhost:8080/health

# Run audit
cd benchmarks/ldbc_snb/scripts
python3 query_audit.py --all

# Check specific failures
python3 query_audit.py --sql --ic  # Show generated SQL for IC queries
```

## Timeline

**Realistic**: 1-2 hours to load data + run audit + document findings  
**If bugs found**: +2-8 hours per bug depending on complexity  
**Total**: Could be 1 day to 1 week depending on number of bugs

## Success Criteria

- [x] Data loaded (sf0.003) - verified with row counts
- [ ] All IS queries pass with correct results
- [ ] All IC queries pass with correct results  
- [ ] All BI queries pass with correct results
- [ ] Bug list documented with root causes
- [ ] Fixes implemented and verified
- [ ] Re-audit shows 100% pass rate

---

**Remember**: Audit first, optimize later. Don't build infrastructure on broken foundations.
