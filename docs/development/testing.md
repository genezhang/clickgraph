# ClickGraph Testing Guide

*Updated: November 2, 2025 - Reflects new directory structure*

## Quick Start

**Run all tests from project root:**
```bash
python run_tests.py              # Run all tests
python run_tests.py <pattern>    # Run specific tests
```

**Or run from tests directory:**
```bash
cd tests/python
python run_all_tests.py          # Run all tests
```

---

## Problem: Terminal Chaos
Multiple PowerShell windows accumulate, port conflicts occur, and it's hard to know if you're testing the latest code.

## Solution: Standardized Test Infrastructure

We provide **three testing approaches** - pick the one that fits your workflow:

---

## Option 1: PowerShell Test Runner (Fastest for Quick Tests)

**Best for**: Rapid iteration during development

### Setup (One Time)
```powershell
# Ensure ClickHouse is running
docker-compose up -d clickhouse-service
```

### Daily Workflow
```powershell
# Start server (runs in background, single window)
.\scripts\server\test_server.ps1 -Start

# Run test query
.\scripts\server\test_server.ps1 -Test

# Run custom query
.\scripts\server\test_server.ps1 -Test -Query "MATCH (u:User) WHERE u.age > 30 RETURN u.name"

# Stop server
.\scripts\server\test_server.ps1 -Stop

# Clean up everything (kills orphaned processes)
.\scripts\server\test_server.ps1 -Clean
```

**Benefits**:
- ✅ Single terminal window
- ✅ Background server management
- ✅ PID tracking prevents duplicates
- ✅ Automatic cleanup
- ✅ No manual process hunting

---

## Option 2: Python Test Runner (Best for Test Suites)

**Best for**: Running comprehensive test suites and CI/CD integration

### Quick Start from Project Root
```bash
# Run all tests
python run_tests.py

# Run specific test
python run_tests.py test_optional_match

# Run tests matching pattern
python run_tests.py benchmark
```

### Run from Tests Directory
```bash
cd tests/python

# Start server
python test_runner.py --start

# Run full test suite
python test_runner.py --test

# Run all tests
python run_all_tests.py

# Run specific test file
python test_optional_match.py

# Stop server
python test_runner.py --stop

# Clean everything
python test_runner.py --clean
```

**Benefits**:
- ✅ Cross-platform (Windows/Linux/Mac)
- ✅ Comprehensive test suite (34+ test files)
- ✅ Run from project root or tests directory
- ✅ Pattern matching for selective testing
- ✅ Structured test results
- ✅ Easy to extend with new tests

---

## Option 3: Docker Compose (Most Isolated)

**Best for**: Clean environment, multiple developers, CI/CD

### Daily Workflow
```bash
# Start everything (ClickHouse + ClickGraph)
docker-compose -f docker-compose.test.yaml up -d

# View logs
docker-compose -f docker-compose.test.yaml logs -f clickgraph-server

# Test from host machine
python test_runner.py --query "MATCH (u:User) RETURN u.name"

# Stop everything
docker-compose -f docker-compose.test.yaml down
```

**Benefits**:
- ✅ Complete isolation
- ✅ Reproducible environment
- ✅ No port conflicts with dev setup
- ✅ Matches production deployment
- ✅ Easy cleanup (just `down`)

---

## Recommended Workflow

### During Active Development
Use **Option 1** (PowerShell runner) for speed:

```powershell
# Morning setup
.\test_server.ps1 -Clean  # Clean slate
.\test_server.ps1 -Start  # Start server

# Edit code, then test
.\test_server.ps1 -Stop   # Stop old server
cargo build --release     # Rebuild (optional: test runner does this)
.\test_server.ps1 -Start  # Start new server
.\test_server.ps1 -Test   # Test changes

# End of day
.\test_server.ps1 -Clean  # Clean up
```

### Before Committing
Use **Option 2** (Python test suite) for validation:

```bash
python test_runner.py --clean
python test_runner.py --start
python test_runner.py --test  # Run all tests
python test_runner.py --stop
```

### Before Merging to Main
Use **Option 3** (Docker) for final verification:

```bash
docker-compose -f docker-compose.test.yaml up --build
# Wait for startup, then test
python test_runner.py --test
docker-compose -f docker-compose.test.yaml down
```

---

## Test Directory Structure

As of November 2, 2025, all tests are organized in the `tests/` directory:

```
tests/
├── python/          # Python test scripts (34+ files)
│   ├── run_all_tests.py          # Main test runner
│   ├── test_runner.py            # Server management + testing
│   ├── test_optional_match.py    # Feature tests
│   ├── test_path_variables.py
│   ├── test_shortest_path*.py
│   ├── test_benchmark*.py        # Benchmark tests
│   └── ...
├── sql/             # SQL test files
│   ├── test_generated.sql
│   ├── test_manual.sql
│   └── shortest_path_sql.txt
├── cypher/          # Cypher query files
│   ├── test_path_functions.cypher
│   └── test_where_clause.cypher
└── data/            # Test data files
    ├── test_*.json              # JSON test data
    ├── customers.csv
    └── *.ipynb                  # Jupyter notebooks
```

**Running tests from new locations:**

```bash
# From project root (recommended)
python run_tests.py                    # Convenience wrapper
python tests/python/run_all_tests.py   # Direct invocation

# From tests directory
cd tests/python
python run_all_tests.py                # Run all tests
python test_optional_match.py          # Run specific test
```

**Server management scripts moved to `scripts/server/`:**
```powershell
.\scripts\server\test_server.ps1       # PowerShell test runner
.\scripts\server\start_server*.ps1     # Various server start scripts
```

**Setup scripts moved to `scripts/setup/`:**
```bash
scripts/setup/setup_demo_data.sql      # Demo data
scripts/setup/setup_test_data.sql      # Test data
```

---

## Troubleshooting

### "Port 8080 already in use"
```powershell
# PowerShell
.\scripts\server\test_server.ps1 -Clean

# Or manually
netstat -ano | findstr :8080
taskkill /PID <pid> /F
```

### "Server won't start"
```powershell
# Check ClickHouse is running
docker ps | findstr clickhouse

# Check server logs
Get-Content server.pid  # Get PID
# Look at cargo output in background window
```

### "Tests fail but query works manually"
```bash
# Ensure you're testing the right server
.\test_server.ps1 -Stop
.\test_server.ps1 -Clean
.\test_server.ps1 -Start
```

### "Too many PowerShell windows"
This is exactly why we created these runners! Use them instead:
```powershell
# Never run cargo run directly anymore
# Always use:
.\test_server.ps1 -Start  # Manages background process
```

---

## Adding New Tests

### PowerShell Runner
Edit `test_server.ps1` to add more test cases in the `Test-Query` function.

### Python Runner
Edit `test_runner.py` and add to the `test_cases` array in `run_tests()`:

```python
test_cases = [
    # ... existing tests ...
    ("MATCH (u:User) WHERE u.age < 30 RETURN u.name",
     "Test: Young users"),
]
```

---

## Best Practices

1. **Always use test runners** - Never run `cargo run` directly
2. **Clean before testing** - Run `--clean` if uncertain
3. **One server at a time** - Let the runner manage lifecycle
4. **Check ClickHouse first** - Ensure it's running before starting server
5. **Use Docker for final validation** - Before pushing to main

---

## Quick Reference

| Task | PowerShell | Python | Docker |
|------|------------|--------|--------|
| Quick test | `.\test_server.ps1 -Test` | `python test_runner.py --query "..."` | N/A |
| Full suite | N/A | `python test_runner.py --test` | `python test_runner.py --test` |
| Start/Stop | `.\test_server.ps1 -Start/Stop` | `python test_runner.py --start/stop` | `docker-compose ... up/down` |
| Clean up | `.\test_server.ps1 -Clean` | `python test_runner.py --clean` | `docker-compose ... down -v` |

---

## Path Variable Testing Challenges & Solutions

### Overview
Path variable testing (`test_path_variable.py`) presents unique challenges due to the complexity of CTE generation, schema mapping, and WHERE clause handling in recursive queries.

### Common Issues & Fixes

#### 1. Server Hanging/Not Responding
**Symptoms**: Server logs show successful binding but doesn't accept connections
**Root Cause**: Port conflicts or improper background execution
**Solution**:
```powershell
# Kill any existing processes
Get-Process -Name brahmand -ErrorAction SilentlyContinue | Stop-Process -Force

# Use the test script for proper background execution
.\test_server.ps1 -Start
```

#### 2. Schema Mapping Errors
**Symptoms**: `UNKNOWN_IDENTIFIER` errors like `end_node.name` not found
**Root Cause**: YAML schema doesn't match actual ClickHouse table structure
**Solution**:
```bash
# Check actual table schema
docker exec clickhouse clickhouse-client --database test_multi_rel --query "DESCRIBE users"

# Update YAML to match (example)
property_mappings:
  name: full_name  # if ClickHouse column is 'full_name'
```

#### 3. CTE WHERE Clause Issues
**Symptoms**: `end_node.name = 'Alice'` fails in CTE filtering
**Root Cause**: WHERE conditions applied at wrong level in recursive CTE
**Current Status**: Known limitation - path variable WHERE clauses need special handling
**Workaround**: Test with simpler queries first, then add WHERE conditions

#### 4. Column Reference Problems
**Symptoms**: `end_node.full_name` cannot be resolved
**Root Cause**: CTE column generation doesn't include all required properties
**Solution**: Ensure path variable CTE includes all referenced columns:
```sql
-- Generated CTE should include:
SELECT ..., end_node.full_name AS end_name, start_node.full_name AS start_name
```

### Testing Strategy for Path Variables

1. **Start Simple**: Test basic path existence first
   ```cypher
   MATCH p = (a:User)-[:FOLLOWS*]-(b:User) RETURN p
   ```

2. **Add Constraints Gradually**: Test range limits before WHERE clauses
   ```cypher
   MATCH p = (a:User)-[:FOLLOWS*1..3]-(b:User) RETURN p
   ```

3. **Verify Schema Mapping**: Ensure YAML matches ClickHouse tables
   ```bash
   docker exec clickhouse clickhouse-client --database test_multi_rel --query "SELECT * FROM users LIMIT 1"
   ```

4. **Check CTE Generation**: Review generated SQL for column references
   - Look for `path_nodes`, `path_relationships` arrays
   - Verify `hop_count` column exists
   - Check WHERE clause placement

### Debugging Commands

```powershell
# View server logs
Get-Content server.log -Tail 20

# Check ClickHouse tables
docker exec clickhouse clickhouse-client --database test_multi_rel --query "SHOW TABLES"
docker exec clickhouse clickhouse-client --database test_multi_rel --query "DESCRIBE users"

# Test basic connectivity
Invoke-WebRequest -Uri "http://localhost:8080/query" -Method POST -ContentType "application/json" -Body '{"query":"MATCH (u:User) RETURN count(u)"}'
```

### Current Path Variable Status
- ✅ Basic path variable parsing: Working
- ✅ CTE generation with arrays: Working
- ✅ Path functions (length, nodes): Working
- ⚠️ WHERE clause filtering: Needs refinement
- ⚠️ Schema mapping validation: Requires attention

### Future Improvements
1. Enhanced WHERE clause handling for path variables
2. Automatic schema validation against ClickHouse
3. Better error messages for CTE generation issues
4. Path relationship type tracking in arrays



