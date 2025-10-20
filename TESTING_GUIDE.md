# ClickGraph Testing Guide

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
.\test_server.ps1 -Start

# Run test query
.\test_server.ps1 -Test

# Run custom query
.\test_server.ps1 -Test -Query "MATCH (u:User) WHERE u.age > 30 RETURN u.name"

# Stop server
.\test_server.ps1 -Stop

# Clean up everything (kills orphaned processes)
.\test_server.ps1 -Clean
```

**Benefits**:
- ✅ Single terminal window
- ✅ Background server management
- ✅ PID tracking prevents duplicates
- ✅ Automatic cleanup
- ✅ No manual process hunting

---

## Option 2: Python Test Runner (Best for Test Suites)

**Best for**: Comprehensive testing, CI/CD integration

### Setup (One Time)
```bash
pip install requests
```

### Daily Workflow
```bash
# Start server
python test_runner.py --start

# Run full test suite
python test_runner.py --test

# Run single query
python test_runner.py --query "MATCH (u:User) RETURN u.name"

# Stop server
python test_runner.py --stop

# Clean everything
python test_runner.py --clean
```

**Benefits**:
- ✅ Cross-platform (Windows/Linux/Mac)
- ✅ Comprehensive test suite built-in
- ✅ Colored output for easy reading
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

## Troubleshooting

### "Port 8080 already in use"
```powershell
# PowerShell
.\test_server.ps1 -Clean

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
