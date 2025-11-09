# Running ClickGraph Integration Tests

## Quick Start

### Complete Setup and Test (Windows)
```powershell
# Run the automated setup script
.\setup_and_test.bat
```

This script will:
1. ✓ Check ClickHouse is running
2. ✓ Create test database and tables
3. ✓ Insert test data
4. ✓ Start ClickGraph server with proper config
5. ✓ Load test schema
6. ✓ Ready to run tests!

Then run tests:
```powershell
cd tests\integration
pytest test_optional_match.py -v
```

### Manual Setup (Step by Step)

#### 1. Start ClickHouse
```powershell
docker-compose up -d clickhouse-service
```

Wait for ClickHouse to be healthy (~30 seconds).

#### 2. Create Test Database and Tables
```powershell
# Using curl (Windows)
curl -X POST "http://localhost:8123/?user=test_user&password=test_pass" `
  -d "CREATE DATABASE IF NOT EXISTS test_integration"

curl -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" `
  -d "CREATE TABLE IF NOT EXISTS users (id UInt32, name String, age UInt32) ENGINE = Memory"

curl -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" `
  -d "CREATE TABLE IF NOT EXISTS follows (follower_id UInt32, followed_id UInt32, since String) ENGINE = Memory"
```

#### 3. Insert Test Data
```powershell
curl -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" `
  -d "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35), (4, 'Diana', 28), (5, 'Eve', 32)"

curl -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" `
  -d "INSERT INTO follows VALUES (1, 2, '2023-01-01'), (1, 3, '2023-01-15'), (2, 3, '2023-02-01'), (3, 4, '2023-02-15'), (4, 5, '2023-03-01'), (2, 4, '2023-03-15')"
```

#### 4. Set Environment Variables
```powershell
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "test_integration"
$env:GRAPH_CONFIG_PATH = "tests/integration/test_integration.yaml"
```

#### 5. Start ClickGraph Server
```powershell
# Build if needed
cargo build

# Start server
cargo run --bin clickgraph
```

Wait for "ClickGraph server is running" message.

#### 6. Load Test Schema (in another terminal)
```powershell
curl -X POST http://localhost:8080/schemas/load `
  -H "Content-Type: application/json" `
  -d '{"schema_path": "tests/integration/test_integration.yaml", "schema_name": "test_integration"}'
```

#### 7. Run Tests
```powershell
cd tests\integration
pytest test_optional_match.py -v
```

## Test Data Structure

### Users Table
```
id | name     | age
---+----------+----
1  | Alice    | 30
2  | Bob      | 25
3  | Charlie  | 35
4  | Diana    | 28
5  | Eve      | 32
```

### Follows Relationships
```
follower_id → followed_id | since
1 (Alice) → 2 (Bob)       | 2023-01-01
1 (Alice) → 3 (Charlie)   | 2023-01-15
2 (Bob) → 3 (Charlie)     | 2023-02-01
2 (Bob) → 4 (Diana)       | 2023-03-15
3 (Charlie) → 4 (Diana)   | 2023-02-15
4 (Diana) → 5 (Eve)       | 2023-03-01
```

## Required Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_USER` | `test_user` | Database username |
| `CLICKHOUSE_PASSWORD` | `test_pass` | Database password |
| `CLICKHOUSE_DATABASE` | `test_integration` | Test database name |
| `GRAPH_CONFIG_PATH` | `tests/integration/test_integration.yaml` | Schema config file |

## Troubleshooting

### Server runs in "YAML-only mode"
**Problem**: Missing ClickHouse environment variables.  
**Solution**: Set all required environment variables before starting server.

### Tests fail with "server not responding"
**Problem**: ClickGraph server not running or not on port 8080.  
**Solution**: 
```powershell
# Check server status
curl http://localhost:8080/health

# Restart server
.\setup_and_test.bat
```

### Tests fail with "Database doesn't exist"
**Problem**: Test database not created.  
**Solution**: Run setup script or manually create database.

### ClickHouse connection errors
**Problem**: ClickHouse not running or wrong credentials.  
**Solution**:
```powershell
# Check ClickHouse
docker ps | Select-String clickhouse

# Restart if needed
docker-compose restart clickhouse-service
```

## Running Specific Tests

```powershell
# Single test file
pytest test_optional_match.py -v

# Single test class
pytest test_optional_match.py::TestSingleOptionalMatch -v

# Single test method
pytest test_optional_match.py::TestSingleOptionalMatch::test_optional_match_existing_node -v

# Stop on first failure
pytest test_optional_match.py -x

# Show print statements
pytest test_optional_match.py -v -s
```

## Cleanup

```powershell
# Stop ClickGraph server
taskkill /F /IM clickgraph.exe

# Stop ClickHouse (keeps data)
docker-compose stop

# Remove all data
docker-compose down -v
```
