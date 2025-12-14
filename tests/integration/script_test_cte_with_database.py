import clickhouse_connect

# Connect WITH database specified
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass',
    database='test_integration'  # ← Connected to this database
)

# Create test table
client.command("CREATE TABLE IF NOT EXISTS users (user_id UInt32, name String) ENGINE = Memory")
client.command("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

print("=== Test 1: Query with database prefix (while connected to that database) ===")
try:
    result = client.query("SELECT a.name FROM test_integration.users AS a")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 2: Query without database prefix ===")
try:
    result = client.query("SELECT a.name FROM users AS a")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 3: CTE with database prefix ===")
try:
    result = client.query("""
        WITH my_cte AS (
            SELECT user_id, name FROM test_integration.users
        )
        SELECT a.name FROM my_cte AS a
    """)
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 4: CTE without database prefix ===")
try:
    result = client.query("""
        WITH my_cte AS (
            SELECT user_id, name FROM users
        )
        SELECT a.name FROM my_cte AS a
    """)
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

# Cleanup
client.command("DROP TABLE users")
