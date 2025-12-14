import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass',
    database='test_integration'
)

# Create a simple test table
print("Creating test table...")
client.command("""
    CREATE TABLE test_table (
        id UInt32,
        name String
    ) ENGINE = Memory
""")

# Insert data
print("Inserting data...")
client.command("""
    INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')
""")

# Test different query forms
print("\n=== Test 1: SELECT without database prefix ===")
try:
    result = client.query("SELECT name FROM test_table")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 2: SELECT with database prefix ===")
try:
    result = client.query("SELECT name FROM test_integration.test_table")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 3: SELECT with database prefix and alias ===")
try:
    result = client.query("SELECT a.name FROM test_integration.test_table AS a")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== Test 4: SELECT without database prefix but with alias ===")
try:
    result = client.query("SELECT a.name FROM test_table AS a")
    print(f"Success: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")

# Cleanup
print("\nCleaning up...")
client.command("DROP TABLE test_table")
