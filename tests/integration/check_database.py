import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass'
)

# Check databases
print("=== Databases ===")
result = client.query("SHOW DATABASES")
print(result.result_rows)
print()

# Check tables in test_integration
print("=== Tables in test_integration ===")
try:
    result = client.query("SHOW TABLES FROM test_integration")
    print(result.result_rows)
except Exception as e:
    print(f"Error: {e}")
print()

# Check if users table exists
print("=== Check users table ===")
try:
    result = client.query("SELECT count() FROM test_integration.users")
    print(f"Users count: {result.result_rows}")
except Exception as e:
    print(f"Error: {e}")
