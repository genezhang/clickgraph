import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost', 
    port=8123, 
    username='test_user', 
    password='test_pass'
)

# Test different SQL patterns
sqls = [
    "SELECT u.name FROM test_integration.users AS u WHERE u.name = 'Alice'",
    "SELECT name FROM test_integration.users WHERE name = 'Alice'",
    "SELECT u.name, u.age FROM test_integration.users AS u WHERE u.name = 'Alice'",
]

for sql in sqls:
    print(f"\nTesting: {sql}")
    try:
        result = client.query(sql)
        print(f"✓ Success: {result.result_rows}")
    except Exception as e:
        print(f"✗ Error: {e}")
