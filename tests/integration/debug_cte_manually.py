import clickhouse_connect
import requests

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass',
    database='test_integration'
)

# Create tables
print("Creating tables...")
client.command("DROP TABLE IF EXISTS users")
client.command("DROP TABLE IF EXISTS follows")

client.command("""
    CREATE TABLE users (
        user_id UInt32,
        name String,
        age UInt32
    ) ENGINE = Memory
""")

client.command("""
    CREATE TABLE follows (
        follower_id UInt32,
        followed_id UInt32,
        since String
    ) ENGINE = Memory
""")

# Insert test data
print("Inserting data...")
client.command("""
    INSERT INTO users VALUES
        (1, 'Alice', 30),
        (2, 'Bob', 25),
        (3, 'Charlie', 35),
        (4, 'Diana', 28),
        (5, 'Eve', 32)
""")

# Insert follows relationships
# Alice (1) → Bob (2) → Diana (4) → Eve (5)
# Alice (1) → Charlie (3) → Diana (4)
client.command("""
    INSERT INTO follows VALUES
        (1, 2, '2023-01-01'),
        (1, 3, '2023-01-15'),
        (2, 3, '2023-02-01'),
        (2, 4, '2023-03-15'),
        (3, 4, '2023-02-15'),
        (4, 5, '2023-03-01')
""")

print("\n=== Data Setup Complete ===")
print("Users:", client.query("SELECT user_id, name FROM users ORDER BY user_id").result_rows)
print("Follows:", client.query("SELECT follower_id, followed_id FROM follows ORDER BY follower_id, followed_id").result_rows)

# Test direct path
print("\n=== Test 1: Check if direct Alice->Eve exists ===")
result = client.query("""
    SELECT a.name, b.name 
    FROM users a 
    JOIN follows rel ON a.user_id = rel.follower_id
    JOIN users b ON rel.followed_id = b.user_id
    WHERE a.name = 'Alice' AND b.name = 'Eve'
""")
print("Direct path:", result.result_rows)

# Test recursive CTE manually
print("\n=== Test 2: Run shortest path CTE ===")
sql = """
WITH RECURSIVE variable_path_inner AS (
    SELECT
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        [start_node.user_id] as path_nodes,
        start_node.name as start_name,
        end_node.name as end_name
    FROM users start_node
    JOIN follows rel ON start_node.user_id = rel.follower_id
    JOIN users end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Alice' AND end_node.name = 'Eve'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_nodes, [current_node.user_id]) as path_nodes,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM variable_path_inner vp
    JOIN users current_node ON vp.end_id = current_node.user_id
    JOIN follows rel ON current_node.user_id = rel.follower_id
    JOIN users end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 10
      AND NOT has(vp.path_nodes, current_node.user_id)
      AND end_node.name = 'Eve'
),
variable_path_shortest AS (
    SELECT * FROM variable_path_inner ORDER BY hop_count ASC LIMIT 1
)
SELECT start_name, end_name, hop_count, path_nodes
FROM variable_path_shortest
SETTINGS max_recursive_cte_evaluation_depth = 100
"""

try:
    result = client.query(sql)
    print("CTE result:", result.result_rows)
except Exception as e:
    print("Error:", e)

# Now test via ClickGraph
print("\n=== Test 3: Query via ClickGraph ===")
response = requests.post('http://localhost:8080/query', json={
    'query': '''
        MATCH path = shortestPath((a:User)-[:FOLLOWS*]-(b:User))
        WHERE a.name = "Alice" AND b.name = "Eve"
        RETURN a.name, b.name
    ''',
    'schema_name': 'test_graph_schema'
})

print("ClickGraph status:", response.status_code)
print("ClickGraph response:", response.json())

# Cleanup
print("\n=== Cleanup ===")
client.command("DROP TABLE users")
client.command("DROP TABLE follows")
