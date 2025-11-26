#!/usr/bin/env python3
"""Test the CTE directly in ClickHouse."""

from clickhouse_driver import Client

client = Client(
    host='localhost',
    port=9000,
    user='test_user',
    password='test_pass'
)

sql = """
WITH RECURSIVE variable_path_inner AS (
    SELECT
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        [start_node.user_id] as path_nodes,
        ['FOLLOWS'] as path_relationships,
        start_node.name as start_name,
        end_node.name as end_name
    FROM test_integration.users start_node
    JOIN test_integration.follows rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_nodes, [current_node.user_id]) as path_nodes,
        arrayConcat(vp.path_relationships, ['FOLLOWS']) as path_relationships,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM variable_path_inner vp
    JOIN test_integration.users current_node ON vp.end_id = current_node.user_id
    JOIN test_integration.follows rel ON current_node.user_id = rel.follower_id
    JOIN test_integration.users end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 10
      AND NOT has(vp.path_nodes, current_node.user_id)
),
variable_path_shortest AS (
    SELECT * FROM variable_path_inner ORDER BY hop_count ASC LIMIT 1
),
variable_path_to_target AS (
    SELECT * FROM variable_path_shortest WHERE end_name = 'Eve'
),
variable_path AS (
    SELECT * FROM variable_path_to_target
)
SELECT
      a.name AS "a.name",
      b.name AS "b.name"
FROM variable_path AS t
JOIN test_integration.users AS a ON t.start_id = a.user_id
JOIN test_integration.users AS b ON t.end_id = b.user_id
SETTINGS max_recursive_cte_evaluation_depth = 100
"""

print("Executing CTE query directly in ClickHouse...")
try:
    result = client.execute(sql)
    print(f"✓ Query succeeded!")
    print(f"  Rows returned: {len(result)}")
    for row in result:
        print(f"  {row}")
except Exception as e:
    print(f"✗ Query failed: {e}")

# Also test just the inner CTE to see what paths are found
print("\n\nTesting inner CTE only (all paths from Alice):")
sql2 = """
WITH RECURSIVE variable_path_inner AS (
    SELECT
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        start_node.name as start_name,
        end_node.name as end_name
    FROM test_integration.users start_node
    JOIN test_integration.follows rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.name = 'Alice'
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        vp.start_name as start_name,
        end_node.name as end_name
    FROM variable_path_inner vp
    JOIN test_integration.users current_node ON vp.end_id = current_node.user_id
    JOIN test_integration.follows rel ON current_node.user_id = rel.follower_id
    JOIN test_integration.users end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 5
)
SELECT DISTINCT start_name, end_name, hop_count 
FROM variable_path_inner 
ORDER BY hop_count, end_name
SETTINGS max_recursive_cte_evaluation_depth = 100
"""

try:
    result = client.execute(sql2)
    print(f"✓ Query succeeded!")
    print(f"  Paths found: {len(result)}")
    for row in result:
        print(f"  {row[0]} -> {row[1]} ({row[2]} hops)")
except Exception as e:
    print(f"✗ Query failed: {e}")
