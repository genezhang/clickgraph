import requests
import json

query = """
MATCH (a:User)
WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
RETURN a.name, b.name
"""

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "schema_name": "test_graph_schema", "sql_only": True}
)

if response.status_code == 200:
    result = response.json()
    sql = result.get('generated_sql', result.get('sql', ''))
    
    print("=" * 80)
    print("Full Response:")
    print(json.dumps(result, indent=2))
    print("=" * 80)
    print("\nGenerated SQL:")
    print("=" * 80)
    print(sql)
    print("\n")
    
    # Check for INNER JOIN vs LEFT JOIN
    inner_count = sql.count('INNER JOIN')
    left_count = sql.count('LEFT JOIN')
    
    print(f"INNER JOIN count: {inner_count}")
    print(f"LEFT JOIN count: {left_count}")
    print("\n")
    
    # Find the actual JOIN keywords
    import re
    joins = re.findall(r'(INNER JOIN|LEFT JOIN).*?(?=INNER JOIN|LEFT JOIN|WHERE|GROUP BY|ORDER BY|$)', sql, re.DOTALL)
    for i, join in enumerate(joins, 1):
        print(f"Join {i}: {join[:100]}...")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
