"""Quick test to see the generated SQL"""
import requests

query = """
MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User))
WHERE a.name = 'Alice Johnson' AND b.name = 'Bob Smith'
RETURN a.name, b.name
"""

print("Testing query:")
print(query)
print()

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "sql_only": True},
    headers={"Content-Type": "application/json"}
)

if response.status_code == 200:
    result = response.json()
    sql = result.get("generated_sql", "No SQL")
    print("Generated SQL:")
    print(sql)
    print()
    
    # Check if WHERE clause is present
    if "Alice Johnson" in sql:
        print("✅ WHERE clause found in SQL")
    else:
        print("❌ WHERE clause NOT found in SQL!")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
