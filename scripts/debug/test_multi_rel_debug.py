import requests
import json

query = """
MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User)
RETURN u1, u2
"""

payload = {
    "query": query.strip(),
    "view": "social_graph",
    "sql_only": True
}

response = requests.post("http://localhost:8080/query", json=payload)
print(f"Status: {response.status_code}")

result = response.json()
if "generated_sql" in result:
    sql = result["generated_sql"]
    print("\n=== Generated SQL ===")
    print(sql)
    print(f"\n=== UNION ALL count: {sql.count('UNION ALL')} ===")
elif "sql" in result:
    sql = result["sql"]
    print("\n=== Generated SQL ===")
    print(sql)
    print(f"\n=== UNION ALL count: {sql.count('UNION ALL')} ===")
else:
    print("\n=== Full Response ===")
    print(json.dumps(result, indent=2))
