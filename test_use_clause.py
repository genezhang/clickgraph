from neo4j import GraphDatabase

# Connect to ClickGraph
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))

# Create session WITHOUT database parameter (since driver doesn't send it anyway)
session = driver.session()

try:
    # Try using USE clause to select schema
    result = session.run('USE ecommerce_demo MATCH (c:Customer) RETURN c.first_name AS name LIMIT 1')
    records = list(result)
    print(f"✅ Query with USE clause returned {len(records)} record(s)")
    for record in records:
        print(f"  Name: {record['name']}")
except Exception as e:
    print(f"❌ Error with USE clause: {e}")

session.close()
driver.close()
