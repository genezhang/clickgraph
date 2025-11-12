from neo4j import GraphDatabase

# Connect to ClickGraph
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'))

# Create session with database parameter
session = driver.session(database='ecommerce_demo')

try:
    # Run a simple query
    result = session.run('MATCH (c:Customer) RETURN c.first_name AS name LIMIT 1')
    records = list(result)
    print(f"Query returned {len(records)} record(s)")
    for record in records:
        print(f"  Name: {record['name']}")
except Exception as e:
    print(f"Error: {e}")
finally:
    session.close()
    driver.close()
