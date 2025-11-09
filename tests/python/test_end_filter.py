import requests
import json

# Test end node filter specifically
query = 'MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = "David Lee" RETURN a'
payload = {'query': query}

try:
    response = requests.post('http://localhost:8080/query', json=payload, timeout=10)
    print(f"Status Code: {response.status_code}")
    print(f"Response Text: {response.text}")

    if response.status_code == 200:
        try:
            result = response.json()
            print("Parsed JSON Response:")
            print(json.dumps(result, indent=2))

            # Look for the generated SQL in the response
            if 'sql' in result:
                sql = result['sql']
                print('\nGenerated SQL:')
                print('=' * 50)
                print(sql)
                print('=' * 50)

                # Check if end_node.name appears in the SQL
                if 'end_node.name' in sql:
                    print('[OK] CORRECT: Found end_node.name in SQL')
                else:
                    print('[FAIL] INCORRECT: end_node.name NOT found in SQL')

                # Check if start_node.name appears (it shouldn't for end filter)
                if 'start_node.name' in sql and 'David Lee' in sql:
                    print('[FAIL] INCORRECT: Found start_node.name with David Lee (should be end_node.name)')
                else:
                    print('[OK] CORRECT: No incorrect start_node.name mapping')
            else:
                print('No SQL found in response')

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
    else:
        print(f"HTTP Error: {response.status_code}")

except Exception as e:
    print('Error:', e)