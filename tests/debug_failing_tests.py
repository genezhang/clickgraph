"""
Debug the two failing E2E tests
"""
import requests
import json
import sys

BASE_URL = "http://localhost:8080"

def load_schema():
    """Load the test schema"""
    schema_yaml = open("e2e/buckets/param_func/schema.yaml", "r").read()
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "test_param_func_schema",
            "config_content": schema_yaml,
            "validate_schema": False
        }
    )
    print(f"Schema load status: {response.status_code}")
    return response.status_code == 200

def test_abs_function():
    """Test 1: abs() function in WHERE clause"""
    print("\n" + "="*60)
    print("TEST 1: abs() function in WHERE clause")
    print("="*60)
    
    query = """
    MATCH (u:User)
    WHERE abs(u.age - $targetAge) < $tolerance
    RETURN u.name, u.age
    ORDER BY u.age
    """
    
    payload = {
        "query": query,
        "parameters": {"targetAge": 30, "tolerance": 5},
        "schema_name": "test_param_func_schema"
    }
    
    print(f"Query: {query}")
    print(f"Parameters: {payload['parameters']}")
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    print(f"\nResponse status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ SUCCESS")
        print(json.dumps(result, indent=2))
        return True
    else:
        print(f"❌ FAILED")
        print(f"Response: {response.text}")
        return False

def test_relationship_properties():
    """Test 2: Missing 'total' property in relationship traversal"""
    print("\n" + "="*60)
    print("TEST 2: Relationship traversal with property selection")
    print("="*60)
    
    query = """
    MATCH (u:User)-[r:PLACED]->(o:Order)
    WHERE o.total > $minTotal
    RETURN 
        toUpper(u.name) AS user_name,
        u.age,
        o.total,
        ceil(o.total) AS rounded_total
    ORDER BY o.total DESC
    """
    
    payload = {
        "query": query,
        "parameters": {"minTotal": 100},
        "schema_name": "test_param_func_schema"
    }
    
    print(f"Query: {query}")
    print(f"Parameters: {payload['parameters']}")
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    print(f"\nResponse status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ SUCCESS")
        print(json.dumps(result, indent=2))
        
        # Check for 'total' key
        if result.get("results"):
            first_row = result["results"][0]
            print(f"\nFirst row keys: {list(first_row.keys())}")
            if 'total' in first_row:
                print("✅ 'total' key present")
                return True
            else:
                print("❌ 'total' key MISSING")
                return False
        return True
    else:
        print(f"❌ FAILED")
        print(f"Response: {response.text}")
        return False

def main():
    print("Loading schema...")
    if not load_schema():
        print("Failed to load schema")
        return 1
    
    test1_passed = test_abs_function()
    test2_passed = test_relationship_properties()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Test 1 (abs function): {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"Test 2 (total property): {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    return 0 if (test1_passed and test2_passed) else 1

if __name__ == "__main__":
    sys.exit(main())
