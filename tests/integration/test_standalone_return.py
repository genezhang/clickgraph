"""
Quick test for standalone RETURN queries (no MATCH clause)
"""
import requests
import json

BASE_URL = "http://localhost:8080"

def test_query(query, parameters=None, description=""):
    """Execute a query and print results"""
    print(f"\n{'='*60}")
    print(f"Test: {description}")
    print(f"Query: {query}")
    if parameters:
        print(f"Parameters: {parameters}")
    
    payload = {"query": query}
    if parameters:
        payload["parameters"] = parameters
    
    try:
        response = requests.post(f"{BASE_URL}/query", json=payload)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Result: {json.dumps(result, indent=2)}")
            return True
        else:
            print(f"Error: {response.text}")
            return False
    except Exception as e:
        print(f"Exception: {e}")
        return False

def main():
    """Run standalone RETURN tests"""
    print("Testing standalone RETURN queries (no MATCH clause)")
    
    tests_passed = 0
    tests_total = 0
    
    # Test 1: Simple literal
    tests_total += 1
    if test_query("RETURN 1 + 1 AS sum", description="Simple arithmetic"):
        tests_passed += 1
    
    # Test 2: String literal
    tests_total += 1
    if test_query("RETURN 'hello' AS greeting", description="String literal"):
        tests_passed += 1
    
    # Test 3: Function call
    tests_total += 1
    if test_query("RETURN toUpper('hello') AS upper", description="Function call"):
        tests_passed += 1
    
    # Test 4: Parameter
    tests_total += 1
    if test_query(
        "RETURN $name AS param_value",
        parameters={"name": "World"},
        description="Parameter reference"
    ):
        tests_passed += 1
    
    # Test 5: Function with parameter
    tests_total += 1
    if test_query(
        "RETURN toUpper($text) AS result",
        parameters={"text": "hello world"},
        description="Function with parameter"
    ):
        tests_passed += 1
    
    # Test 6: Multiple expressions
    tests_total += 1
    if test_query(
        "RETURN 1 + 1 AS sum, 2 * 3 AS product, 'test' AS text",
        description="Multiple expressions"
    ):
        tests_passed += 1
    
    # Test 7: Nested functions
    tests_total += 1
    if test_query(
        "RETURN length(toUpper('hello')) AS len",
        description="Nested functions"
    ):
        tests_passed += 1
    
    print(f"\n{'='*60}")
    print(f"Results: {tests_passed}/{tests_total} tests passed")
    
    if tests_passed == tests_total:
        print("✅ All standalone RETURN tests passed!")
        return 0
    else:
        print(f"❌ {tests_total - tests_passed} tests failed")
        return 1

if __name__ == "__main__":
    exit(main())
