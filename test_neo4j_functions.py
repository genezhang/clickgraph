#!/usr/bin/env python3
"""
Test script for Neo4j function support
"""
import requests
import json

API_URL = "http://localhost:8080/query"

def test_function(cypher_query, description):
    """Test a single function and print results"""
    print(f"\n{'='*60}")
    print(f"Test: {description}")
    print(f"Query: {cypher_query}")
    print(f"{'-'*60}")
    
    try:
        response = requests.post(
            API_URL,
            json={"query": cypher_query, "sql_only": True},
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            if "sql" in result:
                print(f"✓ SQL Generated: {result['sql']}")
                return True
            else:
                print(f"✗ No SQL in response: {result}")
                return False
        else:
            print(f"✗ HTTP {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    print("\n🚀 Testing Neo4j Function Support (SQL Generation)")
    print("=" * 60)
    
    tests = [
        # String functions
        ("RETURN toUpper('hello') AS result", "toUpper() - uppercase"),
        ("RETURN toLower('WORLD') AS result", "toLower() - lowercase"),
        ("RETURN trim('  test  ') AS result", "trim() - remove whitespace"),
        ("RETURN substring('Hello World', 0, 5) AS result", "substring() - extract substring"),
        ("RETURN size('test') AS result", "size() - string length"),
        
        # String advanced
        ("RETURN split('a,b,c', ',') AS result", "split() - split string"),
        ("RETURN replace('hello', 'l', 'r') AS result", "replace() - replace substring"),
        ("RETURN reverse('hello') AS result", "reverse() - reverse string"),
        ("RETURN left('hello', 3) AS result", "left() - first N chars"),
        ("RETURN right('hello', 3) AS result", "right() - last N chars"),
        
        # Math functions
        ("RETURN abs(-5) AS result", "abs() - absolute value"),
        ("RETURN ceil(3.2) AS result", "ceil() - ceiling"),
        ("RETURN floor(3.8) AS result", "floor() - floor"),
        ("RETURN round(3.567) AS result", "round() - round"),
        ("RETURN sqrt(16) AS result", "sqrt() - square root"),
        
        # Type conversion
        ("RETURN toInteger('123') AS result", "toInteger() - string to int"),
        ("RETURN toFloat('3.14') AS result", "toFloat() - string to float"),
        ("RETURN toString(123) AS result", "toString() - int to string"),
    ]
    
    passed = 0
    failed = 0
    
    for query, desc in tests:
        if test_function(query, desc):
            passed += 1
        else:
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"📊 Test Summary:")
    print(f"   ✓ Passed: {passed}/{len(tests)}")
    print(f"   ✗ Failed: {failed}/{len(tests)}")
    print(f"   Success Rate: {passed/len(tests)*100:.1f}%")
    print(f"{'='*60}\n")
    
    if failed == 0:
        print("🎉 ALL TESTS PASSED! Neo4j functions working!")
    else:
        print(f"⚠️  {failed} test(s) failed - check output above")

if __name__ == "__main__":
    main()
