#!/usr/bin/env python3
"""
Load schema and test Neo4j functions with actual graph data
"""
import requests
import json

API_URL = "http://localhost:8080"

def load_schema():
    """Load the social network schema"""
    print("📋 Loading social_network_demo schema...")
    try:
        with open("schemas/demo/social_network.yaml", "r") as f:
            schema_yaml = f.read()
        
        response = requests.post(
            f"{API_URL}/schemas/load",
            json={
                "schema_name": "social_network_demo",
                "config_content": schema_yaml
            },
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            print("✓ Schema loaded successfully!")
            return True
        else:
            print(f"✗ Failed to load schema: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"✗ Error loading schema: {e}")
        return False

def test_function(cypher_query, description, expected_function):
    """Test a function with schema context"""
    print(f"\n{'='*70}")
    print(f"Test: {description}")
    print(f"Query: {cypher_query}")
    print(f"{'-'*70}")
    
    try:
        response = requests.post(
            f"{API_URL}/query",
            json={
                "query": cypher_query,
                "sql_only": True,
                "schema_name": "social_network_demo"
            },
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            if "sql" in result:
                sql = result["sql"]
                # Check if our function was translated
                if expected_function.lower() in sql.lower():
                    print(f"✓ Function {expected_function}() translated!")
                    print(f"  SQL: {sql[:250]}..." if len(sql) > 250 else f"  SQL: {sql}")
                    return True
                else:
                    print(f"✗ Function {expected_function}() NOT found in SQL")
                    print(f"  SQL: {sql[:250]}...")
                    return False
            elif "generated_sql" in result:
                # SQL is in generated_sql field (sql_only mode)
                sql = result["generated_sql"]
                # Check if our function was translated
                if expected_function.lower() in sql.lower():
                    print(f"✓ Function {expected_function}() translated!")
                    print(f"  SQL: {sql[:250]}..." if len(sql) > 250 else f"  SQL: {sql}")
                    return True
                else:
                    print(f"✗ Function {expected_function}() NOT found in SQL")
                    print(f"  SQL: {sql[:250]}...")
                    return False
            else:
                error = result.get("error", str(result))
                print(f"✗ Query error: {error[:200]}...")
                return False
        else:
            print(f"✗ HTTP {response.status_code}: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    print("\n🚀 Testing Neo4j Functions with Schema Context")
    print("=" * 70)
    
    # Load schema first
    if not load_schema():
        print("\n⚠️  Cannot proceed without schema")
        return
    
    print(f"\n{'='*70}")
    print("Testing Function Translations")
    print("=" * 70)
    
    tests = [
        # String functions
        ("MATCH (n:User) RETURN toUpper(n.name) AS upper_name", "toUpper() - uppercase", "upper"),
        ("MATCH (n:User) RETURN toLower(n.name) AS lower_name", "toLower() - lowercase", "lower"),
        ("MATCH (n:User) RETURN trim(n.name) AS trimmed", "trim() - whitespace", "trim"),
        ("MATCH (n:User) RETURN substring(n.name, 0, 5) AS first_five", "substring() - extract", "substring"),
        ("MATCH (n:User) RETURN size(n.name) AS name_length", "size() - length", "length"),
        
        # String advanced
        ("MATCH (n:User) RETURN split(n.name, ' ') AS name_parts", "split() - split string", "splitByChar"),
        ("MATCH (n:User) RETURN replace(n.name, 'a', 'X') AS replaced", "replace() - replace", "replaceAll"),
        ("MATCH (n:User) RETURN reverse(n.name) AS reversed", "reverse() - reverse", "reverse"),
        
        # Math functions
        ("MATCH (n:User) WHERE abs(n.age) > 18 RETURN n", "abs() - absolute", "abs"),
        ("MATCH (n:User) RETURN ceil(n.age) AS age_ceil", "ceil() - ceiling", "ceil"),
        ("MATCH (n:User) RETURN floor(n.age) AS age_floor", "floor() - floor", "floor"),
        ("MATCH (n:User) RETURN round(n.age) AS age_round", "round() - round", "round"),
        ("MATCH (n:User) RETURN sqrt(n.age) AS age_sqrt", "sqrt() - square root", "sqrt"),
        
        # Type conversion
        ("MATCH (n:User) RETURN toString(n.age) AS age_str", "toString() - to string", "toString"),
        ("MATCH (n:User) RETURN toInteger(n.age) AS age_int", "toInteger() - to int", "toInt"),
        ("MATCH (n:User) RETURN toFloat(n.age) AS age_float", "toFloat() - to float", "toFloat"),
    ]
    
    passed = 0
    failed = 0
    
    for query, desc, expected_fn in tests:
        if test_function(query, desc, expected_fn):
            passed += 1
        else:
            failed += 1
    
    print(f"\n{'='*70}")
    print(f"📊 Final Results:")
    print(f"   ✓ Functions Translated: {passed}/{len(tests)}")
    print(f"   ✗ Failed: {failed}/{len(tests)}")
    print(f"   Success Rate: {passed/len(tests)*100:.1f}%")
    print(f"={'='*70}\n")
    
    if passed >= 12:
        print("🎉 EXCELLENT! Most functions working!")
    elif passed >= 8:
        print("✓ GOOD! Majority of functions working!")
    elif passed >= 4:
        print("⚠️  PARTIAL: Some functions working, needs fixes")
    else:
        print("❌ FAILED: Function translation not working")

if __name__ == "__main__":
    main()
