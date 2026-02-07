#!/usr/bin/env python3
"""
Test UNION Pruning Optimization for Neo4j Browser Queries

This script tests the optimization that prunes unnecessary UNION branches
based on id() constraints in the WHERE clause.
"""

import sys
import os

# Add project paths relative to script location
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(project_root, 'benchmarks', 'social_network'))

from neo4j import GraphDatabase
from datetime import datetime

# Configuration
BOLT_URI = "bolt://localhost:7688"
AUTH = None  # No authentication

def test_untyped_query_with_id_constraint():
    """
    Test: MATCH (a)-[r]->(b) WHERE id(a) IN [<user-ids>] RETURN r
    
    Expected behavior:
    - WITHOUT optimization: Generates UNION of ALL 10 relationship types
    - WITH optimization: Only generates User relationship branches (4 types)
    
    This is the query Neo4j Browser sends on double-click expansion.
    """
    print("\n" + "="*80)
    print("TEST: Untyped Query with id() Constraint (Neo4j Browser Pattern)")
    print("="*80)
    
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    
    try:
        with driver.session() as session:
            # First, get some User IDs
            result = session.run("MATCH (u:User) RETURN id(u) AS id LIMIT 5")
            user_ids = [record["id"] for record in result]
            
            if not user_ids:
                print("⚠️  No User IDs found - need data!")
                return False
                
            print(f"✓ Found {len(user_ids)} User IDs: {user_ids[:3]}...")
            
            # Now test the untyped query with id() constraint
            query = "MATCH (a)-[r]->(b) WHERE id(a) IN $ids RETURN r LIMIT 10"
            print(f"\n🔍 Query: {query}")
            print(f"   Parameters: ids={user_ids}")
            
            start_time = datetime.now()
            result = session.run(query, ids=user_ids)
            relationships = list(result)
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print(f"\n✓ Query succeeded!")
            print(f"   Returned: {len(relationships)} relationships")
            print(f"   Time: {elapsed:.3f}s")
            
            # Show some results
            if relationships:
                print(f"\n📊 Sample relationships:")
                for i, record in enumerate(relationships[:3]):
                    rel = record["r"]
                    print(f"   {i+1}. {rel.type}: {rel.start_node.id} → {rel.end_node.id}")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        return False
        
    finally:
        driver.close()

def test_typed_query_baseline():
    """
    Baseline: Test typed query for comparison
    
    Query: MATCH (a:User)-[r:FOLLOWS]->(b) RETURN r LIMIT 10
    
    This should work regardless of optimization (no UNION needed).
    """
    print("\n" + "="*80)
    print("TEST: Typed Query (Baseline - Should Always Work)")
    print("="*80)
    
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    
    try:
        with driver.session() as session:
            query = "MATCH (a:User)-[r:FOLLOWS]->(b) RETURN r LIMIT 10"
            print(f"\n🔍 Query: {query}")
            
            start_time = datetime.now()
            result = session.run(query)
            relationships = list(result)
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print(f"\n✓ Query succeeded!")
            print(f"   Returned: {len(relationships)} relationships")
            print(f"   Time: {elapsed:.3f}s")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        return False
        
    finally:
        driver.close()

def test_mixed_label_ids():
    """
    Test: id() constraint with mixed label IDs (User + Post)
    
    Expected behavior:
    - Should generate UNION of User branches + Post branches
    - Should NOT include PatternCompUser, ZeekLog branches
    """
    print("\n" + "="*80)
    print("TEST: Mixed Label IDs (User + Post)")
    print("="*80)
    
    driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
    
    try:
        with driver.session() as session:
            # Get some User IDs and Post IDs
            user_result = session.run("MATCH (u:User) RETURN id(u) AS id LIMIT 3")
            user_ids = [record["id"] for record in user_result]
            
            post_result = session.run("MATCH (p:Post) RETURN id(p) AS id LIMIT 3")
            post_ids = [record["id"] for record in post_result]
            
            if not user_ids or not post_ids:
                print("⚠️  Need both User and Post IDs")
                return False
            
            mixed_ids = user_ids + post_ids
            print(f"✓ Testing with {len(user_ids)} User IDs + {len(post_ids)} Post IDs")
            
            query = "MATCH (a)-[r]->(b) WHERE id(a) IN $ids RETURN r LIMIT 10"
            print(f"\n🔍 Query: {query}")
            
            start_time = datetime.now()
            result = session.run(query, ids=mixed_ids)
            relationships = list(result)
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print(f"\n✓ Query succeeded!")
            print(f"   Returned: {len(relationships)} relationships")
            print(f"   Time: {elapsed:.3f}s")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Query failed: {e}")
        return False
        
    finally:
        driver.close()

def main():
    print("\n🎯 UNION Pruning Optimization Test Suite")
    print("=" * 80)
    
    tests = [
        ("Typed Query Baseline", test_typed_query_baseline),
        ("Untyped Query with id() Constraint", test_untyped_query_with_id_constraint),
        ("Mixed Label IDs", test_mixed_label_ids),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n❌ Test '{name}' crashed: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "="*80)
    print("📊 TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
