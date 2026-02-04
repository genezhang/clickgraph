#!/usr/bin/env python3
"""
Bug #2 Investigation: Trace where empty branch error occurs
"""
import requests

# Minimal test case: UNION with empty relationship branch
query = """
USE social_benchmark
MATCH (n) WHERE n.country IS NOT NULL 
RETURN n.country
UNION ALL 
MATCH ()-[r]-() WHERE r.country IS NOT NULL 
RETURN r.country
"""

print("="*80)
print("Bug #2: Empty Branch Investigation")
print("="*80)
print("\nQuery:")
print(query)
print("\nExpected: Return nodes with country, skip relationships (none have country)")
print("Actual: Will error with 'Relationship type '' not found'")
print("\nSending query...")

r = requests.post("http://localhost:8080/query", json={"query": query})
print(f"\nStatus: {r.status_code}")
print(f"Response: {r.text[:500]}")

