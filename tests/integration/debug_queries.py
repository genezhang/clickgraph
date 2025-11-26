#!/usr/bin/env python3
"""Test basic queries to debug."""

import requests
import json

# Test 1: Check if Alice exists
response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name, u.user_id",
        "schema_name": "test_graph_schema"
    }
)
print("Test 1 - Alice exists?")
print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}\n")

# Test 2: Check if Eve exists
response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (u:User) WHERE u.name = 'Eve' RETURN u.name, u.user_id",
        "schema_name": "test_graph_schema"
    }
)
print("Test 2 - Eve exists?")
print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}\n")

# Test 3: Check follows relationships
response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name ORDER BY a.name, b.name LIMIT 10",
        "schema_name": "test_graph_schema"
    }
)
print("Test 3 - Follows relationships:")
print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}\n")

# Test 4: Check ANY path from Alice
response = requests.post(
    "http://localhost:8080/query",
    json={
        "query": "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) WHERE a.name = 'Alice' RETURN DISTINCT b.name ORDER BY b.name",
        "schema_name": "test_graph_schema"
    }
)
print("Test 4 - Any paths from Alice (1-3 hops):")
print(f"Status: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}\n")
