import requests
import json

url = "http://localhost:8080/query"

# Test 1: Combined MATCH (should work)
query1 = "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN a.name, b.name"
print("Query 1: Combined MATCH")
resp = requests.post(url, json={"query": query1})
print(f"Status: {resp.status_code}")
if "SELECT" in resp.text:
    sql = resp.text[resp.text.find("SELECT"):resp.text.find(". (UNKNOWN")]
    print(f"SQL: {sql}\n")

# Test 2: Two separate MATCHes 
query2 = "MATCH (a:User) WHERE a.name = 'Alice' MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
print("Query 2: Two separate MATCHes")
resp = requests.post(url, json={"query": query2})
print(f"Status: {resp.status_code}")
if "SELECT" in resp.text:
    sql = resp.text[resp.text.find("SELECT"):resp.text.find(". (UNKNOWN")]
    print(f"SQL: {sql}\n")

# Test 3: OPTIONAL MATCH
query3 = "MATCH (a:User) WHERE a.name = 'Alice' OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
print("Query 3: OPTIONAL MATCH")
resp = requests.post(url, json={"query": query3})
print(f"Status: {resp.status_code}")
if "SELECT" in resp.text:
    sql = resp.text[resp.text.find("SELECT"):resp.text.find(". (UNKNOWN")]
    print(f"SQL: {sql}\n")
