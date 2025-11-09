#!/usr/bin/env python3
"""Test script to verify the SQL generation bug fix."""

import requests
import clickhouse_connect

# Create database and tables
client = clickhouse_connect.get_client(host='localhost', username='test_user', password='test_pass')
client.command('CREATE DATABASE IF NOT EXISTS test_integration')
client.command('CREATE TABLE IF NOT EXISTS test_integration.users (user_id UInt32, name String, age UInt32) ENGINE = Memory')
client.command("INSERT INTO test_integration.users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

# Check data exists
result = client.query('SELECT * FROM test_integration.users')
print(f'✓ Data in ClickHouse: {result.result_rows}')

# Load schema into ClickGraph
schema_resp = requests.post(
    'http://localhost:8080/schemas/load', 
    json={
        'schema_name': 'test_integration',
        'config_path': 'C:/Users/GenZ/clickgraph/tests/integration/test_integration.yaml',
        'validate_schema': False
    }
)
print(f'\n✓ Schema load: {schema_resp.status_code} - {schema_resp.text}')

# Test SQL generation
sql_resp = requests.post(
    'http://localhost:8080/query',
    json={
        'query': 'MATCH (n:User) RETURN n.name',
        'schema_name': 'test_integration',
        'sql_only': True
    }
)
print(f'\n✓ Generated SQL:\n{sql_resp.json().get("generated_sql")}')

# Execute query
query_resp = requests.post(
    'http://localhost:8080/query',
    json={
        'query': 'MATCH (n:User) RETURN n.name',
        'schema_name': 'test_integration'
    }
)
print(f'\n✓ Query execution: {query_resp.status_code}')
print(f'Response: {query_resp.text}')
print(f'Response type: {type(query_resp.json())}')
print(f'Response content: {query_resp.json()}')
