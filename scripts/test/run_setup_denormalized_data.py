#!/usr/bin/env python3
"""Setup denormalized test data for integration tests."""

import clickhouse_connect

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='test_user',
    password='test_pass',
    database='test_integration'
)

print("Setting up denormalized flights test data...")

# Read SQL file
with open('scripts/test/setup_denormalized_test_data.sql', 'r') as f:
    sql_content = f.read()

# Split by semicolons and execute each statement
statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]

for i, statement in enumerate(statements, 1):
    try:
        client.command(statement)
        print(f"✓ Statement {i}/{len(statements)} executed")
    except Exception as e:
        print(f"✗ Statement {i} failed: {e}")
        print(f"Statement: {statement[:100]}...")

# Verify data
print("\nVerifying data...")
try:
    result = client.query("SELECT COUNT(*) as count FROM test_integration.airports")
    print(f"✓ Airports: {result.result_rows[0][0]} rows")
except Exception as e:
    print(f"✗ Airports query failed: {e}")

try:
    result = client.query("SELECT COUNT(*) as count FROM test_integration.flights")
    print(f"✓ Flights: {result.result_rows[0][0]} rows")
except Exception as e:
    print(f"✗ Flights query failed: {e}")

print("\nSetup complete!")
