#!/usr/bin/env python3
"""Setup test database for cross-branch JOIN testing"""

import clickhouse_connect

# Create ClickHouse client
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

print("Setting up test_zeek database...")

# Drop and recreate database
client.command('DROP DATABASE IF EXISTS test_zeek')
client.command('CREATE DATABASE test_zeek')

# Create dns_log table
client.command("""
    CREATE TABLE test_zeek.dns_log (
        ts DateTime,
        orig_h String,
        query String
    ) ENGINE = Memory
""")

# Create conn_log table
client.command("""
    CREATE TABLE test_zeek.conn_log (
        ts DateTime,
        orig_h String,
        resp_h String
    ) ENGINE = Memory
""")

# Insert test data
client.command("""
    INSERT INTO test_zeek.dns_log (ts, orig_h, query) VALUES
    ('2024-01-01 10:00:00', '192.168.1.10', 'example.com'),
    ('2024-01-01 10:01:00', '192.168.1.11', 'google.com')
""")

client.command("""
    INSERT INTO test_zeek.conn_log (ts, orig_h, resp_h) VALUES
    ('2024-01-01 10:00:05', '192.168.1.10', '93.184.216.34'),
    ('2024-01-01 10:01:05', '192.168.1.11', '8.8.8.8')
""")

print("✅ Database setup complete")
print("\nData in dns_log:")
result = client.query("SELECT * FROM test_zeek.dns_log")
for row in result.result_rows:
    print(f"  {row}")

print("\nData in conn_log:")
result = client.query("SELECT * FROM test_zeek.conn_log")
for row in result.result_rows:
    print(f"  {row}")
