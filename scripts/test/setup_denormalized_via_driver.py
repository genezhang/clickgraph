#!/usr/bin/env python3
"""Setup denormalized test data using clickhouse-driver.

Creates flights table with denormalized airport properties (OnTime pattern).
No separate airports table - Airport nodes are virtual, derived from flights.
"""

from clickhouse_driver import Client

# Connect to ClickHouse
client = Client(
    host='localhost',
    port=9000,  # Native protocol port
    user='test_user',
    password='test_pass',
    database='test_integration'
)

print("Setting up denormalized flights test data...")

# Create flights table with denormalized properties
try:
    client.execute("""
        CREATE TABLE IF NOT EXISTS test_integration.flights (
            flight_id UInt32,
            flight_number String,
            airline String,
            origin_code String,
            origin_city String,
            origin_state String,
            dest_code String,
            dest_city String,
            dest_state String,
            dep_time String,
            arr_time String,
            distance_miles UInt32
        ) ENGINE = Memory
    """)
    print("✓ Flights table created")
except Exception as e:
    print(f"✗ Flights table creation failed: {e}")

# Insert flight data
try:
    client.execute("""
        INSERT INTO test_integration.flights VALUES
            (1, 'AA100', 'American Airlines', 
             'LAX', 'Los Angeles', 'CA',
             'SFO', 'San Francisco', 'CA',
             '08:00', '09:30', 337),
            (2, 'UA200', 'United Airlines',
             'SFO', 'San Francisco', 'CA',
             'JFK', 'New York', 'NY',
             '10:00', '18:30', 2586),
            (3, 'DL300', 'Delta Airlines',
             'JFK', 'New York', 'NY',
             'LAX', 'Los Angeles', 'CA',
             '09:00', '12:30', 2475),
            (4, 'AA400', 'American Airlines',
             'ORD', 'Chicago', 'IL',
             'ATL', 'Atlanta', 'GA',
             '07:00', '10:00', 606),
            (5, 'DL500', 'Delta Airlines',
             'ATL', 'Atlanta', 'GA',
             'LAX', 'Los Angeles', 'CA',
             '11:00', '13:30', 1946),
            (6, 'UA600', 'United Airlines',
             'LAX', 'Los Angeles', 'CA',
             'ORD', 'Chicago', 'IL',
             '14:00', '20:00', 1745)
    """)
    print("✓ Flight data inserted")
except Exception as e:
    print(f"✗ Flight data insertion failed: {e}")

# Verify data
print("\nVerifying data...")

try:
    result = client.execute("SELECT COUNT(*) FROM test_integration.flights")
    print(f"✓ Flights: {result[0][0]} rows")
except Exception as e:
    print(f"✗ Flights query failed: {e}")

try:
    result = client.execute("SELECT COUNT(*) FROM test_integration.flights")
    print(f"✓ Flights: {result[0][0]} rows")
except Exception as e:
    print(f"✗ Flights query failed: {e}")

try:
    result = client.execute("SELECT origin_city, dest_city, flight_number FROM test_integration.flights LIMIT 3")
    print(f"\n✓ Sample flight data:")
    for row in result:
        print(f"  {row[0]} -> {row[1]} ({row[2]})")
except Exception as e:
    print(f"✗ Sample query failed: {e}")

print("\nSetup complete!")
