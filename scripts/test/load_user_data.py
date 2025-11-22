"""
Quick data loader for user's actual data.

Usage:
    1. User provides data as CSV or SQL INSERT statements
    2. Modify USER_DATA section below with their actual data
    3. Run: python scripts/test/load_user_data.py
    4. Test: python scripts/test/test_user_fof_bug.py
"""

import clickhouse_connect

# ============================================================================
# USER DATA - Replace with actual data from user
# ============================================================================

DATABASE = "test_integration"

# User's table structures (modify as needed)
USERS_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {database}.users (
        user_id UInt32,
        name String,
        age UInt32
    ) ENGINE = Memory
"""

FOLLOWS_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {database}.follows (
        follower_id UInt32,
        followed_id UInt32,
        since String
    ) ENGINE = Memory
"""

# User's actual data (modify with their data)
USERS_DATA = [
    # (user_id, name, age)
    # Example:
    # (1, 'Alice', 30),
    # (2, 'Bob', 25),
    # (3, 'Charlie', 35),
]

FOLLOWS_DATA = [
    # (follower_id, followed_id, since)
    # Example:
    # (1, 3, '2023-01-01'),  # Alice follows Charlie
    # (2, 3, '2023-02-01'),  # Bob follows Charlie
]

# ============================================================================


def clean_database(client, database):
    """Drop all tables in the database."""
    print(f"Cleaning database: {database}")
    
    tables = client.query(
        f"SELECT name FROM system.tables WHERE database = '{database}'"
    ).result_rows
    
    for (table_name,) in tables:
        client.command(f"DROP TABLE IF EXISTS {database}.{table_name}")
        print(f"  Dropped: {table_name}")


def create_tables(client, database):
    """Create user's table structure."""
    print(f"\nCreating tables in {database}...")
    
    client.command(USERS_TABLE_DDL.format(database=database))
    print("  ✓ users table created")
    
    client.command(FOLLOWS_TABLE_DDL.format(database=database))
    print("  ✓ follows table created")


def load_data(client, database):
    """Load user's actual data."""
    print(f"\nLoading data into {database}...")
    
    if not USERS_DATA:
        print("  ⚠️  WARNING: No user data provided! Add data to USERS_DATA in script.")
        return False
    
    if not FOLLOWS_DATA:
        print("  ⚠️  WARNING: No relationship data provided! Add data to FOLLOWS_DATA in script.")
        return False
    
    # Insert users
    for user in USERS_DATA:
        client.command(
            f"INSERT INTO {database}.users VALUES {user}"
        )
    print(f"  ✓ Inserted {len(USERS_DATA)} users")
    
    # Insert follows
    for follow in FOLLOWS_DATA:
        client.command(
            f"INSERT INTO {database}.follows VALUES {follow}"
        )
    print(f"  ✓ Inserted {len(FOLLOWS_DATA)} follow relationships")
    
    return True


def verify_data(client, database):
    """Verify the loaded data."""
    print(f"\nVerifying data in {database}...")
    
    # Count users
    user_count = client.command(f"SELECT count(*) FROM {database}.users")
    print(f"  Users: {user_count}")
    
    # Count follows
    follow_count = client.command(f"SELECT count(*) FROM {database}.follows")
    print(f"  Follows: {follow_count}")
    
    # Check for Alice and Bob
    alice = client.query(
        f"SELECT * FROM {database}.users WHERE name = 'Alice'"
    ).result_rows
    bob = client.query(
        f"SELECT * FROM {database}.users WHERE name = 'Bob'"
    ).result_rows
    
    if alice:
        print(f"  ✓ Found Alice: {alice[0]}")
    else:
        print(f"  ⚠️  Alice not found!")
    
    if bob:
        print(f"  ✓ Found Bob: {bob[0]}")
    else:
        print(f"  ⚠️  Bob not found!")
    
    # Check for duplicates
    duplicates = client.query(f"""
        SELECT follower_id, followed_id, count(*) as cnt
        FROM {database}.follows
        GROUP BY follower_id, followed_id
        HAVING cnt > 1
    """).result_rows
    
    if duplicates:
        print(f"\n  ⚠️⚠️⚠️  FOUND DUPLICATE RELATIONSHIPS:")
        for follower, followed, count in duplicates:
            print(f"    {follower} -> {followed}: {count} times")
        print("\n  This WILL cause duplicate results in queries!")
    else:
        print("  ✓ No duplicate relationships")


def main():
    print("="*70)
    print("User Data Loader")
    print("="*70)
    
    # Connect
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="test_user",
        password="test_pass"
    )
    
    print(f"\n✓ Connected to ClickHouse")
    
    # Clean
    clean_database(client, DATABASE)
    
    # Create tables
    create_tables(client, DATABASE)
    
    # Load data
    if not load_data(client, DATABASE):
        print("\n❌ Data not loaded. Please add user's data to the script.")
        return 1
    
    # Verify
    verify_data(client, DATABASE)
    
    client.close()
    
    print("\n" + "="*70)
    print("Data loaded successfully!")
    print("="*70)
    print("\nNext steps:")
    print("1. Start ClickGraph server with test_integration schema")
    print("2. Run: python scripts/test/test_user_fof_bug.py")
    print("3. Run: python scripts/test/investigate_user_duplicates.py")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
