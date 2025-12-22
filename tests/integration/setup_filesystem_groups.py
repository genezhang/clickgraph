#!/usr/bin/env python3
"""
Setup filesystem and group_membership test data in test_integration database.

This script creates tables and sample data for integration tests.
Run this before running matrix tests that use filesystem and group_membership schemas.

Usage:
    python tests/integration/setup_filesystem_groups.py
"""

import clickhouse_connect
import os

# ClickHouse connection settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "test_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "test_pass")
TEST_DATABASE = "test_integration"


def setup_filesystem_tables(client):
    """Create filesystem test tables and data."""
    print("Setting up filesystem tables...")
    
    # Create fs_objects table
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.fs_objects (
            object_id UInt32,
            name String,
            object_type String,
            size_bytes UInt64,
            mime_type String,
            created_at String,
            modified_at String,
            owner_id UInt32
        ) ENGINE = MergeTree()
        ORDER BY object_id
    """)
    print("  ✓ Created fs_objects table")
    
    # Create fs_parent table (parent relationships)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.fs_parent (
            child_id UInt32,
            parent_id UInt32
        ) ENGINE = MergeTree()
        ORDER BY (child_id, parent_id)
    """)
    print("  ✓ Created fs_parent table")
    
    # Insert sample folders
    client.command(f"""
        INSERT INTO {TEST_DATABASE}.fs_objects VALUES
            (1, 'Root', 'folder', 0, '', '2024-01-01', '2024-01-01', 1),
            (2, 'Documents', 'folder', 0, '', '2024-01-02', '2024-01-02', 1),
            (3, 'Projects', 'folder', 0, '', '2024-01-03', '2024-01-03', 2),
            (4, 'Work', 'folder', 0, '', '2024-01-04', '2024-01-04', 1),
            (5, 'Personal', 'folder', 0, '', '2024-01-05', '2024-01-05', 2),
            (10, 'report.pdf', 'file', 1024000, 'application/pdf', '2024-02-01', '2024-02-01', 1),
            (11, 'budget.xlsx', 'file', 512000, 'application/vnd.ms-excel', '2024-02-02', '2024-02-02', 1),
            (12, 'notes.txt', 'file', 2048, 'text/plain', '2024-02-03', '2024-02-03', 2),
            (13, 'photo.jpg', 'file', 2048000, 'image/jpeg', '2024-02-04', '2024-02-04', 2),
            (14, 'code.py', 'file', 8192, 'text/x-python', '2024-02-05', '2024-02-05', 1)
    """)
    print("  ✓ Inserted 10 objects (5 folders, 5 files)")
    
    # Insert parent relationships
    client.command(f"""
        INSERT INTO {TEST_DATABASE}.fs_parent VALUES
            (2, 1),
            (3, 1),
            (4, 2),
            (5, 2),
            (10, 4),
            (11, 4),
            (12, 3),
            (13, 5),
            (14, 3)
    """)
    print("  ✓ Inserted 9 parent relationships")
    print("  Directory structure:")
    print("    Root (1)")
    print("    ├── Documents (2)")
    print("    │   ├── Work (4)")
    print("    │   │   ├── report.pdf (10)")
    print("    │   │   └── budget.xlsx (11)")
    print("    │   └── Personal (5)")
    print("    │       └── photo.jpg (13)")
    print("    └── Projects (3)")
    print("        ├── notes.txt (12)")
    print("        └── code.py (14)")


def setup_group_membership_tables(client):
    """Create group_membership test tables and data."""
    print("\nSetting up group_membership tables...")
    
    # Create users table (if not exists from other tests)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.users (
            id UInt32,
            name String,
            email String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✓ Created users table")
    
    # Create groups table
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.groups (
            id UInt32,
            name String,
            description String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("  ✓ Created groups table")
    
    # Create memberships table
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.memberships (
            user_id UInt32,
            group_id UInt32,
            joined_at String,
            role String
        ) ENGINE = MergeTree()
        ORDER BY (user_id, group_id)
    """)
    print("  ✓ Created memberships table")
    
    # Insert sample users (reuse some from other tests, add new ones)
    client.command(f"""
        INSERT INTO {TEST_DATABASE}.users VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com'),
            (4, 'Diana', 'diana@example.com'),
            (5, 'Eve', 'eve@example.com'),
            (6, 'Frank', 'frank@example.com'),
            (7, 'Grace', 'grace@example.com'),
            (8, 'Henry', 'henry@example.com')
    """)
    print("  ✓ Inserted 8 users")
    
    # Insert sample groups
    client.command(f"""
        INSERT INTO {TEST_DATABASE}.groups VALUES
            (100, 'Engineering', 'Engineering team'),
            (101, 'Design', 'Design team'),
            (102, 'Product', 'Product management'),
            (103, 'Admin', 'Administrators'),
            (104, 'External', 'External contractors')
    """)
    print("  ✓ Inserted 5 groups")
    
    # Insert memberships
    client.command(f"""
        INSERT INTO {TEST_DATABASE}.memberships VALUES
            (1, 100, '2023-01-01', 'admin'),
            (1, 103, '2023-01-01', 'admin'),
            (2, 100, '2023-02-01', 'member'),
            (3, 100, '2023-02-15', 'member'),
            (4, 101, '2023-03-01', 'admin'),
            (5, 101, '2023-03-15', 'member'),
            (6, 102, '2023-04-01', 'member'),
            (7, 104, '2023-05-01', 'viewer'),
            (8, 100, '2023-06-01', 'member')
    """)
    print("  ✓ Inserted 9 memberships")
    print("  Group structure:")
    print("    Engineering (100): Alice (admin), Bob, Charlie, Henry")
    print("    Design (101): Diana (admin), Eve")
    print("    Product (102): Frank")
    print("    Admin (103): Alice (admin)")
    print("    External (104): Grace (viewer)")


def main():
    print(f"Setting up filesystem and group_membership test data...")
    print(f"Database: {TEST_DATABASE}")
    print(f"Host: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print()
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        print("✓ Connected to ClickHouse")
    except Exception as e:
        print(f"✗ Failed to connect to ClickHouse: {e}")
        return 1
    
    # Create database if not exists
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {TEST_DATABASE}")
        print(f"✓ Database {TEST_DATABASE} exists")
        print()
    except Exception as e:
        print(f"✗ Failed to create database: {e}")
        return 1
    
    # Setup tables and data
    try:
        setup_filesystem_tables(client)
        setup_group_membership_tables(client)
    except Exception as e:
        print(f"\n✗ Failed to setup tables: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Verify
    print("\nVerification:")
    try:
        fs_count = client.command(f"SELECT count() FROM {TEST_DATABASE}.fs_objects")
        print(f"  ✓ fs_objects: {fs_count} rows")
        
        parent_count = client.command(f"SELECT count() FROM {TEST_DATABASE}.fs_parent")
        print(f"  ✓ fs_parent: {parent_count} rows")
        
        users_count = client.command(f"SELECT count() FROM {TEST_DATABASE}.users")
        print(f"  ✓ users: {users_count} rows")
        
        groups_count = client.command(f"SELECT count() FROM {TEST_DATABASE}.groups")
        print(f"  ✓ groups: {groups_count} rows")
        
        memberships_count = client.command(f"SELECT count() FROM {TEST_DATABASE}.memberships")
        print(f"  ✓ memberships: {memberships_count} rows")
    except Exception as e:
        print(f"  ✗ Verification failed: {e}")
        return 1
    
    print("\n✅ Setup complete!")
    print("\nYou can now run tests with:")
    print("  pytest tests/integration/matrix/test_comprehensive.py -k 'filesystem or group_membership'")
    
    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
