#!/usr/bin/env python3
"""
Integration Test Suite Manager

Manages self-contained integration test suites with automatic:
- Database creation
- Schema registration
- Table setup
- Data population
- Cleanup/teardown

Usage:
    # Set up all suites
    python suite_manager.py setup-all
    
    # Set up specific suite
    python suite_manager.py setup social_integration
    
    # Tear down specific suite
    python suite_manager.py teardown social_integration
    
    # Tear down all suites
    python suite_manager.py teardown-all
    
    # List available suites
    python suite_manager.py list
"""

import sys
import os
import json
import argparse
from pathlib import Path
import clickhouse_connect
import requests

# Configuration from environment
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Suite directory
SUITES_DIR = Path(__file__).parent


def get_available_suites():
    """Get list of available test suites."""
    suites = []
    for suite_dir in SUITES_DIR.iterdir():
        if suite_dir.is_dir() and not suite_dir.name.startswith('__'):
            schema_file = suite_dir / "schema.yaml"
            setup_file = suite_dir / "setup.sql"
            if schema_file.exists() and setup_file.exists():
                suites.append(suite_dir.name)
    return sorted(suites)


def get_database_from_setup(setup_file):
    """Extract database name from setup.sql file."""
    with open(setup_file) as f:
        for line in f:
            if 'CREATE TABLE' in line and '.' in line:
                # Extract database from "database.table" pattern
                parts = line.split('.')
                if len(parts) >= 2:
                    db_part = parts[0].split()[-1]
                    return db_part
    return None


def setup_suite(suite_name, clickhouse_client):
    """Set up a test suite."""
    suite_dir = SUITES_DIR / suite_name
    
    if not suite_dir.exists():
        print(f"❌ Suite '{suite_name}' not found")
        return False
    
    schema_file = suite_dir / "schema.yaml"
    setup_file = suite_dir / "setup.sql"
    
    if not schema_file.exists() or not setup_file.exists():
        print(f"❌ Suite '{suite_name}' missing schema.yaml or setup.sql")
        return False
    
    print(f"\n🚀 Setting up suite: {suite_name}")
    
    # Step 1: Create database
    database = get_database_from_setup(setup_file)
    if database:
        try:
            clickhouse_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
            print(f"  ✓ Database '{database}' created")
        except Exception as e:
            print(f"  ⚠️  Database creation warning: {e}")
    
    # Step 2: Run setup.sql
    try:
        with open(setup_file) as f:
            sql_content = f.read()
        
        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        for stmt in statements:
            if stmt:
                clickhouse_client.command(stmt)
        
        print(f"  ✓ Tables created and data inserted")
    except Exception as e:
        print(f"  ❌ Setup SQL failed: {e}")
        return False
    
    # Step 3: Register schema with ClickGraph
    try:
        with open(schema_file) as f:
            schema_content = f.read()
        
        with open(schema_file) as f:
            import yaml
            schema_data = yaml.safe_load(f)
            schema_name = schema_data.get('name', suite_name)
        
        response = requests.post(
            f"{CLICKGRAPH_URL}/schemas/load",
            json={
                "schema_name": schema_name,
                "config_content": schema_content,
                "validate_schema": True
            },
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            print(f"  ✓ Schema '{schema_name}' registered")
        else:
            print(f"  ⚠️  Schema registration warning: {response.text}")
    except Exception as e:
        print(f"  ⚠️  Schema registration warning: {e}")
    
    print(f"✅ Suite '{suite_name}' ready!\n")
    return True


def teardown_suite(suite_name, clickhouse_client):
    """Tear down a test suite."""
    suite_dir = SUITES_DIR / suite_name
    
    if not suite_dir.exists():
        print(f"❌ Suite '{suite_name}' not found")
        return False
    
    teardown_file = suite_dir / "teardown.sql"
    
    if not teardown_file.exists():
        print(f"⚠️  No teardown.sql for suite '{suite_name}'")
        return True
    
    print(f"\n🧹 Tearing down suite: {suite_name}")
    
    try:
        with open(teardown_file) as f:
            sql_content = f.read()
        
        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        for stmt in statements:
            if stmt:
                try:
                    clickhouse_client.command(stmt)
                except Exception as e:
                    print(f"  ⚠️  Teardown warning: {e}")
        
        print(f"  ✓ Tables dropped")
        print(f"✅ Suite '{suite_name}' cleaned up!\n")
        return True
    except Exception as e:
        print(f"  ❌ Teardown failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Manage integration test suites")
    parser.add_argument("command", choices=["setup", "teardown", "setup-all", "teardown-all", "list"],
                       help="Command to execute")
    parser.add_argument("suite", nargs="?", help="Suite name (for setup/teardown commands)")
    
    args = parser.parse_args()
    
    # List suites
    if args.command == "list":
        suites = get_available_suites()
        print(f"\n📦 Available test suites ({len(suites)}):\n")
        for suite in suites:
            suite_dir = SUITES_DIR / suite
            schema_file = suite_dir / "schema.yaml"
            if schema_file.exists():
                import yaml
                with open(schema_file) as f:
                    schema_data = yaml.safe_load(f)
                    desc = schema_data.get('description', 'No description')
                print(f"  • {suite:<20} - {desc}")
        print()
        return 0
    
    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
    except Exception as e:
        print(f"❌ Failed to connect to ClickHouse: {e}")
        return 1
    
    # Execute command
    try:
        if args.command == "setup":
            if not args.suite:
                print("❌ Suite name required for 'setup' command")
                return 1
            return 0 if setup_suite(args.suite, client) else 1
        
        elif args.command == "teardown":
            if not args.suite:
                print("❌ Suite name required for 'teardown' command")
                return 1
            return 0 if teardown_suite(args.suite, client) else 1
        
        elif args.command == "setup-all":
            suites = get_available_suites()
            print(f"\n🚀 Setting up {len(suites)} test suites...\n")
            success_count = 0
            for suite in suites:
                if setup_suite(suite, client):
                    success_count += 1
            print(f"\n✅ Setup complete: {success_count}/{len(suites)} suites ready\n")
            return 0 if success_count == len(suites) else 1
        
        elif args.command == "teardown-all":
            suites = get_available_suites()
            print(f"\n🧹 Tearing down {len(suites)} test suites...\n")
            for suite in suites:
                teardown_suite(suite, client)
            print(f"\n✅ Teardown complete\n")
            return 0
    
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
