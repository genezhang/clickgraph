#!/usr/bin/env python3
"""
Load LDBC SNB data into ClickHouse.

Usage:
    python load_data.py --scale-factor sf0.1
    python load_data.py --scale-factor sf1 --host localhost --port 8123
"""

import argparse
import os
import subprocess
from pathlib import Path

# Table loading order matters due to views
# First load static data, then dynamic data
STATIC_TABLES = [
    # Static reference data
    ("Place", "static/place_0_0.csv"),
    ("Organisation", "static/organisation_0_0.csv"),
    ("Tag", "static/tag_0_0.csv"),
    ("TagClass", "static/tagclass_0_0.csv"),
    # Static relationships
    ("Place_isPartOf_Place", "static/place_isPartOf_place_0_0.csv"),
    ("Organisation_isLocatedIn_Place", "static/organisation_isLocatedIn_place_0_0.csv"),
    ("Tag_hasType_TagClass", "static/tag_hasType_tagclass_0_0.csv"),
    ("TagClass_isSubclassOf_TagClass", "static/tagclass_isSubclassOf_tagclass_0_0.csv"),
]

DYNAMIC_TABLES = [
    # Dynamic entities
    ("Person", "dynamic/person_0_0.csv"),
    ("Forum", "dynamic/forum_0_0.csv"),
    ("Post", "dynamic/post_0_0.csv"),
    ("Comment", "dynamic/comment_0_0.csv"),
    # Person relationships
    ("Person_isLocatedIn_Place", "dynamic/person_isLocatedIn_place_0_0.csv"),
    ("Person_hasInterest_Tag", "dynamic/person_hasInterest_tag_0_0.csv"),
    ("Person_studyAt_Organisation", "dynamic/person_studyAt_organisation_0_0.csv"),
    ("Person_workAt_Organisation", "dynamic/person_workAt_organisation_0_0.csv"),
    ("Person_knows_Person", "dynamic/person_knows_person_0_0.csv"),
    ("Person_likes_Post", "dynamic/person_likes_post_0_0.csv"),
    ("Person_likes_Comment", "dynamic/person_likes_comment_0_0.csv"),
    # Forum relationships
    ("Forum_hasModerator_Person", "dynamic/forum_hasModerator_person_0_0.csv"),
    ("Forum_hasMember_Person", "dynamic/forum_hasMember_person_0_0.csv"),
    ("Forum_hasTag_Tag", "dynamic/forum_hasTag_tag_0_0.csv"),
    ("Forum_containerOf_Post", "dynamic/forum_containerOf_post_0_0.csv"),
    # Post relationships
    ("Post_hasCreator_Person", "dynamic/post_hasCreator_person_0_0.csv"),
    ("Post_isLocatedIn_Place", "dynamic/post_isLocatedIn_place_0_0.csv"),
    ("Post_hasTag_Tag", "dynamic/post_hasTag_tag_0_0.csv"),
    # Comment relationships
    ("Comment_hasCreator_Person", "dynamic/comment_hasCreator_person_0_0.csv"),
    ("Comment_isLocatedIn_Place", "dynamic/comment_isLocatedIn_place_0_0.csv"),
    ("Comment_hasTag_Tag", "dynamic/comment_hasTag_tag_0_0.csv"),
    ("Comment_replyOf_Post", "dynamic/comment_replyOf_post_0_0.csv"),
    ("Comment_replyOf_Comment", "dynamic/comment_replyOf_comment_0_0.csv"),
]


def load_table(table_name: str, csv_path: Path, host: str, port: int, database: str):
    """Load a CSV file into a ClickHouse table."""
    if not csv_path.exists():
        print(f"  WARNING: File not found: {csv_path}")
        return False
    
    # Use clickhouse-client to load CSV
    cmd = [
        "clickhouse-client",
        f"--host={host}",
        f"--port={port}",
        f"--database={database}",
        f"--query=INSERT INTO {table_name} FORMAT CSVWithNames",
    ]
    
    try:
        with open(csv_path, 'rb') as f:
            result = subprocess.run(
                cmd,
                stdin=f,
                capture_output=True,
                text=True
            )
        
        if result.returncode != 0:
            print(f"  ERROR loading {table_name}: {result.stderr}")
            return False
        
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def get_row_count(table_name: str, host: str, port: int, database: str) -> int:
    """Get row count for a table."""
    cmd = [
        "clickhouse-client",
        f"--host={host}",
        f"--port={port}",
        f"--database={database}",
        f"--query=SELECT count() FROM {table_name}",
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return int(result.stdout.strip())
    except:
        pass
    return 0


def main():
    parser = argparse.ArgumentParser(description="Load LDBC SNB data into ClickHouse")
    parser.add_argument("--scale-factor", "-s", default="sf0.1",
                       help="Scale factor (sf0.1, sf1, sf10, etc.)")
    parser.add_argument("--host", default="localhost",
                       help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000,
                       help="ClickHouse native port (default: 9000)")
    parser.add_argument("--database", default="ldbc",
                       help="Database name (default: ldbc)")
    parser.add_argument("--data-dir", default=None,
                       help="Data directory (default: ./data/<scale-factor>)")
    parser.add_argument("--skip-static", action="store_true",
                       help="Skip loading static tables")
    parser.add_argument("--skip-dynamic", action="store_true",
                       help="Skip loading dynamic tables")
    
    args = parser.parse_args()
    
    # Determine data directory
    script_dir = Path(__file__).parent
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = script_dir.parent / "data" / args.scale_factor
    
    if not data_dir.exists():
        print(f"ERROR: Data directory not found: {data_dir}")
        print(f"Please run: ./scripts/download_data.sh {args.scale_factor}")
        return 1
    
    print("=" * 60)
    print("LDBC SNB Data Loader")
    print("=" * 60)
    print(f"Scale Factor: {args.scale_factor}")
    print(f"Data Directory: {data_dir}")
    print(f"ClickHouse: {args.host}:{args.port}")
    print(f"Database: {args.database}")
    print("=" * 60)
    
    tables_to_load = []
    if not args.skip_static:
        tables_to_load.extend(STATIC_TABLES)
    if not args.skip_dynamic:
        tables_to_load.extend(DYNAMIC_TABLES)
    
    success_count = 0
    error_count = 0
    
    for table_name, csv_file in tables_to_load:
        csv_path = data_dir / csv_file
        print(f"Loading {table_name}...", end=" ", flush=True)
        
        if load_table(table_name, csv_path, args.host, args.port, args.database):
            count = get_row_count(table_name, args.host, args.port, args.database)
            print(f"✓ ({count:,} rows)")
            success_count += 1
        else:
            error_count += 1
    
    print("=" * 60)
    print(f"Load complete: {success_count} tables loaded, {error_count} errors")
    print("=" * 60)
    
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    exit(main())
