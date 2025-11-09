#!/usr/bin/env python3
"""
Debug script to see full SQL generated for relationship queries
Runs server with visible output
"""

import subprocess
import time
import os
import sys

SERVER_PORT = 8080

def main():
    print("[INFO] Starting server with visible logs to see full SQL...\n")
    
    # Set environment
    env = os.environ.copy()
    env.update({
        'GRAPH_CONFIG_PATH': 'social_network.yaml',
        'CLICKHOUSE_URL': 'http://localhost:8123',
        'CLICKHOUSE_USER': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass',
        'CLICKHOUSE_DATABASE': 'social',
        'RUST_LOG': 'debug',  # Enable debug logging
    })
    
    print("=" * 80)
    print("Server output below (press Ctrl+C to stop):")
    print("Look for 'ch_query' or 'Executing SQL' to see full generated SQL")
    print("=" * 80)
    print()
    
    # Start server with visible output
    try:
        process = subprocess.Popen(
            ['cargo', 'run', '--bin', 'brahmand', '--release', '--', '--http-port', str(SERVER_PORT)],
            env=env,
        )
        
        # Wait for Ctrl+C
        process.wait()
    except KeyboardInterrupt:
        print("\n\n[STOP] Stopping server...")
        process.terminate()
        process.wait()
        print("[OK] Server stopped")

if __name__ == '__main__':
    main()
