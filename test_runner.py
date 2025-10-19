#!/usr/bin/env python3
"""
ClickGraph Integration Test Suite
Manages server lifecycle and runs comprehensive tests
"""

import requests
import json
import subprocess
import time
import sys
import os
from pathlib import Path

SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"
PID_FILE = Path("server.pid")

class Colors:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'

def log(msg, color=Colors.RESET):
    print(f"{color}{msg}{Colors.RESET}")

def start_server():
    """Start the ClickGraph server in background"""
    log("🚀 Starting ClickGraph server...", Colors.CYAN)
    
    # Check if already running
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 0)  # Check if process exists
            log(f"⚠️  Server already running (PID: {pid})", Colors.YELLOW)
            return True
        except OSError:
            PID_FILE.unlink()
    
    # Set environment
    env = os.environ.copy()
    env.update({
        'GRAPH_CONFIG_PATH': 'social_network.yaml',
        'CLICKHOUSE_URL': 'http://localhost:8123',
        'CLICKHOUSE_USER': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass',
        'CLICKHOUSE_DATABASE': 'social',
    })
    
    # Start server
    process = subprocess.Popen(
        ['cargo', 'run', '--bin', 'brahmand', '--release', '--', '--http-port', str(SERVER_PORT)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    
    PID_FILE.write_text(str(process.pid))
    
    # Wait for server to start
    log("⏳ Waiting for server to start...", Colors.YELLOW)
    for attempt in range(30):
        time.sleep(1)
        try:
            response = requests.get(f"{SERVER_URL}/health", timeout=1)
            if response.status_code == 200:
                log(f"✅ Server started successfully (PID: {process.pid})", Colors.GREEN)
                return True
        except requests.exceptions.RequestException:
            print(".", end="", flush=True)
    
    log("\n❌ Server failed to start within 30 seconds", Colors.RED)
    stop_server()
    return False

def stop_server():
    """Stop the ClickGraph server"""
    log("🛑 Stopping ClickGraph server...", Colors.CYAN)
    
    if PID_FILE.exists():
        pid = int(PID_FILE.read_text().strip())
        try:
            os.kill(pid, 15)  # SIGTERM
            time.sleep(2)
            os.kill(pid, 9)   # SIGKILL if still running
            log(f"✅ Server stopped (PID: {pid})", Colors.GREEN)
        except OSError:
            log("⚠️  No running process found", Colors.YELLOW)
        PID_FILE.unlink(missing_ok=True)
    else:
        log("⚠️  No PID file found", Colors.YELLOW)

def run_query(query, description=""):
    """Execute a Cypher query and return results"""
    if description:
        log(f"🧪 {description}", Colors.CYAN)
    
    try:
        response = requests.post(
            f"{SERVER_URL}/query",
            json={'query': query},
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            log("✅ Query succeeded!", Colors.GREEN)
            result = response.json()
            print(json.dumps(result, indent=2))
            return True, result
        else:
            log(f"❌ Query failed with status {response.status_code}", Colors.RED)
            print(response.text)
            return False, None
            
    except Exception as e:
        log(f"❌ Query failed: {e}", Colors.RED)
        return False, None

def run_tests():
    """Run comprehensive test suite"""
    log("🧪 Running ClickGraph Test Suite", Colors.CYAN)
    log("=" * 60, Colors.CYAN)
    
    tests_passed = 0
    tests_failed = 0
    
    test_cases = [
        # Node ViewScan tests
        ("MATCH (u:User) RETURN u.name LIMIT 3", 
         "Test 1: Basic node query with ViewScan (label→table via schema)"),
        
        ("MATCH (u:User) WHERE u.age > 25 RETURN u.name, u.age ORDER BY u.age",
         "Test 2: Node query with WHERE clause and ORDER BY"),
        
        ("MATCH (u:User) RETURN count(u) as total_users",
         "Test 3: Aggregation query on nodes"),
        
        # Relationship ViewScan tests
        ("MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) RETURN u.name, f.name LIMIT 5",
         "Test 4: Relationship traversal with schema lookup (type→table via schema)"),
        
        # Complex query combining both
        ("MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) WHERE u.age > 25 RETURN u.name, u.age, f.name ORDER BY u.age LIMIT 3",
         "Test 5: Combined node and relationship ViewScan with filters"),
    ]
    
    for query, description in test_cases:
        log(f"\n{description}", Colors.YELLOW)
        log(f"Query: {query}", Colors.RESET)
        success, _ = run_query(query, "")
        
        if success:
            tests_passed += 1
        else:
            tests_failed += 1
        
        time.sleep(0.5)  # Brief pause between tests
    
    # Summary
    log("\n" + "=" * 60, Colors.CYAN)
    log("Test Summary:", Colors.CYAN)
    log(f"  Passed: {tests_passed}", Colors.GREEN if tests_passed > 0 else Colors.RESET)
    log(f"  Failed: {tests_failed}", Colors.RED if tests_failed > 0 else Colors.RESET)
    
    return tests_failed == 0

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='ClickGraph Test Runner')
    parser.add_argument('--start', action='store_true', help='Start server')
    parser.add_argument('--stop', action='store_true', help='Stop server')
    parser.add_argument('--test', action='store_true', help='Run test suite')
    parser.add_argument('--query', type=str, help='Run single query')
    parser.add_argument('--clean', action='store_true', help='Clean up everything')
    
    args = parser.parse_args()
    
    if args.clean:
        stop_server()
        log("✅ Environment cleaned", Colors.GREEN)
    elif args.start:
        if start_server():
            sys.exit(0)
        else:
            sys.exit(1)
    elif args.stop:
        stop_server()
    elif args.query:
        success, _ = run_query(args.query)
        sys.exit(0 if success else 1)
    elif args.test:
        if run_tests():
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        log("ClickGraph Test Runner", Colors.CYAN)
        log("Usage:", Colors.RESET)
        log("  python test_runner.py --start              Start server")
        log("  python test_runner.py --stop               Stop server")
        log("  python test_runner.py --test               Run test suite")
        log("  python test_runner.py --query 'MATCH ...'  Run single query")
        log("  python test_runner.py --clean              Clean up")
        log("")
        log("Example workflow:", Colors.YELLOW)
        log("  python test_runner.py --start")
        log("  python test_runner.py --test")
        log("  python test_runner.py --stop")

if __name__ == '__main__':
    main()
