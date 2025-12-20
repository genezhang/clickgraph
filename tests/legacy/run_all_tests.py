#!/usr/bin/env python3
"""
Run all Python test files systematically
"""

import subprocess
import sys
import os
from pathlib import Path

def run_test_file(test_file):
    """Run a single test file and return success status"""
    print(f"\n{'='*60}")
    print(f"Running: {test_file}")
    print('='*60)

    try:
        result = subprocess.run([sys.executable, test_file],
                              capture_output=True, text=True, timeout=30)

        print(f"Exit Code: {result.returncode}")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        print("TIMEOUT: Test took too long (>30s)")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    # Get all test_*.py files except test_runner.py (we already ran that)
    test_files = [f for f in Path('.').glob('test_*.py') if f.name != 'test_runner.py']

    print(f"Found {len(test_files)} test files to run:")
    for f in test_files:
        print(f"  - {f.name}")

    results = []
    for test_file in sorted(test_files):
        success = run_test_file(test_file)
        results.append((test_file.name, success))

    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print('='*80)

    passed = 0
    failed = 0

    for name, success in results:
        status = "[OK] PASS" if success else "[FAIL] FAIL"
        print(f"{status}: {name}")
        if success:
            passed += 1
        else:
            failed += 1

    print(f"\nTotal: {len(results)} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(".1f")

    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)