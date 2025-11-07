#!/usr/bin/env python3
"""Run each test file separately to get clean pass rates"""

import subprocess
import re

test_files = [
    "test_basic_queries.py",
    "test_relationships.py", 
    "test_aggregations.py",
    "test_optional_match.py",
    "test_variable_length_paths.py",
    "test_shortest_paths.py",
    "test_path_variables.py",
    "test_case_expressions.py",
    "test_performance.py",
    "test_multi_database.py",
    "test_error_handling.py",
]

print("=" * 80)
print("CLEAN TEST RUN - INDIVIDUAL FILE ANALYSIS")
print("=" * 80)

results = {}
for test_file in test_files:
    cmd = f"python -m pytest {test_file} -v --tb=line"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd="tests/integration")
    output = result.stdout + result.stderr
    
    # Extract pass/fail counts
    match = re.search(r'(\d+) passed', output)
    passed = int(match.group(1)) if match else 0
    
    match = re.search(r'(\d+) failed', output)
    failed = int(match.group(1)) if match else 0
    
    total = passed + failed
    pass_rate = (passed / total * 100) if total > 0 else 0
    
    results[test_file] = {
        'passed': passed,
        'failed': failed,
        'total': total,
        'pass_rate': pass_rate
    }
    
    print(f"\n{test_file:<40} {passed:>3}/{total:<3} ({pass_rate:>5.1f}%)")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

total_passed = sum(r['passed'] for r in results.values())
total_tests = sum(r['total'] for r in results.values())
overall_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

print(f"\nTotal: {total_passed}/{total_tests} ({overall_rate:.1f}%)")

print("\n" + "=" * 80)
print("FILES BY PASS RATE (BEST TO WORST)")
print("=" * 80)

for file, data in sorted(results.items(), key=lambda x: -x[1]['pass_rate']):
    status = "✅" if data['pass_rate'] == 100 else "⚠️" if data['pass_rate'] >= 50 else "❌"
    print(f"{status} {file:<40} {data['passed']:>3}/{data['total']:<3} ({data['pass_rate']:>5.1f}%)")
