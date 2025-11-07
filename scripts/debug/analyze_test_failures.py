#!/usr/bin/env python3
"""Analyze integration test failures"""

import re
from collections import Counter, defaultdict

# Read test results
with open('integration_test_results.txt', 'r', encoding='utf-16') as f:
    content = f.read()

# Extract all failures
failures = re.findall(r'FAILED ([^\s]+)', content)

print(f"=" * 70)
print(f"INTEGRATION TEST FAILURE ANALYSIS")
print(f"=" * 70)
print(f"\nTotal failures: {len(failures)}")
print(f"Total passed: 118")
print(f"Success rate: {118/(118+len(failures))*100:.1f}%")

# Group by test file
categories = Counter([f.split('.py::')[0] + '.py' for f in failures])
print(f"\n{'='*70}")
print("FAILURES BY TEST FILE:")
print(f"{'='*70}")
for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
    print(f"  {cat:<40} {count:>3} failures")

# Categorize by error type
error_types = defaultdict(list)

# Find common error patterns in output
clickhouse_errors = re.findall(r'Clickhouse Error:.*?"exception": "(.*?)"', content)
brahmand_errors = re.findall(r'Brahmand Error: (.*?)(?:\n|$)', content)
assertion_errors = re.findall(r'AssertionError: (.*?)(?:\n|E   )', content)

print(f"\n{'='*70}")
print("ERROR TYPES:")
print(f"{'='*70}")

# Categorize ClickHouse errors
ch_error_categories = defaultdict(int)
for err in clickhouse_errors:
    if 'Unknown expression' in err or 'UNKNOWN_IDENTIFIER' in err:
        ch_error_categories['Missing column/identifier (CTE not generated)'] += 1
    elif 'Multiple table expressions' in err:
        ch_error_categories['Alias collision'] += 1
    else:
        ch_error_categories['Other ClickHouse error'] += 1

print("\nClickHouse Errors:")
for cat, count in sorted(ch_error_categories.items(), key=lambda x: -x[1]):
    print(f"  {cat:<50} {count:>3}")

print(f"\nParser Errors (Brahmand): {len(brahmand_errors)}")
print(f"Assertion Errors (test expectations): {len(set(assertion_errors))}")

# Find specific patterns
print(f"\n{'='*70}")
print("MAJOR ERROR PATTERNS:")
print(f"{'='*70}")

# Pattern 1: hop_count not found
hop_count_missing = len(re.findall(r't\.hop_count.*UNKNOWN_IDENTIFIER', content))
print(f"1. Variable-length path missing CTE (t.hop_count): {hop_count_missing}")

# Pattern 2: Multi-hop queries missing first node
multi_hop_missing = len(re.findall(r'Unknown expression identifier `a\.', content))
print(f"2. Multi-hop traversal missing node 'a': {multi_hop_missing}")

# Pattern 3: COUNT(b) issues  
count_b_issues = len(re.findall(r'COUNT\(b\.user_id\).*UNKNOWN_IDENTIFIER', content))
print(f"3. Aggregation COUNT(b) column missing: {count_b_issues}")

# Pattern 4: Parser failures
parser_unbounded = len(re.findall(r'Unable to parse:.*\.\.[^\d]', content))
print(f"4. Parser fails on unbounded ranges (*0.., *1.., *2..): {parser_unbounded}")

# Pattern 5: Wrong row counts
row_count_errors = len(re.findall(r'AssertionError: Expected \d+ rows, got \d+', content))
print(f"5. Row count mismatches (query logic): {row_count_errors}")

print(f"\n{'='*70}")
print("TOP 3 ROOT CAUSES:")
print(f"{'='*70}")
print("1. ⚠️  Variable-length paths not generating CTEs (hop_count missing)")
print("2. ⚠️  Multi-hop queries losing first node in JOIN chain")
print("3. ⚠️  Parser doesn't support unbounded ranges (*1.., *2.., *0..)")

print(f"\n{'='*70}")
print("RECOMMENDED FIX ORDER:")
print(f"{'='*70}")
print("1. Fix variable-length CTE generation (fixes ~30 tests)")
print("2. Fix multi-hop JOIN chain (fixes ~20 tests)")
print("3. Add unbounded range parser support (fixes ~10 tests)")
print("4. Fix aggregation column resolution (fixes ~15 tests)")
print("5. Address test expectation mismatches (review remaining)")
