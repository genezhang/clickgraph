#!/usr/bin/env python3
"""
Script to automatically update test_aggregations.py to use helper functions.
Replaces manual assertion patterns with get_single_value() calls.
"""

import re

def fix_aggregation_tests():
    filepath = 'tests/integration/test_aggregations.py'
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern: Manual dict/list checking for single values
    # Matches:
    #   results = response["results"]
    #   if isinstance(results[0], dict):
    #       assert results[0]["column"] == value
    #   else:
    #       col_idx = response["columns"].index("column")
    #       assert results[0][col_idx] == value
    
    pattern = (
        r'results = response\["results"\]\s+'
        r'if isinstance\(results\[0\], dict\):\s+'
        r'assert results\[0\]\["(\w+)"\] == (\d+)\s+'
        r'else:\s+'
        r'col_idx = response\["columns"\]\.index\("(\w+)"\)\s+'
        r'assert results\[0\]\[col_idx\] == \2'
    )
    
    replacement = r'assert get_single_value(response, "\1", convert_to_int=True) == \2'
    
    # Apply the replacement
    new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    # Count how many replacements were made
    count = len(re.findall(pattern, content, flags=re.MULTILINE))
    
    # Write back
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ Fixed {count} manual assertion patterns in test_aggregations.py")
    return count

if __name__ == '__main__':
    count = fix_aggregation_tests()
    if count > 0:
        print(f"\n✓ Updated test_aggregations.py")
        print(f"✓ {count} assertion patterns converted to use get_single_value()")
        print(f"\nRun: python -m pytest tests/integration/test_aggregations.py -v")
    else:
        print("\nNo patterns found to fix (already fixed or different pattern)")
