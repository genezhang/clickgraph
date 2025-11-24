#!/usr/bin/env python3
"""
Fix test code that accesses PropertyValue.0 - should use .raw()
"""
import re

file_path = r'src\query_planner\analyzer\graph_join_inference.rs'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace .column.0 with .column.raw() in test assertions
# Pattern: prop.column.0 or property.column.0 or left.column.0 etc.
content = re.sub(r'\.column\.0\b', r'.column.raw()', content)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"✓ Fixed PropertyValue access in {file_path}")
print("  Changed .column.0 → .column.raw() for test assertions")
