#!/usr/bin/env python3
"""
Wrap Column(...) in PropertyValue::Column(...) for PropertyAccess struct literals
"""
import re

filepath = r'src\query_planner\analyzer\graph_join_inference.rs'

with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace: column: Column(...) with column: crate::graph_catalog::expression_parser::PropertyValue::Column(...)
# in PropertyAccess struct literals
pattern = r'(PropertyAccess\s*\{[^}]*?column:\s*)Column\(([^)]+)\)'
replacement = r'\1crate::graph_catalog::expression_parser::PropertyValue::Column(\2)'

content = re.sub(pattern, replacement, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print(f'✓ Updated {filepath}')
