#!/usr/bin/env python3
"""
Remove is_expression: false from render_expr::PropertyAccess
"""
import re

filepath = r'src\render_plan\plan_builder.rs'

with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove is_expression from super::render_expr::PropertyAccess literals
pattern = r'(super::render_expr::PropertyAccess\s*\{[^}]*),\s*is_expression:\s*false,?\s*'
content = re.sub(pattern, r'\1', content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print(f'✓ Fixed {filepath}')
