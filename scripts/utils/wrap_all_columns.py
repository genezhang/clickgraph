#!/usr/bin/env python3
"""
Wrap Column(...) in PropertyValue::Column(...) for all files with PropertyAccess
"""
import re
import os

files_to_update = [
    r'src\query_planner\analyzer\projection_tagging.rs',
    r'src\query_planner\analyzer\filter_tagging.rs',
    r'src\query_planner\analyzer\plan_sanitization.rs',
    r'src\query_planner\analyzer\schema_inference.rs',
    r'src\query_planner\optimizer\filter_into_graph_rel.rs',
    r'src\render_plan\plan_builder.rs',
    r'src\render_plan\filter_pipeline.rs',
]

for filepath in files_to_update:
    if not os.path.exists(filepath):
        print(f'⚠️  Skipping {filepath} (not found)')
        continue
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace: column: Column(...) with column: PropertyValue::Column(...)
    pattern = r'(column:\s*)Column\(([^)]+)\)([,\s\}])'
    replacement = r'\1crate::graph_catalog::expression_parser::PropertyValue::Column(\2)\3'
    
    content = re.sub(pattern, replacement, content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f'✓ Updated {filepath}')

print('\nDone!')
