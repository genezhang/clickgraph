#!/usr/bin/env python3
"""
Replace .column.0 with .column.raw() for PropertyValue migration
"""
import re
import os

files_to_update = [
    r'src\query_planner\analyzer\filter_tagging.rs',
    r'src\query_planner\analyzer\graph_traversal_planning.rs',
    r'src\query_planner\analyzer\schema_inference.rs',
    r'src\query_planner\analyzer\group_by_building.rs',
    r'src\render_plan\plan_builder.rs',
    r'src\render_plan\cte_extraction.rs',
    r'src\render_plan\filter_pipeline.rs',
    r'src\render_plan\cte_generation.rs',
    r'src\render_plan\plan_builder_helpers.rs',
]

for filepath in files_to_update:
    if not os.path.exists(filepath):
        print(f'⚠️  Skipping {filepath} (not found)')
        continue
    
    print(f'Processing {filepath}...')
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace .column.0 with .column.raw()
    # Be careful to only replace in PropertyAccess context
    content = re.sub(r'\.column\.0', r'.column.raw()', content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f'  ✓ Updated')

print('\nDone! Replaced .column.0 with .column.raw()')
