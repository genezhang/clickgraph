#!/usr/bin/env python3
"""
Revert .raw() calls on render_expr::Column back to .0
render_expr::Column is Column(String), should use .0 not .raw()
"""
import re

files_to_fix = [
    r'src\render_plan\cte_extraction.rs',
    r'src\render_plan\cte_generation.rs',
    r'src\render_plan\filter_pipeline.rs',
    r'src\render_plan\plan_builder.rs',
]

for file_path in files_to_fix:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace .column.raw() with .column.0 (for render_expr::PropertyAccess)
    # But be careful not to change logical_expr usage
    original_content = content
    content = re.sub(r'\.column\.raw\(\)', r'.column.0', content)
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✓ Fixed {file_path}")
    else:
        print(f"  No changes needed in {file_path}")

print("\nDone! Reverted .raw() to .0 for render_expr::Column")
