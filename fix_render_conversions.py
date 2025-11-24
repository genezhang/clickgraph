#!/usr/bin/env python3
"""
Fix logical→render layer conversions.
render_expr::PropertyAccess uses Column(String), not PropertyValue.
"""
import re

# Lines that create render_expr::PropertyAccess incorrectly wrapped in PropertyValue::Column
fixes = [
    # plan_builder.rs - render_expr::PropertyAccess creations
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 1819,
        'old': 'column: crate::graph_catalog::expression_parser::PropertyValue::Column(start_id_col),',
        'new': 'column: Column(start_id_col),'
    },
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 1855,
        'old': 'column: crate::graph_catalog::expression_parser::PropertyValue::Column(start_id_col),',
        'new': 'column: Column(end_id_col),'
    },
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 1872,
        'old': 'column: crate::graph_catalog::expression_parser::PropertyValue::Column(end_id_col),',
        'new': 'column: Column(start_id_col.clone()),'
    },
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 1876,
        'old': 'column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_cols.to_id),',
        'new': 'column: Column(end_id_col.clone()),'
    },
]

for fix in fixes:
    file_path = fix['file']
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Line numbers are 1-indexed
    idx = fix['line'] - 1
    if idx < len(lines):
        original_line = lines[idx]
        if fix['old'] in original_line:
            lines[idx] = original_line.replace(fix['old'], fix['new'])
            print(f"✓ Fixed line {fix['line']} in {file_path}")
        else:
            print(f"⚠ Line {fix['line']} doesn't match expected pattern:")
            print(f"  Expected: {fix['old']}")
            print(f"  Found: {original_line.strip()}")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)

print("\nDone! Check cargo check to see remaining errors.")
