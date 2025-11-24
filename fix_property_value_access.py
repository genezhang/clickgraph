#!/usr/bin/env python3
"""
Fix .0 calls on PropertyValue to use .raw() instead
This is for logical_expr::PropertyAccess which has PropertyValue
"""

fixes = [
    # graph_traversal_planning.rs - logical_expr::PropertyAccess
    {
        'file': r'src\query_planner\analyzer\graph_traversal_planning.rs',
        'line': 244,
        'old': 'column.0 == graph_context.left.id_column',
        'new': 'column.raw() == graph_context.left.id_column'
    },
    {
        'file': r'src\query_planner\analyzer\graph_traversal_planning.rs',
        'line': 272,
        'old': 'column.0 == graph_context.right.id_column',
        'new': 'column.raw() == graph_context.right.id_column'
    },
    # cte_generation.rs - logical_expr::PropertyAccess
    {
        'file': r'src\render_plan\cte_generation.rs',
        'line': 349,
        'old': '&prop.column.0, &prop.table_alias',
        'new': 'prop.column.raw(), &prop.table_alias'
    },
    # plan_builder.rs - logical_expr::PropertyAccess
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 834,
        'old': 'prop.column.0.clone()',
        'new': 'prop.column.raw().to_string()'
    },
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 2263,
        'old': 'left_prop.column.0.clone()',
        'new': 'left_prop.column.raw().to_string()'
    },
    {
        'file': r'src\render_plan\plan_builder.rs',
        'line': 2292,
        'old': 'right_prop.column.0.clone()',
        'new': 'right_prop.column.raw().to_string()'
    },
    # render_expr.rs - logical_expr in debug print
    {
        'file': r'src\render_plan\render_expr.rs',
        'line': 162,
        'old': 'prop_access.column.0',
        'new': 'prop_access.column.raw()'
    },
]

for fix in fixes:
    file_path = fix['file']
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    idx = fix['line'] - 1
    if idx < len(lines):
        original_line = lines[idx]
        if fix['old'] in original_line:
            lines[idx] = original_line.replace(fix['old'], fix['new'])
            print(f"✓ Fixed line {fix['line']} in {file_path}")
        else:
            print(f"⚠ Line {fix['line']} doesn't match:")
            print(f"  Expected: {fix['old']}")
            print(f"  Found: {original_line.strip()}")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)

print("\nDone! Fixed PropertyValue .0 → .raw()")
