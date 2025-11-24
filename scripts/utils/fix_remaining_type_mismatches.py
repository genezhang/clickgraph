#!/usr/bin/env python3
"""
Fix remaining Column <-> PropertyValue mismatches
"""
import re

def fix_file(filepath, replacements):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

# Fix plan_builder.rs - wrap Column() in PropertyValue::Column()
# These are in logical_expr::PropertyAccess context
plan_builder_fixes = [
    # Lines 818, 857, 2299, 2309 - wrap Column in PropertyValue::Column
    (r'column: Column\(([^)]+)\),', 
     r'column: crate::graph_catalog::expression_parser::PropertyValue::Column(\1),'),
]

# Fix places where PropertyValue needs to be unwrapped to Column
# Lines 1819, 1855, 1872, 1876 - these are render_expr::PropertyAccess expecting Column
plan_builder_unwrap_fixes = [
    # For render_expr::PropertyAccess, extract column string
    (r'column: ([\w.]+\.column),\s*\n\s*}\)', 
     r'column: Column(\1.raw().to_string()),\n            })'),
]

print("Fixing plan_builder.rs...")
fix_file(r'src\render_plan\plan_builder.rs', plan_builder_fixes)
print("  ✓ Wrapped Column in PropertyValue::Column")

# Now handle the render_expr conversions - these need Column extracted from PropertyValue
# Let's read and manually identify these locations
print("\nNote: Lines 1819, 1855, 1872, 1876 need manual inspection")
print("These convert logical_expr::PropertyAccess -> render_expr::PropertyAccess")
print("which means: PropertyValue -> Column conversion needed")

print("\nDone!")
