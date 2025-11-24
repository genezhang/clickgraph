#!/usr/bin/env python3
"""
Fix test HashMap inserts to use PropertyValue::Column
Pattern: .insert("key", "value".to_string()) 
→ .insert("key", PropertyValue::Column("value".to_string()))
"""
import re

files = [
    r'src\render_plan\tests\denormalized_property_tests.rs',
    r'src\render_plan\tests\polymorphic_edge_tests.rs',
]

for file_path in files:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Pattern: props.insert("key", "value".to_string());
    # Replace with: props.insert("key", PropertyValue::Column("value".to_string()));
    content = re.sub(
        r'(props\.insert\([^,]+,\s*)("[\w_]+".to_string\(\))\);',
        r'\1crate::graph_catalog::expression_parser::PropertyValue::Column(\2));',
        content
    )
    
    # Also add import at the top if not present
    if 'PropertyValue' not in content:
        # Find where imports are
        if 'use crate::' in content:
            content = content.replace(
                'use crate::',
                'use crate::graph_catalog::expression_parser::PropertyValue;\nuse crate::',
                1
            )
    
    if content != original:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✓ Fixed {file_path}")
    else:
        print(f"  No changes in {file_path}")

print("\nDone!")
