#!/usr/bin/env python3
"""
Fix all test code for PropertyValue migration:
1. Column(...) → PropertyValue::Column(...)
2. HashMap<String, String> → HashMap<String, PropertyValue>
"""
import re

# Files with Column → PropertyValue::Column issues
files_to_fix = [
    r'src\query_planner\analyzer\filter_tagging.rs',
    r'src\query_planner\analyzer\group_by_building.rs',
    r'src\query_planner\logical_plan\mod.rs',
    r'src\render_plan\tests\denormalized_property_tests.rs',
    r'src\render_plan\tests\polymorphic_edge_tests.rs',
]

for file_path in files_to_fix:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # Pattern 1: column: Column("...") → column: PropertyValue::Column("...")
        content = re.sub(
            r'\bcolumn:\s*Column\(',
            r'column: crate::graph_catalog::expression_parser::PropertyValue::Column(',
            content
        )
        
        # Pattern 2: HashMap<String, String> for properties → HashMap<String, PropertyValue>
        # (Only in test contexts)
        if 'tests' in file_path or '#[cfg(test)]' in content:
            # Look for HashMap<String, String> in property contexts
            content = re.sub(
                r'HashMap<String,\s*String>',
                r'HashMap<String, crate::graph_catalog::expression_parser::PropertyValue>',
                content
            )
            # Fix hashmap! macro invocations: "prop" => "value" → "prop" => PropertyValue::Column("value")
            # This is trickier - need to handle multiline hashmap definitions
        
        if content != original:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Fixed {file_path}")
        else:
            print(f"  No changes in {file_path}")
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")

print("\nDone! Fixed PropertyValue in test code")
