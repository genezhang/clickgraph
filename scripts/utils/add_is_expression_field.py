#!/usr/bin/env python3
"""
Add is_expression: false field to all PropertyAccess struct literals
"""
import re
import sys

def add_is_expression(content):
    # Pattern: PropertyAccess { ... column: ..., } or PropertyAccess { ... column: ... }
    # Add is_expression: false before the closing }
    pattern = r'(PropertyAccess\s*\{[^}]*column:\s*[^,}]+)([\s,]*)\}'
    
    def replacer(match):
        before = match.group(1)
        whitespace = match.group(2) if match.group(2) else ''
        # Check if is_expression is already there
        if 'is_expression' in before:
            return match.group(0)  # Already has the field
        # Add the field
        if whitespace.endswith(','):
            return f'{before}{whitespace}\n            is_expression: false,\n        }}'
        else:
            return f'{before},\n            is_expression: false,\n        }}'
    
    return re.sub(pattern, replacer, content, flags=re.DOTALL)

if __name__ == '__main__':
    files = [
        r'src\query_planner\analyzer\graph_join_inference.rs',
        r'src\render_plan\plan_builder.rs',
    ]
    
    for filepath in files:
        print(f'Processing {filepath}...')
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            new_content = add_is_expression(content)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            print(f'  ✓ Updated {filepath}')
        except Exception as e:
            print(f'  ✗ Error processing {filepath}: {e}')
    
    print('Done!')
