#!/usr/bin/env python3
"""
Systematically replace all graph_context.* references in graph_join_inference.rs
with the appropriate parameter variables.
"""

import re

def fix_file():
    filepath = r"c:\Users\GenZ\clickgraph\src\query_planner\analyzer\graph_join_inference.rs"
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace all graph_context references with parameter variables
    replacements = [
        (r'graph_context\.left\.schema', 'left_node_schema'),
        (r'graph_context\.right\.schema', 'right_node_schema'),
        (r'graph_context\.rel\.schema', 'rel_schema'),
        (r'graph_context\.left\.label', 'left_label'),
        (r'graph_context\.right\.label', 'right_label'),
        (r'graph_context\.rel\.label', 'rel_label'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Fixed all graph_context references")
    print("Replaced patterns:")
    for pattern, replacement in replacements:
        count = len(re.findall(pattern, content))
        print(f"  {pattern} → {replacement} ({count} occurrences)")

if __name__ == "__main__":
    fix_file()
