#!/usr/bin/env python3
"""Add where_predicate: None to all GraphRel initializations"""

import re
import glob

files = [
    'brahmand/src/query_planner/logical_plan/mod.rs',
    'brahmand/src/query_planner/optimizer/anchor_node_selection.rs',
    'brahmand/src/query_planner/analyzer/duplicate_scans_removing.rs',
    'brahmand/src/query_planner/analyzer/graph_join_inference.rs',
]

for filepath in files:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Pattern: path_variable followed by }, or ))
        # Add where_predicate before the closing
        pattern = r'(path_variable:\s*[^,\n]+),(\s*\})'
        replacement = r'\1,\2\n                where_predicate: None,'
        
        # Also handle graph_rel.where_predicate.clone() case
        pattern2 = r'(path_variable:\s*graph_rel\.path_variable\.clone\(\)),(\s*\})'
        replacement2 = r'\1,\n                where_predicate: graph_rel.where_predicate.clone(),\2'
        
        pattern3 = r'(path_variable:\s*prev_graph_rel\.path_variable\.clone\(\)),(\s*\})'
        replacement3 = r'\1,\n                where_predicate: prev_graph_rel.where_predicate.clone(),\2'
        
        modified = re.sub(pattern2, replacement2, content)
        modified = re.sub(pattern3, replacement3, modified)
        modified = re.sub(pattern, replacement, modified)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(modified)
        
        print(f"✓ Updated {filepath}")
    except Exception as e:
        print(f"✗ Error with {filepath}: {e}")
