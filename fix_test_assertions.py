#!/usr/bin/env python3
"""Fix test assertions to expect actual table names from schema instead of label_alias format."""

import re

file_path = r'c:\Users\GenZ\clickgraph\brahmand\src\query_planner\analyzer\graph_join_inference.rs'

# Read the file
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Define replacements (old pattern -> new value)
replacements = [
    (r'assert_eq!\(rel_join\.table_name, "WORKS_AT_w1"\);', 'assert_eq!(rel_join.table_name, "WORKS_AT");  // Now uses actual table name'),
    (r'assert_eq!\(rel_join\.table_name, "FOLLOWS_outgoing_f1"\);', 'assert_eq!(rel_join.table_name, "FOLLOWS");  // Now uses actual table name'),
    (r'assert_eq!\(rel_join\.table_name, "FOLLOWS_f2"\);', 'assert_eq!(rel_join.table_name, "FOLLOWS");  // Now uses actual table name'),
    (r'assert_eq!\(rel_join\.table_name, "FOLLOWS_f1"\);', 'assert_eq!(rel_join.table_name, "FOLLOWS");  // Now uses actual table name'),
    (r'assert_eq!\(join\.table_name, "WORKS_AT_w1"\);', 'assert_eq!(join.table_name, "WORKS_AT");  // Now uses actual table name'),
    (r'assert_eq!\(join\.table_name, "Person_p2"\);', 'assert_eq!(join.table_name, "Person");  // Now uses actual table name'),
    (r'assert_eq!\(join\.table_name, "Person_p1"\);', 'assert_eq!(join.table_name, "Person");  // Now uses actual table name'),
]

# Apply each replacement
for old_pattern, new_text in replacements:
    content = re.sub(old_pattern, new_text, content)

# Write back
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Updated test assertions in {file_path}")
print(f"Applied {len(replacements)} replacements")
