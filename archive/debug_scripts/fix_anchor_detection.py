#!/usr/bin/env python3
"""Fix anchor detection logic in graph_join_inference.rs"""

file_path = r"brahmand\src\query_planner\analyzer\graph_join_inference.rs"

# Read the file
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the line with "CRITICAL FIX: Check if LEFT is ACTUALLY joined yet" (around line 914)
for i, line in enumerate(lines):
    if "CRITICAL FIX: Check if LEFT is ACTUALLY joined yet" in line:
        print(f"Found CRITICAL FIX comment at line {i+1}")
        
        # Replace lines 915-930 (indices 914-929)
        # Keep lines up to and including line 914 (the comment)
        new_code = """                // If LEFT is not joined, we must connect the relationship to RIGHT (the anchor) instead!
                let left_is_joined = joined_entities.contains(left_alias);
                let right_is_joined = joined_entities.contains(right_alias);
                
                // Check if LEFT or RIGHT is the anchor (first relationship AND required)
                let is_first_relationship = collected_graph_joins.is_empty();
                let left_is_anchor = is_first_relationship && !left_is_optional;
                let right_is_anchor = is_first_relationship && !right_is_optional;
                
                let rel_conn_with_left_node = rel_from_col.clone();
                let right_conn_with_rel = rel_to_col.clone();
                
                // Choose which node to connect the relationship to (priority order)
                let (rel_connect_column, node_alias, node_id_column) = 
                    if left_is_joined {
                        eprintln!("    │ LEFT joined - connecting to LEFT");
                        (rel_conn_with_left_node.clone(), left_alias.to_string(), left_node_id_column.clone())
                    } else if right_is_joined {
                        eprintln!("    │ RIGHT joined - connecting to RIGHT");
                        (right_conn_with_rel.clone(), right_alias.to_string(), right_node_id_column.clone())
                    } else if left_is_anchor {
                        eprintln!("    │ LEFT is ANCHOR - connecting to LEFT");
                        (rel_conn_with_left_node.clone(), left_alias.to_string(), left_node_id_column.clone())
                    } else if right_is_anchor {
                        eprintln!("    │ RIGHT is ANCHOR - connecting to RIGHT");
                        (right_conn_with_rel.clone(), right_alias.to_string(), right_node_id_column.clone())
                    } else {
                        eprintln!("    │ FALLBACK - connecting to LEFT");
                        (rel_conn_with_left_node.clone(), left_alias.to_string(), left_node_id_column.clone())
                    };
"""
        
        # Find the end of the section to replace (line after the closing brace of the if-else)
        # Looking for the blank line after the closing brace, which is around line 930
        end_idx = i + 16  # Line 930 is i+15 (i is 914, so 914+16=930)
        
        # Replace the section
        new_lines = lines[:i+1] + [new_code] + lines[end_idx+1:]
        
        # Write back
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        
        print(f"Replaced lines {i+2} to {end_idx+1}")
        print("Fix applied successfully!")
        break
else:
    print("ERROR: Could not find CRITICAL FIX comment!")
