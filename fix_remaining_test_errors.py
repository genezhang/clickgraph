#!/usr/bin/env python3
"""
Comprehensive fix for remaining test PropertyValue issues
"""
import re

print("Fixing remaining test compilation errors...")

# 1. Fix graph_join_inference.rs - still has 1 RelationshipSchema without new fields
# This is the WORKS_AT one that failed earlier
file = r'src\query_planner\analyzer\graph_join_inference.rs'
with open(file, 'r', encoding='utf-8') as f:
    content = f.read()

# Find WORKS_AT and add missing fields
content = content.replace(
    '''                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
            },
        );

        GraphSchema::build(1, "default".to_string(), nodes, relationships)
    }

    fn setup_plan_ctx_with_graph_entities() -> PlanCtx {''',
    '''                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_node_properties: None,
                to_node_properties: None,
            },
        );

        GraphSchema::build(1, "default".to_string(), nodes, relationships)
    }

    fn setup_plan_ctx_with_graph_entities() -> PlanCtx {'''
)

with open(file, 'w', encoding='utf-8') as f:
    f.write(content)
print(f"✓ Fixed {file}")

print("\nDone! Remaining errors should be HashMap construction issues.")
print("Run: cargo test --lib 2>&1 | Select-String 'error\\[E' to see remaining issues")
