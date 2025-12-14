#!/usr/bin/env python3
"""
Adapt LDBC queries: Convert inline property parameters to WHERE clauses

Transforms:
  MATCH (n:Person {id: $personId})
To:
  MATCH (n:Person) WHERE n.id = $personId
"""

import re
from pathlib import Path

def adapt_inline_property_parameters(cypher: str) -> tuple[str, bool]:
    """
    Convert inline property patterns with parameters to WHERE clauses.
    Returns (adapted_query, was_adapted)
    """
    # Pattern: MATCH (var:Label {prop: $param})
    # Captures: var, Label, prop, param
    pattern = r'\((\w+):(\w+)\s*\{(\w+):\s*\$(\w+)\}\)'
    
    matches = list(re.finditer(pattern, cypher))
    if not matches:
        return cypher, False
    
    adapted = cypher
    offset = 0
    
    for match in matches:
        var, label, prop, param = match.groups()
        
        # Original text
        original = match.group(0)
        # Replacement: (var:Label)
        replacement = f'({var}:{label})'
        
        # Calculate position with offset
        start = match.start() + offset
        end = match.end() + offset
        
        # Replace in adapted string
        adapted = adapted[:start] + replacement + adapted[end:]
        offset += len(replacement) - len(original)
        
        # Add WHERE clause
        # Find the end of the MATCH clause (before RETURN/WITH/WHERE)
        match_end = adapted.find('RETURN', start)
        if match_end == -1:
            match_end = adapted.find('WITH', start)
        if match_end == -1:
            match_end = adapted.find('WHERE', start)
        
        if match_end != -1:
            where_clause = f'\nWHERE {var}.{prop} = ${param}\n'
            adapted = adapted[:match_end] + where_clause + adapted[match_end:]
            offset += len(where_clause)
    
    return adapted, True


def process_query_file(input_path: Path, output_path: Path):
    """Process a single query file."""
    content = input_path.read_text()
    
    adapted, was_adapted = adapt_inline_property_parameters(content)
    
    if was_adapted:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(adapted)
        return True
    return False


def main():
    """Adapt all LDBC official queries."""
    base_dir = Path(__file__).parent.parent
    official_dir = base_dir / "queries" / "official"
    adapted_dir = base_dir / "queries" / "adapted_auto"
    
    adapted_count = 0
    total_count = 0
    
    for query_file in official_dir.rglob("*.cypher"):
        total_count += 1
        relative_path = query_file.relative_to(official_dir)
        output_file = adapted_dir / relative_path
        
        if process_query_file(query_file, output_file):
            adapted_count += 1
            print(f"✓ Adapted: {relative_path}")
        else:
            print(f"  Unchanged: {relative_path}")
    
    print(f"\nProcessed {total_count} queries, adapted {adapted_count}")
    print(f"Output directory: {adapted_dir}")


if __name__ == "__main__":
    main()
