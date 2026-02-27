"""Output generation for schema suggestions."""

import yaml
from typing import Any

from rich.console import Console
from rich.table import Table


def generate_yaml(tables: list[dict[str, Any]], suggestions: list[dict[str, Any]]) -> str:
    """Generate YAML schema from suggestions.
    
    Args:
        tables: Original table metadata
        suggestions: ML-analyzed suggestions
        
    Returns:
        YAML string
    """
    nodes = []
    edges = []
    
    for suggestion in suggestions:
        table = suggestion["table"]
        classification = suggestion["classification"]
        pattern = suggestion["pattern"]
        pk_columns = suggestion.get("pk_columns", [])
        fk_columns = suggestion.get("fk_columns", [])
        columns = suggestion.get("columns", [])
        
        # Determine node_id column
        node_id = pk_columns[0] if pk_columns else "id"
        
        # Extract properties
        properties = {}
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]
            
            # Skip PK and FK in properties
            if col_type in ["pk", "fk"]:
                continue
            
            properties[col_name] = col_name
        
        # Determine if this should be a node or edge based on pattern
        is_edge_pattern = pattern in ["standard_edge", "denormalized_edge", "polymorphic_edge", "event_edge", "fk_edge"]
        
        if is_edge_pattern:
            # Treat as edge
            from_node = None
            to_node = None
            
            # Get ID columns for edge endpoints based on pattern
            id_columns = []
            
            if pattern in ["denormalized_edge", "standard_edge"]:
                # Junction tables - use PK columns as they represent the edge endpoints
                id_columns = pk_columns
            elif pattern == "fk_edge":
                # FK edge - use FK columns (they represent the endpoints)
                # If we don't have enough FKs, could fall back to PK
                id_columns = fk_columns if len(fk_columns) >= 2 else fk_columns + pk_columns
            else:
                # Other edge patterns - try FKs first, then PK
                id_columns = fk_columns if fk_columns else pk_columns
            
            # Try to infer from ID columns
            if len(id_columns) >= 2:
                # First ID is from, second is to
                for col in id_columns:
                    # Extract entity name (handle both snake_case and camelCase)
                    entity = col.replace("_id", "").replace("_key", "").replace("_sk", "")
                    # Handle camelCase: userId -> user, creatorId -> creator, person1Id -> person1
                    import re
                    entity = re.sub(r'([a-zA-Z0-9]+)Id$', r'\1', entity)  # userId -> user, creatorId -> creator, person1Id -> person1
                    entity = re.sub(r'([a-zA-Z0-9]+)ID$', r'\1', entity)  # userID -> user
                    entity = entity.lower()
                    if from_node is None:
                        from_node = entity
                    elif to_node is None:
                        to_node = entity
            
            edge_entry = {
                "type": table,
                "from": {
                    "node": from_node or "node",
                    "id": id_columns[0] if id_columns else "from_id",
                },
                "to": {
                    "node": to_node or "node", 
                    "id": id_columns[1] if len(id_columns) > 1 else "to_id",
                },
            }
            
            if properties:
                edge_entry["properties"] = properties
            
            edges.append(edge_entry)
        else:
            # Treat as node (standard_node, flat_table, denormalized_node, unknown)
            node_entry = {
                "label": table.rstrip("s"),  # Singularize
                "table": table,
                "id": {
                    "column": node_id,
                },
            }
            
            if properties:
                node_entry["properties"] = properties
            
            nodes.append(node_entry)
    
    schema = {}
    
    if nodes:
        schema["nodes"] = nodes
    if edges:
        schema["relationships"] = edges
    
    return yaml.dump(schema, default_flow_style=False, sort_keys=False)


def print_suggestions(suggestions: list[dict[str, Any]], console: Console):
    """Print suggestions in a formatted table.
    
    Args:
        suggestions: List of suggestions
        console: Rich console for output
    """
    table = Table(title="Schema Suggestions")
    
    table.add_column("Table", style="cyan")
    table.add_column("Classification", style="magenta")
    table.add_column("Pattern", style="green")
    table.add_column("PKs", style="yellow")
    table.add_column("FKs", style="red")
    table.add_column("Reason", style="dim")
    
    for s in suggestions:
        table.add_row(
            s["table"],
            s.get("classification", "unknown"),
            s.get("pattern", "-"),
            ", ".join(s.get("pk_columns", [])) or "-",
            ", ".join(s.get("fk_columns", [])) or "-",
            s.get("reason", "-"),
        )
    
    console.print(table)


def print_detailed_suggestion(suggestion: dict[str, Any], console: Console):
    """Print detailed suggestion for a single table.
    
    Args:
        suggestion: Single suggestion dict
        console: Rich console
    """
    console.print(f"\n[bold cyan]Table:[/bold cyan] {suggestion['table']}")
    console.print(f"[bold]Classification:[/bold] {suggestion.get('classification', 'unknown')}")
    console.print(f"[bold]Confidence:[/bold] {suggestion.get('confidence', 0.0):.2f}")
    console.print(f"[bold]Pattern:[/bold] {suggestion.get('pattern', '-')}")
    console.print(f"[bold]Reason:[/bold] {suggestion.get('reason', '-')}")
    
    console.print("\n[bold]Primary Keys:[/bold]")
    for pk in suggestion.get("pk_columns", []):
        console.print(f"  - {pk}")
    
    console.print("\n[bold]Foreign Keys:[/bold]")
    for fk in suggestion.get("fk_columns", []):
        console.print(f"  - {fk}")
    
    console.print("\n[bold]All Columns:[/bold]")
    for col in suggestion.get("columns", []):
        col_type = col.get("type", "unknown")
        entities = col.get("entities", [])
        entity_str = f" -> {[e[0] for e in entities]}" if entities else ""
        console.print(f"  - {col['name']} [{col_type}]{entity_str}")
    
    value_analysis = suggestion.get("value_analysis")
    if value_analysis and value_analysis.get("enabled"):
        console.print("\n[bold]Value Patterns (from sample data):[/bold]")
        patterns = value_analysis.get("patterns", [])
        if patterns:
            for p in patterns:
                console.print(f"  - {p['column']}: {p['type']} ({p.get('sample', '')})")
        else:
            console.print("  (no patterns detected)")
