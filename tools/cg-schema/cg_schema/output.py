"""Output generation for schema suggestions."""

import re
import yaml
from typing import Any

from rich.console import Console
from rich.table import Table


IRREGULAR_PLURALS = {
    "people": "person",
    "analyses": "analysis",
    "indices": "index",
    "vertices": "vertex",
    "matrices": "matrix",
    "statuses": "status",
    "addresses": "address",
    "series": "series",
    "data": "data",
    "media": "media",
    "criteria": "criterion",
    "phenomena": "phenomenon",
}

# Suffixes that should NOT have trailing 's' stripped
NO_STRIP_SUFFIXES = ("ss", "us", "is", "os")


def singularize(name: str) -> str:
    """Singularize a table name to produce a node label."""
    lower = name.lower()
    if lower in IRREGULAR_PLURALS:
        result = IRREGULAR_PLURALS[lower]
        return result.capitalize() if name[0].isupper() else result
    if lower.endswith("ies") and len(lower) > 3:
        return name[:-3] + "y"
    if lower.endswith("ses") or lower.endswith("xes") or lower.endswith("zes") or lower.endswith("ches") or lower.endswith("shes"):
        return name[:-2]
    if lower.endswith("s") and not lower.endswith(NO_STRIP_SUFFIXES):
        return name[:-1]
    return name


def _extract_entity_name(col: str) -> str:
    """Extract entity name from a FK/PK column (snake_case and camelCase)."""
    entity = col.replace("_id", "").replace("_key", "").replace("_sk", "")
    entity = re.sub(r'([a-zA-Z0-9]+)Id$', r'\1', entity)
    entity = re.sub(r'([a-zA-Z0-9]+)ID$', r'\1', entity)
    return entity.lower()


def _make_edge_type(from_label: str, to_label: str, fk_column: str) -> str:
    """Generate an edge type name from context.

    Uses the FK column stem to derive the relationship verb.
    E.g., creatorId -> HAS_CREATOR, folder_id -> IN_FOLDER, replyOfCommentId -> REPLY_OF_COMMENT
    """
    stem = _extract_entity_name(fk_column)
    # Convert camelCase to UPPER_SNAKE
    edge_type = re.sub(r'([a-z])([A-Z])', r'\1_\2', stem).upper()
    return f"HAS_{edge_type}" if not edge_type.startswith("HAS_") else edge_type


def generate_yaml(
    tables: list[dict[str, Any]],
    suggestions: list[dict[str, Any]],
    database: str = "default",
) -> str:
    """Generate YAML schema from suggestions.

    Handles multi-role tables: a table can produce both a node entry
    and FK-edge entries. Includes polymorphic sub-labels and
    table-name-derived edge endpoints.

    Args:
        tables: Original table metadata
        suggestions: Analyzed suggestions
        database: Database name for the schema

    Returns:
        YAML string in ClickGraph schema format
    """
    nodes = []
    edges = []

    for suggestion in suggestions:
        table = suggestion["table"]
        pattern = suggestion["pattern"]
        pk_columns = suggestion.get("pk_columns", [])
        fk_columns = suggestion.get("fk_columns", [])
        columns = suggestion.get("columns", [])
        node_role = suggestion.get("node_role")
        edge_roles = suggestion.get("edge_roles", [])
        poly_labels = suggestion.get("polymorphic_labels")
        name_edge = suggestion.get("name_edge_info")

        node_id = pk_columns[0] if pk_columns else "id"

        # Collect non-key properties
        properties = {}
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]
            if col_type not in ("pk", "fk"):
                properties[col_name] = col_name

        # --- Emit node entry if table has a node role ---
        if node_role is not None:
            label = singularize(table)
            node_entry = {
                "label": label,
                "database": database,
                "table": table,
                "node_id": node_id,
            }
            if properties:
                node_entry["property_mappings"] = properties
            nodes.append(node_entry)

            # Emit polymorphic sub-labels
            if poly_labels:
                for pl in poly_labels:
                    sub_entry = {
                        "label": pl["label"],
                        "database": database,
                        "table": table,
                        "node_id": node_id,
                        "filter": pl["filter"],
                    }
                    if properties:
                        sub_entry["property_mappings"] = properties
                    nodes.append(sub_entry)

            # Emit FK-edge entries for each FK relationship
            for edge in edge_roles:
                fk_col = edge["fk_column"]
                to_table = edge.get("to_table")
                to_label = singularize(to_table) if to_table else _extract_entity_name(fk_col).capitalize()
                from_label = label

                edge_entry = {
                    "type": _make_edge_type(from_label, to_label, fk_col),
                    "database": database,
                    "table": table,
                    "from_id": node_id,
                    "to_id": fk_col,
                    "from_node": from_label,
                    "to_node": to_label,
                }
                edges.append(edge_entry)

        else:
            # --- Pure edge table ---
            is_edge = pattern in (
                "standard_edge", "denormalized_edge", "polymorphic_edge",
                "event_edge", "fk_edge",
            )

            if is_edge:
                from_node = None
                to_node = None
                id_columns = []

                # Use table name edge info if available (Entity_verb_Entity)
                if name_edge:
                    from_node = singularize(name_edge["from_node"])
                    to_node = singularize(name_edge["to_node"])
                    edge_type = name_edge["type"]
                    # Determine id columns from PK or FK
                    if pk_columns:
                        id_columns = pk_columns
                    elif fk_columns:
                        id_columns = fk_columns
                else:
                    edge_type = table.upper()

                # Resolve endpoints from columns if not already set
                if from_node is None:
                    if pattern in ("denormalized_edge", "standard_edge"):
                        id_columns = pk_columns
                    elif pattern == "fk_edge":
                        id_columns = fk_columns if len(fk_columns) >= 2 else fk_columns + pk_columns
                    else:
                        id_columns = fk_columns if fk_columns else pk_columns

                    if len(id_columns) >= 2:
                        from_node = singularize(_extract_entity_name(id_columns[0]).capitalize())
                        to_node = singularize(_extract_entity_name(id_columns[1]).capitalize())

                edge_entry = {
                    "type": edge_type,
                    "database": database,
                    "table": table,
                    "from_id": id_columns[0] if id_columns else "from_id",
                    "to_id": id_columns[1] if len(id_columns) > 1 else "to_id",
                    "from_node": from_node or "Node",
                    "to_node": to_node or "Node",
                }

                if properties:
                    edge_entry["property_mappings"] = properties

                edges.append(edge_entry)
            else:
                # Unknown/flat table -> emit as node with a TODO comment
                node_entry = {
                    "label": singularize(table),
                    "database": database,
                    "table": table,
                    "node_id": node_id,
                }
                if properties:
                    node_entry["property_mappings"] = properties
                nodes.append(node_entry)

    schema = {"graph_schema": {}}
    if nodes:
        schema["graph_schema"]["nodes"] = nodes
    if edges:
        schema["graph_schema"]["edges"] = edges

    return yaml.dump(schema, default_flow_style=False, sort_keys=False)


def print_suggestions(suggestions: list[dict[str, Any]], console: Console):
    """Print suggestions in a formatted table."""
    table = Table(title="Schema Suggestions")

    table.add_column("Table", style="cyan")
    table.add_column("Role", style="magenta")
    table.add_column("Pattern", style="green")
    table.add_column("PKs", style="yellow")
    table.add_column("FKs", style="red")
    table.add_column("Reason", style="dim")

    for s in suggestions:
        node_role = s.get("node_role")
        edge_roles = s.get("edge_roles", [])

        if node_role and edge_roles:
            role = f"node + {len(edge_roles)} FK-edge(s)"
        elif node_role:
            role = "node"
        else:
            role = s.get("pattern", "-")

        table.add_row(
            s["table"],
            role,
            s.get("pattern", "-"),
            ", ".join(s.get("pk_columns", [])) or "-",
            ", ".join(s.get("fk_columns", [])) or "-",
            s.get("reason", "-"),
        )

    console.print(table)


def print_detailed_suggestion(suggestion: dict[str, Any], console: Console):
    """Print detailed suggestion for a single table."""
    console.print(f"\n[bold cyan]Table:[/bold cyan] {suggestion['table']}")
    console.print(f"[bold]Classification:[/bold] {suggestion.get('classification', 'unknown')}")
    console.print(f"[bold]Confidence:[/bold] {suggestion.get('confidence', 0.0):.2f}")
    console.print(f"[bold]Pattern:[/bold] {suggestion.get('pattern', '-')}")
    console.print(f"[bold]Node Role:[/bold] {suggestion.get('node_role', 'None')}")
    console.print(f"[bold]Reason:[/bold] {suggestion.get('reason', '-')}")

    edge_roles = suggestion.get("edge_roles", [])
    if edge_roles:
        console.print(f"\n[bold]FK-Edge Relationships:[/bold]")
        for e in edge_roles:
            to_table = e.get("to_table", "?")
            console.print(f"  - {e['fk_column']} -> {to_table}")

    poly = suggestion.get("polymorphic_labels")
    if poly:
        console.print(f"\n[bold]Polymorphic Sub-Labels:[/bold]")
        for p in poly:
            console.print(f"  - {p['label']} (filter: {p['filter']})")

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
