"""Schema analysis - combines introspection data with ML classification."""

import re
from typing import Any

from cg_schema.gliner import (
    classify_table_name,
    classify_column_type,
    extract_entity_from_column,
    classify_table_name_fallback,
    is_model_available,
)


EMAIL_REGEX = re.compile(r'^[^@]+@[^@]+\.[^@]+$')
URL_REGEX = re.compile(r'^https?://')
UUID_REGEX = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)


def analyze_sample_values(sample: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze sample values to detect patterns.

    Args:
        sample: List of sample rows from introspection

    Returns:
        Dictionary with value-based insights
    """
    if not sample:
        return {"enabled": False, "patterns": []}

    patterns = []

    for row in sample[:3]:
        for col_name, value in row.items():
            if value is None:
                continue

            value_str = str(value)

            # Email detection
            if "@" in value_str and "." in value_str and " " not in value_str:
                if EMAIL_REGEX.match(value_str):
                    patterns.append({
                        "column": col_name,
                        "type": "email",
                        "sample": value_str[:50],
                    })
                    continue

            # URL detection
            if URL_REGEX.match(value_str):
                patterns.append({
                    "column": col_name,
                    "type": "url",
                    "sample": value_str[:50],
                })
                continue

            # UUID detection
            if UUID_REGEX.match(value_str):
                patterns.append({
                    "column": col_name,
                    "type": "uuid",
                    "sample": value_str,
                })
                continue

    return {
        "enabled": True,
        "patterns": patterns,
    }


def extract_entity_stem(column_name: str) -> str:
    """Extract entity stem from an FK column name.

    Examples:
        user_id -> user
        creatorId -> creator
        person1Id -> person1
        Person2Id -> person2
        follower_id -> follower
        replyOfCommentId -> replyofcomment
    """
    name = column_name
    # snake_case: remove _id, _key, _sk suffixes
    for suffix in ("_id", "_key", "_sk"):
        if name.lower().endswith(suffix):
            return name[: -len(suffix)].lower()
    # camelCase: remove Id or ID suffix
    m = re.match(r'^(.+?)(Id|ID)$', name)
    if m:
        return m.group(1).lower()
    return name.lower()


def detect_edge_from_table_name(table_name: str, node_tables: set[str]) -> dict[str, str] | None:
    """Detect edge endpoints from Entity_verb_Entity table naming conventions.

    Tries all possible splits of underscore-separated table name and matches
    segments against known node table names.

    Args:
        table_name: The table name to analyze
        node_tables: Set of known node table names (lowercase)

    Returns:
        Dict with type, from_node, to_node if pattern matches, else None
    """
    parts = table_name.split("_")
    if len(parts) < 3:
        return None

    # Build lowercase lookup
    node_lower = {n.lower(): n for n in node_tables}

    # Try all splits: parts[:i] = from, parts[i:j] = verb, parts[j:] = to
    for i in range(1, len(parts)):
        for j in range(i + 1, len(parts)):
            from_candidate = "_".join(parts[:i]).lower()
            verb_parts = parts[i:j]
            to_candidate = "_".join(parts[j:]).lower()

            if from_candidate in node_lower and to_candidate in node_lower:
                # Convert verb to edge type: camelCase verb -> UPPER_SNAKE
                verb = "_".join(verb_parts)
                edge_type = re.sub(r'([a-z])([A-Z])', r'\1_\2', verb).upper()
                return {
                    "type": edge_type,
                    "from_node": node_lower[from_candidate],
                    "to_node": node_lower[to_candidate],
                }

    return None


def detect_polymorphic_labels(
    table_name: str, columns: list[dict[str, Any]], sample: list[dict[str, Any]]
) -> list[dict[str, str]] | None:
    """Detect polymorphic sub-labels from a type column with few distinct values.

    Args:
        table_name: Table name
        columns: Column metadata
        sample: Sample rows

    Returns:
        List of {label, filter} dicts, or None
    """
    # Find type-discriminator columns
    type_col = None
    for col in columns:
        name = col.get("name", "")
        name_lower = name.lower()
        if name_lower == "type" or name_lower.endswith("_type"):
            type_col = name
            break

    if not type_col or not sample:
        return None

    # Collect distinct values from sample
    values = set()
    for row in sample:
        val = row.get(type_col)
        if val is not None and isinstance(val, str) and val.strip():
            values.add(val.strip())

    # Only suggest sub-labels if we have 2-10 distinct values (enum-like)
    if len(values) < 2 or len(values) > 10:
        return None

    return [
        {"label": val, "filter": f"{type_col} = '{val}'"}
        for val in sorted(values)
    ]


def classify_table_roles(
    table_name: str,
    pk_columns: list[str],
    fk_columns: list[str],
    columns: list[dict[str, Any]],
) -> dict[str, Any]:
    """Classify a table into node_role and edge_roles.

    A table can serve as both a node and a source of FK-edge relationships.
    This replaces the old single-pattern approach.

    Args:
        table_name: Table name
        pk_columns: Primary key column names
        fk_columns: Foreign key column names
        columns: All column metadata

    Returns:
        Dict with:
          node_role: "standard_node" | None
          edge_roles: list of {type, fk_column, target_stem} dicts
          pattern: legacy single pattern string for backward compat
    """
    pk_count = len(pk_columns)
    fk_count = len(fk_columns)
    all_col_names = [c.get("name", "").lower() for c in columns]

    # Count non-key attribute columns
    pk_set = {p.lower() for p in pk_columns}
    fk_set = {f.lower() for f in fk_columns}
    attr_columns = [
        c for c in columns
        if c.get("name", "").lower() not in pk_set and c.get("name", "").lower() not in fk_set
    ]
    attr_count = len(attr_columns)

    # Check for type column (polymorphic edge indicator)
    has_type = any(
        c.get("name", "").lower().endswith("_type") or c.get("name", "").lower() == "type"
        for c in columns
    )

    # Check for origin/dest pattern (denormalized) — only in non-PK columns
    # Supports: origin_/src_/source_/from_ and dest_/dst_/target_/to_ prefixes
    non_pk_col_names = [c.get("name", "").lower() for c in columns if c.get("name", "").lower() not in pk_set]
    origin_prefixes = ("origin_", "src_", "source_", "from_")
    dest_prefixes = ("dest_", "dst_", "target_", "to_")
    has_origin = any(any(c.startswith(p) for p in origin_prefixes) for c in non_pk_col_names)
    has_dest = any(any(c.startswith(p) for p in dest_prefixes) for c in non_pk_col_names)

    # Check for timestamp columns (event/log tables)
    has_timestamp = any(
        "_time" in c.get("name", "").lower()
        or "_date" in c.get("name", "").lower()
        or c.get("name", "").lower().endswith("_at")
        or c.get("type", "").lower().startswith("datetime")
        for c in columns
    )

    # Check for action/event_type column
    has_action = any(
        "action" in c.get("name", "").lower()
        or "event_type" in c.get("name", "").lower()
        or "operation" in c.get("name", "").lower()
        for c in columns
    )

    # Check for entity reference columns (camelCase without underscore)
    entity_refs = [c for c in all_col_names if c.endswith("id") and not c.endswith("_id")]

    result = {"node_role": None, "edge_roles": [], "pattern": "flat_table"}

    # --- Composite PK: almost always an edge/junction table ---
    if pk_count >= 2:
        if has_origin and has_dest:
            result["pattern"] = "denormalized_edge"
        else:
            result["pattern"] = "standard_edge"
        return result

    # --- Single PK: primary candidate for node ---
    if pk_count == 1:
        # Denormalized edge check (origin/dest columns override node)
        if has_origin and has_dest:
            result["pattern"] = "denormalized_edge"
            return result

        # Node with attribute richness heuristic:
        # A table with PK + multiple non-key attributes is a node,
        # even if it has FK columns (those become FK-edge relationships)
        total_cols = len(columns)
        is_attribute_rich = attr_count >= 2 or (total_cols > 0 and attr_count / total_cols > 0.3)

        if fk_count == 0:
            result["node_role"] = "standard_node"
            result["pattern"] = "standard_node"
        elif fk_count == 1 and is_attribute_rich:
            # Node with one FK relationship (e.g., Post with creatorId)
            result["node_role"] = "standard_node"
            result["pattern"] = "fk_node"
            fk_col = fk_columns[0]
            stem = extract_entity_stem(fk_col)
            result["edge_roles"].append({
                "fk_column": fk_col,
                "target_stem": stem,
            })
        elif fk_count >= 2 and is_attribute_rich:
            # Node with multiple FK relationships (e.g., Comment with creatorId + replyOfCommentId)
            result["node_role"] = "standard_node"
            result["pattern"] = "fk_node"
            for fk_col in fk_columns:
                stem = extract_entity_stem(fk_col)
                result["edge_roles"].append({
                    "fk_column": fk_col,
                    "target_stem": stem,
                })
        elif fk_count >= 2:
            # Thin table with multiple FKs = edge
            result["pattern"] = "fk_edge"
        else:
            result["pattern"] = "fk_node"
            result["node_role"] = "standard_node"
            for fk_col in fk_columns:
                stem = extract_entity_stem(fk_col)
                result["edge_roles"].append({
                    "fk_column": fk_col,
                    "target_stem": stem,
                })

        return result

    # --- No PK ---
    # Attribute-rich tables with many non-key columns are flat/log tables,
    # not edges (e.g., OTel traces have TraceId/SpanId but are event tables)
    no_pk_attr_rich = attr_count >= fk_count * 2 and attr_count >= 5

    if no_pk_attr_rich:
        result["pattern"] = "flat_table"
    elif has_type and fk_count >= 2:
        result["pattern"] = "polymorphic_edge"
    elif fk_count >= 2:
        result["pattern"] = "standard_edge"
    elif has_origin and has_dest:
        result["pattern"] = "denormalized_edge"
    elif fk_count >= 1:
        result["pattern"] = "fk_edge"
    elif has_timestamp and has_action:
        result["pattern"] = "event_edge"
    elif has_timestamp and len(entity_refs) >= 2:
        result["pattern"] = "event_edge"
    else:
        result["pattern"] = "flat_table"

    return result


def resolve_fk_targets(suggestions: list[dict[str, Any]]) -> None:
    """Second pass: resolve FK columns to target node tables.

    Builds a PK stem index from all node tables, then matches each FK's
    entity stem against it to determine edge endpoints.

    Mutates suggestions in place.
    """
    # Build PK stem -> table info index
    # stem "person" -> {table: "Person", pk: "id"}
    pk_index: dict[str, dict[str, str]] = {}
    table_name_stems: dict[str, dict[str, str]] = {}

    for s in suggestions:
        if s.get("node_role") is None:
            continue
        table = s["table"]
        # Index by table name stem (singularized, lowercase)
        from cg_schema.output import singularize
        table_stem = singularize(table).lower()
        table_name_stems[table_stem] = {"table": table, "pk": s["pk_columns"][0] if s["pk_columns"] else "id"}

        # Index by PK column stems
        for pk in s.get("pk_columns", []):
            stem = extract_entity_stem(pk)
            pk_index[stem] = {"table": table, "pk": pk}

    # Resolve each FK's target
    for s in suggestions:
        for edge in s.get("edge_roles", []):
            stem = edge["target_stem"]
            target = None

            # Try exact PK stem match first
            if stem in pk_index:
                target = pk_index[stem]
            else:
                # Try matching against table name stems
                # e.g., "creator" won't match "person" directly,
                # but we can try suffix removal: "replyofcomment" -> "comment"
                for table_stem, info in table_name_stems.items():
                    if stem == table_stem or stem.endswith(table_stem):
                        target = info
                        break

            if target:
                edge["to_table"] = target["table"]
                edge["to_id"] = target["pk"]


def detect_edges_from_table_names(suggestions: list[dict[str, Any]]) -> None:
    """Detect edges from Entity_verb_Entity table naming conventions.

    For tables classified as edges, tries to match the table name pattern
    against known node tables to resolve from/to endpoints.

    Mutates suggestions in place.
    """
    node_tables = set()
    for s in suggestions:
        if s.get("node_role") is not None:
            node_tables.add(s["table"])

    if not node_tables:
        return

    for s in suggestions:
        if s.get("node_role") is not None:
            continue  # Skip node tables

        edge_info = detect_edge_from_table_name(s["table"], node_tables)
        if edge_info:
            s["name_edge_info"] = edge_info


def analyze(tables: list[dict[str, Any]], use_sample_values: bool = True) -> list[dict[str, Any]]:
    """Analyze tables and generate schema suggestions.

    Uses multi-role classification: a table can be both a node and a source
    of FK-edge relationships. Includes cross-table FK resolution, table name
    edge detection, and polymorphic sub-label detection.

    Args:
        tables: List of table metadata from /schemas/introspect
        use_sample_values: Whether to analyze sample values

    Returns:
        List of suggestions, one per table
    """
    suggestions = []
    gliner_enabled = is_model_available()

    for table in tables:
        table_name = table.get("name", "unknown")
        columns = table.get("columns", [])

        # Analyze columns
        column_analysis = []
        fk_columns = []
        pk_columns = []

        for col in columns:
            col_name = col.get("name", "")
            col_type = classify_column_type(col_name)
            is_pk_metadata = col.get("is_primary_key", False)

            # Extract entity references if GLiNER available
            entities = []
            if gliner_enabled:
                entities = extract_entity_from_column(col_name)

            # Determine if this is actually a PK - prefer metadata over column name
            if is_pk_metadata:
                actual_type = "pk"
                if col_name not in pk_columns:
                    pk_columns.append(col_name)
            elif col_type == "fk":
                actual_type = "fk"
                fk_columns.append(col_name)
            else:
                actual_type = col_type

            col_info = {
                "name": col_name,
                "type": actual_type,
                "entities": entities,
                "is_pk": is_pk_metadata,
            }
            column_analysis.append(col_info)

        # Classify table using GLiNER or fallback
        if gliner_enabled:
            table_class, confidence = classify_table_name(table_name)
        else:
            table_class, reason = classify_table_name_fallback(table_name)
            confidence = 0.3

        # Multi-role classification
        roles = classify_table_roles(table_name, pk_columns, fk_columns, columns)

        # Analyze sample values if available and enabled
        value_analysis = None
        sample = table.get("sample", [])
        if use_sample_values:
            value_analysis = analyze_sample_values(sample)

        # Detect polymorphic sub-labels
        poly_labels = None
        if roles["node_role"] is not None:
            poly_labels = detect_polymorphic_labels(table_name, columns, sample)

        suggestion = {
            "table": table_name,
            "classification": table_class,
            "confidence": confidence if gliner_enabled else 0.3,
            "pattern": roles["pattern"],
            "node_role": roles["node_role"],
            "edge_roles": roles["edge_roles"],
            "pk_columns": pk_columns,
            "fk_columns": fk_columns,
            "columns": column_analysis,
            "value_analysis": value_analysis,
            "polymorphic_labels": poly_labels,
            "reason": generate_reason(
                table_class, roles["pattern"], pk_columns, fk_columns,
                roles["node_role"], roles["edge_roles"],
            ),
        }

        suggestions.append(suggestion)

    # Second pass: cross-table FK resolution
    resolve_fk_targets(suggestions)

    # Third pass: table name edge detection
    detect_edges_from_table_names(suggestions)

    return suggestions


def determine_pattern(pk_columns: list, fk_columns: list, columns: list) -> str:
    """Determine schema pattern based on column analysis.

    This is the backward-compatible single-pattern API. Internally delegates
    to classify_table_roles.

    Args:
        pk_columns: List of primary key column names
        fk_columns: List of foreign key column names
        columns: All column metadata

    Returns:
        Pattern name: standard_node, standard_edge, fk_edge, etc.
    """
    roles = classify_table_roles("unknown", pk_columns, fk_columns, columns)
    return roles["pattern"]


def generate_reason(
    classification: str,
    pattern: str,
    pk_columns: list,
    fk_columns: list,
    node_role: str | None = None,
    edge_roles: list | None = None,
) -> str:
    """Generate human-readable reason for classification."""
    edge_roles = edge_roles or []

    if node_role and edge_roles:
        fk_targets = [e.get("target_stem", "?") for e in edge_roles]
        return (
            f"Node (PK: {pk_columns[0] if pk_columns else '?'}) "
            f"with {len(edge_roles)} FK-edge(s) to: {', '.join(fk_targets)}"
        )

    if pattern == "event_edge":
        return "Event/log table - contains timestamps and entity references"

    if pattern == "standard_edge":
        return f"Edge table with composite key or {len(fk_columns)} foreign keys"

    if pattern == "standard_node":
        return f"Node with primary key: {pk_columns[0] if pk_columns else 'none'}"

    if pattern == "denormalized_edge":
        return "Edge with embedded node properties (origin/dest columns)"

    if pattern == "polymorphic_edge":
        return "Polymorphic edge with type discriminator"

    if pattern == "fk_edge":
        return f"FK-edge with {len(fk_columns)} reference(s)"

    if pattern == "fk_node":
        return f"Node with {len(fk_columns)} FK reference(s)"

    if pattern == "flat_table":
        return "Flat table - verify if node or edge"

    return f"Classified as {classification}"


class SchemaAnalyzer:
    """Main analyzer class for schema analysis."""

    def __init__(self, use_sample_values: bool = True):
        self.gliner_enabled = is_model_available()
        self.use_sample_values = use_sample_values

    def analyze(self, tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return analyze(tables, use_sample_values=self.use_sample_values)
