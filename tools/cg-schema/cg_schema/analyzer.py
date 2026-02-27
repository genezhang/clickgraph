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


def analyze(tables: list[dict[str, Any]], use_sample_values: bool = True) -> list[dict[str, Any]]:
    """Analyze tables and generate schema suggestions.
    
    Args:
        tables: List of table metadata from /schemas/introspect
        
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
            # If ClickHouse says it's a PK, treat as PK regardless of column name
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
            confidence = 0.3  # Lower confidence for fallback
        
        # Determine schema pattern based on columns
        pattern = determine_pattern(pk_columns, fk_columns, columns)
        
        # Analyze sample values if available and enabled
        value_analysis = None
        if use_sample_values:
            sample = table.get("sample", [])
            value_analysis = analyze_sample_values(sample)
        
        suggestion = {
            "table": table_name,
            "classification": table_class,
            "confidence": confidence if gliner_enabled else 0.3,
            "pattern": pattern,
            "pk_columns": pk_columns,
            "fk_columns": fk_columns,
            "columns": column_analysis,
            "value_analysis": value_analysis,
            "reason": generate_reason(table_class, pattern, pk_columns, fk_columns),
        }
        
        suggestions.append(suggestion)
    
    return suggestions


def determine_pattern(pk_columns: list, fk_columns: list, columns: list) -> str:
    """Determine schema pattern based on ALL column analysis.
    
    Always analyzes column structure first - table name is secondary.
    
    Args:
        pk_columns: List of primary key column names
        fk_columns: List of foreign key column names  
        columns: All column metadata (full info including types)
        
    Returns:
        Pattern name: standard_node, standard_edge, fk_edge, etc.
    """
    pk_count = len(pk_columns)
    fk_count = len(fk_columns)
    all_col_names = [c.get("name", "").lower() for c in columns]
    
    # Check for type column (polymorphic)
    has_type = any(
        c.get("name", "").lower().endswith("_type") or 
        c.get("name", "").lower() == "type"
        for c in columns
    )
    
    # Check for action/event_type column (event tables)
    has_action = any(
        "action" in c.get("name", "").lower() or
        "event_type" in c.get("name", "").lower() or
        "operation" in c.get("name", "").lower() or
        "status" in c.get("name", "").lower()
        for c in columns
    )
    
    # Check for timestamp columns (event/log tables)
    has_timestamp = any(
        "time" in c.get("name", "").lower() or
        "date" in c.get("name", "").lower() or
        c.get("type", "").lower().startswith("datetime")
        for c in columns
    )
    
    # Check for origin/dest pattern (denormalized)
    has_origin = any("origin_" in c or c.startswith("src_") or c.startswith("from_") for c in all_col_names)
    has_dest = any("dest_" in c or c.startswith("dst_") or c.startswith("to_") for c in all_col_names)
    
    # Check for entity reference columns (userId, accountId - camelCase)
    entity_refs = [c for c in all_col_names if c.endswith("id") and not c.endswith("_id")]
    
    # Composite PK + multiple FKs = fact table (standard edge)
    if pk_count >= 2 and fk_count >= 2:
        return "standard_edge"  # Fact table
    
    # Composite PK = check if it's a junction table or denormalized node
    if pk_count >= 2:
        pk_set = set(pk_columns)
        all_col_names_set = set(c.get("name", "") for c in columns)
        non_pk_cols = all_col_names_set - pk_set
        
        if not non_pk_cols:
            return "standard_edge"  # Junction table: only PK/FK columns
        
        # Has additional columns - check for denormalization (node properties embedded in edge)
        # Denormalized = edge contains node properties like origin_city, dest_name, etc.
        has_origin_props = any(
            c.startswith("origin_") or c.startswith("src_") or "_name" in c or "_city" in c or "_country" in c
            for c in non_pk_cols
        )
        has_dest_props = any(
            c.startswith("dest_") or c.startswith("dst_") or "_name" in c or "_city" in c or "_country" in c
            for c in non_pk_cols
        )
        
        if has_origin_props and has_dest_props:
            return "denormalized_edge"  # Edge embeds node properties
        
        return "standard_edge"  # Regular edge with properties
    
    # Single PK = check if denormalized edge or node
    if pk_count == 1:
        # Check if this is a denormalized edge (embeds node properties)
        # Look for origin/dest patterns in column names
        all_col_names = [c.get("name", "").lower() for c in columns]
        has_origin_props = any(c.startswith("origin_") or c.startswith("src_") for c in all_col_names)
        has_dest_props = any(c.startswith("dest_") or c.startswith("dst_") for c in all_col_names)
        
        if has_origin_props and has_dest_props:
            return "denormalized_edge"  # Edge embeds node properties
        
        if fk_count == 0:
            return "standard_node"
        
        if fk_count >= 2:
            return "fk_edge"
        
        return "fk_node"
    
    # Type column + multiple FKs = polymorphic edge (check before standard_edge)
    if has_type and fk_count >= 2:
        return "polymorphic_edge"
    
    # Multiple FKs (no PK) = standard edge  
    if fk_count >= 2:
        return "standard_edge"
    
    # Origin/dest columns = denormalized edge
    if has_origin and has_dest:
        return "denormalized_edge"
    
    # No PK, has FKs = orphan relationship (edge without own identity)
    if pk_count == 0 and fk_count >= 1:
        return "fk_edge"
    
    # Event table: NO PK + has timestamp + action/status + entity references
    if pk_count == 0 and has_timestamp and has_action:
        return "event_edge"
    
    # Log table: NO PK + has timestamp + multiple entity-like columns
    if pk_count == 0 and has_timestamp and len(entity_refs) >= 2:
        return "event_edge"
    
    # Flat table (no PK, no FKs)
    return "flat_table"


def generate_reason(classification: str, pattern: str, pk_columns: list, fk_columns: list) -> str:
    """Generate human-readable reason for classification."""
    
    if pattern == "event_edge":
        return "Event/log table - contains timestamps and entity references"
    
    if pattern == "standard_edge":
        return f"Edge table with {len(fk_columns)} foreign keys"
    
    if pattern == "standard_node":
        return f"Node/dimension with primary key: {pk_columns[0] if pk_columns else 'none'}"
    
    if pattern == "denormalized_edge":
        return "Edge with embedded node properties (origin/dest columns)"
    
    if pattern == "polymorphic_edge":
        return "Polymorphic edge with type discriminator"
    
    if pattern == "fk_edge":
        return f"FK-edge with {len(fk_columns)} reference(s)"
    
    if pattern == "flat_table":
        return "Flat table - verify if node or edge"
    
    return f"Classified as {classification}"


class SchemaAnalyzer:
    """Main analyzer class for schema analysis."""
    
    def __init__(self, use_sample_values: bool = True):
        """Initialize the analyzer.
        
        Args:
            use_sample_values: Whether to analyze sample values for patterns
        """
        self.gliner_enabled = is_model_available()
        self.use_sample_values = use_sample_values
    
    def analyze(self, tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Analyze tables and generate suggestions.
        
        Args:
            tables: List of table metadata from introspection
            
        Returns:
            List of suggestions
        """
        return analyze(tables, use_sample_values=self.use_sample_values)
